"""Execute the actual dbt SQL via Trino->DuckDB translation, not a second set of marts.

This tests business SQL against typed fixture Silver. It is NOT a Spark/Trino E2E test.
"""
from pathlib import Path
import re
import json
from bank.contracts import fields, column_type
from bank.generator import generate


class GoldHarness:
    def __init__(self, data=None, variables=None):
        import duckdb
        self.db = duckdb.connect()
        self.db.execute('CREATE SCHEMA silver')
        self.db.execute('CREATE SCHEMA gold')
        self.db.execute('CREATE SCHEMA bronze')
        self.data = generate(customers=8, transactions=100) if data is None else data
        self.variables = {'as_of_date':'2026-01-31','start_date':'2026-01-01','reporting_currency':'USD',
                          **(variables or {})}
        for table, rows in self.data.items():
            cols = fields(table)
            definitions = ','.join(f'"{c}" {column_type(c).replace("string","varchar")}' for c in cols)
            self.db.execute(f'CREATE TABLE silver.{table} ({definitions})')
            self.db.executemany(f'INSERT INTO silver.{table} VALUES ({",".join("?" for c in cols)})',
                               [tuple(row[c] for c in cols) for row in rows])

        self.db.execute('CREATE TABLE bronze.transactions (record_id varchar, source_updated_at varchar, business_date varchar, payload varchar)')
        self.db.executemany('INSERT INTO bronze.transactions VALUES (?,?,?,?)',
            [(r['transaction_id'],r['updated_at'],r['business_date'],json.dumps(r)) for r in self.data['transactions']])

    def render(self, path):
        import jinja2
        import sqlglot
        sql = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(Path(path).read_text()).render(
            ref=lambda name: f'gold.{name}', source=lambda schema,name: f'{schema}.{name}',
            var=lambda name, *default: self.variables.get(name, *default))
        return sqlglot.transpile(sql, read='trino', write='duckdb')[0]

    def build(self):
        pending = {p.stem: p for p in Path('dbt/models').glob('*.sql')}
        done = set()
        while pending:
            ready = [name for name,path in pending.items()
                     if set(re.findall(r"ref\('([^']+)'\)",path.read_text())) <= done]
            if not ready:
                raise ValueError('Unresolved model dependencies')
            for name in ready:
                self.db.execute(f'CREATE OR REPLACE TABLE gold.{name} AS {self.render(pending.pop(name))}')
                done.add(name)

    def scalar(self, query):
        return self.db.execute(query).fetchone()[0]

    def close(self):
        self.db.close()
