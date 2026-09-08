"""Local acceptance demonstration; explicitly substitutes typed fixtures for Spark."""
import json
from pathlib import Path
from bank.generator import generate
from bank.ingestion import FileRaw,State,MemorySource,ingest
from bank.adapters import IcebergBronze
from bank.observability import metrics


def main():
    from pyiceberg.catalog import load_catalog
    from tests.gold_harness import GoldHarness
    root=Path('runtime/local-demo')
    root.mkdir(parents=True,exist_ok=True)
    catalog=load_catalog('demo',type='sql',uri='sqlite:///'+(root/'catalog.db').resolve().as_posix(),
                         warehouse=(root/'warehouse').as_posix())
    data=generate(customers=8,transactions=100)
    state=State(root/'control.db')
    bronze=IcebergBronze(catalog,'transactions')
    source=MemorySource(data['transactions'])
    before=state.watermark('core.transactions')
    try:
        ingest(source,FileRaw(root/'raw'),bronze,state,fail_at='bronze')
    except IOError:
        pass
    assert state.watermark('core.transactions')==before
    ingest(source,FileRaw(root/'raw'),bronze,state)
    assert bronze.table.scan().to_arrow().num_rows==len(data['transactions'])
    h=GoldHarness(data)
    try:
        h.build()
        failures=h.scalar("SELECT count(*) FROM gold.mart_reconciliation_daily WHERE reconciliation_status='FAIL'")
        assert failures==0
        result={'status':'PASS','scope':'Real local Iceberg; typed fixture Silver; actual dbt SQL via DuckDB. NOT distributed E2E.',
            'source_events':len(data['transactions']),'iceberg_versions':bronze.table.scan().to_arrow().num_rows,
            'gold_events':h.scalar('SELECT count(*) FROM gold.fct_transactions'),
            'customer_360_rows':h.scalar('SELECT count(*) FROM gold.mart_customer_360'),
            'balance_rows':h.scalar('SELECT count(*) FROM gold.fct_account_balance_daily'),
            'reconciliation_failures':failures,'recovery_metrics':metrics(state)}
        (root/'result.json').write_text(json.dumps(result,indent=2))
        print(json.dumps(result,indent=2))
    finally:
        h.close()
        catalog.engine.dispose()


if __name__ == '__main__':
    main()
