"""Compare the complete CSV row multiset of two certified releases using disk-backed SQL."""
import argparse
import csv
import json
from pathlib import Path
import sqlite3
import tempfile

from bank.release import directory, verify


def compare(left, right, root=None):
    documents = [verify(value, root) for value in (left, right)]
    if documents[0]['exports'].keys() != documents[1]['exports'].keys():
        raise ValueError('Different model inventories')
    counts = {}
    with tempfile.TemporaryDirectory() as temp:
        db = sqlite3.connect(str(Path(temp) / 'comparison.db'))
        try:
            db.execute('CREATE TABLE delta(row TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID')
            for model in sorted(documents[0]['exports']):
                header = None
                for document, sign in zip(documents, (1, -1)):
                    path = directory(document['release_id'], root) / document['exports'][model]['file']
                    with path.open(encoding='utf-8', newline='') as stream:
                        reader = csv.reader(stream)
                        columns = next(reader)
                        if header is not None and header != columns:
                            raise ValueError(f'Different columns for {model}')
                        header = columns
                        db.executemany('INSERT INTO delta VALUES (?,?) ON CONFLICT(row) DO UPDATE SET n=n+excluded.n',
                                       ((json.dumps(row, separators=(',', ':')), sign) for row in reader))
                if db.execute('SELECT 1 FROM delta WHERE n<>0 LIMIT 1').fetchone():
                    raise ValueError(f'Different row multiset for {model}')
                counts[model] = documents[0]['exports'][model]['rows']
                db.execute('DELETE FROM delta')
                db.commit()
        finally:
            db.close()
    return counts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('left')
    parser.add_argument('right')
    parser.add_argument('--root', type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.left, args.right, args.root)))
