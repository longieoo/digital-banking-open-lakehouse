from pathlib import Path
import json
import unittest
from bank.external import rates


class SecurityTest(unittest.TestCase):
    def test_fixture_is_explicit(self):
        self.assertEqual(rates()[0]['origin'],'OFFLINE_FIXTURE')
        self.assertEqual(rates()[0]['rate'],'0.85')

    def test_analyst_rule_is_gold_only(self):
        rules=json.loads(Path('query/access-rules.json').read_text())
        analyst=[r for r in rules['tables'] if r.get('user')=='analyst']
        self.assertEqual(analyst,[{'user':'analyst','schema':'gold','privileges':['SELECT']}])

    def test_gold_does_not_select_raw_pii(self):
        import re
        for path in Path('dbt/models').glob('*.sql'):
            sql=re.sub(r'--.*','',path.read_text())
            for field in ['full_name','email','phone','date_of_birth']:
                self.assertIsNone(re.search(r'\b'+field+r'\b',sql),f'{path}: {field}')
