import os
import unittest


@unittest.skipUnless(os.environ.get('RUN_SPARK') == '1', 'Set RUN_SPARK=1 with Spark/Iceberg runtime')
class SparkTest(unittest.TestCase):
    def test_typing_and_quarantine(self):
        from processing.silver import session, parse_frame
        from bank.generator import generate
        import json
        spark = session()
        try:
            rows = generate(customers=2, transactions=3)['transactions']
            rows = [rows[0], {**rows[1], 'amount':'-5.00'}, {**rows[2], 'amount':'bad'}]
            df = spark.createDataFrame([(json.dumps(r), 'test') for r in rows], ['payload','batch_id'])
            parsed = parse_frame(df, 'transactions')
            self.assertEqual(parsed.filter("reject_reason = ''").count(), 1)
            self.assertEqual(parsed.filter("reject_reason <> ''").count(), 2)
            self.assertEqual(str(parsed.schema['amount'].dataType), 'DecimalType(22,2)')
        finally:
            spark.stop()
