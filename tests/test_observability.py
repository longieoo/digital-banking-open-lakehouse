from pathlib import Path
import tempfile
import unittest
from bank.ingestion import State,MemorySource,FileRaw,SQLiteBronze,ingest
from bank.observability import metrics


class ObservabilityTest(unittest.TestCase):
    def test_retry_and_count_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            state=State(root/'state.db')
            source=MemorySource([{'transaction_id':'T1','updated_at':'2026-01-01T00:00:00Z'}])
            raw=FileRaw(root/'raw')
            bronze=SQLiteBronze(root/'bronze.db')
            with self.assertRaises(IOError):
                ingest(source,raw,bronze,state,fail_at='bronze')
            self.assertEqual(metrics(state)['failed_batches'],1)
            ingest(source,raw,bronze,state)
            result=metrics(state)
            self.assertEqual((result['completed_batches'],result['retry_count'],result['rows_written']),(1,1,1))
            self.assertEqual(result['manifest_count_mismatches'],0)
