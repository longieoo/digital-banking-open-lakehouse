import io
from pathlib import Path
import tempfile
import unittest

from bank.membership import Membership, verify_membership


class SourceMembershipTest(unittest.TestCase):
    def test_equivalent_zone_and_corruption(self):
        record = io.BytesIO()
        writer = Membership(record, 'id')
        writer.record([{'id': 'SYN-1', 'updated_at': '2026-01-01T07:00:00+07:00'}])
        self.assertIn(b'00:00:00.000000+00:00', record.getvalue())
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'page.jsonl'
            path.write_bytes(record.getvalue())
            verify_membership(path, writer.metadata())
            path.write_bytes(b'')
            with self.assertRaises(ValueError):
                verify_membership(path, writer.metadata())
