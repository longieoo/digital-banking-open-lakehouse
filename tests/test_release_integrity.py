import hashlib
import tempfile
from pathlib import Path
import unittest

from bank.release import atomic_json, verify


class ReleaseIntegrityTest(unittest.TestCase):
    def bundle(self,root,content='id,amount\nx,10.00\n',rows=1):
        value = 'a'*32
        folder = root/value
        path = folder/'exports/fct_transactions.csv'
        path.parent.mkdir(parents=True)
        path.write_text(content,encoding='utf-8')
        document = dict(release_id=value,status='CERTIFIED',**{
            f'{layer}_schema':f'{layer}_r_{value}' for layer in ('bronze','silver','gold')},
            exports={'fct_transactions':{'file':'exports/fct_transactions.csv','rows':rows,
                     'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}})
        atomic_json(folder/'release.json',document)
        return value,path

    def test_rows_checked_even_when_file_hash_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value,_ = self.bundle(root,rows=2)
            with self.assertRaisesRegex(ValueError,'row count'):
                verify(value,root=root)

    def test_malformed_csv_and_duplicate_header_block_readers(self):
        for content in ['id,id\nx,x\n','id,amount\nx,1,extra\n']:
            with self.subTest(content=content), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                value,_ = self.bundle(root,content=content)
                with self.assertRaises(ValueError):
                    verify(value,root=root)

    def test_symlink_export_cannot_escape_trusted_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value,path = self.bundle(root)
            external = root/'outside.csv'
            external.write_bytes(path.read_bytes())
            path.unlink()
            try:
                path.symlink_to(external)
            except OSError:
                self.skipTest('OS does not allow creating symbolic links')
            with self.assertRaises(ValueError):
                verify(value,root=root)
