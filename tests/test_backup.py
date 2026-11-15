import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from bank.backup import create_backup, restore_backup, verify_archive
from bank.release import atomic_json


def make_release(root, value='a' * 32, as_of='2026-01-31', currency='USD', status='CERTIFIED'):
    folder = root / value
    (folder / 'exports').mkdir(parents=True)
    exports = {}
    for model, content in {
        'mart_reconciliation_daily': f'business_date,currency,reconciliation_status\n{as_of},{currency},PASS\n',
        'fct_transactions': 'transaction_id,amount,currency\nSYN-T001,12.34,USD\n',
    }.items():
        path = folder / 'exports' / f'{model}.csv'
        path.write_text(content, encoding='utf-8', newline='')
        exports[model] = {'file': f'exports/{model}.csv', 'rows': 1,
                          'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    document = {'release_id': value, 'start_date': '2026-01-01', 'as_of_date': as_of,
                'reporting_currency': currency, 'status': status,
                'bronze_schema': 'bronze_r_' + value, 'silver_schema': 'silver_r_' + value,
                'gold_schema': 'gold_r_' + value, 'exports': exports,
                'certified_at': '2026-01-31T12:00:00+00:00',
                'bronze_snapshots': {'transactions': 123}, 'silver_quality': {'rejected': 0}}
    atomic_json(folder / 'release.json', document)
    return document


class BackupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.releases = self.root / 'source/releases'
        self.value = 'a' * 32
        self.document = make_release(self.releases, self.value)
        self.archive = self.root / 'backups/certified.zip'
        env = patch.dict(os.environ, {'CONTROL_PATH': str(self.root / 'control.db')})
        env.start()
        self.addCleanup(env.stop)
        atomic_json(self.releases / 'current.json', {
            'release_id': self.value, 'as_of_date': '2026-01-31',
            'manifest': f'{self.value}/release.json'})

    def rewrite_archive(self, change):
        with zipfile.ZipFile(self.archive) as bundle:
            entries = [(entry, bundle.read(entry)) for entry in bundle.infolist()]
        with zipfile.ZipFile(self.archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
            for entry, content in entries:
                bundle.writestr(entry, change(entry.filename, content))

    def test_roundtrip_checks_every_file_and_never_publishes(self):
        result = create_backup(self.releases, self.archive)
        self.assertEqual(result['exports'], 2)
        self.assertEqual(verify_archive(self.archive)['status'], 'PASS')
        destination = self.root / 'restored/releases'
        pointer = destination / 'current.json'
        atomic_json(pointer, {'previous': 'must not change'})
        before = pointer.read_bytes()
        restored = restore_backup(self.archive, destination)
        self.assertFalse(restored['published'])
        self.assertEqual(pointer.read_bytes(), before)
        for relative in ['release.json', 'exports/fct_transactions.csv',
                         'exports/mart_reconciliation_daily.csv']:
            self.assertEqual((self.releases / self.value / relative).read_bytes(),
                             (destination / self.value / relative).read_bytes())
        with self.assertRaises(FileExistsError):
            restore_backup(self.archive, destination)
        with self.assertRaises(FileExistsError):
            create_backup(self.releases, self.archive)

    def test_corrupt_source_does_not_publish_backup(self):
        (self.releases / self.value / 'exports/fct_transactions.csv').write_text('corrupt')
        with self.assertRaises(ValueError):
            create_backup(self.releases, self.archive)
        self.assertFalse(self.archive.exists())

    def test_corrupt_archive_does_not_leave_a_restored_release(self):
        create_backup(self.releases, self.archive)
        self.rewrite_archive(lambda name, data: b'corrupt' if name.endswith('fct_transactions.csv') else data)
        destination = self.root / 'restore/releases'
        with self.assertRaises(ValueError):
            restore_backup(self.archive, destination)
        self.assertFalse((destination / self.value).exists())
        self.assertEqual(list(destination.iterdir()), [])

    def test_zip_slip_extra_members_symlinks_and_size_limit_are_rejected(self):
        create_backup(self.releases, self.archive)
        with self.assertRaises(ValueError):
            verify_archive(self.archive, max_bytes=10)
        for name, symlink in [('../escaped.txt', False), ('extra.txt', False), ('bad-link', True)]:
            malicious = self.root / ('malicious-link.zip' if symlink else name.replace('/', '_') + '.zip')
            malicious.write_bytes(self.archive.read_bytes())
            with zipfile.ZipFile(malicious, 'a') as bundle:
                if symlink:
                    info = zipfile.ZipInfo(name)
                    info.create_system = 3
                    info.external_attr = (stat.S_IFLNK | 0o777) << 16
                    bundle.writestr(info, 'outside')
                else:
                    bundle.writestr(name, 'outside')
            with self.assertRaises(ValueError):
                verify_archive(malicious)
        self.assertFalse((self.root / 'escaped.txt').exists())

    def test_manifest_path_and_duplicate_case_members_are_rejected(self):
        create_backup(self.releases, self.archive)
        duplicate = self.root / 'duplicate.zip'
        duplicate.write_bytes(self.archive.read_bytes())
        with zipfile.ZipFile(duplicate, 'a') as bundle:
            bundle.writestr('BACKUP.JSON', '{}')
        with self.assertRaises(ValueError):
            verify_archive(duplicate)
        def change(name, content):
            if name == 'backup.json':
                metadata = json.loads(content)
                metadata['members'][0]['path'] = '../outside'
                return json.dumps(metadata).encode()
            return content
        self.rewrite_archive(change)
        with self.assertRaises(ValueError):
            verify_archive(self.archive)

    def test_backup_requires_certified_release_and_pointer_agreement(self):
        atomic_json(self.releases / 'current.json', {'release_id': self.value,
                    'manifest': f'{self.value}/release.json', 'as_of_date': '2026-02-01'})
        with self.assertRaises(ValueError):
            create_backup(self.releases, self.archive)
        self.document['status'] = 'SOURCE_READY'
        atomic_json(self.releases / self.value / 'release.json', self.document)
        with self.assertRaises(ValueError):
            create_backup(self.releases, self.archive, self.value)
        with self.assertRaises(ValueError):
            create_backup(self.releases, self.releases / 'unsafe.zip', self.value)

    def test_failure_during_copy_does_not_publish_or_overwrite(self):
        with patch('bank.backup.verify_archive', side_effect=ValueError('injected incomplete backup')):
            with self.assertRaises(ValueError):
                create_backup(self.releases, self.archive)
        self.assertFalse(self.archive.exists())
        self.assertEqual(list(self.archive.parent.iterdir()), [])

    def test_invalid_backup_metadata_does_not_install_release(self):
        create_backup(self.releases, self.archive)
        def change(name, content):
            if name == 'backup.json':
                metadata = json.loads(content)
                metadata['created_at'] = '2026-01-01T00:00:00'
                return json.dumps(metadata).encode()
            return content
        self.rewrite_archive(change)
        destination = self.root / 'restore-invalid/releases'
        with self.assertRaises(ValueError):
            restore_backup(self.archive, destination)
        self.assertFalse((destination / self.value).exists())
