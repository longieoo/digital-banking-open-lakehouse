import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from scripts.package_project import package, selected


@unittest.skipUnless(shutil.which('git'), 'Git required for real source selection')
class PackagingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / 'source'
        self.root.mkdir()
        self.git('init', '--quiet')
        self.write('.gitignore', '.env\n.env.*\n!.env.example\n.venv/\nruntime/\nwork/\n')
        self.write('README.md', '# Complete synthetic project\n')
        self.write('bank/__init__.py', '')
        self.write('bank/example.py', 'VALUE = 1\n')
        self.write('.env.example', 'STORAGE_BACKEND=s3\n')
        self.git('add', '.')
        self.write('tests/new_test.py', '# working addition must be preserved\n')
        self.write('.env', 'credential=private\n')
        self.write('.env.production', 'credential=private\n')
        self.write('.venv/dependency.py', '# dependency\n')
        self.write('runtime/secret.log', 'private log\n')
        self.output = self.base / 'complete.zip'

    def git(self, *args):
        return subprocess.check_output(['git', '-c', f'safe.directory={self.root.as_posix()}',
                                        '-C', str(self.root), *args], stderr=subprocess.DEVNULL)

    def write(self, name, content):
        target = self.root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding='utf-8')

    def test_zip_contains_current_source_additions_manifest_and_no_ignored_credentials(self):
        result = package(self.root, self.output)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(hashlib.sha256(self.output.read_bytes()).hexdigest(), result['sha256'])
        with zipfile.ZipFile(self.output) as archive:
            self.assertIsNone(archive.testzip())
            prefix = 'digital-banking-open-lakehouse/'
            names = archive.namelist()
            self.assertEqual(len(names), len(set(names)))
            self.assertIn(prefix + 'tests/new_test.py', names)
            self.assertIn(prefix + '.env.example', names)
            self.assertNotIn(prefix + '.env', names)
            self.assertNotIn(prefix + '.env.production', names)
            self.assertFalse(any('/runtime/' in name or '/.venv/' in name or '/.git/' in name for name in names))
            manifest = json.loads(archive.read(prefix + 'PACKAGE_MANIFEST.json'))
            self.assertEqual({item['path'] for item in manifest['files']}, set(selected(self.root)))
            for item in manifest['files']:
                payload = archive.read(prefix + item['path'])
                self.assertEqual(item['bytes'], len(payload))
                self.assertEqual(item['sha256'], hashlib.sha256(payload).hexdigest())

    def test_existing_archive_is_never_replaced(self):
        self.output.write_bytes(b'previous delivery')
        with patch('scripts.package_project.selected') as selection:
            with self.assertRaises(FileExistsError):
                package(self.root, self.output)
        selection.assert_not_called()
        self.assertEqual(self.output.read_bytes(), b'previous delivery')

    def test_forbidden_tracked_secret_paths_are_rejected_even_if_git_ignores_them(self):
        for name in ['.env.production', '.env.backup', 'config/secret.key',
                     'infra/demo.tfstate', 'work/old.py']:
            with self.subTest(name=name):
                self.write(name, 'private\n')
                self.git('add', '-f', '--', name)
                with self.assertRaises(ValueError):
                    selected(self.root)
                self.git('rm', '--cached', '-f', '--', name)
                (self.root / name).unlink()

    def test_credential_shaped_contents_are_rejected_without_echoing_the_value(self):
        secret = '-----BEGIN ' + 'PRIVATE KEY-----\nprivate fixture\n'
        self.write('config/live.json', secret)
        self.git('add', 'config/live.json')
        with self.assertRaises(ValueError) as caught:
            package(self.root, self.output)
        self.assertNotIn(secret, str(caught.exception))
        self.assertFalse(self.output.exists())

    def test_source_manifest_collision_cannot_create_duplicate_zip_member(self):
        self.write('PACKAGE_MANIFEST.json', '{}')
        with self.assertRaises(ValueError):
            package(self.root, self.output)
        self.assertFalse(self.output.exists())
