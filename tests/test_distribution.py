from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.run_release import code_revision


class DistributionTest(unittest.TestCase):
    def test_source_archive_runs_without_git(self):
        with tempfile.TemporaryDirectory() as tmp, patch('scripts.run_release.subprocess.check_output') as git:
            revision = code_revision(Path(tmp))
            git.assert_not_called()
            self.assertRegex(revision,r'^source-sha256:[a-f0-9]{64}$')

    def test_git_unavailable_uses_source_digest(self):
        with tempfile.TemporaryDirectory() as tmp, patch('scripts.run_release.subprocess.check_output',side_effect=OSError):
            root = Path(tmp)
            (root/'.git').mkdir()
            self.assertTrue(code_revision(root).startswith('source-sha256:'))
