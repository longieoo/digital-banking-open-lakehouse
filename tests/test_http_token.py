import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest

from scripts.http_token import create_token


class HttpTokenTest(unittest.TestCase):
    def test_private_secret_is_not_logged_or_overwritten_or_written_outside_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous = Path.cwd()
            try:
                os.chdir(tmp)
                output = Path('runtime/secrets/http-token')
                log = io.StringIO()
                with contextlib.redirect_stdout(log):
                    result = create_token(output)
                secret = output.read_text(encoding='ascii').strip()
                self.assertGreaterEqual(len(secret),32)
                self.assertFalse(result['token_logged'])
                self.assertNotIn(secret,log.getvalue())
                if os.name!='nt':
                    self.assertEqual(output.stat().st_mode&0o777,0o600)
                with self.assertRaises(FileExistsError):
                    create_token(output)
                self.assertEqual(output.read_text().strip(),secret)
                with self.assertRaises(ValueError):
                    create_token('public-token.txt')
                self.assertFalse(Path('public-token.txt').exists())
            finally:
                os.chdir(previous)
