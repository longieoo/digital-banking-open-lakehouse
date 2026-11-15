from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from bank.locking import file_lock


class LockingTest(unittest.TestCase):
    def test_reentry_keeps_other_threads_blocked_until_outer_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'release.db'
            entered,started = threading.Event(),threading.Event()
            errors = []
            def other():
                started.set()
                try:
                    with file_lock(path,timeout=3):
                        entered.set()
                except Exception as exc:
                    errors.append(exc)
            with file_lock(path):
                with file_lock(path):
                    pass
                thread = threading.Thread(target=other)
                thread.start()
                self.assertTrue(started.wait(1))
                self.assertFalse(entered.wait(.1))
            thread.join(4)
            self.assertFalse(thread.is_alive())
            self.assertTrue(entered.is_set())
            self.assertEqual(errors,[])

    def test_process_crash_releases_os_lock_without_stale_lock_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            path,marker = Path(tmp)/'release.db',Path(tmp)/'entered'
            child = subprocess.Popen([sys.executable,'-c',
                'from bank.locking import file_lock;from pathlib import Path;import sys,time;'
                'lock=file_lock(sys.argv[1]);lock.__enter__();Path(sys.argv[2]).write_text("locked");time.sleep(30)',
                str(path),str(marker)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic()+8
                while not marker.exists() and child.poll() is None and time.monotonic()<deadline:
                    time.sleep(.02)
                self.assertTrue(marker.exists(),'Child did not acquire the process lock')
                with self.assertRaises(sqlite3.OperationalError):
                    with file_lock(path,timeout=.1):
                        self.fail('Another process owned the lock')
                child.terminate()
                child.communicate(timeout=5)
                with file_lock(path,timeout=2):
                    self.assertTrue(path.exists())
            finally:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)
