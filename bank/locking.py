"""Reentrant thread/process coordination on a trusted local SQLite filesystem."""
from contextlib import contextmanager
import math
import os
from pathlib import Path
import sqlite3
import threading

_local = threading.local()


@contextmanager
def file_lock(path, timeout=600):
    """OS releases the transaction lock if its owner crashes; no stale PID cleanup."""
    if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('Positive finite lock timeout required')
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError('Lock paths must not be symbolic links')
    key = (os.getpid(), os.path.normcase(str(path.resolve())))
    held = getattr(_local, 'held', set())
    if key in held:
        yield
        return
    db = sqlite3.connect(path, timeout=timeout)
    acquired = False
    try:
        db.execute('BEGIN IMMEDIATE')
        held.add(key)
        _local.held = held
        acquired = True
        yield
    finally:
        if acquired:
            held.remove(key)
        db.rollback()
        db.close()
