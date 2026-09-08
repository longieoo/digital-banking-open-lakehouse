"""Failure-safe ingestion. Adapters contain storage details, not control semantics."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Protocol


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def utc(value) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp requires timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True, order=True)
class Watermark:
    updated_at: str = "1970-01-01T00:00:00.000000+00:00"
    record_id: str = ""


class Source(Protocol):
    def extract(self, start: Watermark, limit: int, dates: tuple[str, str] | None) -> list[dict]: ...


class RawStore(Protocol):
    def put_verified(self, key: str, content: bytes) -> None: ...


class BronzeStore(Protocol):
    def commit(self, rows: list[dict]) -> str: ...
    def validate(self, rows: list[dict]) -> int: ...


class State:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS watermark (source TEXT PRIMARY KEY, ts TEXT, id TEXT);
            CREATE TABLE IF NOT EXISTS batches (
              batch_id TEXT PRIMARY KEY, source_system TEXT, source_table TEXT, mode TEXT,
              started_at TEXT, completed_at TEXT, status TEXT,
              start_watermark TEXT, end_watermark TEXT, extracted_count INTEGER DEFAULT 0,
              raw_written_count INTEGER DEFAULT 0, bronze_written_count INTEGER DEFAULT 0,
              target_snapshot TEXT, error TEXT, attempts INTEGER DEFAULT 1);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=60)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def exclusive(self):
        # A separate SQLite transaction serializes writers without making STARTED
        # manifests uncommitted. OS releases locks after a process crash.
        db = sqlite3.connect(str(self.path) + ".lock", timeout=60)
        try:
            db.execute("BEGIN IMMEDIATE")
            yield
        finally:
            db.rollback()
            db.close()

    def watermark(self, source):
        with self.connect() as db:
            row = db.execute("SELECT ts,id FROM watermark WHERE source=?", (source,)).fetchone()
        return Watermark(*row) if row else Watermark()

    def manifests(self):
        with self.connect() as db:
            return [dict(r) for r in db.execute("SELECT * FROM batches ORDER BY started_at")]


class FileRaw:
    def __init__(self, root):
        self.root = Path(root)

    def put_verified(self, key, content):
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != content:
                raise ValueError("Raw content conflict")
        else:
            import os
            temporary = path.with_suffix(".tmp")
            with temporary.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(path)
            if os.name != 'nt':
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        if path.read_bytes() != content:
            raise IOError("Raw verification failed")


class SQLiteBronze:
    """Durable contract-test double ONLY. Deployed Bronze uses real Iceberg."""
    def __init__(self, path):
        self.path = path
        with transaction(path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS versions (id TEXT, ts TEXT, payload TEXT, '
                       'PRIMARY KEY(id,ts))')

    def commit(self, rows):
        with transaction(self.path) as db:
            for row in rows:
                old = db.execute("SELECT payload FROM versions WHERE id=? AND ts=?",
                                 (row['record_id'], row['source_updated_at'])).fetchone()
                if old and old[0] != row['payload']:
                    raise ValueError("Source changed without updated_at change")
                db.execute("INSERT OR IGNORE INTO versions VALUES (?,?,?)",
                           (row['record_id'], row['source_updated_at'], row['payload']))
        return "contract-test-double"

    def validate(self, rows):
        with transaction(self.path) as db:
            for row in rows:
                saved = db.execute("SELECT payload FROM versions WHERE id=? AND ts=?",
                                   (row['record_id'], row['source_updated_at'])).fetchone()
                if not saved or saved[0] != row['payload']:
                    raise ValueError("Bronze verification failed")
        return len(rows)


class MemorySource:
    def __init__(self, rows, key="transaction_id"):
        self.rows, self.key = rows, key

    def extract(self, start, limit, dates):
        rows = [dict(r, updated_at=utc(r['updated_at'])) for r in self.rows]
        rows = sorted(rows, key=lambda r: (r['updated_at'], r[self.key]))
        if dates:
            return [r for r in rows if dates[0] <= r['business_date'] <= dates[1]]
        return [r for r in rows if Watermark(r['updated_at'], r[self.key]) > start][:limit]


def ingest(source: Source, raw: RawStore, bronze: BronzeStore, state: State,
           source_name="core.transactions", key="transaction_id", limit=10000,
           dates: tuple[str, str] | None = None, fail_at=None):
    if limit < 1:
        raise ValueError("Positive batch limit required")
    if dates and (date.fromisoformat(dates[0]) > date.fromisoformat(dates[1])):
        raise ValueError("Invalid backfill range")
    with state.exclusive():
        start = state.watermark(source_name)
        mode = "BACKFILL" if dates else "INCREMENTAL"
        # A request identity exists even if extraction itself fails.
        request = dict(source=source_name, start=start.__dict__, dates=dates, limit=limit)
        batch_id = hashlib.sha256(canonical(request)).hexdigest()
        now = datetime.now(timezone.utc).isoformat()
        system, table = source_name.split(".")
        with state.connect() as db:
            db.execute('''INSERT INTO batches(batch_id,source_system,source_table,mode,started_at,
                status,start_watermark) VALUES(?,?,?,?,?,'STARTED',?)
                ON CONFLICT(batch_id) DO UPDATE SET status='STARTED',error=NULL,
                completed_at=NULL, attempts=attempts+1''',
                (batch_id, system, table, mode, now, canonical(start.__dict__).decode()))
        try:
            extracted = source.extract(start, limit, dates)
            rows_by_version = {}
            for row in extracted:
                row = dict(row, updated_at=utc(row['updated_at']))
                identity = (row[key], row['updated_at'])
                if identity in rows_by_version and rows_by_version[identity] != row:
                    raise ValueError("Conflicting source version")
                rows_by_version[identity] = row
            normalized = sorted(rows_by_version.values(), key=lambda r: (r['updated_at'], r[key]))
            end = Watermark(normalized[-1]['updated_at'], normalized[-1][key]) if normalized else start
            content = canonical(normalized)
            digest = hashlib.sha256(content).hexdigest()
            raw_key = f"{system}/{table}/{digest}.json"
            with state.connect() as db:
                db.execute("UPDATE batches SET extracted_count=?,end_watermark=? WHERE batch_id=?",
                           (len(extracted), canonical(end.__dict__).decode(), batch_id))
            if fail_at == "raw":
                raise IOError("Injected raw failure")
            raw.put_verified(raw_key, content)
            with state.connect() as db:
                db.execute("UPDATE batches SET raw_written_count=? WHERE batch_id=?",
                           (len(normalized), batch_id))
            envelopes = [dict(record_id=r[key], source_updated_at=r['updated_at'],
                business_date=r.get('business_date', r['updated_at'][:10]),
                payload=canonical(r).decode(), batch_id=batch_id, source_system=system,
                source_table=table) for r in normalized]
            if fail_at == "bronze":
                raise IOError("Injected Bronze failure")
            snapshot = bronze.commit(envelopes) if envelopes else None
            if fail_at == "after_commit":
                raise IOError("Injected post-commit failure")
            written = bronze.validate(envelopes)
            if written != len(envelopes):
                raise ValueError("Bronze count mismatch")
            # Completion and forward-state update are one local atomic transaction.
            with state.connect() as db:
                db.execute("UPDATE batches SET status='COMPLETED',completed_at=?,bronze_written_count=?,"
                           "target_snapshot=? WHERE batch_id=?",
                           (datetime.now(timezone.utc).isoformat(), written, snapshot, batch_id))
                if not dates:
                    db.execute("INSERT INTO watermark VALUES(?,?,?) ON CONFLICT(source) DO UPDATE "
                               "SET ts=excluded.ts,id=excluded.id", (source_name, end.updated_at, end.record_id))
            return dict(batch_id=batch_id, count=written, snapshot=snapshot, status="COMPLETED")
        except Exception as exc:
            with state.connect() as db:
                # Exception messages can contain connection strings; store type only.
                db.execute("UPDATE batches SET status='FAILED',error=?,completed_at=? WHERE batch_id=?",
                           (type(exc).__name__, datetime.now(timezone.utc).isoformat(), batch_id))
            raise


@contextmanager
def transaction(path):
    db = sqlite3.connect(path)
    try:
        with db:
            yield db
    finally:
        db.close()
