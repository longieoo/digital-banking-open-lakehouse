"""Verified, portable certified-export backups; not an infrastructure backup.

Restore never changes current.json and never overwrites an existing release.
PostgreSQL, raw objects, catalogs, control databases and checkpoints are outside
this bundle format and require separate coordinated infrastructure recovery.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tempfile
import zipfile

from bank.ingestion import State
from bank.release import load, release_id, verify


FORMAT = 'bank-certified-bundle-v1'
MAX_BYTES = 2 * 1024**3
MAX_MEMBERS = 1000
MAX_MANIFEST_BYTES = 2 * 1024**2


def _sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _root(value):
    path = Path(value).absolute()
    # No reparse point/symlink routing for a recovery target or source root.
    if any(_is_link(parent) for parent in (path, *path.parents)):
        raise ValueError('Symlink release root is not supported')
    return path.resolve()


def _is_link(path):
    try:
        attributes = getattr(path.lstat(), 'st_file_attributes', 0)
    except FileNotFoundError:
        return False
    return path.is_symlink() or bool(attributes & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0))


def _current(root):
    pointer = root / 'current.json'
    if pointer.is_symlink():
        raise ValueError('Symlink publication pointer')
    document = json.loads(pointer.read_text(encoding='utf-8'))
    value = release_id(document['release_id'])
    if document.get('manifest') != f'{value}/release.json':
        raise ValueError('Invalid publication pointer')
    manifest = load(value, root=root)
    if document.get('as_of_date') != manifest['as_of_date']:
        raise ValueError('Publication date does not match release')
    return value


@contextmanager
def _lock(root):
    # Match publication's CONTROL_PATH when supplied; otherwise use the local
    # runtime convention. All cooperating writers must share the same lock.
    state = State(os.environ.get('CONTROL_PATH', str(root.parent / 'control.db')))
    with state.exclusive():
        yield


def _members(document):
    return ['release.json', *sorted(item['file'] for item in document['exports'].values())]


def _safe_member(name):
    parts = PurePosixPath(name).parts
    if not parts or name.startswith('/') or '\\' in name or ':' in name:
        raise ValueError('Unsafe archive path')
    if any(part in {'.', '..', ''} for part in parts) or str(PurePosixPath(name)) != name:
        raise ValueError('Unsafe archive path')
    return parts


def _extract(archive, target, max_bytes=MAX_BYTES):
    """Validate the entire member index before writing any extracted payload."""
    if max_bytes < 1:
        raise ValueError('Positive extraction byte limit required')
    with zipfile.ZipFile(archive) as bundle:
        entries = bundle.infolist()
        names = [entry.filename for entry in entries]
        if not entries or len(entries) > MAX_MEMBERS or len(set(n.casefold() for n in names)) != len(names):
            raise ValueError('Duplicate or excessive archive members')
        if sum(entry.file_size for entry in entries) > max_bytes:
            raise ValueError('Archive exceeds extraction byte limit')
        for entry in entries:
            _safe_member(entry.filename)
            mode = entry.external_attr >> 16
            if entry.is_dir() or stat.S_IFMT(mode) not in {0, stat.S_IFREG} or entry.flag_bits & 1:
                raise ValueError('Only regular unencrypted archive files are supported')
        if 'backup.json' not in names or bundle.getinfo('backup.json').file_size > MAX_MANIFEST_BYTES:
            raise ValueError('Missing or oversized backup manifest')
        metadata = json.loads(bundle.read('backup.json'))
        if metadata.get('format') != FORMAT or metadata.get('scope') != 'certified_export_bundle':
            raise ValueError('Unsupported backup format')
        created = datetime.fromisoformat(metadata['created_at'].replace('Z', '+00:00'))
        if created.tzinfo is None:
            raise ValueError('Backup creation time requires a timezone')
        value = release_id(metadata['release_id'])
        members = metadata['members']
        if not isinstance(members, list) or not members or len(members) > MAX_MEMBERS - 1:
            raise ValueError('Invalid backup member list')
        expected = {'backup.json'}
        for item in members:
            relative = item['path']
            _safe_member(relative)
            if relative != 'release.json' and not (
                relative.startswith('exports/') and len(PurePosixPath(relative).parts) == 2
            ):
                raise ValueError('Unexpected backup payload')
            name = f'{value}/{relative}'
            if name in expected:
                raise ValueError('Duplicate backup payload')
            expected.add(name)
            size = item['bytes']
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise ValueError('Invalid backup member size')
            if name not in names or bundle.getinfo(name).file_size != size:
                raise ValueError('Backup member size mismatch')
        if expected != set(names):
            raise ValueError('Unexpected or missing archive members')
        for item in members:
            destination = target / value / item['path']
            destination.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(f'{value}/{item["path"]}') as source, destination.open('xb') as sink:
                shutil.copyfileobj(source, sink, length=1024 * 1024)
                sink.flush()
                os.fsync(sink.fileno())
            if _sha256(destination) != item['sha256']:
                raise ValueError('Backup member checksum mismatch')
    document = verify(value, root=target)
    if sorted(item['path'] for item in members) != sorted(_members(document)):
        raise ValueError('Backup contains incomplete or unrelated exports')
    return metadata, document


def verify_archive(archive, max_bytes=MAX_BYTES):
    """Read every payload, verify manifest hashes and validate certified exports."""
    with tempfile.TemporaryDirectory(prefix='bank-backup-verify-') as temporary:
        metadata, document = _extract(archive, Path(temporary), max_bytes)
    return {'status': 'PASS', 'scope': 'certified_export_bundle',
            'release_id': document['release_id'], 'as_of_date': document['as_of_date'],
            'exports': len(document['exports']), 'archive_sha256': _sha256(Path(archive)),
            'created_at': metadata['created_at']}


def create_backup(release_root, archive, value=None):
    """Copy one immutable certified bundle to a new ZIP, then verify before publication."""
    root = _root(release_root)
    destination = Path(archive).absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError('Backup destination already exists')
    if destination == root or root in destination.parents:
        raise ValueError('Backup destination must be outside the release tree')
    destination.parent.mkdir(parents=True, exist_ok=True)
    if any(_is_link(parent) for parent in (destination.parent, *destination.parent.parents)):
        raise ValueError('Symlink backup destination is not supported')
    temporary = None
    try:
        with _lock(root):
            value = release_id(value) if value is not None else _current(root)
            document = verify(value, root=root)
            manifest_before = (root / value / 'release.json').read_bytes()
            metadata = {'format': FORMAT, 'release_id': value,
                        'created_at': datetime.now(timezone.utc).isoformat(),
                        'scope': 'certified_export_bundle', 'members': []}
            for name in _members(document):
                path = root / value / name
                metadata['members'].append({'path': name, 'bytes': path.stat().st_size,
                                             'sha256': _sha256(path)})
            with tempfile.NamedTemporaryFile(dir=destination.parent, suffix='.zip', delete=False) as handle:
                temporary = Path(handle.name)
            with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as bundle:
                bundle.writestr('backup.json', json.dumps(metadata, indent=2, sort_keys=True))
                for name in _members(document):
                    bundle.write(root / value / name, f'{value}/{name}')
            result = verify_archive(temporary)
            if (root / value / 'release.json').read_bytes() != manifest_before:
                raise ValueError('Release manifest changed while backing up')
            with temporary.open('rb+') as handle:
                os.fsync(handle.fileno())
            # Hard-link publication fails if another backup already owns this
            # destination; unlike replace(), it never overwrites that backup.
            os.link(temporary, destination)
        return {**result, 'archive': str(destination)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def restore_backup(archive, release_root, max_bytes=MAX_BYTES):
    """Restore into a new release directory, with no changes to current.json."""
    root = _root(release_root)
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.restore-', dir=root) as temporary:
        staging = Path(temporary)
        metadata, document = _extract(archive, staging, max_bytes)
        value = document['release_id']
        destination = root / value
        with _lock(root):
            if destination.exists() or destination.is_symlink():
                raise FileExistsError('Restore never overwrites an existing release')
            # Same-volume rename gives readers either an absent or complete
            # directory. Publication is a separate explicitly authorized action.
            (staging / value).rename(destination)
    return {'status': 'PASS', 'scope': 'certified_export_bundle', 'release_id': value,
            'exports': len(document['exports']), 'published': False,
            'created_at': metadata['created_at']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    create = commands.add_parser('create')
    create.add_argument('--release-root', default=os.environ.get('RELEASE_ROOT', 'runtime/releases'))
    create.add_argument('--archive', required=True)
    create.add_argument('--release-id')
    check = commands.add_parser('verify')
    check.add_argument('--archive', required=True)
    check.add_argument('--max-bytes', type=int, default=MAX_BYTES)
    restore = commands.add_parser('restore')
    restore.add_argument('--archive', required=True)
    restore.add_argument('--release-root', required=True)
    restore.add_argument('--max-bytes', type=int, default=MAX_BYTES)
    args = parser.parse_args(argv)
    if args.action == 'create':
        result = create_backup(args.release_root, args.archive, args.release_id)
    elif args.action == 'verify':
        result = verify_archive(args.archive, args.max_bytes)
    else:
        result = restore_backup(args.archive, args.release_root, args.max_bytes)
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        raise SystemExit(type(exc).__name__) from None
