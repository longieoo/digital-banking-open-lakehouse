"""Capture source row identities within the release read transaction."""
import os
import hashlib
import json

from bank.ingestion import utc


class Membership:
    def __init__(self, file, key):
        self.file = file
        self.key = key
        self.count = 0
        self.digest = hashlib.sha256()

    def record(self, rows):
        for row in rows:
            identity = {'record_id': row[self.key], 'source_updated_at': utc(row['updated_at'])}
            line = (json.dumps(identity, separators=(',', ':')) + '\n').encode()
            self.file.write(line)
            self.digest.update(line)
            self.count += 1

    def metadata(self):
        self.file.flush()
        if hasattr(self.file, 'name'):
            os.fsync(self.file.fileno())
        return {'count': self.count, 'sha256': self.digest.hexdigest()}


def verify_membership(path, metadata):
    checksum = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            checksum.update(block)
    if checksum.hexdigest() != metadata['sha256']:
        raise ValueError('Source membership checksum mismatch')
