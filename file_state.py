"""Atomic per-file state with restartable migration of the aggregate ledgers."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path

import storage


class FileState:
    def __init__(self, state, name):
        if name not in ('transfer-state', 'recovery-state'):
            raise ValueError('Unknown file state')
        self.state = Path(state)
        self.directory = self.state/name
        self.legacy = self.state/(name+'.json')

    def path(self, key):
        digest = hashlib.sha256(key.encode('utf-8')).hexdigest()
        return self.directory/digest[:2]/(digest+'.json')

    @contextmanager
    def locked(self):
        from download import STATE_LOCK
        storage.check(self.state)
        with STATE_LOCK, (self.state/'.file-state.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def read(self, key, default=None):
        try:
            row = json.loads(self.path(key).read_text())
        except FileNotFoundError:
            storage.check(self.state)
            return default
        if not isinstance(row, dict) or row.get('path') != key or not isinstance(row.get('record'), dict):
            raise ValueError('File state identity mismatch: '+key)
        return row['record']

    def write(self, key, record):
        from download import write_json
        if not isinstance(record, dict):
            raise ValueError('Invalid file state record')
        if self.directory.name == 'transfer-state' and record.get('phase') == 'finished':
            record = {key: value for key, value in record.items() if key != 'event'}
        path = self.path(key)
        self.directory.mkdir(exist_ok=True)
        path.parent.mkdir(exist_ok=True)
        write_json(path, dict(path=key, record=record))
        # Retry the parent syncs too if a previous creation was interrupted.
        storage.sync_directory(self.directory)
        storage.sync_directory(self.state)

    def migrate(self):
        if not self.legacy.exists():
            return
        with self.locked():
            if not self.legacy.exists():
                return
            ledger = json.loads(self.legacy.read_text())
            if not isinstance(ledger, dict):
                raise ValueError('Invalid legacy file state')
            for key, record in ledger.items():
                if self.read(key) is None:
                    self.write(key, record)
                else:
                    # A previous replacement may have failed before its directory sync.
                    storage.sync_directory(self.path(key).parent)
                    storage.sync_directory(self.directory)
            self.legacy.replace(self.legacy.with_name(self.legacy.name+'.migrated'))
            storage.sync_directory(self.state)

    def get(self, key, default=None):
        storage.check(self.state)
        self.migrate()
        return self.read(key, default)

    def set(self, key, record):
        self.migrate()
        with self.locked():
            self.write(key, record)
