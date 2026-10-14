"""Per-file checkpoints, durable migration and concurrent writers."""
import json
import multiprocessing
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
from file_state import FileState


def write_record(state, key):
    FileState(Path(state), 'transfer-state').set(key, {'phase': 'finished'})


class FileStateChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state = Path(temporary.name)
        self.store = FileState(self.state, 'transfer-state')

    def test_checkpoint_changes_only_one_file_and_reads_no_other_record(self):
        self.store.set('/Course/first.mp4', {'phase': 'verifying'})
        self.store.set('/Course/second.mp4', {'phase': 'finished'})
        second = self.store.path('/Course/second.mp4')
        before = second.read_bytes(), second.stat().st_mtime_ns
        self.store.set('/Course/first.mp4', {'phase': 'finished'})
        self.assertEqual((second.read_bytes(), second.stat().st_mtime_ns), before)
        original = Path.read_text
        reads = []
        def read(path, *args, **kwargs):
            reads.append(path)
            return original(path, *args, **kwargs)
        with patch.object(Path, 'read_text', read):
            self.assertEqual(self.store.get('/Course/first.mp4'), {'phase': 'finished'})
        self.assertEqual(reads, [self.store.path('/Course/first.mp4')])
        self.assertFalse((self.state/'transfer-state.json').exists())

    def test_interrupted_migration_resumes_without_overwriting_written_records(self):
        legacy = self.state/'transfer-state.json'
        download.write_json(legacy, {'/one': {'phase': 'running'}, '/two': {'phase': 'finished'}})
        original = download.write_json
        def write(path, value):
            if path == self.store.path('/two'):
                raise OSError('disk interrupted during migration')
            original(path, value)
        with patch.object(download, 'write_json', write), self.assertRaises(OSError):
            self.store.get('/one')
        self.assertTrue(legacy.exists())
        self.assertTrue(self.store.path('/one').exists())
        restarted = FileState(self.state, 'transfer-state')
        self.assertEqual(restarted.get('/two'), {'phase': 'finished'})
        self.assertEqual(restarted.get('/one'), {'phase': 'running'})
        self.assertFalse(legacy.exists())
        self.assertEqual(json.loads((self.state/'transfer-state.json.migrated').read_text())['/one'], {'phase': 'running'})

    def test_migration_resyncs_existing_records_before_retiring_legacy_backup(self):
        legacy = self.state/'transfer-state.json'
        download.write_json(legacy, {'/one': {'phase': 'running'}})
        original = download.write_json
        def write(path, value):
            original(path, value)
            raise OSError('checkpoint replaced but directory sync was not confirmed')
        with patch.object(download, 'write_json', write), self.assertRaises(OSError):
            self.store.get('/one')
        self.assertTrue(legacy.exists())
        import storage
        with patch.object(storage, 'sync_directory', wraps=storage.sync_directory) as synced:
            self.assertEqual(FileState(self.state, 'transfer-state').get('/one'), {'phase': 'running'})
        self.assertIn(unittest.mock.call(self.store.path('/one').parent), synced.call_args_list)
        self.assertIn(unittest.mock.call(self.store.directory), synced.call_args_list)

    def test_concurrent_processes_preserve_independent_records(self):
        context = multiprocessing.get_context('spawn')
        processes = [context.Process(target=write_record, args=(str(self.state), '/video/'+str(i))) for i in range(4)]
        for process in processes: process.start()
        for process in processes:
            process.join(10)
            self.assertEqual(process.exitcode, 0)
        for i in range(4): self.assertEqual(self.store.get('/video/'+str(i)), {'phase': 'finished'})

    def test_failed_atomic_replace_keeps_previous_checkpoint(self):
        self.store.set('/video', {'phase': 'running'})
        with patch.object(Path, 'replace', side_effect=OSError('disk unavailable')), self.assertRaises(OSError):
            self.store.set('/video', {'phase': 'finished'})
        self.assertEqual(self.store.get('/video'), {'phase': 'running'})

    def test_record_identity_is_validated(self):
        self.store.set('/video', {'phase': 'running'})
        download.write_json(self.store.path('/video'), {'path': '/other', 'record': {'phase': 'finished'}})
        with self.assertRaises(ValueError): self.store.get('/video')
