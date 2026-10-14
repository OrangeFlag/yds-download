from file_state import FileState
import errno
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import download
import pipeline
import network
import recovery
import storage


class OutageIntegrationChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / '.download'; self.state.mkdir()
        guard = patch.object(network, 'GUARD', network.NetworkGuard())
        guard.start(); self.addCleanup(guard.stop)
        self.item = dict(path='/Course/video.mp4', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(),
                         file='https://example.invalid/video')

    def test_api_waits_without_spending_metadata_attempt(self):
        with patch.object(download.OPENER, 'open', side_effect=[urllib_error(), io.BytesIO(b'{"name":"course"}')]) as call, \
                patch.object(download.time, 'sleep') as delay:
            self.assertEqual(download.api('public-source'), {'name': 'course'})
        self.assertEqual(call.call_count, 2)
        delay.assert_not_called()

    def test_api_incomplete_body_uses_bounded_metadata_retry(self):
        import http.client
        with patch.object(download.OPENER, 'open', side_effect=[http.client.IncompleteRead(b'{'), io.BytesIO(b'{"name":"course"}')]), \
                patch.object(download.time, 'sleep') as delay:
            self.assertEqual(download.api('source'), {'name': 'course'})
        delay.assert_called_once_with(2)

    def test_api_truncated_close_delimited_json_uses_bounded_metadata_retry(self):
        for body in (b'{"name":', b'{"name":"\xd0'):
            with self.subTest(body=body):
                socket = Mock()
                socket.makefile.return_value = io.BytesIO(
                    b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n' + body)
                response = http.client.HTTPResponse(socket); response.begin()
                with patch.object(download.OPENER, 'open', side_effect=[response, io.BytesIO(b'{"name":"course"}')]), \
                        patch.object(download.time, 'sleep') as delay:
                    self.assertEqual(download.api('source'), {'name': 'course'})
                delay.assert_called_once_with(2)

    def test_api_invalid_json_online_has_bounded_server_retries(self):
        with patch.object(download.OPENER, 'open', side_effect=lambda *args, **kwargs: io.BytesIO(b'{')) as request, \
                patch.object(download.time, 'sleep'):
            with self.assertRaises(json.JSONDecodeError):
                download.api('source')
        self.assertEqual(request.call_count, 5)

    def test_api_wait_is_interruptible(self):
        from threading import Event
        with patch.object(download.OPENER, 'open', side_effect=urllib_error()), \
                patch.object(network.Event, 'wait', return_value=True), patch.object(download.time, 'sleep'):
            with self.assertRaises(InterruptedError):
                download.api('public-source', stopping=Event())

    def test_api_server_backoff_stops_before_another_request(self):
        from threading import Event
        from urllib.error import HTTPError
        stop = Event()
        error = HTTPError('hidden', 503, 'Unavailable', {}, None)
        def interrupt(_):
            stop.set()
            return True
        with patch.object(download.OPENER, 'open', side_effect=error) as request, \
                patch.object(stop, 'wait', side_effect=interrupt), \
                patch.object(download.time, 'sleep', side_effect=interrupt):
            with self.assertRaises(InterruptedError):
                download.api('source', stopping=stop)
        self.assertEqual(request.call_count, 1)

    def test_aria_disk_full_remains_retryable(self):
        background = pipeline.Background(self.root, 'source', 2, pipeline.resolve())
        self.addCleanup(background.close)
        with patch.object(pipeline, 'transfer_process', return_value=9) as transfer:
            with self.assertRaises(OSError) as error:
                background.transfer([self.item], 'Course')
        self.assertTrue(storage.disk_error(error.exception))
        transfer.assert_called_once()
        self.assertEqual(background.continuation(self.item)['phase'], 'downloading')

    def test_event_proof_is_synced_before_record_returns(self):
        local = self.root / 'Course/video.mp4';local.parent.mkdir();local.write_bytes(b'lecture')
        stamp = [local.stat().st_ino, local.stat().st_size, local.stat().st_mtime_ns]
        with patch('download.os.fsync') as sync:
            result = download.record(self.state, self.item, 'Course', 1, 'valid', 'Verified', verified_stat=stamp)
        self.assertEqual(result['status'], 'valid')
        self.assertGreater(sync.call_count, 0)

    def test_valid_original_directory_sync_failure_does_not_record_proof(self):
        local = self.root / 'Course/video.mp4'; local.parent.mkdir(); local.write_bytes(b'lecture')
        stamp = [local.stat().st_ino, local.stat().st_size, local.stat().st_mtime_ns]
        with patch.object(storage, 'sync_directory', side_effect=OSError(errno.EIO, 'Directory unavailable')) as sync:
            with self.assertRaises(OSError):
                download.record(self.state, self.item, 'Course', 1, 'valid', 'Verified', verified_stat=stamp)
        sync.assert_called_once_with(local.parent)
        self.assertEqual(download.read_events(self.state / 'events.jsonl'), [])

    def test_copy_body_and_directory_synced_before_proof(self):
        source = self.root / 'archive.mp4';source.write_bytes(b'lecture');target = self.root / 'Course/video.mp4'
        order = []
        original = os.fsync
        def sync(fd):
            directory = stat.S_ISDIR(os.fstat(fd).st_mode)
            order.append('directory' if directory else 'body')
            self.assertEqual(target.exists(), directory)
            original(fd)
        with patch('recovery.os.fsync', side_effect=sync):
            recovery.copy_saved(source, target, overwrite=False)
        self.assertEqual(order, ['body', 'directory'])

    def test_failed_copy_sync_does_not_publish_destination(self):
        source = self.root / 'archive.mp4';source.write_bytes(b'lecture');target = self.root / 'Course/video.mp4'
        with patch('recovery.os.fsync', side_effect=OSError(errno.EIO, 'Disk disconnected')):
            with self.assertRaises(OSError):
                recovery.copy_saved(source, target, overwrite=False)
        self.assertFalse(target.exists())

    def test_full_original_replay_syncs_directory_before_valid_proof(self):
        event = dict(self.item, course='Course', corrupted_path='corrupted/old/Course/video.mp4')
        body = self.root / event['corrupted_path']; body.parent.mkdir(parents=True)
        body.write_bytes(b'lecture')
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=7, sha256=self.item['sha256'], ranges=[[0, 7]]))
        target = self.root / 'Course/video.mp4'
        original = storage.sync_directory
        def sync(path):
            if Path(path) == target.parent:
                raise OSError(errno.EIO, 'Publication directory sync interrupted')
            return original(path)
        with patch.object(recovery, 'retry_ranges', return_value=[[0, 7]]):
            with patch.object(storage, 'sync_directory', side_effect=sync):
                with self.assertRaises(OSError):
                    recovery.finish_recovery(self.root, event, self.item, background=True)
                self.assertTrue(target.exists())
                with self.assertRaises(OSError):
                    recovery.finish_recovery(self.root, event, self.item, background=True)
            ledger = FileState(self.state, 'recovery-state')
            self.assertEqual(ledger.get(self.item['path'])['state'], 'running')
            self.assertEqual(download.read_events(self.state / 'events.jsonl'), [])
            with patch.object(storage, 'sync_directory', wraps=original) as synced:
                self.assertTrue(recovery.finish_recovery(self.root, event, self.item, background=True))
            self.assertIn(unittest.mock.call(target.parent), synced.call_args_list)
        self.assertEqual(ledger.get(self.item['path'])['state'], 'finished')
        self.assertEqual(download.read_events(self.state / 'events.jsonl')[-1]['status'], 'valid')

    def test_copy_io_error_is_retryable_disk_error(self):
        source = self.root / 'archive.mp4';source.write_bytes(b'lecture');target = self.root / 'Course/video.mp4'
        failure = subprocess.CalledProcessError(1, ['cp'], stderr=b'cp: error reading archive: Input/output error')
        with patch.object(recovery.subprocess, 'run', side_effect=failure):
            with self.assertRaises(OSError) as error:
                recovery.copy_saved(source, target)
        self.assertTrue(storage.disk_error(error.exception))
        self.assertFalse(target.exists())


def urllib_error():
    import urllib.error
    return urllib.error.URLError(OSError(errno.ENETUNREACH, 'Network unavailable'))
