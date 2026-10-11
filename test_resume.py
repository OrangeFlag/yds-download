"""Resume interrupted archives and bound listing retries without live requests."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import download
import recovery
from test_regressions import Response


class ResumeChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / '.download'
        self.state.mkdir()
        self.source = 'https://disk.yandex.ru/d/test'
        self.content = b'A' * 128
        self.item = dict(path='/Course/video.mp4', size=128,
                         sha256=hashlib.sha256(self.content).hexdigest(), file='https://example.invalid/video')
        self.event = dict(path=self.item['path'], size=128, sha256=self.item['sha256'],
                          course='Course', corrupted_path='corrupted/old/Course/video.mp4',
                          time='2026-01-01', status='partial', attempt=3, detail='interrupted')
        self.body = self.root / self.event['corrupted_path']
        self.body.parent.mkdir(parents=True)
        self.body.write_bytes(self.content[:64])
        download.write_json(Path(str(self.body) + '.ranges.json'),
                            dict(size=128, sha256=self.item['sha256'], ranges=[[0, 64]]))
        download.write_json(self.body.parents[1] / 'failed.json', [self.event])
        download.write_json(self.state / 'source.json', dict(source=self.source, title='test'))
        self.status('running')
        self.requests = []
        for target, kwargs in (
                ('download.files', dict(return_value=[self.item])),
                ('download.time.sleep', {}),
                ('download.subprocess.call', dict(side_effect=AssertionError('aria2 restarted an archived file')))):
            context = patch(target, **kwargs)
            context.start()
            self.addCleanup(context.stop)

    def status(self, state):
        download.write_json(self.state / 'course-history.json', {'Course': dict(
            course='Course', source=self.source, state=state, total=1, verified=0,
            failed=1, complete=False, time='2026-01-02')})

    def run_download(self, **kwargs):
        return download.download(dict(source=self.source, title='test'), ['Course'], self.root, retries=0, **kwargs)

    def serve(self, request, timeout):
        start, last = map(int, request.get_header('Range')[6:].split('-'))
        self.requests.append((start, last + 1))
        return Response(start, last + 1, 128, self.content[start:last + 1])

    def fail_range(self, request, timeout):
        self.requests.append(request.get_header('Range'))
        raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)

    def test_running_archive_resumes_saved_ranges_before_aria2(self):
        with patch.object(download.OPENER, 'open', side_effect=self.serve):
            self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.requests, [(64, 128)])
        self.assertEqual((self.root / 'Course/video.mp4').read_bytes(), self.content)
        self.assertEqual(self.body.read_bytes(), self.content)
        self.assertEqual(json.loads((self.state / 'summary.json').read_text())['verified'], 1)

    def test_incomplete_running_archive_is_not_retried_twice(self):
        with patch.object(download.OPENER, 'open', side_effect=self.fail_range), \
                patch.object(recovery, 'promote_partial', return_value=False):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.body.read_bytes(), self.content[:64])
        self.assertEqual(json.loads((self.state / '01-status.json').read_text())['failed'], 1)

    def test_explicit_retry_does_not_repeat_running_range_recovery(self):
        with patch.object(download.OPENER, 'open', side_effect=self.fail_range), \
                patch.object(recovery, 'promote_partial', return_value=False):
            self.assertEqual(self.run_download(retry_corrupted=True), 1)
        self.assertEqual(len(self.requests), 2)

    def test_finished_archive_needs_explicit_retry(self):
        self.status('finished')
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Finished archive retried')):
            self.assertEqual(self.run_download(), 1)

    def test_api_does_not_retry_permanent_404(self):
        error = urllib.error.HTTPError('https://example.invalid', 404, 'Gone', {}, None)
        attempts = []
        def fail(*args, **kwargs):
            attempts.append(1)
            raise error
        with patch.object(download.OPENER, 'open', side_effect=fail):
            with self.assertRaises(urllib.error.HTTPError):
                download.api(self.source, '/Course')
        self.assertEqual(len(attempts), 1)

    def test_listing_does_not_retry_permanent_404(self):
        error = urllib.error.HTTPError('https://example.invalid', 404, 'Gone', {}, None)
        with patch.object(download, 'files', side_effect=[error, AssertionError('Permanent error retried')]):
            with self.assertRaises(urllib.error.HTTPError):
                download.list_course(self.source, 'Course')

    def test_transient_listing_failure_is_bounded(self):
        error = urllib.error.HTTPError('https://example.invalid', 503, 'Unavailable', {}, None)
        with patch.object(download, 'files', side_effect=[error, error, error, AssertionError('Unbounded retries')]):
            with self.assertRaises(urllib.error.HTTPError):
                download.list_course(self.source, 'Course')


if __name__ == '__main__':
    unittest.main()
