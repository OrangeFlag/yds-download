"""Regression checks from the independent downloader review; no network or real archives."""
import hashlib
from concurrent.futures import wait as wait_for_requests
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import download
import progress
import recovery
import report


class Response:
    status = 206

    def __init__(self, start, end, total, data, timeout=False):
        self.headers = {'Content-Range': f'bytes {start}-{end - 1}/{total}'}
        self.data = data
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read1(self, size):
        if not self.data and self.timeout:
            raise TimeoutError('stream stopped')
        result, self.data = self.data[:size], self.data[size:]
        return result

    def read(self, size):
        if self.timeout:
            raise TimeoutError('stream stopped')
        return self.read1(size)


class ReviewRegression(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / '.download'
        self.state.mkdir()
        (self.root / 'corrupted').mkdir()
        self.source = 'https://disk.yandex.ru/d/test'
        download.write_json(self.state / 'source.json', dict(source=self.source, title='test'))
        (self.state / 'events.jsonl').write_text('')
        self.content = b'A' * (2 * 1024 ** 2)
        self.item = dict(path='/Course/video.mp4', size=len(self.content),
                         sha256=hashlib.sha256(self.content).hexdigest(), file='https://example.invalid/old')
        self.event = dict(self.item, course='Course', corrupted_path='corrupted/old/Course/video.mp4',
                          time='2026-01-01', status='partial', attempt=3, detail='test')
        self.event.pop('file')
        self.body = self.root / self.event['corrupted_path']
        self.body.parent.mkdir(parents=True)
        download.write_json(self.root / 'corrupted/old/failed.json', [self.event])
        self.no_sleep = patch.object(recovery.time, 'sleep')
        self.no_sleep.start()
        self.addCleanup(self.no_sleep.stop)

    def serve(self, request, timeout):
        start, end = map(int, request.get_header('Range')[6:].split('-'))
        end += 1
        return Response(start, end, self.item['size'], self.content[start:end])

    def journal(self, body, ranges):
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=self.item['size'], sha256=self.item['sha256'], ranges=ranges))

    def test_new_archive_without_events(self):
        (self.state / 'events.jsonl').unlink()
        (self.root / 'corrupted/old/failed.json').unlink()
        self.assertEqual(recovery.recover_corrupted(self.root, self.source, retry=True), [])

    def test_torn_event_is_repaired_before_append(self):
        path = self.state / 'events.jsonl'
        valid = json.dumps(self.event) + '\n'
        path.write_text(valid + '{"path":')
        events = download.read_events(path, repair=True)
        self.assertEqual(events, [self.event])
        download.record(self.state, self.item, 'Course', 1, 'missing', 'test')
        self.assertEqual(len(download.read_events(path)), 2)
        path.write_text('{broken}\n' + valid)
        with self.assertRaises(ValueError):
            download.read_events(path, repair=True)

    def test_selected_courses_only(self):
        event = dict(self.event, course='Excluded', path='/Excluded/video.mp4')
        download.write_json(self.root / 'corrupted/old/failed.json', [event])
        with patch.object(download, 'files', side_effect=AssertionError('Excluded course queried')):
            self.assertEqual(recovery.recover_corrupted(self.root, self.source, retry=True, courses=['Course']), [])

    def test_archive_registered_before_interrupt(self):
        def aria(*args, **kwargs):
            path = self.root / 'Course/video.mp4'
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b'A' * 10)
            return 1
        data = dict(source=self.source, title='test')
        with patch.object(download, 'list_course', return_value=[self.item]), \
                patch.object(download.subprocess, 'call', side_effect=aria), \
                patch.object(download, 'inspect', return_value=('invalid', 'test')), \
                patch.object(recovery, 'finish_recovery', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                download.download(data, ['Course'], self.root, retries=0)
        rows = [row for manifest in (self.root / 'corrupted').glob('*/failed.json')
                for row in json.loads(manifest.read_text())]
        self.assertTrue(any(row.get('corrupted_path') and (self.root / row['corrupted_path']).exists() for row in rows))

    def test_full_bad_sha_not_promoted_and_redownloaded_separately(self):
        self.body.write_bytes(b'B' * self.item['size'])
        self.journal(self.body, [[0, self.item['size']]])
        old_body = self.body
        with patch.object(download, 'files', return_value=[self.item]), \
                patch.object(download.OPENER, 'open', side_effect=self.serve), \
                patch.object(recovery, 'promote_partial', side_effect=AssertionError('Bad SHA promoted')):
            self.assertTrue(recovery.finish_recovery(self.root, self.event, self.item, retries=0))
        self.assertTrue(old_body.exists())
        self.assertEqual(old_body.read_bytes(), b'B' * self.item['size'])
        self.assertEqual((self.root / 'Course/video.mp4').read_bytes(), self.content)
        self.assertNotEqual(self.event['corrupted_path'], str(old_body.relative_to(self.root)))

    def test_source_links_refreshed(self):
        fresh = dict(self.item, file='https://example.invalid/fresh')
        urls = []
        def serve(request, timeout):
            urls.append(request.full_url)
            if request.full_url.endswith('/old'):
                raise urllib.error.HTTPError('hidden', 403, '', {}, None)
            return self.serve(request, timeout)
        with patch.object(download, 'files', return_value=[fresh]), patch.object(download.OPENER, 'open', side_effect=serve):
            ranges = recovery.retry_ranges(self.root, self.event, self.item, 2)
        self.assertEqual(ranges, [[0, self.item['size']]])
        self.assertIn(fresh['file'], urls)

    def test_fine_ranges_skip_isolated_bad_bytes(self):
        def serve(request, timeout):
            start, last = map(int, request.get_header('Range')[6:].split('-'))
            if any(start <= bad <= last for bad in (0, 1024 ** 2)):
                raise urllib.error.HTTPError('hidden', 500, '', {}, None)
            return self.serve(request, timeout)
        with patch.object(download, 'files', return_value=[self.item]), patch.object(download.OPENER, 'open', side_effect=serve):
            ranges = recovery.retry_ranges(self.root, self.event, self.item, 2)
        self.assertGreater(sum(end - start for start, end in ranges), 1024 ** 2)

    def test_timeout_keeps_received_prefix(self):
        def serve(request, timeout):
            start, last = map(int, request.get_header('Range')[6:].split('-'))
            return Response(start, last + 1, self.item['size'], self.content[start:start + 100], timeout=True)
        with patch.object(download, 'files', return_value=[self.item]), patch.object(download.OPENER, 'open', side_effect=serve):
            ranges = recovery.retry_ranges(self.root, self.event, self.item, 0)
        self.assertGreater(sum(end - start for start, end in ranges), 0)

    def test_interrupted_move_without_control_preserves_old_body(self):
        original = self.root / 'Course/video.mp4'
        original.parent.mkdir()
        original.write_bytes(b'B' * self.item['size'])
        with patch.object(download, 'files', return_value=[self.item]), \
                patch.object(download.OPENER, 'open', side_effect=self.serve):
            recovery.recover_corrupted(self.root, self.source, retries=0, retry=True)
        self.assertTrue(self.body.exists())
        self.assertEqual(self.body.read_bytes(), b'B' * self.item['size'])
        self.assertEqual(original.read_bytes(), self.content)

    def test_valid_file_repairs_counts_after_interruption(self):
        original = self.root / 'Course/video.mp4'
        original.parent.mkdir()
        original.write_bytes(self.content)
        download.record(self.state, self.item, 'Course', 0, 'valid', 'SHA verified')
        status = dict(total=1, verified=0, failed=1, complete=False, state='finished', course='Course')
        download.write_json(self.state / 'course-history.json', {'Course': status})
        download.write_json(self.state / '01-manifest.json', [self.item])
        download.write_json(self.state / '01-status.json', status)
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Ready original downloaded again')):
            recovery.recover_corrupted(self.root, self.source, retry=True)
            recovery.recover_corrupted(self.root, self.source, retry=True)
        counts = json.loads((self.state / 'course-history.json').read_text())['Course']
        self.assertEqual((counts['verified'], counts['failed'], counts['complete']), (1, 0, True))

    def test_interrupted_deep_check_resumes_ordinary_download(self):
        status = dict(total=1, verified=1, failed=0, complete=True, state='finished',
                      course='Course', source=self.source, time='2026-01-01')
        download.write_json(self.state / 'course-history.json', {'Course': status})
        download.write_json(self.state / '01-status.json', status)
        with patch.object(download, 'list_course', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                download.download(dict(source=self.source, title='test'), ['Course'], self.root, deep_check=True)
        with patch.object(download, 'list_course', side_effect=RuntimeError('Resume reached unfinished course')):
            with self.assertRaisesRegex(RuntimeError, 'Resume reached'):
                download.download(dict(source=self.source, title='test'), ['Course'], self.root)

    def test_legacy_running_without_timestamp_is_resumed(self):
        finished = dict(total=1, verified=1, failed=0, complete=True, state='finished',
                        course='Course', source=self.source, time='2026-01-01')
        download.write_json(self.state / 'course-history.json', {'Course': finished})
        download.write_json(self.state / '01-status.json', dict(course='Course', source=self.source, state='running', complete=False))
        with patch.object(download, 'list_course', side_effect=RuntimeError('Resume legacy course')):
            with self.assertRaisesRegex(RuntimeError, 'Resume legacy'):
                download.download(dict(source=self.source, title='test'), ['Course'], self.root)

    def test_short_content_range_prefix_is_retained(self):
        def serve(request, timeout):
            start, last = map(int, request.get_header('Range')[6:].split('-'))
            end = min(last + 1, start + 100)
            return Response(start, end, self.item['size'], self.content[start:end])
        with patch.object(download, 'files', return_value=[self.item]), patch.object(download.OPENER, 'open', side_effect=serve):
            ranges = recovery.retry_ranges(self.root, self.event, self.item, 0)
        self.assertGreater(sum(end - start for start, end in ranges), 0)

    def test_completed_request_is_saved_on_interrupt(self):
        def interrupt(futures, **kwargs):
            wait_for_requests(futures)
            raise KeyboardInterrupt
        with patch.object(download, 'files', return_value=[self.item]), \
                patch.object(download.OPENER, 'open', side_effect=self.serve), \
                patch.object(recovery, 'wait', side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                recovery.retry_ranges(self.root, self.event, self.item, 0)
        self.assertTrue(self.body.exists())
        self.assertEqual(self.body.read_bytes(), self.content)
        self.assertEqual(progress.saved_ranges(self.body, self.item['size'], self.item['sha256']), [[0, self.item['size']]])

    def test_report_does_not_confirm_changed_same_size_original(self):
        original = self.root / 'Course/video.mp4'
        original.parent.mkdir()
        original.write_bytes(self.content)
        event = download.record(self.state, self.item, 'Course', 1, 'valid', 'SHA verified')
        original.write_bytes(b'B' * self.item['size'])
        import os
        os.utime(original, ns=(event['mtime_ns'] + 1000000000, event['mtime_ns'] + 1000000000))
        repo = self.root / 'repo'
        (repo / 'courses').mkdir(parents=True)
        card = repo / 'courses/course.md'
        card.write_text('- Год: 2026\n- Источник: [disk](' + self.source + ')\n- Путь: `Course`\n'
                        '**Статус:** не все видео есть, какие-то не удается скачать\n'
                        '| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n'
                        '| --- | --- | --- | --- | --- |\n| `video.mp4` | 1 | Other | | |\n')
        report.update_report(self.root, repo, 'Walterit')
        self.assertNotIn('| Walterit |', card.read_text())

    def test_truncated_control_keeps_independent_journal(self):
        self.body.write_bytes(self.content)
        control = Path(str(self.body) + '.aria2')
        control.write_bytes(b'\x00\x01')
        with self.assertRaises(ValueError):
            progress.received_ranges(control, self.item['size'])
        self.journal(self.body, [[0, 100]])
        self.assertEqual(progress.saved_ranges(self.body, self.item['size'], self.item['sha256']), [[0, 100]])
        self.assertIn('progress_error', progress.progress(self.body, self.item['size'], sha256=self.item['sha256']))
        Path(str(self.body) + '.ranges.json').unlink()
        self.assertIsNone(progress.progress(self.body, self.item['size'], sha256=self.item['sha256'])['downloaded_bytes'])
        with patch.object(download, 'files', return_value=[self.item]), patch.object(download.OPENER, 'open', side_effect=self.serve):
            recovery.retry_ranges(self.root, self.event, self.item, 0)
        self.assertEqual(self.body.read_bytes(), self.content)
        self.assertEqual(control.read_bytes(), b'\x00\x01')
        self.assertNotEqual(recovery.body_path(self.root, self.event), self.body)

    def test_full_original_replaces_own_partial_in_full_table(self):
        original = self.root / 'Course/video.mp4'
        original.parent.mkdir()
        original.write_bytes(self.content)
        download.record(self.state, self.item, 'Course', 1, 'valid', 'SHA verified')
        partial = self.root / 'Course/video (partially available).mp4'
        partial.write_bytes(b'A')
        download.write_json(self.state / 'partial-copies.json', [dict(self.event, source=self.source,
                            recovered_path=str(partial.relative_to(self.root)), local_size=1,
                            mtime_ns=partial.stat().st_mtime_ns, available_time_percent=70)])
        repo = self.root / 'repo'
        (repo / 'courses').mkdir(parents=True)
        card = repo / 'courses/course.md'
        card.write_text('- Год: 2026\n- Источник: [disk](' + self.source + ')\n- Путь: `Course`\n'
                        '**Статус:** не все видео есть, какие-то не удается скачать\n'
                        '| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n'
                        '| --- | --- | --- | --- | --- |\n'
                        '| `video.mp4` | 1 | Walterit (partially available; время 70.00%) | Other | Third |\n')
        report.update_report(self.root, repo, 'Walterit')
        self.assertIn('| Walterit | Other | Third |', card.read_text())
        self.assertNotIn('partially available;', card.read_text())
        card.write_text(card.read_text().replace('| Walterit | Other | Third |',
                        '| Walterit | Walterit (partially available; время 70.00%) | Other |'))
        report.update_report(self.root, repo, 'Walterit')
        self.assertIn('| Walterit |  | Other |', card.read_text())

    def test_promotes_by_confirmed_bytes(self):
        self.body.write_bytes(self.content)
        self.journal(self.body, [[0, 3 * self.item['size'] // 4]])
        self.assertTrue(recovery.promote_partial(self.root, self.event))
        copy = json.loads((self.state / 'partial-copies.json').read_text())[0]
        self.assertNotIn('available_time_percent', copy)
        self.assertEqual(copy['downloaded_percent'], 75)
        repo = self.root / 'repo'
        (repo / 'courses').mkdir(parents=True)
        card = repo / 'courses/course.md'
        card.write_text('- Год: 2026\n- Источник: [disk](' + self.source + ')\n- Путь: `Course`\n'
                        '**Статус:** не все видео есть, какие-то не удается скачать\n'
                        '| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n'
                        '| --- | --- | --- | --- | --- |\n| `video.mp4` | 1 | Other | | |\n')
        report.update_report(self.root, repo, 'Walterit')
        self.assertIn('Walterit (partially available; байтов 75.00%)', card.read_text())
        self.assertNotIn('время ', card.read_text())

    def test_does_not_promote_half_bytes(self):
        self.body.write_bytes(self.content)
        self.journal(self.body, [[0, self.item['size'] // 2]])
        self.assertFalse(recovery.promote_partial(self.root, self.event))

    def test_legacy_time_percentage_does_not_override_byte_threshold(self):
        self.body.write_bytes(self.content)
        self.journal(self.body, [[0, self.item['size'] // 2]])
        self.event.update(available_time_percent=90, available_seconds=9)
        self.assertFalse(recovery.promote_partial(self.root, self.event))
        self.assertNotIn('available_time_percent', self.event)

    def test_byte_threshold_uses_exact_count_not_rounded_percent(self):
        self.body.write_bytes(self.content)
        self.journal(self.body, [[0, self.item['size'] // 2 + 1]])
        self.assertTrue(recovery.promote_partial(self.root, self.event))

    def test_saved_ranges_cannot_exceed_retained_body(self):
        self.body.write_bytes(self.content[:100])
        self.journal(self.body, [[0, self.item['size']]])
        self.assertEqual(progress.saved_ranges(self.body, self.item['size'], self.item['sha256']), [[0, 100]])
        self.body.unlink()
        self.assertEqual(progress.saved_ranges(self.body, self.item['size'], self.item['sha256']), [])

    def test_old_journal_ranges_do_not_reappear_after_body_grows(self):
        block = 65536
        self.content = b'A' * (20 * block)
        self.item.update(size=len(self.content), sha256=hashlib.sha256(self.content).hexdigest())
        self.event.update(size=self.item['size'], sha256=self.item['sha256'])
        self.body.write_bytes(self.content[:6 * block])
        self.journal(self.body, [[0, 12 * block]])
        def serve(request, timeout):
            start = int(request.get_header('Range')[6:].split('-')[0])
            if start < 16 * block:
                raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
            return self.serve(request, timeout)
        with patch.object(download, 'files', return_value=[self.item]), patch.object(download.OPENER, 'open', side_effect=serve):
            self.assertFalse(recovery.finish_recovery(self.root, self.event, self.item, retries=0))
        self.assertEqual(self.event['downloaded_bytes'], 10 * block)
        self.assertFalse((self.state / 'partial-copies.json').exists())

    def test_metadata_failure_still_promotes_existing_confirmed_bytes(self):
        self.body.write_bytes(self.content)
        self.journal(self.body, [[0, 3 * self.item['size'] // 4]])
        error = urllib.error.HTTPError('https://example.invalid', 500, 'Unavailable', {}, None)
        with patch.object(download, 'files', side_effect=error):
            recovery.recover_corrupted(self.root, self.source, retry=True)
        copy = json.loads((self.state / 'partial-copies.json').read_text())[0]
        self.assertEqual(copy['downloaded_percent'], 75)
        self.assertIn('recovery_error', copy)

    def test_first_archive_metadata_failure_still_promotes(self):
        self.body.write_bytes(self.content)
        self.journal(self.body, [[0, 3 * self.item['size'] // 4]])
        error = urllib.error.HTTPError('https://example.invalid', 500, 'Unavailable', {}, None)
        with patch.object(download, 'files', side_effect=error):
            self.assertTrue(recovery.finish_recovery(self.root, self.event, self.item, retries=0))
        copy = json.loads((self.state / 'partial-copies.json').read_text())[0]
        self.assertEqual(copy['downloaded_percent'], 75)


if __name__ == '__main__':
    unittest.main()
