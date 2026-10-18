"""Report marks follow the availability of registered copies without an event log."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
import report


class RegistryReports(unittest.TestCase):
    def test_fresh_deep_proof_overrides_old_manifest_until_new_source_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            state.mkdir()
            source = 'https://disk.yandex.ru/d/test'
            data = dict(source=source, title='test')
            item = dict(path='/Course/video.mp4', size=4,
                        sha256=hashlib.sha256(b'NEW!').hexdigest(), file='https://example.test/file')
            download.write_json(state / 'source.json', data)
            download.write_json(state / 'courses.json', ['Course'])
            manifest = state / '01-manifest.json'
            old = dict(item, sha256=hashlib.sha256(b'OLD!').hexdigest())
            download.write_json(manifest, [old])
            manifest_before = manifest.read_bytes()
            local = root / 'Course/video.mp4'
            local.parent.mkdir()
            local.write_bytes(b'NEW!')
            repo = root / 'repo'
            (repo / 'courses').mkdir(parents=True)
            card = repo / 'courses/course.md'
            card.write_text('- Год: 2026\n- Источник: [disk](' + source + ')\n- Путь: `Course`\n'
                            '**Статус:** копия есть\n'
                            '| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n'
                            '| --- | --- | --- | --- | --- |\n'
                            '| `video.mp4` | 4 | Walterit | Other | |\n')
            with patch.object(download, 'list_course', return_value=[item]):
                self.assertEqual(download.download(data, ['Course'], root, retries=0, deep=True,
                                                   report_repo=repo, mark='Walterit'), 0)
            self.assertIn('Walterit', card.read_text())
            self.assertEqual(manifest.read_bytes(), manifest_before)
            download.record(state, old, 'Course', 1, 'invalid', 'Old pinned Range result')
            report.update_report(root, repo, 'Walterit')
            self.assertIn('Walterit', card.read_text())
            download.write_json(manifest, [old])
            report.update_report(root, repo, 'Walterit')
            self.assertNotIn('Walterit', card.read_text())
            self.assertIn('Other', card.read_text())

    def test_report_streams_events_instead_of_materializing_the_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            state.mkdir()
            source = 'https://disk.yandex.ru/d/test'
            download.write_json(state/'source.json', dict(source=source))
            repo = root/'report'
            (repo/'courses').mkdir(parents=True)
            card = repo/'courses/course.md'
            card.write_text('- Год: 2026\n- Источник: [disk](' + source + ')\n'
                            '- Путь: `Course`\n**Статус:** копия есть\n'
                            '| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n'
                            '| --- | --- | --- | --- | --- |\n'
                            '| `video.mp4` | 4 | Walterit | | |\n')
            local = root/'Course/video.mp4'
            local.parent.mkdir()
            local.write_bytes(b'data')
            entry = dict(path='/Course/video.mp4', size=4, sha256=hashlib.sha256(b'data').hexdigest(),
                         local=str(local), status='valid', mtime_ns=local.stat().st_mtime_ns,
                         source=source, time='2026-10-14T07:00:00+03:00', attempt=1, detail='verified')
            download.append_events(state/'events.jsonl', [entry])
            with patch.object(download, 'read_events', side_effect=AssertionError('Bulk history loaded')), \
                    patch.object(download, 'scan_events', wraps=download.scan_events) as scanned:
                report.update_report(root, repo, 'Walterit')
                self.assertIn('Walterit', card.read_text())
                offset = (state/'events.jsonl').stat().st_size
                boundary = len(scanned.call_args_list)
                download.append_events(state/'events.jsonl', [dict(entry, status='missing')])
                report.update_report(root, repo, 'Walterit')
                self.assertNotIn('Walterit', card.read_text())
                self.assertTrue(all(call.kwargs.get('start', 0) >= offset
                                    for call in scanned.call_args_list[boundary:]),
                                'Repeated report reread the old journal history')

    def test_deleted_registered_copy_removes_only_its_owner_mark(self):
        for partial in (False, True):
            with self.subTest(partial=partial), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                state = root / '.download'
                state.mkdir()
                repo = root / 'repo'
                (repo / 'courses').mkdir(parents=True)
                source = 'https://disk.yandex.ru/d/test'
                (state / 'source.json').write_text(json.dumps(dict(source=source)))
                local = root / 'registered.mp4'
                local.write_bytes(b'confirmed bytes')
                copy = dict(source=source, path='/Course/video.mp4', size=local.stat().st_size,
                            sha256=hashlib.sha256(local.read_bytes()).hexdigest(),
                            mtime_ns=local.stat().st_mtime_ns, time='2026-10-13',
                            status='partial' if partial else 'valid', attempt=0, detail='checked')
                if partial:
                    copy.update(recovered_path=local.name, local_size=copy['size'],
                                downloaded_bytes=10, downloaded_percent=66.67)
                else:
                    copy['local'] = str(local)
                (state / ('partial-copies.json' if partial else 'local-copies.json')).write_text(json.dumps([copy]))
                card = repo / 'courses/course.md'
                card.write_text('- Год: 2026\n- Источник: [disk](' + source + ')\n- Путь: `Course`\n'
                                '**Статус:** не все видео есть, какие-то не удается скачать\n'
                                '| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n'
                                '| --- | --- | --- | --- | --- |\n'
                                '| `video.mp4` | 1 | Other (partially available; байтов 60%) | | |\n')
                report.update_report(root, repo, 'Walterit')
                self.assertIn('Walterit', card.read_text())
                local.unlink()
                report.update_report(root, repo, 'Walterit')
                self.assertNotIn('Walterit', card.read_text())
                self.assertIn('Other (partially available; байтов 60%)', card.read_text())
                self.assertIn('**Статус:** не все видео есть, какие-то не удается скачать', card.read_text())


class ReportEventCacheChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'events.jsonl'
        self.row = dict(path='/Course/video.mp4', size=4, sha256='a'*64, status='valid',
                        time='2026-10-18', attempt=1, detail='verified', source_current=True)

    def test_append_keeps_latest_proof_and_source_observation_without_payload_history(self):
        download.append_events(self.path, [dict(self.row, range_attempts=['large diagnostic'])])
        index = report._event_index(self.path)
        before, observed = index.refresh()
        self.assertNotIn('range_attempts', before[self.row['path']])
        offset = self.path.stat().st_size
        download.append_events(self.path, [dict(self.row, status='invalid', source_current=False)])
        with patch.object(download, 'scan_events', wraps=download.scan_events) as scanned:
            after, observed = report._event_index(self.path).refresh()
        self.assertEqual(before[self.row['path']]['status'], 'valid')
        self.assertEqual(after[self.row['path']]['status'], 'invalid')
        self.assertEqual(observed[self.row['path']]['status'], 'valid')
        self.assertTrue(all(call.kwargs.get('start', 0) >= offset for call in scanned.call_args_list))

    def test_rotation_truncation_and_removal_discard_cached_proofs(self):
        download.append_events(self.path, [self.row])
        index = report._event_index(self.path)
        index.refresh()
        replacement = self.path.with_suffix('.new')
        replacement.write_text(json.dumps(dict(self.row, path='/Course/new.mp4'))+'\n')
        replacement.replace(self.path)
        events, _ = index.refresh()
        self.assertNotIn(self.row['path'], events)
        self.path.write_text('')
        self.assertEqual(index.refresh(), ({}, {}))
        download.append_events(self.path, [self.row])
        index.refresh()
        self.path.unlink()
        self.assertEqual(index.refresh(), ({}, {}))

    def test_valid_unterminated_row_is_visible_but_incomplete_row_never_is(self):
        self.path.write_text(json.dumps(self.row))
        index = report._event_index(self.path)
        self.assertEqual(index.refresh()[0][self.row['path']]['status'], 'valid')
        next_row = dict(self.row, status='missing', source_current=False)
        with self.path.open('a') as stream:
            stream.write('\n'+json.dumps(next_row)[:-1])
        self.assertEqual(index.refresh()[0][self.row['path']]['status'], 'valid')
        with self.path.open('a') as stream:
            stream.write('}\n')
        events, observed = index.refresh()
        self.assertEqual(events[self.row['path']]['status'], 'missing')
        self.assertEqual(observed[self.row['path']]['status'], 'valid')

    def test_same_inode_rewrite_does_not_reuse_the_previous_proof(self):
        download.append_events(self.path, [self.row])
        index = report._event_index(self.path)
        index.refresh()
        updated = dict(self.row, path='/Course/other.mp4')
        self.path.write_text(json.dumps(updated)+'\n')
        events, observed = index.refresh()
        self.assertEqual(set(events), {updated['path']})
        self.assertEqual(set(observed), {updated['path']})


if __name__ == '__main__':
    unittest.main()
