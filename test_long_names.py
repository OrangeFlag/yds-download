"""Long UTF-8 source names must work through transfer, SHA and recovery."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
import pipeline
import recovery
from settings import resolve


class LongNameChecks(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.content = b'lecture'
        self.item = dict(path='/Course/' + 'Длинное название лекции ' * 20 + '.mp4',
                         size=len(self.content), sha256=hashlib.sha256(self.content).hexdigest(),
                         file='https://example.invalid/video')
        self.source = 'https://disk.yandex.ru/d/test'

    def test_target_fits_utf8_names_and_metadata_suffixes(self):
        local = download.target(self.root, self.item, 'Course')
        self.assertLessEqual(len(local.name.encode()), 230)
        self.assertEqual(local.suffix, '.mp4')
        other = download.target(self.root, dict(self.item, path=self.item['path'].replace('.mp4', '2.mp4')), 'Course')
        self.assertNotEqual(local, other)
        local.parent.mkdir(parents=True)
        for suffix in ('', '.aria2', '.ranges.json.tmp'):
            Path(str(local) + suffix).write_bytes(b'proof')

    def test_long_partial_name_keeps_label_and_fits_filesystem(self):
        state = self.root / '.download'
        state.mkdir()
        download.write_json(state / 'source.json', dict(source=self.source))
        event = dict(self.item, course='Course', corrupted_path='corrupted/old', status='partial')
        body = self.root / event['corrupted_path']
        body.parent.mkdir()
        body.write_bytes(self.content)
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=self.item['size'], sha256=self.item['sha256'],
                                 ranges=[[0, 5]], control_imported=True))
        self.assertTrue(recovery.promote_partial(self.root, event))
        copy = json.loads((state / 'partial-copies.json').read_text())[0]
        retained = self.root / copy['recovered_path']
        self.assertIn('(partially available)', retained.name)
        self.assertLessEqual(len(retained.name.encode()), 255)
        self.assertEqual(retained.read_bytes(), self.content)

    def test_long_name_survives_transfer_record_archive_and_recovery(self):
        data = dict(source=self.source, title='test')
        def transfer(*args, **kwargs):
            rows = (self.root / '.download/queue.txt').read_text().splitlines()
            folder = Path(next(x.split('=', 1)[1] for x in rows if x.startswith(' dir=')))
            name = next(x.split('=', 1)[1] for x in rows if x.startswith(' out='))
            (folder / name).write_bytes(self.content)
            return 0
        with patch.object(download, 'list_course', return_value=[self.item]), \
                patch.object(download.subprocess, 'call', side_effect=transfer):
            self.assertEqual(download.download(data, ['Course'], self.root, retries=0), 0)
        state = self.root / '.download'
        event = download.read_events(state / 'events.jsonl')[-1]
        self.assertEqual(event['path'], self.item['path'])
        self.assertEqual(event['status'], 'valid')
        local = download.target(self.root, self.item, 'Course')
        background = pipeline.Background(self.root, self.source, 0, resolve())
        self.addCleanup(background.pools['sha'].shutdown)
        self.addCleanup(background.pools['range'].shutdown)
        event = background.archive(self.item, 'Course', 1, event)
        body = recovery.body_path(self.root, event)
        self.assertEqual(body.read_bytes(), self.content)
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=self.item['size'], sha256=self.item['sha256'],
                                 ranges=[[0, self.item['size']]], control_imported=True))
        with patch.object(recovery, 'retry_ranges'):
            self.assertTrue(recovery.finish_recovery(self.root, event, self.item, background=True))
        self.assertEqual(local.read_bytes(), self.content)
        recovery.reconcile_course(self.root, 'Course')
        self.assertTrue(json.loads((state / 'course-history.json').read_text())['Course']['complete'])


if __name__ == '__main__':
    unittest.main()
