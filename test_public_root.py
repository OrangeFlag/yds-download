"""Published personal folders and files use the ordinary verified pipeline."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
import recovery


class PublicRootChecks(unittest.TestCase):
    def test_root_folder_and_nested_files(self):
        direct = dict(type='file', name='photo.jpg', path='/photo.jpg')
        nested = dict(type='file', name='clip.mkv', path='/nested/clip.mkv')
        def children(source, path):
            return iter([direct, dict(type='dir', path='/nested')] if path == '/' else [nested])
        with patch.object(download, 'api', return_value=dict(name='Album', type='dir')), \
                patch.object(download, 'children', side_effect=children):
            data = download.catalog('source', True)
            self.assertEqual(download.select_courses(data, None, []), ['.'])
            self.assertEqual(download.files('source', '/.'), [direct, nested])

    def test_published_file_verified_in_destination_root(self):
        content = b'personal file'
        item = dict(name='photo.jpg', type='file', path='/', size=len(content),
                    sha256=hashlib.sha256(content).hexdigest(), file='https://example.invalid/photo')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def transfer(*args, **kwargs):
                (root / 'photo.jpg').write_bytes(content)
                return 0
            with patch.object(download, 'api', return_value=item), \
                    patch.object(download.subprocess, 'call', side_effect=transfer):
                data = download.catalog('https://disk.yandex.ru/d/test', True)
                self.assertEqual(download.download(data, ['.'], root, retries=0), 0)
            status = json.loads((root / '.download/course-history.json').read_text())['.']
            self.assertTrue(status['complete'])
            self.assertEqual(status['verified'], 1)
            self.assertEqual((root / 'photo.jpg').read_bytes(), content)

    def test_root_partial_and_reserved_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            state.mkdir()
            download.write_json(state / 'source.json', dict(source='source'))
            (root / 'corrupted').mkdir()
            body = root / 'corrupted/damaged'
            body.write_bytes(b'abcdef')
            event = dict(path='/photo.jpg', size=10, sha256='a' * 64, corrupted_path='corrupted/damaged')
            with patch.object(recovery, 'progress', return_value=dict(downloaded_bytes=6, downloaded_percent=60)):
                self.assertTrue(recovery.promote_partial(root, event))
                self.assertTrue((root / 'photo (partially available).jpg').exists())
                with self.assertRaises(ValueError):
                    recovery.promote_partial(root, dict(event, path='/.download/private'))


if __name__ == '__main__':
    unittest.main()
