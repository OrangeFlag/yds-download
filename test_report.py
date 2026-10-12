"""Report marks follow the availability of registered copies without an event log."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import report


class RegistryReports(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
