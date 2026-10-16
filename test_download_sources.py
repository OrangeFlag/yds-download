"""Cloud source configuration and offline CLI forwarding."""
import argparse
import hashlib
import json
from pathlib import Path
import tempfile
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import download
import semester_queue
import range_queue
import settings


class DownloadSourceChecks(unittest.TestCase):
    def parse(self, argv):
        parser = argparse.ArgumentParser()
        settings.add_source_arguments(parser)
        return settings.source_settings(parser.parse_args(argv))

    def test_defaults_and_shared_budget_limits(self):
        self.assertEqual(self.parse([]), (None, None))
        cloud, budget = self.parse(['--download-budget', './budget'])
        self.assertIsNone(cloud)
        self.assertEqual(budget, dict(directory=str(Path('budget').resolve()), minimum=2, maximum=8))
        for argv in (['--download-budget', 'b', '--download-min-files', '0'],
                     ['--download-budget', 'b', '--download-min-files', '9'],
                     ['--download-max-files', '0']):
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                self.parse(argv)

    def test_opencloud_requires_complete_https_origin_without_reading_password(self):
        flags = ['--opencloud-username', 'student', '--opencloud-password-file', './secret']
        with patch.object(Path, 'read_text', side_effect=AssertionError('Credential read')):
            cloud, budget = self.parse(['--opencloud-fallback', 'https://cloud.example/', *flags])
        self.assertIsNone(budget)
        self.assertEqual(cloud, dict(server='https://cloud.example', username='student',
                                    password_file=str(Path('secret').resolve())))
        for server in ('http://cloud.example', 'https://user:pass@cloud.example',
                       'https://cloud.example/path', 'https://cloud.example/?token=secret',
                       'https://cloud.example/#fragment', 'https://cloud.example:bad',
                       'https://cloud.example\n'):
            with self.subTest(server=server), self.assertRaises(ValueError):
                self.parse(['--opencloud-fallback', server, *flags])
        for argv in (flags, ['--opencloud-fallback', 'https://cloud.example']):
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                self.parse(argv)

    def test_download_rejects_personal_fallback_before_loading_pipeline(self):
        cloud = dict(server='https://cloud.example', username='student', password_file='/secret')
        with self.assertRaisesRegex(ValueError, 'personal/root'):
            download.download(dict(root=True), ['.'], Path('/unused'), opencloud_fallback=cloud)

    def test_download_cli_validates_sources_before_yandex_listing(self):
        with patch('sys.argv', ['download.py', 'download', '--source', 'https://disk.yandex.ru/d/example',
                               '--destination', '/unused', '--opencloud-fallback', 'http://cloud.example']), \
                patch.object(download, 'catalog', side_effect=AssertionError('Network listing')):
            with self.assertRaises(SystemExit) as error:
                download.main()
        self.assertEqual(error.exception.code, 2)

    def test_semester_queue_forwards_both_clouds_and_shared_limits(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            queue = root / 'queue.json'
            queue.write_text(json.dumps(dict(archives=[dict(title='course', catalog='catalog.json',
                                                            destination=str(root))])))
            argv = ['semester_queue.py', str(queue), '--nextcloud-fallback', 'https://nc.example/s/share',
                    '--opencloud-fallback', 'https://oc.example', '--opencloud-username', 'student',
                    '--opencloud-password-file', str(root/'secret'), '--download-budget', str(root/'budget'),
                    '--download-min-files', '3', '--download-max-files', '11']
            with patch('sys.argv', argv), patch.object(semester_queue, 'transfer_process', return_value=0) as transfer, \
                    patch.object(semester_queue, 'wait_reports'), patch.object(semester_queue, 'retry_reports'):
                semester_queue.main()
            command = transfer.call_args.args[0]
            for flag in ('--nextcloud-fallback', '--opencloud-fallback', '--opencloud-username',
                         '--opencloud-password-file', '--download-budget', '--download-min-files', '--download-max-files'):
                self.assertEqual(command[command.index(flag)+1], argv[argv.index(flag)+1])

    def test_range_uses_cli_budget_override_and_caches_cloud_config_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state = root / '.download'
            state.mkdir()
            cloud = dict(server='https://oc.example', username='student', password_file=str(root/'secret'))
            old_budget = dict(directory=str(root/'old'), minimum=2, maximum=4)
            new_budget = dict(directory=str(root/'shared'), minimum=3, maximum=9)
            download.write_json(state/'source.json', dict(source='https://disk.yandex.ru/d/test'))
            download.write_json(state/'catalog.json', dict(root=False))
            download.write_json(state/'summary.json', dict(downloads_complete=True))
            download.write_json(state/'settings.json', dict(opencloud_fallback=cloud, download_budget=old_budget))
            queue = root/'queue.json'
            download.write_json(queue, dict(archives=[dict(destination=str(root), state='finished')]))
            stopping = Event()
            calls = []
            def wait(_):
                calls.append(True)
                if len(calls) == 2:
                    stopping.set()
                return stopping.is_set()
            budgets = dict(nextcloud=object(), opencloud=object(), yandex=object())
            engine = SimpleNamespace(get_budgets=Mock(return_value=budgets))
            with patch.dict('sys.modules', adaptive_downloads=engine), \
                    patch('cloud_fallback.CloudFallback') as fallback, patch.object(stopping, 'wait', side_effect=wait), \
                    patch.object(range_queue, 'retry_reports'):
                fallback.return_value.link = '["sanitized-provider-identity"]'
                range_queue.run([queue], stopping=stopping, download_budget=new_budget)
            fallback.assert_called_once_with(root, None, stopping, opencloud=cloud, budget=budgets)
            self.assertTrue(engine.get_budgets.called)
            self.assertTrue(all(call.args == (new_budget,) for call in engine.get_budgets.call_args_list))

    def test_deep_retry_download_uses_measured_yandex_runner(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            content = b'verified course original'
            item = dict(path='/Course/video.mp4', size=len(content), sha256=hashlib.sha256(content).hexdigest(),
                        file='https://example.invalid/video')
            destination = root/'Course/video.mp4'
            config = dict(directory=str(root/'budget'), minimum=2, maximum=8)
            budget = object()
            def complete(*args, **kwargs):
                destination.write_bytes(content)
                return 0
            def transfer(command, **kwargs):
                return kwargs['runner'](command, env=kwargs['env'], on_tick=kwargs['on_tick'])
            with patch.object(download.shutil, 'which', return_value='/bin/true'), \
                    patch.object(download, 'list_course', return_value=[item]), \
                    patch('recovery.recover_corrupted', return_value=[]), \
                    patch('adaptive_downloads.get_budgets', return_value={'yandex': budget}), \
                    patch('adaptive_downloads.AriaBatch') as batch, \
                    patch('pipeline.transfer_process', side_effect=transfer) as process:
                batch.return_value.run.side_effect = complete
                code = download.download(dict(source='https://disk.yandex.ru/d/test', title='test'),
                                         ['Course'], root, deep=True, retry_check=True, retries=0,
                                         download_budget=config)
            self.assertEqual(code, 0)
            batch.assert_called_once_with(budget, progress={str(destination): 0},rpc_timeout=30.0,rpc_status_retries=2)
            self.assertEqual(process.call_args.kwargs['runner'], batch.return_value.run)
            saved = json.loads((root/'.download/settings.json').read_text())
            self.assertEqual(saved['download_budget'], config)


if __name__ == '__main__':
    unittest.main()
