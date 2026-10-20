"""Completed files enter durable SHA work while sibling downloads remain active."""
from pathlib import Path
from threading import Event
import unittest
from unittest.mock import patch

import download as d
import pipeline
import test_pipeline


class StreamingSHAChecks(unittest.TestCase):
    setUp = test_pipeline.PipelineChecks.setUp

    def background(self):
        (self.root / '.download').mkdir()
        bg = pipeline.Background(self.root, self.data['source'], 2, pipeline.resolve())
        self.addCleanup(bg.close)
        first = self.items['First']
        second = dict(first, path='/First/second.mp4')
        return bg, first, second

    def exercise(self, interrupted=False, retry=False, control=False):
        bg, first, second = self.background()
        started = Event()
        checks = []
        original = d.inspect_available_copies
        def inspect(root, item, *args):
            checks.append(item['path'])
            if item == first:
                self.assertEqual(bg.continuation(first)['phase'], 'verifying')
                started.set()
            return original(root, item, *args)
        calls = []
        def transfer(command, **kwargs):
            calls.append(command)
            queue = (bg.state / 'queue.txt').read_text()
            if len(calls) == 2:
                self.assertNotIn('out=video.mp4', queue)
                self.assertIn('out=second.mp4', queue)
                return 0
            local = d.target(self.root, first, 'First')
            local.parent.mkdir(exist_ok=True)
            local.write_bytes(self.content)
            partial = d.target(self.root, second, 'First')
            partial.write_bytes(b'lec')
            Path(str(partial) + '.aria2').write_bytes(b'resume')
            if control:
                Path(str(local) + '.aria2').write_bytes(b'active')
            with (bg.state / 'aria2.log').open('a') as log:
                log.write('2026-10-20 10:00:00 [NOTICE] Download complete: ' + str(local) + '\n')
                if control:
                    log.write('2026-10-20 10:00:00 [NOTICE] Download complete: ' + str(partial) + '\n')
            kwargs['on_tick']()
            if control:
                self.assertFalse(started.is_set())
                Path(str(local) + '.aria2').unlink()
                kwargs['on_tick']()
            self.assertTrue(started.wait(2), 'SHA waited for the unfinished sibling')
            for future in list(bg.futures.values()):
                future.result(timeout=2)
            kwargs['on_tick']()
            self.assertEqual(bg.continuation(first)['phase'], 'finished')
            self.assertEqual(bg.continuation(second)['phase'], 'downloading')
            if interrupted:
                return 130
            partial.write_bytes(self.content)
            Path(str(partial) + '.aria2').unlink()
            if retry:
                with (bg.state / 'aria2.log').open('a') as log:
                    log.write('2026-10-20 10:00:00 [ERROR] Download aborted. URI=https://example.invalid/video\nException: Network is unreachable\n')
                return 1
            return 0
        with patch.object(pipeline, '_transfer_process_once', side_effect=transfer), \
                patch.object(d, 'inspect_available_copies', side_effect=inspect), \
                patch('network.RequestRetry.retry', side_effect=[True, False] if retry else [False]):
            if interrupted:
                with self.assertRaises(InterruptedError):
                    bg.transfer([first, second], 'First')
            else:
                bg.transfer([first, second], 'First')
                for future in list(bg.futures.values()):
                    future.result(timeout=2)
                bg.poll()
        self.assertEqual(checks.count(first['path']), 1)
        if interrupted:
            self.assertEqual(bg.continuation(second)['attempt'], 1)
            self.assertEqual(d.target(self.root, second, 'First').read_bytes(), b'lec')
            self.assertTrue(Path(str(d.target(self.root, second, 'First')) + '.aria2').exists())
            self.assertEqual(bg.continuation(first)['phase'], 'finished')
        else:
            self.assertEqual(checks.count(second['path']), 1)
        if retry:
            self.assertEqual(len(calls), 2)

    def test_sha_starts_before_sibling_finishes_without_duplicate_final_submission(self):
        self.exercise()

    def test_interrupt_preserves_completed_proof_and_unfinished_resume(self):
        self.exercise(interrupted=True)

    def test_runner_retry_excludes_already_verified_files(self):
        self.exercise(retry=True)

    def test_completion_with_active_control_is_not_hashed(self):
        self.exercise(control=True)


class AdaptiveCompletionChecks(unittest.TestCase):
    def test_finished_future_ticks_while_another_file_is_blocked(self):
        import os
        import tempfile
        import adaptive_downloads as ad
        from nextcloud import SharedUploadBudget
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queue = root / 'queue.txt'
            queue.write_text(''.join(f'http://example.invalid/{name}\n dir={root}\n out={name}\n'
                                     for name in ('first', 'second')))
            with SharedUploadBudget(root / 'budget', 'yandex', 2, 2, probe_zero=True) as budget:
                runner = ad.AriaBatch(budget, Event())
                initial_tick, observed = Event(), Event()
                overlapped = []
                def file(rpc, process, entry):
                    self.assertTrue(initial_tick.wait(2))
                    if entry[2].name == 'first':
                        entry[2].write_bytes(b'finished')
                        runner.completed[str(entry[2])] = runner.stamp(entry[2])
                    else:
                        overlapped.append(observed.wait(2))
                    return 0
                def tick():
                    if runner.completed:
                        observed.set()
                    initial_tick.set()
                with patch.object(runner, 'file', side_effect=file):
                    code = runner.run(['aria2c', '--input-file=' + str(queue), '--console-log-level=error'],
                                      env={key: value for key, value in os.environ.items()
                                           if key.lower() not in ('all_proxy', 'http_proxy', 'https_proxy', 'ftp_proxy')},
                                      on_tick=tick)
                self.assertEqual(code, 0)
                self.assertEqual(overlapped, [True], 'Completion waited for the slow sibling')
