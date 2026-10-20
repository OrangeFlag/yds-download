#!/usr/bin/env python3
"""Run an editable semester queue, preserving per-archive download state."""
import argparse
import fcntl
import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime
import storage
from report import retry_reports, wait_reports
from download import cli, write_json
from pipeline import transfer_process
from metrics import Metrics
from collections import Counter
from settings import arguments, resolve, add_source_arguments, source_settings, source_arguments

def queue_exit(code, path):
    if code in (130, -2, 143, -15):
        raise KeyboardInterrupt
    if code == storage.TEMPFAIL:
        raise storage.StorageUnavailable(path)
    if code not in (0, 1):
        raise RuntimeError('Queue stopped: ' + str(path) + '; exit=' + str(code))


def parallel_queues(commands):
    children = []
    try:
        for command, env in commands:
            children.append((subprocess.Popen(command, env=env, start_new_session=True), command[2]))
        pending = list(children)
        while pending:
            for child, path in pending[:]:
                code = child.poll()
                if code is not None:
                    queue_exit(code, path)
                    pending.remove((child, path))
            if pending:
                time.sleep(0.2)
    except BaseException:
        # Signal leaders once; their normal transfer cleanup signals their own children.
        for child, _ in children:
            if child.poll() is None:
                try:
                    child.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 90
        for child, _ in children:
            timed_out = False
            try:
                child.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                timed_out = True
            # A crashed leader can leave transfers alive in its isolated session.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if timed_out:
                child.wait()
        raise


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('queue', type=Path)
    parser.add_argument('--before', type=Path, action='append', default=[],
                        help='Run this personal queue first without course cloud filters (repeatable)')
    parser.add_argument('--parallel-before', action='store_true',
                        help='Run personal queues alongside the course queue, starting personal queues first')
    parser.add_argument('--no-wait-range', action='store_true',
                        help='Return after foreground downloads while deferred Range recovery continues')
    parser.add_argument('--defer-range', action='store_true', help='Continue archives while range_queue.py recovers failures')
    parser.add_argument('--range-budget', type=Path, help='Shared budget directory for overlapping foreground and worker Range')
    parser.add_argument('--range-files', type=int, default=16)
    parser.add_argument('--nextcloud-existing', help='Skip originals already available in this public Nextcloud share')
    parser.add_argument('--nextcloud-fallback', help='Verified fallback for failed course videos')
    add_source_arguments(parser)
    args = parser.parse_args()
    try:
        opencloud_fallback, download_budget = source_settings(args)
    except ValueError as error:
        parser.error(str(error))
    if args.nextcloud_existing and (args.nextcloud_fallback or opencloud_fallback):
        parser.error('Choose remote skipping or verified fallback, not both')
    if args.range_budget is not None and (not args.defer_range or args.range_files < 1):
        parser.error('--range-budget requires --defer-range and positive --range-files')
    storage.check(args.queue)
    path = args.queue.resolve()
    before = [queue.resolve() for queue in args.before]
    if path in before or len(set(before)) != len(before):
        parser.error('--before queues must be distinct and cannot include the current queue')
    if args.parallel_before and before and download_budget is None:
        parser.error('--parallel-before requires --download-budget to share transfer limits')
    commands = []
    for queue in before:
        storage.check(queue)
        command = [sys.executable, str(Path(__file__).resolve()), str(queue),
                   '--defer-range', '--no-wait-range']
        command += source_arguments(budget=download_budget)
        if args.range_budget is not None:
            command += ['--range-budget', str(args.range_budget.resolve()),
                        '--range-files', str(args.range_files)]
        commands.append((command, {**os.environ, 'YDS_DOWNLOAD_PRIORITY': 'personal'}))
    if args.parallel_before and before:
        course_args = []
        remaining = iter(sys.argv[1:])
        for argument in remaining:
            if argument == '--before':
                next(remaining)
            elif argument != '--parallel-before' and not argument.startswith('--before='):
                course_args.append(argument)
        commands.append(([sys.executable, str(Path(__file__).resolve()), *course_args],
                         {**os.environ, 'YDS_DOWNLOAD_PRIORITY': 'normal'}))
        parallel_queues(commands)
        return
    with path.with_suffix('.lock').open('a') as lock, Metrics(path.with_suffix('.metrics.jsonl'),'semester-queue',
            interval=resolve(json.loads(path.read_text()).get('tuning',{}))['metrics_interval']) as metrics:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for command, env in commands:
            code = transfer_process(command, env=env, on_tick=lambda: None, stop_timeout=80)
            queue_exit(code, command[2])
        data = storage.read_queue(path)
        resolve(data.get('tuning', {}))
        for job in data['archives']:
            resolve({**data.get('tuning', {}), **job.get('tuning', {})})
        wait = data.get('wait_for')
        if wait:
            print('WAITING FOR CURRENT ARCHIVE: ' + wait['destination'], flush=True)
            while subprocess.run(['systemctl', '--user', 'show', wait['unit'], '-p', 'ActiveState', '--value'], capture_output=True, text=True).stdout.strip() in ('active', 'activating', 'deactivating', 'reloading'):
                time.sleep(30)
            state = Path(wait['destination']) / '.download'
            courses = json.loads((state / 'courses.json').read_text())
            for index, course in enumerate(courses, 1):
                status = json.loads((state / f'{index:02d}-status.json').read_text())
                if status.get('course') != course or status.get('state') != 'finished':
                    raise RuntimeError('Current archive did not finish; queue remains paused')
        data = storage.read_queue(path)
        for job in data['archives']:
            metrics.gauge('archives',dict(Counter(a.get('state','pending') for a in data['archives'])))
            metrics.gauge('current_archive',job['title'])
            if job.get('enabled', True):
                retry_reports(Path(job['destination']))
            summary_path = Path(job['destination']) / '.download/summary.json'
            summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
            remote_archive = bool(summary.get('remote_available') or summary.get('offline_available'))
            if not job.get('enabled', True) or ((job.get('state') == 'finished' or (args.defer_range and job.get('downloads_complete'))) and not (data.get('deep', False) or data.get('retry_check', False) or remote_archive)):
                continue
            if args.defer_range and not (data.get('deep') or data.get('retry_check')):
                summary_path = Path(job['destination']) / '.download/summary.json'
                summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
                if summary.get('downloads_complete') and not remote_archive:
                    job.update(downloads_complete=True, state='recovering' if summary.get('pending_courses') else 'finished')
                    write_json(path, data)
                    continue
            previous_state = job.get('state', 'pending')
            if not (data.get('deep') and not data.get('retry_check')):
                job['state'] = 'running'
                write_json(path, data)
            print('ARCHIVE START: ' + job['title'], flush=True)
            command = ['python3', str(Path(__file__).with_name('download.py')), 'download',
                       '--catalog', job['catalog'], '--destination', job['destination'],
                       '--retries', str(data.get('retries', 2))]
            command += arguments({**data.get('tuning', {}), **job.get('tuning', {})})
            command += source_arguments(opencloud_fallback, download_budget)
            if args.nextcloud_existing:
                command += ['--nextcloud-existing', args.nextcloud_existing]
            if args.nextcloud_fallback:
                command += ['--nextcloud-fallback',args.nextcloud_fallback]
            if args.defer_range and not data.get('deep'):
                command += ['--defer-range']
                if args.range_budget is not None:
                    command += ['--range-budget', str(args.range_budget.resolve()), '--range-files', str(args.range_files)]
            if data.get('retry_check', False):
                command += ['--retry-check']
            if data.get('deep', False):
                command += ['--deep']
            if data.get('report_repo'):
                command += ['--report-repo', data['report_repo'], '--mark', data['mark']]
            if data.get('git_push'):
                command += ['--git-push']
            code = transfer_process(command, env=os.environ.copy(), on_tick=lambda: metrics.gauge('archives',dict(Counter(a.get('state','pending') for a in data['archives']))), stop_timeout=80)
            if code in (130, -2, 143, -15):
                raise KeyboardInterrupt
            if code == storage.TEMPFAIL:
                raise storage.StorageUnavailable(args.queue)
            job.update(exit_code=code, time=datetime.now().astimezone().isoformat())
            if code not in (0, 1):
                job['state'] = 'blocked'
                write_json(path, data)
                raise RuntimeError('Download stopped: ' + job['title'] + '; exit=' + str(code))
            job['state'] = previous_state if data.get('deep') and not data.get('retry_check') else 'finished'
            if args.defer_range and not data.get('deep'):
                summary = json.loads((Path(job['destination']) / '.download/summary.json').read_text())
                job['downloads_complete'] = summary.get('downloads_complete', False)
                if summary.get('pending_courses'):
                    job['state'] = 'recovering'
            write_json(path, data)
            print('ARCHIVE FINISHED: ' + job['title'] + '; exit=' + str(code), flush=True)
        if args.defer_range and not args.no_wait_range:
            while any(job.get('state') == 'recovering' for job in data['archives']):
                metrics.gauge('archives',dict(Counter(a.get('state','pending') for a in data['archives'])))
                changed = False
                for job in data['archives']:
                    if job.get('state') != 'recovering':
                        continue
                    summary = json.loads((Path(job['destination']) / '.download/summary.json').read_text())
                    if summary.get('downloads_complete') and not summary.get('pending_courses'):
                        job.update(state='finished', exit_code=1 if summary.get('failed') else 0)
                        changed = True
                if changed:
                    write_json(path, data)
                time.sleep(10)
        wait_reports([Path(job['destination']) for job in data['archives'] if job.get('enabled', True)])
        print('ALL ARCHIVES PROCESSED', flush=True)

if __name__ == '__main__':
    raise SystemExit(cli(main))
