#!/usr/bin/env python3
"""Recover durable Range tasks across archives without blocking ordinary downloads."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import math
from threading import Event, Lock, Semaphore
import time

import download as d
from pipeline import Background, failure_manifests, now
import recovery as r
from settings import resolve
from metrics import Metrics


class RangeBudget:
    def __init__(self, requests=8, rps=12):
        if requests < 1 or not math.isfinite(rps) or rps <= 0:
            raise ValueError('Range request limits must be positive')
        self.slots = Semaphore(requests)
        self.rate = Lock()
        self.interval = 1 / rps
        self.next_request = 0.0

    @contextmanager
    def request(self, stopping):
        while not self.slots.acquire(timeout=.25):
            if stopping.is_set():
                raise InterruptedError('Range interrupted')
        try:
            with self.rate:
                if stopping.wait(max(0, self.next_request - time.monotonic())):
                    raise InterruptedError('Range interrupted')
                self.next_request = time.monotonic() + self.interval
            yield
        finally:
            self.slots.release()


class JsonCache:
    def __init__(self):
        self.entries = {}
        self.directories = {}

    def read(self, path, default=None):
        if not path.exists():
            self.entries.pop(path, None)
            return default
        stamp = Background.stat(path)
        saved = self.entries.get(path)
        if saved is None or saved[0] != stamp:
            saved = stamp, json.loads(path.read_text())
            self.entries[path] = saved
        return saved[1]

    def tasks(self, directory):
        if not directory.exists():
            return []
        stamp = Background.stat(directory)
        saved = self.directories.get(directory)
        if saved is None or saved[0] != stamp:
            saved = stamp, [(path, self.read(path)) for path in sorted(directory.glob('*.json'))]
            self.directories[directory] = saved
        return saved[1]


def retire_cached_copy(background, path, job, proofs):
    item = job['item']
    destination = d.target(background.root, item, job['course'])
    for proof in proofs:
        if (proof.get('status') != 'valid' or proof.get('path') != item['path']
                or proof.get('source', background.source).rstrip('/') != background.source.rstrip('/')
                or (proof.get('size'), proof.get('sha256')) != (item['size'], item['sha256'])
                or proof.get('mtime_ns') is None):
            continue
        local = Path(proof['local']) if proof.get('local') else destination
        try:
            stat = local.stat()
        except FileNotFoundError:
            continue
        if ((stat.st_size, stat.st_mtime_ns) != (item['size'], proof['mtime_ns'])
                or not local.is_file() or Path(str(local) + '.aria2').exists()):
            continue
        recovery_path = background.state / 'recovery-state.json'
        saved = json.loads(recovery_path.read_text()).get(item['path'], {}).get('event', {}) if recovery_path.exists() else {}
        previous = saved if (saved.get('size'), saved.get('sha256')) == (item['size'], item['sha256']) else job.get('event', {})
        archived = previous.get('corrupted_path')
        if archived:
            r.body_path(background.root, dict(corrupted_path=archived))
        event = d.record(background.state, item, job['course'], job['attempt'], 'valid',
                         'Cached SHA-256 verified local copy; obsolete Range task retired', local=local)
        event = dict(previous, **event, range_recovered_complete=True)
        if archived:
            event['corrupted_path'] = archived
            failure_manifests(background.root, event)
        r.recovery_state(background.root, event, 'finished')
        background.checkpoint(item, job['course'], job['attempt'], 'finished', event)
        job.update(state='applied', result={'event': event}, time=now(), superseded_by_local_copy=True)
        d.write_json(path, job)
        return True
    return False


def finish_courses(background, config, changed_courses=()):
    root, state = background.root, background.state
    courses = json.loads((state / 'courses.json').read_text())
    history_path = state / 'course-history.json'
    history = json.loads(history_path.read_text())
    events = {row['path']: row for row in d.read_events(state / 'events.jsonl')}
    registry = state / 'local-copies.json'
    external = {row['path']: row for row in (json.loads(registry.read_text()) if registry.exists() else [])
                if row.get('source', '').rstrip('/') == background.source.rstrip('/') and row.get('status') == 'valid'}
    jobs = [json.loads(path.read_text()) for path in background.directory.glob('*.json')]
    for index, course in enumerate(courses, 1):
        pending = any(job['kind'] == 'range' and job['course'] == course and job['state'] != 'applied' for job in jobs)
        if history.get(course, {}).get('state') == 'finished' and not pending and course not in changed_courses:
            continue
        entries = json.loads((state / f'{index:02d}-manifest.json').read_text())
        valid = 0
        for item in entries:
            for proof in (events.get(item['path'], {}), external.get(item['path'], {})):
                # Manifests deliberately omit expiring URLs; only the destination is needed here.
                local = Path(proof['local']) if proof.get('local') else d.target(root, dict(item, file='https://disk.yandex.ru'), course)
                if (proof.get('status') == 'valid' and (proof.get('size'), proof.get('sha256')) == (item['size'], item['sha256'])
                        and local.exists() and local.stat().st_size == item['size']
                        and ('mtime_ns' not in proof or local.stat().st_mtime_ns == proof['mtime_ns'])
                        and not Path(str(local) + '.aria2').exists()):
                    valid += 1
                    break
        status = dict(course=course, source=background.source, state='recovering' if pending else 'finished',
                      complete=not pending and valid == len(entries), total=len(entries), verified=valid,
                      failed=0 if pending else len(entries) - valid, pending=len(entries) - valid if pending else 0, time=now())
        history[course] = status
        d.write_json(state / f'{index:02d}-status.json', status)
        if not pending:
            print(('COURSE VERIFIED COMPLETE: ' if status['complete'] else 'COURSE FINISHED WITH ERRORS: ') + str(root) + ' / ' + course, flush=True)
            if config.get('report_repo'):
                try:
                    from report import update_report
                    update_report(root, Path(config['report_repo']), config['mark'], config.get('git_push', False),
                                  f"Update {config['mark']} copies: {root.name} / {course}")
                except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                    print(f'REPORT ERROR: {error}', flush=True)
    d.write_json(history_path, history)
    pending = [course for course in courses if history[course]['state'] != 'finished']
    failed = sum(history[course]['failed'] for course in courses)
    d.write_json(state / 'summary.json', dict(verified=sum(history[course]['verified'] for course in courses),
                 failed=failed, pending_courses=pending, downloads_complete=True, complete=not failed and not pending))


def run(queues, files=4, per_file=2, requests=8, rps=12, stopping=None, secondary_slots=1, budget_directory=None, metrics_interval=60):
    if files < 1 or per_file < 1:
        raise ValueError('Range file limits must be positive')
    if not 1 <= secondary_slots <= max(1, files - 1):
        raise ValueError('Secondary slots must leave a slot for the first queue')
    stopping = stopping if stopping is not None else Event()
    if budget_directory is None:
        budget = RangeBudget(requests, rps)
    else:
        from request_budget import ProcessRangeBudget
        budget = ProcessRangeBudget(budget_directory, requests, rps)
    cache = JsonCache()
    roots = {}
    priorities = {}
    with Metrics(queues[0].with_suffix('.range.metrics.jsonl'),'range-queue',interval=metrics_interval,
                 settings=dict(files=files,per_file=per_file,requests=requests,rps=rps,secondary_slots=secondary_slots)) as metrics, \
            ThreadPoolExecutor(max_workers=files, thread_name_prefix='range-file') as pool:
        try:
            while not stopping.is_set():
                # Poll applies every result under the archive's exclusive download lock.
                for root, (background, lock, config) in list(roots.items()):
                    before = len(background.futures)
                    changed_courses = {job['course'] for job in background.jobs.values()}
                    background.poll()
                    if len(background.futures) != before:
                        finish_courses(background, config, changed_courses)
                eligible = []
                for priority, queue in enumerate(queues):
                    config = cache.read(queue)
                    for archive in config['archives']:
                        if not archive.get('enabled', True):
                            continue
                        root = Path(archive['destination']).resolve()
                        state = root / '.download'
                        summary = cache.read(state / 'summary.json', {})
                        if not summary.get('downloads_complete') and archive.get('state') != 'finished':
                            continue
                        tasks = cache.tasks(state / 'tasks')
                        if not summary.get('pending_courses') and not any(job['kind'] == 'range' and job['state'] in ('pending', 'running', 'done') for _, job in tasks):
                            continue
                        if root not in roots:
                            lock = (state / 'download.lock').open('a')
                            try:
                                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            except BlockingIOError:
                                lock.close()
                                continue
                            try:
                                identity = cache.read(state / 'source.json')
                                catalog = cache.read(Path(archive['catalog']))
                                if identity['source'] != catalog['source']:
                                    raise ValueError('Range archive source does not match catalog')
                                settings = cache.read(state / 'settings.json')
                                tuning = resolve({**settings['tuning'], 'range_workers': per_file, 'range_rps': rps})
                                background = Background(root, identity['source'], settings['retries'], tuning,
                                                        range_pool=pool, budget=budget, range_files=files)
                            except BaseException:
                                lock.close()
                                raise
                            roots[root] = background, lock, config
                            priorities[root] = priority
                            background.cached_events = {row['path']: row for row in d.read_events(state / 'events.jsonl', repair=True)}
                            background.manifest_items = {(course, item['path']): (item['size'], item['sha256'])
                                for index, course in enumerate(cache.read(state / 'courses.json'), 1)
                                for item in cache.read(state / f'{index:02d}-manifest.json')}
                        background = roots[root][0]
                        retired = set()
                        external = cache.read(state / 'local-copies.json', [])
                        for path, job in tasks:
                            if path in background.futures:
                                continue
                            if job['kind'] == 'range' and job['state'] in ('pending', 'running', 'done'):
                                if job['source'] != background.source:
                                    raise ValueError('Range task source mismatch')
                                d.target(root, job['item'], job['course'])
                                item = job['item']
                                if background.manifest_items.get((job['course'], item['path'])) != (item['size'], item['sha256']):
                                    job.update(state='applied', time=now(), superseded_by_manifest=True)
                                    d.write_json(path, job)
                                    continue
                                proofs = [background.cached_events.get(job['item']['path'], {})]
                                proofs.extend(row for row in external if row.get('source', '').rstrip('/') == background.source.rstrip('/') and row.get('path') == job['item']['path'])
                                if retire_cached_copy(background, path, job, proofs):
                                    retired.add(job['course'])
                                    continue
                                eligible.append((priority, background, job))
                        if retired:
                            finish_courses(background, config, retired)
                active = sum(len(background.futures) for background, _, _ in roots.values())
                while eligible and active < files:
                    # Reserve slots for courses while retaining priority for personal files.
                    chosen = 0
                    if len(queues) > 1 and any(row[0] > 0 for row in eligible):
                        first_active = sum(len(bg.futures) for bg, _, config in roots.values()
                                           if priorities[bg.root] == 0)
                        if first_active >= max(1, files - secondary_slots):
                            chosen = next(i for i, row in enumerate(eligible) if row[0] > 0)
                    _, background, job = eligible.pop(chosen)
                    print('RANGE START: ' + str(background.root) + job['item']['path'], flush=True)
                    background.submit('range', job['item'], job['course'], job['attempt'], job['event'])
                    active += 1
                for root, (background, lock, config) in list(roots.items()):
                    if not background.futures:
                        if any(row[1] is background for row in eligible):
                            continue
                        if cache.read(background.state / 'summary.json', {}).get('pending_courses'):
                            finish_courses(background, config)
                        background.close()
                        lock.close()
                        del roots[root]
                metrics.gauge('range_active_files',sum(len(bg.futures) for bg,_,_ in roots.values()))
                metrics.gauge('range_eligible_files',len(eligible))
                metrics.gauge('archives_owned',len(roots))
                stopping.wait(2)
        finally:
            for background, _, _ in roots.values():
                background.stopping.set()
            for background, lock, _ in roots.values():
                background.close()
            pool.shutdown(wait=True, cancel_futures=True)
            for background, lock, _ in roots.values():
                background.metrics.close()
                lock.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue', action='append', required=True, type=Path, help='Repeat in priority order')
    parser.add_argument('--files', type=int, default=4)
    parser.add_argument('--secondary-slots', type=int, default=1, help='File slots reserved for later queues while the first queue has work')
    parser.add_argument('--requests-per-file', type=int, default=2)
    parser.add_argument('--requests', type=int, default=8)
    parser.add_argument('--rps', type=float, default=12)
    parser.add_argument('--metrics-interval',type=int,default=60,help='Service metrics interval, seconds')
    parser.add_argument('--budget', type=Path, help='Share the HTTP budget with overlapping ordinary downloaders')
    args = parser.parse_args()
    stopping = Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, lambda *_: stopping.set())
    # Prevent two workers from multiplying the global limits.
    with args.queue[0].with_suffix('.range.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args.queue, args.files, args.requests_per_file, args.requests, args.rps, stopping, args.secondary_slots, args.budget, args.metrics_interval)


if __name__ == '__main__':
    main()
