#!/usr/bin/env python3
"""Recover durable Range tasks across archives without blocking ordinary downloads."""
import time

PROCESS_STARTED = time.monotonic()

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import math
import hashlib
from datetime import datetime
from threading import Event, Lock, Semaphore

import download as d
import storage
from report import queue_report, retry_reports
from pipeline import Background, failure_manifests, now
from file_state import FileState
import recovery as r
from settings import resolve, add_source_arguments, source_settings, validate_source_settings
from metrics import Metrics


class RangeBudget:
    def __init__(self, requests=12, rps=12):
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


class DiscoveryProfile:
    def __init__(self, metrics):
        self.metrics = metrics
        self.first_pass = True
        self.milestones = set()
        self.active = ('idle', '')

    def log(self, prefix, row):
        try:
            print(prefix + json.dumps(row, ensure_ascii=False), flush=True)
        except OSError as error:
            self.metrics.warning(error)

    @contextmanager
    def stage(self, name, path):
        previous = self.active
        self.active = (name, str(path))
        self.metrics.gauge('range_discovery_stage', name)
        self.metrics.gauge('range_discovery_path', str(path))
        started = time.monotonic()
        counts = {}
        complete = False
        try:
            yield counts
            complete = True
        finally:
            seconds = max(0, time.monotonic() - started)
            prefix = 'range_discovery_' + name + '_'
            self.metrics.add(**{prefix + 'calls': 1, prefix + 'seconds': seconds,
                               prefix + 'failures': int(not complete),
                               **{prefix + k: v for k, v in counts.items() if k != 'journal_bytes'}})
            if 'journal_bytes' in counts:
                self.metrics.gauge(prefix + 'journal_bytes', counts['journal_bytes'])
            if self.first_pass or seconds >= 1 or not complete:
                self.log('RANGE PROFILE: ', dict(stage=name, path=str(path), seconds=seconds,
                                                complete=complete, **counts))
            self.active = previous
            self.metrics.gauge('range_discovery_stage', previous[0])
            self.metrics.gauge('range_discovery_path', previous[1])

    def events(self, index):
        records, size = index.scanned_events, index.scanned_bytes
        with self.stage('events', index.path) as counts:
            try:
                return index.refresh()
            finally:
                counts.update(records=index.scanned_events - records, bytes=index.scanned_bytes - size,
                              journal_bytes=index.journal_bytes)

    def manifests(self, cache, state):
        reads = cache.json_reads
        with self.stage('manifests', state) as counts:
            try:
                result = {(course, item['path']): (item['size'], item['sha256'])
                          for index, course in enumerate(cache.read(state / 'courses.json'), 1)
                          for item in cache.read(state / f'{index:02d}-manifest.json')}
                counts['items'] = len(result)
                return result
            finally:
                counts['json_reads'] = cache.json_reads - reads

    def milestone(self, name):
        if name in self.milestones:
            return
        self.milestones.add(name)
        seconds = max(0, time.monotonic() - PROCESS_STARTED)
        self.metrics.gauge('range_startup_' + name + '_seconds', seconds)
        if name == 'first_pass':
            self.first_pass = False
        self.log('RANGE STARTUP: ', dict(milestone=name, seconds=seconds))


class JsonCache:
    def __init__(self, profile=None):
        self.profile = profile
        self.entries = {}
        self.directories = {}
        self.scans = {}
        self.task_reads = self.json_reads = 0

    def read(self, path, default=None):
        if not path.exists():
            self.entries.pop(path, None)
            return default
        stamp = Background.stat(path)
        saved = self.entries.get(path)
        if saved is None or saved[0] != stamp:
            saved = stamp, json.loads(path.read_text())
            self.json_reads += 1
            self.entries[path] = saved
        return saved[1]

    def tasks_complete(self, directory):
        if not directory.exists():
            return True
        saved = self.directories.get(directory)
        return directory not in self.scans and saved is not None and saved[0] == Background.stat(directory)

    def tasks(self, directory, limit=None):
        if self.profile is None:
            return self._tasks(directory, limit)
        reads = self.task_reads
        with self.profile.stage('tasks', directory) as counts:
            try:
                result = self._tasks(directory, limit)
                counts['returned_tasks'] = len(result)
                return result
            finally:
                counts['json_reads'] = self.task_reads - reads

    def _paths(self, directory):
        if self.profile is None:
            return iter(sorted(directory.glob('*.json')))
        with self.profile.stage('task_enumeration', directory) as counts:
            paths = sorted(directory.glob('*.json'))
            counts['entries'] = len(paths)
            return iter(paths)

    def _tasks(self, directory, limit=None):
        if not directory.exists():
            self.directories.pop(directory, None)
            self.scans.pop(directory, None)
            return []
        stamp = Background.stat(directory)
        saved = self.directories.get(directory)
        if saved is not None and saved[0] == stamp and directory not in self.scans:
            return saved[2]
        scan = self.scans.get(directory)
        if scan is None:
            previous = dict(saved[1]) if saved else {}
            scan = stamp, self._paths(directory), {}, previous
            self.scans[directory] = scan
        _, paths, current, previous = scan
        reads = 0
        complete = False
        # shortcut: cold starts still read every task; persist summaries if bounded batches remain too costly.
        while limit is None or reads < limit:
            try:
                path = next(paths)
            except StopIteration:
                complete = True
                break
            try:
                file_stamp = Background.stat(path)
            except FileNotFoundError:
                continue
            if path in previous and previous[path][0] == file_stamp:
                summary = previous[path][1]
            else:
                job = json.loads(path.read_text())
                reads += 1
                self.task_reads += 1
                summary = None
                if job['kind'] == 'range':
                    item = job['item']
                    summary = dict(kind='range', state=job['state'], course=job['course'],
                                   source=job['source'], attempt=job['attempt'],
                                   item={key: item[key] for key in ('path', 'size', 'sha256', 'file') if key in item},
                                   superseded_by_manifest=job.get('superseded_by_manifest', False),
                                   superseded_by_nextcloud=job.get('superseded_by_nextcloud', False),
                                   superseded_by_local_copy=job.get('superseded_by_local_copy', False),
                                   yandex_recheck=job.get('yandex_recheck',False),
                                   yandex_checked_at=job.get('yandex_checked_at'),
                                   result={'event': {'range_recovered_complete':
                                       job.get('result', {}).get('event', {}).get('range_recovered_complete', False),
                                       'status':job.get('result',{}).get('event',{}).get('status')}})
            current[path] = file_stamp, summary
        latest = Background.stat(directory)
        tasks = []
        for path, (file_stamp, row) in current.items():
            if row is None:
                continue
            if latest != scan[0]:
                try:
                    if Background.stat(path) != file_stamp:
                        continue
                except FileNotFoundError:
                    continue
            tasks.append((path, row))
        if complete:
            if latest != scan[0]:
                previous.update(current)
                self.scans[directory] = latest, self._paths(directory), {}, previous
            else:
                self.directories[directory] = latest, current, tasks
                del self.scans[directory]
        return tasks



EventIndex = d.EventIndex


def retire_cached_copy(background, path, job, proofs):
    item = job['item']
    destination = d.target(background.root, item, job['course'])
    for proof in proofs:
        if (proof.get('status') != 'valid' or proof.get('path') != item['path']
                or proof.get('source', background.source).rstrip('/') != background.source.rstrip('/')
                or (proof.get('size'), proof.get('sha256')) != (item['size'], item['sha256'])
                or proof.get('mtime_ns') is None):
            continue
        local = storage.saved_local(background.root, proof['local']) if proof.get('local') else destination
        try:
            stat = local.stat()
        except FileNotFoundError:
            continue
        if ((stat.st_size, stat.st_mtime_ns) != (item['size'], proof['mtime_ns'])
                or not local.is_file() or Path(str(local) + '.aria2').exists()):
            continue
        saved = FileState(background.state, 'recovery-state').get(item['path'], {}).get('event', {})
        previous = saved if (saved.get('size'), saved.get('sha256')) == (item['size'], item['sha256']) else job.get('event', {})
        archived = previous.get('corrupted_path')
        if archived:
            r.body_path(background.root, dict(corrupted_path=archived))
        event = d.record(background.state, item, job['course'], job['attempt'], 'valid',
                         'Cached SHA-256 verified local copy; obsolete Range task retired', local=local,
                         verified_stat=[stat.st_ino, stat.st_size, stat.st_mtime_ns])
        if event['status'] != 'valid':
            continue
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
    if hasattr(background, 'task_cache'):
        # An unseen task can still keep its course pending.
        if background.directory in background.task_cache.scans:
            return False
        jobs = [job for _, job in background.task_cache.tasks(background.directory, limit=32)]
        if not background.task_cache.tasks_complete(background.directory):
            return False
    else:
        jobs = [json.loads(path.read_text()) for path in background.directory.glob('*.json')]
    courses = json.loads((state / 'courses.json').read_text())
    history_path = state / 'course-history.json'
    history = json.loads(history_path.read_text())
    events = (background.event_index.refresh() if hasattr(background, 'event_index')
              else {row['path']: row for row in d.read_events(state / 'events.jsonl')})
    registry = state / 'local-copies.json'
    external = {row['path']: row for row in (json.loads(registry.read_text()) if registry.exists() else [])
                if row.get('source', '').rstrip('/') == background.source.rstrip('/') and row.get('status') == 'valid'}
    from nextcloud import saved_remote_files
    settings = json.loads((state/'settings.json').read_text())
    link = None if getattr(background, 'fallback', None) else settings.get('nextcloud_existing')
    remote_path = state/'nextcloud-existing.json'
    saved = json.loads(remote_path.read_text()) if link and remote_path.exists() else {}
    remote_files = saved_remote_files(state, link, source=background.source, saved=saved)
    for index, course in enumerate(courses, 1):
        pending = any(job['kind'] == 'range' and job['course'] == course and job['state'] != 'applied' for job in jobs)
        if history.get(course, {}).get('state') == 'finished' and not pending and course not in changed_courses:
            continue
        entries = json.loads((state / f'{index:02d}-manifest.json').read_text())
        valid = 0
        verified_paths=set()
        for item in entries:
            for proof in (events.get(item['path'], {}), external.get(item['path'], {})):
                # Manifests deliberately omit expiring URLs; only the destination is needed here.
                local = storage.saved_local(root, proof['local']) if proof.get('local') else d.target(root, dict(item, file='https://disk.yandex.ru'), course)
                if (proof.get('status') == 'valid' and (proof.get('size'), proof.get('sha256')) == (item['size'], item['sha256'])
                        and local.exists() and local.stat().st_size == item['size']
                        and ('mtime_ns' not in proof or local.stat().st_mtime_ns == proof['mtime_ns'])
                        and not Path(str(local) + '.aria2').exists()):
                    valid += 1
                    verified_paths.add(item['path'])
                    break
        remote=sum((remote_files.get(item['path'], {}).get('size'),
                    remote_files.get(item['path'], {}).get('sha256')) == (item['size'], item['sha256'])
                   for item in entries if item['path'] not in verified_paths)
        status = dict(course=course, source=background.source, state='recovering' if pending else 'finished',
                      complete=not pending and valid + remote == len(entries), total=len(entries), verified=valid, remote_available=remote,
                      failed=0 if pending else len(entries) - valid - remote, pending=len(entries) - valid - remote if pending else 0, time=now())
        if not pending and config.get('report_repo'):
            queue_report(root, Path(config['report_repo']), config['mark'], config.get('git_push', False),
                         f"Update {config['mark']} copies: {root.name} / {course}")
        history[course] = status
        d.write_json(state / f'{index:02d}-status.json', status)
        if not pending:
            print(('COURSE AVAILABLE: ' if status['complete'] and remote else 'COURSE VERIFIED COMPLETE: ' if status['complete'] else 'COURSE FINISHED WITH ERRORS: ') + str(root) + ' / ' + course, flush=True)
    d.write_json(history_path, history)
    pending = [course for course in courses if history[course]['state'] != 'finished']
    failed = sum(history[course]['failed'] for course in courses)
    d.write_json(state / 'summary.json', dict(verified=sum(history[course]['verified'] for course in courses),remote_available=sum(history[course].get('remote_available',0) for course in courses),
                 failed=failed, pending_courses=pending, downloads_complete=True, complete=not failed and not pending))


def fallback_due(fallback,job):
    return (fallback is not None and job['state']=='applied' and job['kind']=='range'
            and not job.get('superseded_by_manifest')
            and not job.get('result',{}).get('event',{}).get('range_recovered_complete')
            and fallback.ready(job['item']))


def yandex_checked_time(job):
    try:return datetime.fromisoformat(job.get('yandex_checked_at')).timestamp()
    except (TypeError,ValueError):return 0


def recheck_due(job, interval, timestamp=None):
    result=job.get('result',{}).get('event',{})
    if (not interval or job['kind']!='range' or job['state']!='applied'
            or any(job.get('superseded_by_'+reason) for reason in
                   ('manifest', 'nextcloud', 'local_copy'))
            or result.get('range_recovered_complete') or result.get('status')=='valid'):
        return False
    last=yandex_checked_time(job)
    if not last:return True
    return (time.time() if timestamp is None else timestamp)-last>=interval


def seed_legacy_tasks(background):
    """Convert legacy failed manifests to replayable tasks under the archive lock."""
    root,state=background.root,background.state
    marker=state/'legacy-recheck-seeded.json'
    if marker.exists() and json.loads(marker.read_text()).get('complete'):return
    aggregate=root/'corrupted/failed.json'
    paths=[aggregate] if aggregate.exists() else list((root/'corrupted').glob('*/failed.json'))
    failures={row['path']:row for path in paths for row in json.loads(path.read_text())}
    saved=FileState(state, 'recovery-state')
    count=0
    for index,course in enumerate(json.loads((state/'courses.json').read_text()),1):
        for item in json.loads((state/f'{index:02d}-manifest.json').read_text()):
            event=failures.get(item['path'])
            if not event or (event.get('size'),event.get('sha256'))!=(item['size'],item['sha256']):continue
            previous=saved.get(item['path'],{}).get('event',{})
            if r.prefer_saved_event(root,event,previous):event=previous
            if event.get('status')=='valid' or event.get('range_recovered_complete'):continue
            r.body_path(root,event)
            item=dict(item,file=background.source)
            d.target(root,item,course)
            key=hashlib.sha256(json.dumps([background.source,item['path'],item['size'],item['sha256'],'range']).encode()).hexdigest()
            path=background.directory/(key+'.json')
            if path.exists():continue
            d.write_json(path,dict(kind='range',state='applied',source=background.source,item=item,course=course,
                                  attempt=event.get('attempt',background.retries+1),event=event,result={'event':event},time=now()))
            count+=1
    d.write_json(marker,dict(complete=True,time=now(),files_seeded=count))
    if count:print('YANDEX RECHECK LEGACY QUEUED: '+str(root)+'; files='+str(count),flush=True)


def run(queues, files=4, per_file=2, requests=12, rps=12, stopping=None, secondary_slots=1, budget_directory=None, metrics_interval=60, nextcloud_fallback=None, recheck_interval=0, recheck_files=8, recheck_probes=16, recheck_full_interval=604800, opencloud_fallback=None, download_budget=None):
    opencloud_fallback, download_budget = validate_source_settings(opencloud_fallback, download_budget)
    if files < 1 or per_file < 1:
        raise ValueError('Range file limits must be positive')
    if not 1 <= secondary_slots <= max(1, files - 1):
        raise ValueError('Secondary slots must leave a slot for the first queue')
    if not math.isfinite(recheck_interval) or recheck_interval<0 or recheck_files<1:
        raise ValueError('Recheck interval must be nonnegative and recheck files positive')
    if recheck_probes < 0 or not math.isfinite(recheck_full_interval) or recheck_full_interval < 0:
        raise ValueError('Recheck probes and full interval must be nonnegative')
    if recheck_interval and len(queues)>1 and recheck_files<2:
        raise ValueError('Rechecks across queues require at least two file slots')
    stopping = stopping if stopping is not None else Event()
    if budget_directory is None:
        budget = RangeBudget(requests, rps)
    else:
        from request_budget import ProcessRangeBudget
        budget = ProcessRangeBudget(budget_directory, requests, rps)
    cache = JsonCache()
    roots = {}
    changed_courses_by_root = {}
    priorities = {}
    fallbacks = {}
    recheck_roots = {}
    def mark_courses(background, courses):
        marker = background.state / 'range-dirty-courses.json'
        saved = changed_courses_by_root.setdefault(background.root, set(cache.read(marker, [])))
        added = set(courses) - saved
        if added:
            saved.update(added)
            d.write_json(marker, sorted(saved))

    recheck_limits = ([recheck_files] if len(queues)==1 else [(recheck_files+1)//2,recheck_files//2])
    with Metrics(queues[0].with_suffix('.range.metrics.jsonl'),'range-queue',interval=metrics_interval,
                 settings=dict(files=files,per_file=per_file,requests=requests,rps=rps,secondary_slots=secondary_slots,recheck_interval=recheck_interval,recheck_files=recheck_files,recheck_probes=recheck_probes,recheck_full_interval=recheck_full_interval)) as metrics, \
            ThreadPoolExecutor(max_workers=files+(recheck_files if recheck_interval else 0), thread_name_prefix='range-file') as pool:
        profile = DiscoveryProfile(metrics)
        cache.profile = profile
        try:
            while not stopping.is_set():
                for queue in queues:
                    storage.check(queue)
                # Poll applies every result under the archive's exclusive download lock.
                for root, (background, lock, config) in list(roots.items()):
                    if not any(future.done() for future in background.futures.values()):
                        storage.check(root)
                        continue
                    mark_courses(background, (job['course'] for job in background.jobs.values()))
                    background.poll()
                eligible = []
                rechecks = []
                recheck_times = {}
                selected_roots = set()
                def is_recheck(background,path):
                    return bool(recheck_interval and background.jobs[path].get('yandex_recheck'))

                active = sum(not is_recheck(bg,path) for bg,_,_ in roots.values() for path in bg.futures)
                def dispatch_regular(cap):
                    nonlocal active
                    while eligible and active < cap:
                        # Reserve capacity for later queues without delaying ready work.
                        chosen = 0
                        if len(queues) > 1 and any(row[0] > 0 for row in eligible):
                            first_active = sum(not is_recheck(bg,path) for bg,_,_ in roots.values()
                                               if priorities[bg.root] == 0 for path in bg.futures)
                            if first_active >= max(1, files - secondary_slots):
                                chosen = next(i for i, row in enumerate(eligible) if row[0] > 0)
                        _, background, path = eligible.pop(chosen)
                        with profile.stage('submit', path):
                            job = json.loads(path.read_text())
                            profile.milestone('first_range_submit')
                            print('RANGE START: ' + str(background.root) + job['item']['path'], flush=True)
                            background.submit('range', job['item'], job['course'], job['attempt'], job['event'],
                                              force=job['state']=='applied',fallback_only=job['state']=='applied')
                        active += 1

                for priority, queue in enumerate(queues):
                    config = storage.read_queue(queue, cache.read(queue))
                    archives=list(config['archives'])
                    predecessor=config.get('wait_for',{}).get('destination')
                    if recheck_interval and predecessor and all(row['destination']!=predecessor for row in archives):
                        old_root=Path(predecessor)
                        if (old_root/'.download/catalog.json').exists():
                            archives.insert(0,dict(destination=predecessor,catalog=str(old_root/'.download/catalog.json'),state='finished',legacy_recheck=True))
                    for archive in archives:
                        if not archive.get('enabled', True):
                            continue
                        root = Path(archive['destination']).resolve()
                        retry_reports(root, stopping)
                        state = root / '.download'
                        summary = cache.read(state / 'summary.json', {})
                        if not summary.get('downloads_complete') and archive.get('state') != 'finished':
                            continue
                        selected_roots.add(root)
                        tasks = cache.tasks(state / 'tasks', limit=32)
                        dirty_courses = cache.read(state / 'range-dirty-courses.json', [])
                        settings=cache.read(state/'settings.json',{})
                        link = nextcloud_fallback or archive.get('nextcloud_fallback', config.get('nextcloud_fallback', settings.get('nextcloud_fallback')))
                        cloud = opencloud_fallback or archive.get('opencloud_fallback', config.get('opencloud_fallback', settings.get('opencloud_fallback')))
                        download_config = download_budget or archive.get('download_budget', config.get('download_budget', settings.get('download_budget')))
                        cloud, download_config = validate_source_settings(cloud, download_config)
                        fallback = None
                        if (link or cloud) and state.joinpath('source.json').exists() and not cache.read(state/'catalog.json',{}).get('root'):
                            fallback_key = json.dumps([link, cloud, download_config], sort_keys=True)
                            if root not in fallbacks or fallbacks[root][0] != fallback_key:
                                budgets = None
                                if download_config:
                                    from adaptive_downloads import get_budgets
                                    budgets = get_budgets(download_config)
                                if cloud:
                                    from cloud_fallback import CloudFallback
                                    source_fallback = CloudFallback(root, link, stopping, opencloud=cloud, budget=budgets)
                                else:
                                    from nextcloud_fallback import Fallback
                                    source_fallback = Fallback(root, link, stopping, **({'budget': budgets['nextcloud']} if budgets else {}))
                                fallbacks[root] = fallback_key, source_fallback
                            fallback = fallbacks[root][1]
                        legacy=recheck_interval and archive.get('legacy_recheck') and not cache.read(state/'legacy-recheck-seeded.json',{}).get('complete')
                        if not legacy and not dirty_courses and not summary.get('pending_courses') and not any(job['kind'] == 'range' and (job['state'] in ('pending', 'running', 'done') or recheck_due(job,recheck_interval) or fallback_due(fallback,job)) for _, job in tasks):
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
                                with profile.stage('archive_setup', root):
                                    background = Background(root, identity['source'], settings['retries'], tuning,
                                                            range_pool=pool, budget=budget, range_files=files, nextcloud_fallback=link if fallback else None,retry_check=settings.get('retry_check',False),
                                                            recheck_probes=recheck_probes,recheck_full_interval=recheck_full_interval,
                                                            opencloud_fallback=cloud if fallback else None,download_budget=download_config)
                            except BaseException:
                                lock.close()
                                raise
                            roots[root] = background, lock, config
                            if dirty_courses:
                                changed_courses_by_root[root] = set(dirty_courses)
                            priorities[root] = priority
                            background.event_index = EventIndex(state / 'events.jsonl')
                            background.cached_events = profile.events(background.event_index)
                            background.task_cache = cache
                            if legacy:
                                seed_legacy_tasks(background)
                                tasks=cache.tasks(state/'tasks', limit=32)
                            background.manifest_items = profile.manifests(cache, state)
                        background = roots[root][0]
                        background.cached_events = profile.events(background.event_index)
                        from nextcloud import saved_remote_files
                        remote_link = None if fallback else settings.get('nextcloud_existing')
                        remote_files = saved_remote_files(state, remote_link, source=background.source,
                            saved=cache.read(state/'nextcloud-existing.json', {}) if remote_link else {})
                        external = cache.read(state / 'local-copies.json', [])
                        external_by_path = {}
                        for copy in external:
                            if copy.get('source', '').rstrip('/') == background.source.rstrip('/'):
                                external_by_path.setdefault(copy['path'], []).append(copy)
                        for path, job in tasks:
                            if path in background.futures:
                                continue
                            checking=bool(recheck_interval and (job.get('yandex_recheck') and job['state']!='applied' or recheck_due(job,recheck_interval)))
                            if job['kind'] == 'range' and (job['state'] in ('pending', 'running', 'done') or checking or fallback_due(fallback,job)):
                                if job['source'] != background.source:
                                    raise ValueError('Range task source mismatch')
                                d.target(root, job['item'], job['course'])
                                item = job['item']
                                remote = remote_files.get(item['path'], {})
                                if (remote.get('size'), remote.get('sha256')) == (item['size'], item['sha256']):
                                    mark_courses(background, [job['course']])
                                    background.checkpoint(item,job['course'],job['attempt'],'remote')
                                    saved = json.loads(path.read_text())
                                    saved.update(state='applied',time=now(),superseded_by_nextcloud=True)
                                    d.write_json(path,saved)
                                    continue
                                if background.manifest_items.get((job['course'], item['path'])) != (item['size'], item['sha256']):
                                    mark_courses(background, [job['course']])
                                    saved = json.loads(path.read_text())
                                    saved.update(state='applied', time=now(), superseded_by_manifest=True)
                                    d.write_json(path, saved)
                                    continue
                                proofs = [background.cached_events.get(item['path'], {}), *external_by_path.get(item['path'], [])]
                                if any(proof.get('status') == 'valid' for proof in proofs):
                                    mark_courses(background, [job['course']])
                                    if retire_cached_copy(background, path, json.loads(path.read_text()), proofs):
                                        continue
                                (rechecks if checking else eligible).append((priority, background, path))
                                if checking:recheck_times[path]=(job['state']=='applied',yandex_checked_time(job))
                        # Ready tasks need not wait for unrelated archives to finish discovery.
                        dispatch_regular(max(1, files-secondary_slots) if priority==0 and len(queues)>1 else files)
                dispatch_regular(files)
                checking_active=Counter(min(priorities[bg.root],1) for bg,_,_ in roots.values()
                                        for path in bg.futures if is_recheck(bg,path))
                rechecks.sort(key=lambda row:recheck_times[row[2]])
                for group,limit in enumerate(recheck_limits):
                    while checking_active[group]<limit:
                        options=[row for row in rechecks if min(row[0],1)==group]
                        if not options:break
                        candidates=list(dict.fromkeys(row[1].root for row in options))
                        previous=recheck_roots.get(group)
                        root=candidates[(candidates.index(previous)+1)%len(candidates)] if previous in candidates else candidates[0]
                        row=next(row for row in options if row[1].root==root);rechecks.remove(row)
                        _,background,path=row
                        with profile.stage('recheck_submit', path):
                            job=json.loads(path.read_text())
                            event=job.get('result',{}).get('event') or job['event']
                            profile.milestone('first_recheck_submit')
                            print('YANDEX RECHECK START: '+str(background.root)+job['item']['path'],flush=True)
                            background.submit('range',job['item'],job['course'],job['attempt'],event,
                                              force=job['state']=='applied',yandex_recheck=True)
                        recheck_roots[group]=root;checking_active[group]+=1
                for root, (background, lock, config) in list(roots.items()):
                    if root not in selected_roots and not background.futures:
                        background.close()
                        lock.close()
                        del roots[root]
                        changed_courses_by_root.pop(root, None)
                        continue
                    if not cache.tasks_complete(background.directory):
                        continue
                    if root in changed_courses_by_root:
                        if finish_courses(background, config, changed_courses_by_root[root]) is False:
                            continue
                        marker = background.state / 'range-dirty-courses.json'
                        marker.unlink(missing_ok=True)
                        storage.sync_directory(marker.parent)
                        del changed_courses_by_root[root]
                    if not background.futures:
                        if any(row[1] is background for row in eligible+rechecks):
                            continue
                        if cache.read(background.state / 'summary.json', {}).get('pending_courses'):
                            if finish_courses(background, config) is False:
                                continue
                        background.close()
                        lock.close()
                        del roots[root]
                metrics.gauge('range_active_files',sum(len(bg.futures) for bg,_,_ in roots.values()))
                metrics.gauge('range_eligible_files',len(eligible))
                metrics.gauge('range_recheck_active_files',sum(checking_active.values()))
                metrics.gauge('range_recheck_eligible_files',len(rechecks))
                metrics.gauge('range_regular_active_files',active)
                metrics.gauge('archives_owned',len(roots))
                metrics.gauge('range_task_discovery_pending_archives',len(cache.scans))
                metrics.gauge('range_task_discovery_reads',cache.task_reads)
                profile.milestone('first_pass')
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
    parser.add_argument('--requests', type=int, default=12)
    parser.add_argument('--rps', type=float, default=12)
    parser.add_argument('--metrics-interval',type=int,default=60,help='Service metrics interval, seconds')
    parser.add_argument('--budget', type=Path, help='Share the HTTP budget with overlapping ordinary downloaders')
    parser.add_argument('--nextcloud-fallback',help='Also backfill completed failed course tasks from this public share')
    parser.add_argument('--recheck-interval',type=float,default=0,help='Seconds between completed Yandex rechecks; 0 disables')
    parser.add_argument('--recheck-files',type=int,default=8,help='Extra recheck file slots, split between first and later queues')
    parser.add_argument('--recheck-probes',type=int,default=16,help='Maximum cheap availability requests before full recovery; 0 always runs full recovery')
    parser.add_argument('--recheck-full-interval',type=float,default=604800,help='Seconds between full rechecks even after negative probes; 0 always runs full recovery')
    add_source_arguments(parser)
    args = parser.parse_args()
    try:
        opencloud_fallback, download_budget = source_settings(args)
    except ValueError as error:
        parser.error(str(error))
    stopping = Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, lambda *_: stopping.set())
    # Prevent two workers from multiplying the global limits.
    with args.queue[0].with_suffix('.range.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args.queue, args.files, args.requests_per_file, args.requests, args.rps, stopping, args.secondary_slots, args.budget, args.metrics_interval, args.nextcloud_fallback,args.recheck_interval,args.recheck_files,args.recheck_probes,args.recheck_full_interval,opencloud_fallback=opencloud_fallback,download_budget=download_budget)


if __name__ == '__main__':
    try:
        main()
    except OSError as error:
        if not storage.disk_error(error):
            raise
        print('DISK OFFLINE: resume state retained', flush=True)
        raise SystemExit(storage.TEMPFAIL)
