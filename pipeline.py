"""Ordered course transfers with durable SHA and Range background tasks."""
from concurrent.futures import CancelledError, ThreadPoolExecutor
from datetime import datetime
import fcntl
import errno
import hashlib
import json
import re
from local_paths import local_path
from metrics import Metrics, AriaMetrics
from pathlib import Path
import shutil
import signal
import subprocess
from threading import Event
import time

import download as d
import network
from network import aria_resource
import storage
import recovery as r
from settings import resolve


def now():
    return datetime.now().astimezone().isoformat()


def transfer_process(command, *, env, on_tick, stop_timeout=30, stopping=None):
    log = next((Path(arg[6:]) for arg in command if arg.startswith('--log=')), None)
    inputs = next((Path(arg[13:]) for arg in command if arg.startswith('--input-file=')), None)
    destinations = {}
    if inputs is not None:
        resource, directory = None, None
        for line in inputs.read_text().splitlines():
            if line.startswith(('http://', 'https://')):
                resource, directory = aria_resource(line), None
            elif line.startswith(' dir='):
                directory = Path(line[5:])
            elif line.startswith(' out=') and resource is not None and directory is not None:
                destinations.setdefault(resource, set()).add(str(directory / line[5:]))
    retry = network.RequestRetry(str(log), stopping)
    while True:
        offset = log.stat().st_size if log is not None and log.exists() else 0
        code = _transfer_process_once(command, env=env, on_tick=on_tick, stop_timeout=stop_timeout)
        if (command[0] != 'aria2c' or code in (0, 9, 130, -2, 143, -15)
                or stopping is not None and stopping.is_set() or log is None):
            return code
        failures, completed = {}, {}
        target = str(log)
        message = []
        error_target, error_position = target, 0
        phases = {}
        target_phase = error_phase = 'unknown'
        final_error = True
        def finish_error():
            if message:
                failures[error_target] = error_position, network.failure(''.join(message), error_phase)
                message.clear()
        with log.open() as stream:
            stream.seek(offset)
            for position, line in enumerate(stream):
                level = re.match(r'^\d{4}-\d{2}-\d{2} \S+ \[(\w+)\]',line)
                if level:
                    final_error = level[1] == 'ERROR'
                cuid = re.search(r'\bCUID#(\d+)\b', line)
                if level and cuid:
                    if ' - Connecting to ' in line:
                        phases[cuid[1]] = 'connect'
                    elif ' - Requesting:' in line:
                        phases[cuid[1]] = 'response_headers'
                    elif ' - Response received:' in line:
                        phases[cuid[1]] = 'body'
                if line.startswith('Exception:') and final_error:
                    finish_error()
                    error_target, error_position = target, position
                    error_phase = target_phase
                    message.append(line)
                elif message and line.lstrip().startswith('->'):
                    message.append(line)
                else:
                    finish_error()
                    if 'Download aborted. URI=' in line:
                        target = aria_resource(line.partition('URI=')[2].strip())
                        target_phase = phases.get(cuid[1], 'unknown') if cuid else 'unknown'
                    elif 'Download complete:' in line:
                        local = line.partition('Download complete:')[2].strip()
                        completed[local] = position
            finish_error()
        error = None
        observations = [(position, local, None) for local, position in completed.items()]
        priority = {'other': 0, 'http': 1, 'ambiguous': 2, 'routing': 3}
        for target, (position, current) in failures.items():
            if destinations.get(target) and destinations[target] <= completed.keys():
                continue
            observations.append((position, target, current))
            rank = (priority[current['category']], current['phase'] not in ('body', 'headers', 'response_headers'))
            if error is None or rank > (priority[error['category']], error['phase'] not in ('body', 'headers', 'response_headers')):
                error, retry.target = current, target
        for _, target, current in sorted(observations, key=lambda row: row[0]):
            network.GUARD.observe(target, current)
        if error is None or not retry.retry(error):
            return code


def _transfer_process_once(command, *, env, on_tick, stop_timeout=30):
    process = subprocess.Popen(command, env=env)
    try:
        while True:
            try:
                return process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                on_tick()
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=stop_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def failure_manifests(root, event):
    failure_manifests_batch(root, [event])


def failure_manifests_batch(root, events):
    with d.STATE_LOCK:
        updates = {}
        for event in events:
            paths = [root / 'corrupted/failed.json', (root / event['corrupted_path']).parents[len(Path(event['path']).parts) - 2] / 'failed.json']
            for path in dict.fromkeys(paths):
                if path not in updates:
                    rows = json.loads(path.read_text()) if path.exists() else []
                    updates[path] = {(row['path'], row['sha256']): row for row in rows}
                key = event['path'], event['sha256']
                updates[path].pop(key, None)
                if not event.get('range_recovered_complete'):
                    updates[path][key] = event
        for path, rows in updates.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            d.write_json(path, list(rows.values()))


class Background:
    def __init__(self, root, source, retries, tuning, defer_range=False, range_pool=None, budget=None, range_files=1):
        self.root, self.source, self.retries, self.tuning = root, source, retries, tuning
        self.state = root / '.download'
        self.directory = self.state / 'tasks'
        self.directory.mkdir(exist_ok=True)
        self.pools = {kind: ThreadPoolExecutor(max_workers=range_files if kind == 'range' else 1, thread_name_prefix=kind) for kind in ('sha', 'range')}
        self.futures = {}
        self.jobs = {}
        self.deferred = {}
        self.defer_range = defer_range
        self.budget = budget
        self.shared_pool = range_pool
        if range_pool is not None:
            self.pools['range'].shutdown()
            self.pools['range'] = range_pool
        self.stopping = Event()
        self.download_active = Event()
        self.wake = Event()
        self.retry = []
        self.transferring = None
        self.on_tick = self.poll
        self.ledger_path = self.state / 'transfer-state.json'
        self.ledger = json.loads(self.ledger_path.read_text()) if self.ledger_path.exists() else {}
        self.metrics = Metrics(self.state/'metrics.jsonl', 'range-worker' if range_pool else 'downloader',
                               interval=tuning['metrics_interval'], settings=dict(tuning=tuning, retries=retries, range_files=range_files))
        self.aria_metrics = AriaMetrics(self.state/'aria2.log',self.metrics)
        self.metrics.sample = self.aria_metrics.sample

    def checkpoint(self, item, course, attempt, phase, event=None, persist=True):
        row = dict(source=self.source, size=item['size'], sha256=item['sha256'],
                   course=course, attempt=attempt, phase=phase, time=now())
        if event is not None:
            row['event'] = event
        self.ledger[item['path']] = row
        if persist:
            d.write_json(self.ledger_path, self.ledger)

    def continuation(self, item):
        row = self.ledger.get(item['path'], {})
        return row if (row.get('source'), row.get('size'), row.get('sha256')) == (self.source, item['size'], item['sha256']) else {}

    def submit(self, kind, item, course, attempt, event=None, force=False):
        key = hashlib.sha256(json.dumps([self.source, item['path'], item['size'], item['sha256'], kind]).encode()).hexdigest()
        path = self.directory / (key + '.json')
        if path in self.futures:
            return
        job = dict(source=self.source, kind=kind, item=item, course=course, attempt=attempt,
                   state='pending', time=now(), queued_at=now())
        if event is not None:
            job['event'] = event
        old = {}
        if path.exists():
            try:
                old = json.loads(path.read_text())
                if not isinstance(old, dict):
                    raise ValueError('Invalid background task')
            except (OSError, ValueError) as error:
                if not force:
                    raise
                old = {}
                self.metrics.warning(error)
        if old and not force:
            job['queued_at'] = old.get('queued_at') or old.get('time',job['time'])
            if old['state'] == 'done':
                # Reapply a result saved just before interruption, but never trust changed bytes.
                local = d.target(self.root, item, course)
                result = old.get('result', {})
                if (kind == 'range' and r.prefer_saved_event(self.root, event, result.get('event', {}))
                        and self.range_result_current(item, result.get('event', {}))
                        or kind != 'range' and local.exists() and result.get('stat') == self.stat(local)
                        and not Path(str(local) + '.aria2').exists()):
                    job = old
            elif kind == 'range':
                ledger = self.state / 'recovery-state.json'
                saved = json.loads(ledger.read_text()).get(item['path'], {}) if ledger.exists() else {}
                proof = saved.get('event', {})
                if r.prefer_saved_event(self.root, event, proof):
                    job['event'] = proof
                    if saved.get('state') == 'finished' and self.range_result_current(item, proof):
                        job.update(state='done', result=dict(event=proof))
        previous = old.get('timing')
        timing = dict(previous) if isinstance(previous, dict) else {}
        if timing.get('started_at') and not timing.get('stopped_at'):
            value = timing.get('unmeasured_interrupts', 0)
            timing['unmeasured_interrupts'] = (value if isinstance(value, int) else 0) + 1
            timing.pop('started_at', None)
            timing.pop('queued_since', None)
        if job['state'] != 'done':
            timing.setdefault('coverage_since', job['time'])
            timing.setdefault('queued_since', job['time'])
            job['timing'] = timing
        elif timing:
            timing.pop('queued_since', None)
            job['timing'] = timing
        d.write_json(path, job)
        self.jobs[path] = job
        self.metrics.task(path,kind,job['state'],job.get('queued_at') or job.get('time',now()))
        if kind == 'range' and self.defer_range:
            self.deferred[path] = job
            return
        self.futures[path] = self.pools[kind].submit(self.work, path, job)
        self.futures[path].add_done_callback(lambda future: self.wake.set())

    @staticmethod
    def stat(local):
        stat = local.stat()
        return [stat.st_ino, stat.st_size, stat.st_mtime_ns]

    def range_result_current(self, item, event):
        return r.range_result_current(self.root, item, event)

    def work(self, path, job):
        if job['state'] == 'done':
            return job
        if self.stopping.is_set():
            raise InterruptedError('Background task interrupted')
        job['state'] = 'running'
        self.metrics.task(path,job['kind'],'running',job.get('queued_at') or job.get('time',now()))
        started = time.monotonic()
        queued = 0
        try:
            queued = self.metrics.begin_file(job, now())
        except (OSError, ValueError, TypeError, KeyError, OverflowError) as error:
            self.metrics.warning(error)
        outcome = 'failed'
        try:
            d.write_json(path, job)
            if job['kind'] == 'sha':
                local = d.target(self.root, job['item'], job['course'])
                before = self.stat(local) if local.exists() else None
                status, detail = d.inspect(self.root, job['item'], job['course'], stopping=self.stopping)
                after = self.stat(local) if local.exists() else None
                if status == 'valid' and before != after:
                    status, detail = 'invalid', 'File changed during SHA verification'
                result = dict(status=status, detail=detail, stat=before)
            else:
                event = job['event']
                event.pop('range_recovered_complete', None)
                try:
                    r.finish_recovery(self.root, event, job['item'], self.retries, tuning=self.tuning,
                                      stopping=self.stopping, download_active=self.download_active if self.budget is None else None,
                                      background=True, request_budget=self.budget, metrics=self.metrics)
                except InterruptedError:
                    raise
                except (ValueError, FileExistsError) as error:
                    event['recovery_error'] = str(error)
                    print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
                    r.recovery_state(self.root, event, 'finished')
                result = dict(event=event)
            seconds = time.monotonic()-started
            if job['kind'] == 'sha':
                self.metrics.add(sha_checks=1, sha_seconds=seconds, sha_verified_bytes=job['item']['size'] if result['status']=='valid' else 0)
                self.metrics.add(**{'sha_'+result['status']+'_files':1})
            else:
                self.metrics.add(range_finished_files=1, range_full_files=int(event.get('range_recovered_complete',False)))
            self.metrics.task(path,job['kind'],'done',job.get('queued_at') or job.get('time',now()))
            job.update(state='done', result=result, time=now())
            outcome = 'done'
        except InterruptedError:
            outcome = 'interrupted'
            raise
        finally:
            try:
                self.metrics.finish_file(job, time.monotonic()-started, queued, now(), outcome)
            except (OSError, ValueError, TypeError, KeyError, OverflowError) as error:
                self.metrics.warning(error)
            if outcome != 'done':
                try:
                    d.write_json(path, job)
                except OSError as error:
                    self.metrics.warning(error)
        d.write_json(path, job)
        return job

    def archive(self, item, course, attempt, event):
        local = d.target(self.root, item, course)
        archive = self.root / 'corrupted' / datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        broken = archive / local.relative_to(self.root)
        broken.parent.mkdir(parents=True, exist_ok=True)
        event['corrupted_path'] = str(broken.relative_to(self.root))
        self.checkpoint(item, course, attempt, self.continuation(item).get('phase', 'range'), event)
        # Persist the destination before moving either the body or the resume control.
        failure_manifests(self.root, event)
        self.repair_archive(item, course, event)
        return event

    def repair_archive(self, item, course, event):
        local = d.target(self.root, item, course)
        broken = r.body_path(self.root, event)
        broken.parent.mkdir(parents=True, exist_ok=True)
        if (self.continuation(item).get('phase') == 'range' and local.exists() and broken.exists()
                and r.saved_ranges(broken, item['size'], item['sha256']) == [[0, item['size']]]):
            # Recovery may have copied the original before interruption; revalidate it in finish_recovery.
            return
        for suffix in ('', '.aria2', '.ranges.json'):
            existing = Path(str(local) + suffix)
            if existing.exists():
                destination = Path(str(broken) + suffix)
                if destination.exists():
                    raise FileExistsError('Both archive and original exist: ' + str(existing))
                existing.rename(destination)

    def failed(self, item, course, attempt, event):
        if attempt <= self.retries:
            self.checkpoint(item, course, attempt, 'retry', event)
            # aria without checksums would otherwise skip a complete body with a bad SHA.
            if event['status'] == 'invalid':
                self.archive(item, course, attempt, event)
            self.retry.append((item, course, attempt))
        else:
            self.checkpoint(item, course, attempt, 'range', event)
            self.submit('range', item, course, attempt, self.archive(item, course, attempt, event), force=True)

    def poll(self):
        storage.check(self.root)
        completed, failures, events = [], [], []
        for path, future in list(self.futures.items()):
            if len(completed) >= 64:
                break
            if not future.done():
                continue
            try:
                job = future.result()
            except (CancelledError, InterruptedError):
                if not self.stopping.is_set() or self.jobs[path]['kind'] != 'range':
                    raise
                job = self.jobs[path]
                job.update(state='pending', time=now())
                d.write_json(path, job)
                self.deferred[path] = job
                self.metrics.task(path,'range','pending',job.get('queued_at') or job.get('time',now()))
                del self.futures[path]
                continue
            item, course, attempt = job['item'], job['course'], job['attempt']
            result = job['result']
            if job['kind'] == 'sha':
                local = d.target(self.root, item, course)
                if result['status'] == 'valid' and (not local.exists() or result['stat'] != self.stat(local)
                                                   or Path(str(local) + '.aria2').exists()):
                    result.update(status='invalid', detail='File changed after SHA verification')
                event = d.record(self.state, item, course, attempt, result['status'], result['detail'],
                                 verified_stat=result['stat'])
                if event['status'] != 'valid':
                    self.failed(item, course, attempt, event)
                else:
                    self.checkpoint(item, course, attempt, 'finished', event, persist=False)
            else:
                event = result['event']
                if not self.range_result_current(item, event):
                    del self.futures[path]
                    self.jobs.pop(path, None)
                    self.submit('range', item, course, attempt, event, force=True)
                    continue
                if not event.get('range_recovered_complete'):
                    events.append(event)
                failures.append(event)
                self.checkpoint(item, course, attempt, 'finished', event, persist=False)
            completed.append((path, job))
        if not completed:
            return
        # Done tasks remain replayable until proofs and aggregate journals are durable.
        if events:
            d.append_events(self.state / 'events.jsonl', events)
        failure_manifests_batch(self.root, failures)
        d.write_json(self.ledger_path, self.ledger)
        for path, job in completed:
            job['state'] = 'applied'
            self.metrics.task(path,job['kind'],'applied',job.get('queued_at') or job.get('time',now()))
            d.write_json(path, job)
            del self.futures[path]
            self.jobs.pop(path, None)

    def transfer(self, pending, course, previous_attempt=0):
        storage.check(self.root)
        if previous_attempt:
            time.sleep(self.tuning['retry_delay'])
            fresh = {item['path']: item for item in d.list_course(self.source, course, self.retries, self.tuning['listing_retry_delay'])}
            for item in pending:
                current = fresh.get(item['path'])
                if current is None or (current['size'], current['sha256']) != (item['size'], item['sha256']):
                    raise ValueError('Source changed during download retry: ' + item['path'])
                item['file'] = current['file']
        progress = {item['path']: r.progress(d.target(self.root,item,course),item['size'],sha256=item['sha256']).get('downloaded_bytes') for item in pending}
        needed = sum(max(0,item['size']-(progress[item['path']] or 0)) for item in pending)
        self.aria_metrics.begin({str(d.target(self.root,item,course)):(item['size'],progress[item['path']]) for item in pending},
                                {str(d.target(self.root,item,course)):item['file'] for item in pending})
        if shutil.disk_usage(self.root).free < needed + self.tuning['free_space_mib'] * 1024 ** 2:
            raise OSError(errno.ENOSPC, 'Insufficient free space for the course')
        lines = []
        for item in pending:
            local = d.target(self.root, item, course)
            local.parent.mkdir(parents=True, exist_ok=True)
            lines.append(f"{item['file']}\n dir={local.parent}\n out={local.name}\n")
            self.checkpoint(item, course, previous_attempt + 1, 'downloading', persist=False)
        d.write_json(self.ledger_path, self.ledger)
        (self.state / 'queue.txt').write_text(''.join(lines))
        t = self.tuning
        command = ['aria2c', '--input-file=' + str(self.state / 'queue.txt'), '--continue=true',
                   '--disable-ipv6=true', '--always-resume=true', '--check-integrity=false', '--auto-file-renaming=false', '--allow-overwrite=true',
                   f"--max-concurrent-downloads={t['download_concurrency']}",
                   '--optimize-concurrent-downloads=' + (f"{t['auto_concurrency_a']:g}:{t['auto_concurrency_b']:g}" if t['auto_concurrency'] else 'false'),
                   f"--max-connection-per-server={t['connections']}", f"--split={t['split']}",
                   f"--min-split-size={t['min_split_size_mib']}M", '--file-allocation=none', f"--disk-cache={t['disk_cache_mib']}M",
                   f"--max-overall-download-limit={t['bandwidth_limit']}", f"--auto-save-interval={t['auto_save_interval']}",
                   '--max-tries=1', f"--connect-timeout={t['connect_timeout']}", f"--timeout={t['timeout']}",
                   f"--summary-interval={t['summary_interval']}", '--enable-color=false', '--console-log-level=warn',
                   '--log=' + str(self.state / 'aria2.log'), '--log-level=info']
        self.download_active.set()
        self.transferring = course
        try:
            code = transfer_process(command, env=d.ENV, on_tick=self.on_tick, stopping=self.stopping)
        finally:
            self.download_active.clear()
            self.transferring = None
        if code == 9:
            raise OSError(errno.ENOSPC, 'aria2 reports insufficient disk space')
        if code in (130, -2) or self.stopping.is_set():
            raise InterruptedError('Download interrupted')
        attempt = previous_attempt + 1
        for item in pending:
            self.checkpoint(item, course, attempt, 'verifying', persist=False)
        d.write_json(self.ledger_path, self.ledger)
        for item in pending:
            self.submit('sha', item, course, attempt, force=True)
        self.poll()

    def retry_ready(self):
        self.poll()
        if self.retry:
            course, attempt = self.retry[0][1:]
            pending = [item for item, name, number in self.retry if (name, number) == (course, attempt)]
            self.retry = [row for row in self.retry if row[1:] != (course, attempt)]
            self.transfer(pending, course, attempt)

    def close(self):
        self.stopping.set()
        for pool in self.pools.values():
            if pool is self.shared_pool:
                continue
            pool.shutdown(wait=True, cancel_futures=True)
        if self.shared_pool is None or not self.futures:
            self.metrics.close()


def run(data, courses, root, retries, report_repo, mark, git_push, retry_check, tuning, defer_range=False, range_budget=None, range_files=16, nextcloud_existing=None):
    storage.check(root)
    tuning = resolve(tuning)
    if not all(shutil.which(name) for name in ('aria2c', 'cp')):
        raise RuntimeError('Install aria2c and coreutils first')
    root.mkdir(parents=True, exist_ok=True)
    state = root / '.download'
    state.mkdir(mode=0o700, exist_ok=True)
    with (state / 'download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source = data['source']
        identity = state / 'source.json'
        if identity.exists() and json.loads(identity.read_text())['source'] != source:
            raise ValueError('Use a separate destination for a different public folder/year')
        d.write_json(identity, dict(source=source, title=data['title']))
        d.write_json(state / 'settings.json', dict(retries=retries, deep=False, retry_check=retry_check, tuning=tuning, defer_range=defer_range, nextcloud_existing=nextcloud_existing))
        remote_index = None
        remote_files = {}
        if nextcloud_existing:
            from nextcloud import Index
            remote_index = Index(root, nextcloud_existing)
            saved_path=state/'nextcloud-existing.json'
            saved=json.loads(saved_path.read_text()) if saved_path.exists() else {}
            if saved.get('source')==source and saved.get('share')==remote_index.link:
                remote_files=saved.get('files',{})
        events = {row['path']: row for row in d.read_events(state / 'events.jsonl', repair=True)}
        history_path = state / 'course-history.json'
        history = json.loads(history_path.read_text()) if history_path.exists() else {}
        for path in state.glob('*-status.json'):
            old = json.loads(path.read_text())
            if not isinstance(old, dict):
                continue
            if old.get('state') == 'running' and not old.get('time'):
                old['time'] = datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()
            if old.get('source') == source and old.get('time', '') >= history.get(old['course'], {}).get('time', ''):
                history[old['course']] = old
        registry = state / 'local-copies.json'
        for row in json.loads(registry.read_text()) if registry.exists() else []:
            local = Path(row['local'])
            if (row['source'].rstrip('/') == source.rstrip('/') and row['status'] == 'valid' and local.exists()
                    and (local.stat().st_size, local.stat().st_mtime_ns) == (row['size'], row['mtime_ns'])
                    and not Path(str(local) + '.aria2').exists()):
                events[row['path']] = row
        candidates = {}
        for manifest in (root / 'corrupted').glob('*/failed.json'):
            for row in json.loads(manifest.read_text()):
                if row['course'] not in courses:
                    continue
                row = dict(row, corrupted_path=row.get('corrupted_path') or str(local_path(manifest.parent, row['path']).relative_to(root)))
                amount = r.progress(r.body_path(root, row), row['size'], sha256=row['sha256']).get('downloaded_bytes') or 0
                rank = (not row.get('sha256_mismatch', False), amount, row['time'])
                if row['path'] not in candidates or rank > candidates[row['path']][0]:
                    candidates[row['path']] = rank, row
        d.write_json(state / 'catalog.json', data)
        d.write_json(state / 'courses.json', courses)
        lines = [data['title'] + ' — порядок скачивания', 'Источник: ' + source, '']
        for index, course in enumerate(courses, 1):
            lines += [f'{index:02d}. {course}', source.rstrip('/') + '/' + d.urllib.parse.quote(course, safe=''), '']
        (root / 'Порядок скачивания.txt').write_text('\n'.join(lines))
        budget = None
        if range_budget is not None:
            if not defer_range or range_files < 1:
                raise ValueError('Overlapping Range requires --defer-range and positive --range-files')
            from request_budget import ProcessRangeBudget
            budget = ProcessRangeBudget(range_budget, rps=tuning['range_rps'])
        background = Background(root, source, retries, tuning, defer_range=defer_range and budget is None,
                                budget=budget, range_files=range_files if budget else 1)
        for path in background.directory.glob('*.json'):
            job = json.loads(path.read_text())
            if (job['source'] == source and job['course'] in courses
                    and job['kind'] == 'sha' and job['state'] == 'done'
                    and background.continuation(job['item']).get('phase') == 'verifying'):
                background.submit('sha', job['item'], job['course'], job['attempt'])
        if report_repo:
            from report import retry_reports
            retry_reports(root)
        active = {}
        event_stamp = None
        proofs = {}
        last_summary = None

        def finalize():
            nonlocal event_stamp, proofs, last_summary
            background.poll()
            event_path = state / 'events.jsonl'
            stamp = Background.stat(event_path) if event_path.exists() else None
            if stamp != event_stamp:
                proofs = {row['path']: row for row in d.read_events(event_path)}
                event_stamp = stamp
            for course, (index, entries) in list(active.items()):
                jobs = [job for job in background.jobs.values() if job['course'] == course]
                waiting = bool(background.transferring == course or jobs or any(name == course for _, name, _ in background.retry))
                valid = 0
                for item in entries:
                    for proof in (proofs.get(item['path'], {}), events.get(item['path'], {})):
                        local = Path(proof['local']) if proof.get('local') else d.target(root, item, course)
                        if (proof.get('status') == 'valid' and (proof.get('size'), proof.get('sha256')) == (item['size'], item['sha256'])
                                and local.exists() and local.stat().st_size == item['size']
                                and ('mtime_ns' not in proof or local.stat().st_mtime_ns == proof['mtime_ns'])
                                and not Path(str(local) + '.aria2').exists()):
                            valid += 1
                            break
                remote = sum(item['path'] in remote_files for item in entries)
                status = dict(course=course, source=source, state=('recovering' if any(job['kind'] == 'range' for job in jobs) else 'verifying') if waiting else 'finished',
                              complete=not waiting and valid + remote == len(entries), total=len(entries), verified=valid, remote_available=remote,
                              failed=0 if waiting else len(entries) - valid - remote, pending=len(entries) - valid - remote if waiting else 0, time=now())
                if not waiting and report_repo:
                    from report import queue_report
                    queue_report(root, report_repo, mark, git_push,
                                 f"Update {mark} copies: {data['title']} / {course}")
                if {key: value for key, value in history.get(course, {}).items() if key != 'time'} != {key: value for key, value in status.items() if key != 'time'}:
                    history[course] = status
                    d.write_json(state / f'{index:02d}-status.json', status)
                    d.write_json(history_path, history)
                if not waiting:
                    del active[course]
                    print(('COURSE AVAILABLE: ' if status['complete'] and remote else 'COURSE VERIFIED COMPLETE: ' if status['complete'] else 'COURSE FINISHED WITH ERRORS: ') + course, flush=True)
            summary = dict(verified=sum(history.get(course, {}).get('verified', 0) for course in courses),
                         failed=sum(history.get(course, {}).get('failed', 0) for course in courses),
                         pending_courses=list(active), complete=False)
            summary_path = state / 'summary.json'
            if last_summary != summary:
                d.write_json(summary_path, summary)
                last_summary = summary

        background.on_tick = finalize
        try:
            for index, course in enumerate(courses, 1):
                background.retry_ready()
                finalize()
                events.update({path: row for path, row in proofs.items() if row['status'] == 'valid'})
                previous = history.get(course, {})
                if (previous.get('state') == 'finished' and previous.get('source') == source
                        and not (retry_check and previous.get('failed')) and not previous.get('remote_available')):
                    d.write_json(state / f'{index:02d}-status.json', previous)
                    print('COURSE SKIPPED: ' + course, flush=True)
                    continue
                print(f'COURSE {index}/{len(courses)}: {course}', flush=True)
                history[course] = dict(previous, course=course, source=source, state='running', complete=False, time=now())
                d.write_json(history_path, history)
                d.write_json(state / f'{index:02d}-status.json', history[course])
                entries = d.list_course(source, course, retries, tuning['listing_retry_delay'])
                for item in entries:
                    d.target(root, item, course)
                old_entries = {}
                for manifest in sorted(state.glob('*-manifest.json'), key=lambda path: path.stat().st_mtime_ns):
                    old_entries.update({row['path']: row for row in json.loads(manifest.read_text())})
                d.bind_resume(root, entries, old_entries, events, False)
                d.write_json(state / f'{index:02d}-manifest.json', [{key: item[key] for key in ('path', 'size', 'sha256')} for item in entries])
                active[course] = index, entries
                pending = []
                for item in entries:
                    remote_files.pop(item['path'],None)
                    if remote_index:
                        d.write_json(state/'nextcloud-existing.json',dict(source=source,share=remote_index.link,files=remote_files))
                    cached = events.get(item['path'], {})
                    local = Path(cached['local']) if cached.get('local') and cached.get('status') == 'valid' else d.target(root, item, course)
                    if (cached.get('status') == 'valid' and (cached.get('size'), cached.get('sha256')) == (item['size'], item['sha256'])
                            and local.exists() and local.stat().st_size == item['size']
                            and ('mtime_ns' not in cached or local.stat().st_mtime_ns == cached['mtime_ns'])
                            and not Path(str(local) + '.aria2').exists()):
                        continue
                    if remote_index and remote_index.contains(data,item):
                        remote_files[item['path']] = {key:item[key] for key in ('size','sha256')}
                        d.write_json(state/'nextcloud-existing.json',dict(source=source,share=remote_index.link,files=remote_files))
                        background.checkpoint(item,course,0,'remote')
                        print('NEXTCLOUD SKIPPED: '+item['path'],flush=True)
                        continue
                    archived = candidates.get(item['path'], (None, None))[1]
                    continuation = background.continuation(item)
                    if continuation.get('phase') in ('retry', 'downloading'):
                        attempt = continuation['attempt']
                        event = continuation.get('event')
                        if continuation['phase'] == 'retry' and event and event['status'] == 'invalid':
                            if event.get('corrupted_path'):
                                background.repair_archive(item, course, event)
                            else:
                                background.archive(item, course, attempt, dict(event))
                        background.retry.append((item, course, attempt if continuation['phase'] == 'retry' else attempt - 1))
                    elif continuation.get('phase') == 'verifying':
                        background.submit('sha', item, course, continuation['attempt'])
                    elif continuation.get('phase') == 'range':
                        event = continuation['event']
                        if (archived and (archived['size'], archived['sha256']) == (item['size'], item['sha256'])
                                and not r.prefer_saved_event(root, archived, event)):
                            event = archived
                        if not event.get('corrupted_path'):
                            event = background.archive(item, course, continuation['attempt'], event)
                        else:
                            background.repair_archive(item, course, event)
                        background.submit('range', item, course, continuation['attempt'], event, force=retry_check)
                    elif (archived and (retry_check or previous.get('state') in ('running', 'verifying', 'recovering'))
                          and (archived['size'], archived['sha256']) == (item['size'], item['sha256'])):
                        ledger_path = state / 'recovery-state.json'
                        ledger = json.loads(ledger_path.read_text()).get(item['path'], {}) if ledger_path.exists() else {}
                        old = ledger.get('event', {})
                        same = r.prefer_saved_event(root, archived, old)
                        if same:
                            archived = old
                        if not retry_check and (same and ledger.get('state') == 'finished'
                                                and background.range_result_current(item, archived)
                                                or not ledger and archived.get('range_attempts')):
                            if not ledger:
                                r.recovery_state(root, archived, 'finished')
                            d.append_events(state / 'events.jsonl', [archived])
                        else:
                            background.repair_archive(item, course, archived)
                            background.submit('range', item, course, archived['attempt'], archived, force=retry_check)
                    elif local.exists() and local.stat().st_size == item['size'] and not Path(str(local) + '.aria2').exists():
                        background.submit('sha', item, course, 0)
                    else:
                        pending.append(item)
                if pending:
                    background.transfer(pending, course)
                finalize()
            while background.retry or any(background.jobs[path]['kind'] == 'sha' or not defer_range for path in background.futures):
                background.retry_ready()
                finalize()
                if background.futures and not background.retry:
                    background.wake.wait(1)
                    background.wake.clear()
            if budget is not None:
                background.close()
            finalize()
            if budget is not None:
                background.metrics.snapshot('handoff')
        finally:
            background.close()
        failed = sum(history[course]['failed'] for course in courses)
        verified = sum(history[course]['verified'] for course in courses)
        d.write_json(state / 'summary.json', dict(verified=verified, remote_available=sum(history[course].get('remote_available',0) for course in courses), failed=failed, pending_courses=list(active), downloads_complete=True, complete=not failed and not active))
        print(f'ALL SELECTED COURSES PROCESSED: verified={verified}, failed={failed}', flush=True)
        return 1 if failed or active else 0
