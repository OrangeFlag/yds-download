"""Minute telemetry from task results and resume metadata, without reading bodies."""
from collections import Counter, deque
from copy import deepcopy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
import subprocess
from threading import Event, RLock, Thread
import time
import uuid

import network

try:
    REVISION = subprocess.check_output(['git','-C',str(Path(__file__).parent),'rev-parse','--short','HEAD'],
                                       stderr=subprocess.DEVNULL,text=True).strip()
except (OSError, subprocess.SubprocessError):
    REVISION = 'unknown'


class Metrics:
    def __init__(self, path, role, interval=60, settings=None, start=True):
        if interval <= 0:
            raise ValueError('Metrics interval must be positive')
        self.path, self.role, self.interval = Path(path), role, interval
        self.settings = settings or {}
        self.run_id = uuid.uuid4().hex
        self.lock, self.snapshot_lock, self.stop = RLock(), RLock(), Event()
        self.counters, self.previous = Counter(), Counter()
        self.errors, self.gauges, self.tasks = {}, {}, {}
        self.first_bytes = deque(maxlen=1024)
        self.eof_retries = deque(maxlen=1024)
        self.eof_sequence = 0
        self.aria_attempts = deque(maxlen=1024)
        self.aria_sequence = 0
        self.sample = lambda: None
        self.previous_time = time.monotonic()
        self.disk_name = self.disk_device()
        self.gauges['disk_device'] = self.disk_name
        self.host_previous = self.host()
        self.warned = self.closed = False
        self.thread = None
        if start:
            self.thread = Thread(target=self.loop, name='metrics', daemon=True)
            self.thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def add(self, **values):
        with self.lock:
            self.counters.update(values)

    def gauge(self, name, value):
        with self.lock:
            self.gauges[name] = value

    def task(self, key, kind, state, queued_at):
        with self.lock:
            if state == 'applied':
                self.tasks.pop(str(key), None)
            else:
                self.tasks[str(key)] = kind, state, queued_at

    def warning(self, error):
        with self.lock:
            if self.warned:
                return
            self.warned = True
        try:
            print('METRICS WARNING: '+type(error).__name__, flush=True)
        except OSError:
            pass

    def eof_retry(self, detail):
        with self.lock:
            self.eof_sequence += 1
            if len(self.eof_retries) == self.eof_retries.maxlen:
                self.counters['range_eof_retry_buffer_overwrites'] += 1
            self.eof_retries.append(dict(detail, sequence=self.eof_sequence))
            self.add(range_eof_retry_attempts=1,
                     range_eof_retry_useful_attempts=int(detail['accepted_bytes'] > 0),
                     range_eof_retry_accepted_bytes=detail['accepted_bytes'],
                     range_eof_retry_request_seconds=detail['request_seconds'])

    def begin_file(self, job, timestamp):
        if not isinstance(job.get('timing'), dict):
            job['timing'] = {}
        timing = job.setdefault('timing', {})
        for key in ('processing_seconds', 'queue_seconds'):
            value = timing.get(key, 0)
            timing[key] = value if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0 else 0
        try:
            queued = max(0, (datetime.fromisoformat(timestamp) - datetime.fromisoformat(timing['queued_since'])).total_seconds())
        except (ValueError, TypeError, KeyError, OverflowError):
            queued = 0
        timing['queue_seconds'] += queued
        timing['runs'] = (timing.get('runs', 0) if isinstance(timing.get('runs', 0), int) else 0) + 1
        timing['started_at'] = timestamp
        timing.pop('stopped_at', None)
        timing.pop('queued_since', None)
        return queued

    def aria_attempt(self, detail):
        with self.lock:
            self.aria_sequence += 1
            if len(self.aria_attempts) == self.aria_attempts.maxlen:
                self.counters['aria_attempt_buffer_overwrites'] += 1
            self.aria_attempts.append(dict(detail, sequence=self.aria_sequence))
            self.add(aria_observed_connections=1)
            if detail['outcome'] == 'timeout':
                self.add(aria_timeout_connections=1)
                self.errors.setdefault('aria_timeout_phases', Counter()).update([detail['phase']])
            for key in ('connect_ready_seconds', 'response_headers_seconds', 'body_phase_seconds'):
                if detail[key] is not None:
                    self.add(**{'aria_'+key:detail[key], 'aria_'+key.replace('_seconds','_samples'):1})

    def finish_file(self, job, seconds, queued, timestamp, outcome):
        timing = job['timing']
        timing['processing_seconds'] += max(0, seconds)
        timing.update(stopped_at=timestamp, outcome=outcome)
        if outcome != 'done':
            timing['queued_since'] = timestamp
        self.add(**{job['kind']+'_work_runs': 1, job['kind']+'_processing_seconds': max(0, seconds),
                    job['kind']+'_queue_seconds': queued, job['kind']+'_'+outcome+'_work_runs': 1})

    def range_request(self, row):
        with self.lock:
            self.add(range_requests=1, range_received_bytes=row['saved_bytes'],
                     range_request_seconds=row['request_seconds'], range_io_seconds=row['save_seconds'],
                     range_wait_seconds=max(0,row['seconds']-row['request_seconds']))
            self.add(range_http_attempts=row.get('request_attempts', 1))
            for reason, count in row.get('retry_reasons', {}).items():
                self.add(**{'range_retries_' + reason: count})
            self.errors.setdefault('range_causes', Counter()).update(row.get('request_errors', {}))
            if row['error']:
                self.errors.setdefault('range',Counter()).update([row['error']])
            if row['first_byte_seconds'] is not None:
                self.first_bytes.append(row['first_byte_seconds'])

    def range_pass(self, outcomes, seconds, count_requests=True):
        with self.lock:
            self.add(range_passes=1, range_zero_byte_passes=int(not any(r['saved_bytes'] for r in outcomes)),
                     range_pass_seconds=seconds)
            if count_requests:
                for row in outcomes:
                    self.range_request(row)

    def disk_device(self):
        try:
            dev = self.path.parent.stat().st_dev
            device = (Path('/sys/dev/block')/f'{os.major(dev)}:{os.minor(dev)}').resolve()
            if not device.exists():
                # Btrfs st_dev is virtual; mountinfo names its actual backing device.
                destination = str(self.path.parent.resolve())
                closest = (0,None)
                for row in Path('/proc/self/mountinfo').read_text().splitlines():
                    fields, filesystem = row.split(' - ',1)
                    mount = re.sub(r'\\([0-7]{3})',lambda m:chr(int(m[1],8)),fields.split()[4])
                    if destination == mount or destination.startswith(mount.rstrip('/')+'/'):
                        if len(mount) > closest[0]:
                            closest = len(mount),filesystem.split()[1]
                source = closest[1]
                if not source or not source.startswith('/dev/'):
                    return None
                device = (Path('/sys/class/block')/Path(source).resolve().name).resolve()
            return device.parent.name if (device/'partition').exists() else device.name
        except (OSError, ValueError, IndexError):
            return None

    def host(self):
        result = {}
        for interface in Path('/sys/class/net').glob('*'):
            if (interface/'device').exists():
                try:
                    result['rx_bytes:'+interface.name] = int((interface/'statistics/rx_bytes').read_text())
                    result['tx_bytes:'+interface.name] = int((interface/'statistics/tx_bytes').read_text())
                except OSError:
                    pass
        try:
            for row in Path('/proc/diskstats').read_text().splitlines():
                fields = row.split()
                if fields[2] == self.disk_name:
                    result.update(disk_read_bytes=int(fields[5])*512, disk_write_bytes=int(fields[9])*512,
                                  disk_busy_ms=int(fields[12]))
                    break
            cpu = os.times()
            result['process_cpu_seconds'] = cpu.user + cpu.system
        except (OSError, ValueError):
            pass
        return result

    def snapshot(self, reason='periodic'):
        try:
            with self.snapshot_lock:
                self.sample()
                now = time.monotonic(); elapsed = max(.001,now-self.previous_time)
                host = self.host()
                rates = {key:(value-self.host_previous[key])/elapsed for key,value in host.items()
                         if key in self.host_previous and value >= self.host_previous[key]}
                with self.lock:
                    queue = Counter(); oldest = {}
                    for kind, state, queued in self.tasks.values():
                        queue[kind+':'+state] += 1
                        if state == 'pending':
                            try:
                                age = max(0,time.time()-datetime.fromisoformat(queued).timestamp())
                                oldest[kind] = max(oldest.get(kind,0),age)
                            except (ValueError, TypeError):
                                pass
                    first = sorted(self.first_bytes)
                    row = dict(time=datetime.now().astimezone().isoformat(), role=self.role, reason=reason, pid=os.getpid(),
                               run_id=self.run_id, revision=REVISION, settings=self.settings,
                               seconds=elapsed, counters=dict(self.counters),
                               interval_counters={k:v-self.previous.get(k,0) for k,v in self.counters.items()},
                               errors=deepcopy(self.errors), gauges=deepcopy(self.gauges), tasks=queue, oldest_pending_seconds=oldest,
                               first_byte_p95_last_1024=first[min(len(first)-1,int(len(first)*.95))] if first else None,
                               host_rates=rates, range_eof_retry_details=list(self.eof_retries),
                               aria_attempt_details=list(self.aria_attempts))
                self.path.parent.mkdir(parents=True,exist_ok=True)
                with self.path.open('a',encoding='utf-8') as stream:
                    stream.write(json.dumps(row,ensure_ascii=False)+'\n')
                latest = self.path.with_name(self.path.stem+'-latest.json')
                temporary = latest.with_name(latest.name+'.tmp')
                temporary.write_text(json.dumps(row,ensure_ascii=False,indent=2),encoding='utf-8')
                temporary.replace(latest)
                with self.lock:
                    for buffer, key in ((self.eof_retries, 'range_eof_retry_details'),
                                        (self.aria_attempts, 'aria_attempt_details')):
                        cutoff = row[key][-1]['sequence'] if row[key] else 0
                        while buffer and buffer[0]['sequence'] <= cutoff:
                            buffer.popleft()
                self.previous = row['counters']; self.previous_time = now; self.host_previous = host
        except (OSError, ValueError, KeyError, TypeError) as error:
            self.warning(error)

    def loop(self):
        while not self.stop.wait(self.interval):
            self.snapshot()

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
        self.snapshot('shutdown')


class AriaMetrics:
    def __init__(self, path, metrics):
        self.path, self.metrics = Path(path), metrics
        try:
            stat = self.path.stat()
            self.offset, self.inode = stat.st_size, stat.st_ino
        except OSError:
            self.offset, self.inode = 0, None
        self.buffer = b''
        self.files = {}
        self.resources = {}
        self.file_names = {}
        self.connections = {}
        self.request_cuid = self.response_cuid = self.error_cuid = None
        self.log_error = True
        self.lock = RLock()

    def begin(self, files, urls=None):
        with self.lock:
            self.sample()
            self.finish_connections('new_batch')
            self.files = {path:dict(size=size, seen=received, complete=False) for path,(size,received) in files.items()}
            self.resources = {}
            for path,url in (urls or {}).items():
                resource = network.aria_resource(url)
                self.resources[resource] = None if resource in self.resources else path
            self.file_names = {}
            for path in self.files:
                self.file_names.setdefault(Path(path).name, []).append(path)

    @staticmethod
    def elapsed(end, start):
        return end-start if end is not None and start is not None and end >= start else None

    def bind_path(self, context, uri):
        try:
            resource = network.aria_resource(uri)
        except ValueError:
            return
        if resource in self.resources:
            path = self.resources[resource]
        else:
            candidates = self.file_names.get(resource[0], [])
            path = candidates[0] if len(candidates) == 1 else None
        if path is not None:
            context['path'] = path

    def finish_connection(self, cuid, ended, outcome):
        context = self.connections.pop(cuid, None)
        if context is None:
            return
        if self.error_cuid == cuid:
            self.error_cuid = None
        if 'started' not in context and 'requested' not in context and outcome != 'aborted':
            return
        start, requested, response = (context.get(k) for k in ('started','requested','response'))
        status = context.get('http_status')
        body = response is not None and status is not None and 200 <= status < 300
        phase = ('body' if body else 'http_error' if response is not None and status is not None and status >= 400
                 else 'response_headers' if requested is not None else 'connect' if start is not None else 'unknown')
        codes = context.get('error_codes', [])
        if outcome == 'aborted' and codes and codes[-1] == '2':
            outcome = 'timeout'
        self.metrics.aria_attempt(dict(cuid=cuid, path=context.get('path'),
            started_at=start, ended_at=ended, outcome=outcome, phase=phase,
            censored=outcome != 'file_complete', error_codes=codes, http_status=status,
            elapsed_seconds=self.elapsed(ended,start), request_seconds=self.elapsed(ended,requested),
            connect_ready_seconds=self.elapsed(requested,start),
            response_headers_seconds=self.elapsed(response,requested),
            body_phase_seconds=self.elapsed(ended,response) if body else None,
            stage_seconds=self.elapsed(ended,response if body else requested if requested is not None else start),
            segments_completed=context.get('segments_completed',0),
            seconds_since_last_segment=self.elapsed(ended,context.get('last_segment'))))

    def finish_connections(self, outcome):
        for cuid, context in list(self.connections.items()):
            self.finish_connection(cuid,context.get('last_time'),
                                   'aborted' if cuid == self.error_cuid else outcome)
        self.request_cuid = self.response_cuid = self.error_cuid = None

    def timing_line(self, line):
        prefix = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[(\w+)\]',line)
        if prefix:
            self.log_error = prefix[2] == 'ERROR'
            try:
                stamp = datetime.fromisoformat(prefix[1]).timestamp()
            except (ValueError, OverflowError):
                return
            if self.error_cuid is not None:
                context = self.connections[self.error_cuid]
                self.finish_connection(self.error_cuid,context['last_time'],'aborted')
            self.request_cuid = self.response_cuid = None
            match = re.search(r'CUID#(\d+)',line)
            if not match:
                if 'Download complete: ' in line:
                    path = line.partition('Download complete: ')[2].strip()
                    for cuid, context in list(self.connections.items()):
                        if context.get('path') == path:
                            self.finish_connection(cuid,stamp,'file_complete')
                return
            cuid = match[1]
            relevant = any(word in line for word in ('Connecting to ', 'Requesting:', 'Response received:',
                            'Redirecting to ', 'Download aborted. URI=', 'segment completed successfully.'))
            if not relevant:
                return
            if cuid not in self.connections and len(self.connections) >= 1024:
                oldest = next(iter(self.connections))
                self.finish_connection(oldest,self.connections[oldest].get('last_time'),'tracking_limit')
            context = self.connections.setdefault(cuid,{})
            context['last_time'] = stamp
            if 'Connecting to ' in line or 'Requesting:' in line and 'requested' in context:
                path = context.get('path')
                self.finish_connection(cuid,stamp,'next_request')
                context = self.connections[cuid] = dict(path=path,last_time=stamp)
            if 'Connecting to ' in line:
                context['started'] = stamp
            elif 'Requesting:' in line:
                context['requested'] = stamp
                self.request_cuid = cuid
            elif 'Response received:' in line:
                context['response'] = stamp
                self.response_cuid = cuid
            elif 'Redirecting to ' in line:
                path = context.get('path')
                self.finish_connection(cuid,stamp,'redirect')
                self.connections[cuid] = dict(path=path,last_time=stamp)
            elif 'Download aborted. URI=' in line:
                self.bind_path(context,line.partition('URI=')[2].strip())
                self.error_cuid = cuid
            elif 'segment completed successfully.' in line:
                context['segments_completed'] = context.get('segments_completed',0) + 1
                context['last_segment'] = stamp
        elif self.request_cuid is not None:
            request = re.match(r'(?:GET|HEAD) (\S+) HTTP/',line)
            if request:
                self.bind_path(self.connections[self.request_cuid],request[1])
        elif self.response_cuid is not None:
            status = re.match(r'HTTP/[\d.]+ (\d{3})',line)
            if status:
                self.connections[self.response_cuid]['http_status'] = int(status[1])
        elif self.error_cuid is not None and line.lstrip().startswith(('Exception:','->')):
            self.connections[self.error_cuid].setdefault('error_codes', []).extend(re.findall(r'errorCode=(\d+)',line))

    def received(self, entry, amount):
        if entry['seen'] is None:
            self.metrics.add(aria_initial_unmeasured_bytes=amount)
            entry['seen'] = amount
        elif amount > entry['seen']:
            network.GUARD.observe(str(id(entry)))
            self.metrics.add(aria_received_bytes=amount-entry['seen'])
            entry['seen'] = amount

    def sample(self):
        with self.lock:
            self._sample()
            if self.metrics.closed:
                self.finish_connections('unfinished_at_shutdown')

    def _sample(self):
        from progress import received_bytes
        try:
            stat = self.path.stat()
            if stat.st_ino != self.inode or stat.st_size < self.offset:
                self.finish_connections('log_reset')
                self.offset = 0; self.buffer = b''
                self.log_error = True
            self.inode = stat.st_ino
            with self.path.open('rb') as stream:
                stream.seek(self.offset); data=stream.read(); self.offset=stream.tell()
            lines = (self.buffer+data).split(b'\n'); self.buffer=lines.pop()
            for raw in lines:
                line = raw.decode('utf-8',errors='replace')
                self.timing_line(line)
                if 'Download complete: ' in line:
                    path = line.partition('Download complete: ')[2].strip()
                    entry = self.files.get(path)
                    if entry and not entry['complete']:
                        self.received(entry,entry['size']); entry['complete']=True
                        self.metrics.add(aria_completed_files=1,aria_completed_bytes=entry['size'])
                if 'Download aborted. URI=' in line:
                    self.metrics.add(aria_aborted_files=1)
                codes = re.findall(r'errorCode=(\d+)',line)
                if codes and self.log_error:
                    with self.metrics.lock:
                        self.metrics.errors.setdefault('aria_error_messages',Counter()).update(codes)
            self.metrics.gauge('aria_log_backlog_bytes',max(0,stat.st_size-self.offset))
        except OSError:
            pass
        active = 0
        for path,entry in self.files.items():
            if entry['complete']:
                continue
            control = Path(path+'.aria2')
            if control.exists():
                active += 1
                try:
                    self.received(entry,received_bytes(control,entry['size']))
                except (OSError, ValueError):
                    pass
        self.metrics.gauge('aria_files_with_resume_control',active)
