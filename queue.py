#!/usr/bin/env python3
"""Run an editable semester queue, preserving per-archive download state."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time
from datetime import datetime
from download import write_json

def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('queue', type=Path)
    args = parser.parse_args()
    path = args.queue.resolve()
    lock = path.with_suffix('.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    data = json.loads(path.read_text())
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
    data = json.loads(path.read_text())
    for job in data['archives']:
        if not job.get('enabled', True) or job.get('state') == 'finished':
            continue
        job['state'] = 'running'
        write_json(path, data)
        print('ARCHIVE START: ' + job['title'], flush=True)
        command = ['python3', str(Path(__file__).with_name('download.py')), 'download',
                   '--catalog', job['catalog'], '--destination', job['destination'],
                   '--retries', str(data.get('retries', 2))]
        if data.get('report_repo'):
            command += ['--report-repo', data['report_repo'], '--mark', data['mark']]
        if data.get('git_push'):
            command += ['--git-push']
        code = subprocess.call(command)
        job.update(exit_code=code, time=datetime.now().astimezone().isoformat())
        if code not in (0, 1):
            job['state'] = 'blocked'
            write_json(path, data)
            raise RuntimeError('Download stopped: ' + job['title'] + '; exit=' + str(code))
        job['state'] = 'finished'
        write_json(path, data)
        print('ARCHIVE FINISHED: ' + job['title'] + '; exit=' + str(code), flush=True)
    print('ALL ARCHIVES PROCESSED', flush=True)

if __name__ == '__main__':
    main()
