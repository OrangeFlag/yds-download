"""Read service telemetry and retained logs without touching video bodies or state."""
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import re
import shutil
from string import Template
import subprocess
import time

import storage

SOURCES=('yandex','nextcloud','opencloud')
STAMP=re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,.]\d+) (.*)$')
PROGRESS=re.compile(r'^(NEXTCLOUD|OPENCLOUD) (UPLOAD|DOWNLOAD) PROGRESS: (.+); (\d+)/(\d+) bytes$')


def duration(value):
    match=re.fullmatch(r'(\d+)([smhd])',value)
    if not match or int(match[1])<1:
        raise ValueError('--since requires a positive duration, for example 15m, 1h or 2d')
    return int(match[1])*dict(s=1,m=60,h=3600,d=86400)[match[2]]


def timestamp(value):
    return datetime.fromisoformat(value.replace(',', '.')).timestamp()


def log_rows(path,warnings=None):
    rotated=[p for p in path.parent.glob(path.name+'.*') if p.suffix[1:].isdigit()]
    seen=set()
    for candidate in sorted(rotated,key=lambda p:int(p.suffix[1:]),reverse=True)+[path]:
        try:
            with candidate.open(encoding='utf-8',errors='replace') as stream:
                import os
                stat=os.fstat(stream.fileno());identity=stat.st_dev,stat.st_ino
                if identity in seen:continue
                seen.add(identity)
                for line in stream:
                    if not line.endswith('\n'):continue
                    match=STAMP.match(line.rstrip('\n'))
                    if match:
                        try:yield timestamp(match[1]),match[2]
                        except ValueError:continue
        except FileNotFoundError:
            continue
        except OSError as error:
            if warnings is not None:warnings.append(candidate.name+': '+type(error).__name__)


def log_summary(rows,name,cutoff):
    result=dict(files=Counter(),progress=Counter(),errors=Counter(),packets=[],first=None,last=None)
    previous={}
    for stamp,message in rows:
        result['first']=min(result['first'] or stamp,stamp)
        result['last']=max(result['last'] or stamp,stamp)
        inside=cutoff is None or stamp>=cutoff
        if 'TRANSFER STATS: ' in message:
            try:
                row=json.loads(message.split('TRANSFER STATS: ',1)[1]);row['time']=timestamp(row['time'])
                if (isinstance(row['epoch'],str) and isinstance(row['budget'],str)
                        and isinstance(row['sources'],dict) and not set(row['sources'])-set(SOURCES)
                        and all(type(n) is int and n>=0 for n in row['sources'].values())):
                    result['packets'].append(row)
            except (KeyError,TypeError,ValueError):pass
        start=re.match(r'^(NEXTCLOUD|OPENCLOUD) (UPLOAD|DOWNLOAD) START: (.+)$',message)
        if start:
            for key in list(previous):
                if key[:3]==(start[1].lower(),start[2],start[3]):previous.pop(key)
        progress=PROGRESS.match(message)
        if progress:
            source,kind,path,value,total=progress.groups();value,total=int(value),int(total)
            # Body.read logs describe a client buffer, not server-accepted upload chunks.
            if '/' in path and value<=total:
                key=source.lower(),kind,path,total
                old=previous.get(key)
                if inside and old and (cutoff is None or old[0]>=cutoff):
                    result['progress'][source.lower()]+=max(0,value-old[1])
                previous[key]=stamp,value
        if not inside:continue
        uploaded=re.match(r'^(NEXTCLOUD|OPENCLOUD) UPLOADED: ',message)
        if uploaded:result['files'][uploaded[1].lower()]+=1
        if name in ('queue','range') and message.startswith('VALID: ') and 'SHA-256 verified' in message:
            attempt=re.search(r'\(attempt (\d+)\) (.*)$',message)
            if attempt and int(attempt[1])>0:
                reason=attempt[2].lower()
                source=next((s for s in ('nextcloud','opencloud') if reason.startswith(s+' ')),'yandex')
                result['files'][source]+=1
        problem=next((key for key in ('Unsafe Nextcloud path','METRICS WARNING','Traceback','DISK OFFLINE',
                     'NEXTCLOUD ERROR:','OPENCLOUD ERROR:','DOWNLOAD RETRY:','[ERROR]','SHA MISMATCH') if key in message),None)
        if problem:
            detail=re.sub(r'https?://\S+','<url>',message.split('; ',1)[-1])
            result['errors'][(problem,detail[:180])]+=1
    return result


def traffic(rows,cutoff,now):
    groups=defaultdict(dict);result={}
    for row in rows:groups[row['budget'],row['epoch']][row['time']]=row
    for group in groups.values():
        samples=[group[k] for k in sorted(group)]
        names=set().union(*(r['sources'] for r in samples))
        for name in names:
            item=result.setdefault(name,dict(bytes=0,measured=False,mbit_s=None,rate_time=0))
            for a,b in zip(samples,samples[1:]):
                if cutoff is None or a['time']>=cutoff:
                    item['bytes']+=max(0,b['sources'].get(name,0)-a['sources'].get(name,0))
                    item['measured']=True
            if samples and now-samples[-1]['time']<=120:
                end=samples[-1]
                base=next((r for r in reversed(samples[:-1]) if end['time']-r['time']>=30),None)
                if base and end['time']>=item['rate_time']:
                    item['mbit_s']=max(0,end['sources'].get(name,0)-base['sources'].get(name,0))*8/(end['time']-base['time'])/1e6
                    item['rate_time']=end['time']
    return result


def read_json(path,warnings):
    try:
        result=json.loads(path.read_text())
        if not isinstance(result,dict):raise ValueError('Expected an object')
        return result
    except (OSError,ValueError) as error:
        warnings.append(f'{path.name}: {type(error).__name__}')
        return {}


def exists(path,warnings):
    try:return path.exists()
    except OSError as error:
        warnings.append(f'{path.name}: {type(error).__name__}')
        return False


def number(value):
    return int(value) if str(value).isdigit() else 0


def systemd(names,warnings):
    properties=('Id','ActiveState','MainPID','NRestarts','MemoryCurrent','MemoryPeak','MemoryHigh','MemoryMax','ControlGroup')
    try:
        run=subprocess.run(['systemctl','--user','show',*['yds-download-'+n+'.service' for n in names],
                            *['--property='+p for p in properties]],capture_output=True,text=True,timeout=10)
    except (OSError,subprocess.TimeoutExpired) as error:
        warnings.append('systemd: '+type(error).__name__);return {}
    result={}
    for block in run.stdout.strip().split('\n\n'):
        row=dict(line.split('=',1) for line in block.splitlines() if '=' in line)
        name=row.get('Id','').removeprefix('yds-download-').removesuffix('.service')
        if name not in names:continue
        result[name]=row
        if row.get('ActiveState')!='active':warnings.append(name+': '+row.get('ActiveState','unknown'))
        cg=row.get('ControlGroup')
        if not cg:continue
        root=Path('/sys/fs/cgroup')/cg.lstrip('/')
        for filename in ('memory.stat','memory.events'):
            try:row[filename]=dict(s.split() for s in (root/filename).read_text().splitlines())
            except (OSError,ValueError):pass
        try:
            row['pressure']={line.split()[0]:dict(s.split('=') for s in line.split()[1:])
                             for line in (root/'memory.pressure').read_text().splitlines()}
            if float(row['pressure'].get('full',{}).get('avg10',0))>10:
                warnings.append(name+': memory stalls >10% in the last 10 seconds')
        except (OSError,ValueError):pass
        if number(row.get('memory.events',{}).get('oom_kill',0)):
            warnings.append(name+': OOM-kill since service start')
    for name in names:
        if name not in result:warnings.append(name+': cannot read user systemd state')
    return result


def collect(config,names,seconds=None):
    now=time.time();cutoff=now-seconds if seconds is not None else None
    warnings=[];state=Path(config['state_dir']);root=Path(config['mount_root'])
    marker=state/(config['disk_uuid']+'.json')
    if exists(marker,warnings):root=Path(read_json(marker,warnings).get('mount',root))
    try:
        attached=any(row.get('uuid')==config['disk_uuid'] and Path(row['target'])==root
                     for row in storage.mounts())
    except (OSError,ValueError,subprocess.SubprocessError) as error:
        attached=False;warnings.append('Archive mount check: '+type(error).__name__)
    if not attached:warnings.append('Archive disk UUID '+config['disk_uuid']+' is not mounted')
    values=dict(home=str(Path.home()),repo=config['repo'],state_dir=str(state),mount_root=str(root))
    queues=set();budgets={}
    if config.get('course_queue'):queues.add(Path(Template(config['course_queue']).substitute(values)))
    for name,service in config['services'].items():
        args=[Template(s).substitute(values) for s in service['command']]
        for i,flag in enumerate(args[:-1]):
            if flag=='--queue':queues.add(Path(args[i+1]))
            if flag in ('--download-budget','--upload-budget'):
                budgets['download' if flag=='--download-budget' else 'upload']=Path(args[i+1]).resolve()
    # Read all four logs: one shared-budget snapshot can be emitted by either participant.
    summaries={n:log_summary(log_rows(state/(n+'.log'),warnings),n,cutoff) for n in ('queue','range','nextcloud','opencloud')}
    packets=[row for summary in summaries.values() for row in summary['packets']]
    directions={}
    for direction,path in budgets.items():
        data=read_json(path/'state.json',warnings)
        stamp=None
        try:stamp=(path/'state.json').stat().st_mtime
        except OSError:pass
        age=max(0,now-stamp) if stamp is not None else float('inf')
        telemetry=data.get('transfer_stats',{})
        if telemetry and stamp is not None:
            packets.append(dict(time=stamp,budget=str(path),
                                epoch=telemetry['epoch'],sources=telemetry['sources']))
        directions[direction]=dict(traffic=traffic([r for r in packets if r['budget']==str(path)],cutoff,now),
                                   controller=data.get('controller',{}),members=list(data.get('members',{}).values()),
                                   age=age,stale=age>120)
        if age>120:warnings.append(direction+': budget slots are stale; no recent updates')
    archives=Counter();tasks=Counter();gauges={};latest_time=0;host={};queue_states={}
    for queue in queues if attached else ():
        data=read_json(queue,warnings)
        try:data=storage.read_queue(queue,data) if data else {}
        except (KeyError,TypeError,ValueError) as error:
            warnings.append(queue.name+': '+type(error).__name__);continue
        counts=Counter(a.get('state','pending') for a in data.get('archives',[]) if a.get('enabled',True))
        archives.update(counts)
        queue_states[queue.name]=dict(counts)
        metric=queue.with_suffix('.range.metrics-latest.json')
        if exists(metric,warnings):
            record=read_json(metric,warnings)
            if record.get('time') and now-timestamp(record['time'])<=180:
                gauges.update(record.get('gauges',{}));host=record.get('host_rates',{})
                latest_time=max(latest_time,timestamp(record['time']))
        for archive in data.get('archives',[]):
            metric=Path(archive['destination'])/'.download/metrics-latest.json'
            if not exists(metric,warnings):continue
            record=read_json(metric,warnings)
            if record.get('time') and now-timestamp(record['time'])<=180:
                tasks.update(record.get('tasks',{}));latest_time=max(latest_time,timestamp(record['time']))
    services=systemd(names,warnings)
    disk={}
    for name,path in (('system',Path('/')),('archive',root)):
        if name=='archive' and not attached:continue
        try:disk[name]=shutil.disk_usage(path).free
        except OSError as error:warnings.append(name+': '+type(error).__name__)
    if disk.get('system',2**63)<5*1024**3:warnings.append('System disk has less than 5 GiB free')
    return dict(now=now,cutoff=cutoff,summaries=summaries,directions=directions,archives=archives,tasks=tasks,
                gauges=gauges,host=host,services=services,disk=disk,warnings=warnings,queue_states=queue_states,
                metrics_age=now-latest_time if latest_time else None)


def show(config,names,seconds=None):
    data=collect(config,names,seconds)
    first=[s['first'] for s in data['summaries'].values() if s['first'] is not None]
    start=data['cutoff'] if data['cutoff'] is not None else min(first,default=data['now'])
    print('Период: '+datetime.fromtimestamp(start).astimezone().isoformat(timespec='seconds')+' — '+
          datetime.fromtimestamp(data['now']).astimezone().isoformat(timespec='seconds')+'; МБ=10⁶ байт, Мбит/с=10⁶ бит/с')
    for name,row in data['services'].items():
        stat=row.get('memory.stat',{});events=row.get('memory.events',{})
        print(f'{name}: {row.get("ActiveState","?")}; рестартов={row.get("NRestarts","?")}; '
              f'память={number(row.get("MemoryCurrent",0)) / 1024**2:.0f} МиБ '
              f'(anon={number(stat.get("anon",0))/1024**2:.0f}, cache={number(stat.get("file",0))/1024**2:.0f}); '
              f'OOM={events.get("oom_kill","?")}; memory.high={events.get("high","?")}')
    for name,summary in data['summaries'].items():
        if summary['first'] is not None:
            print(f'Логи {name}: '+datetime.fromtimestamp(summary['first']).isoformat(timespec='seconds')+' — '+
                  datetime.fromtimestamp(summary['last']).isoformat(timespec='seconds'))
    print('\nПередачи: источник | завершения файлов | подтверждённые МБ за период | текущие Мбит/с')
    for direction,label in (('download','Скачивание'),('upload','Отправка')):
        group=data['directions'].get(direction,{})
        for source in SOURCES if direction=='download' else SOURCES[1:]:
            names_for_direction=('queue','range') if direction=='download' else (source,)
            files=sum(data['summaries'][n]['files'][source] for n in names_for_direction)
            row=group.get('traffic',{}).get(source,{})
            legacy=sum(data['summaries'][n]['progress'][source] for n in names_for_direction)
            amount=row.get('bytes',0) if row.get('measured') else legacy
            volume=f'≥{amount/1e6:.1f}' if row.get('measured') or legacy else 'нет данных'
            rate=f'{row["mbit_s"]:.2f}' if row.get('mbit_s') is not None else 'нет свежего замера'
            print(f'{label} {source} | {files} | {volume} | {rate}')
        if group:
            print('  Устаревший снимок слотов:' if group['stale'] else '  Общий лимит файлов:',
                  group['controller'].get('limit'),f'; возраст={group["age"]:.0f} с; active/waiting:',
                  [(m['name'],m.get('active',0),m.get('waiting',0)) for m in group['members']])
    print('\nОчереди архивов:',data['queue_states'],'; SHA/Range по свежим метрикам:',dict(data['tasks']))
    print('Range/Recheck:',{k:v for k,v in data['gauges'].items() if k.startswith('range_')})
    print('Возраст свежих метрик:',f'{data["metrics_age"]:.0f} с' if data['metrics_age'] is not None else 'нет свежих данных')
    print('Свободно ГиБ:',{k:round(v/1024**3,1) for k,v in data['disk'].items()})
    for key,value in data['host'].items():
        if key.startswith('rx_bytes:'):
            iface=key.split(':',1)[1]
            print(f'Интерфейс {iface} (весь трафик): вход={value*8/1e6:.2f}, '
                  f'выход={data["host"].get("tx_bytes:"+iface,0)*8/1e6:.2f} Мбит/с')
    if 'disk_busy_ms' in data['host']:print(f'Занятость диска: {data["host"]["disk_busy_ms"]/10:.1f}%')
    for name in names:
        summary=data['summaries'][name]
        print(name+' ошибки за период:',sum(summary['errors'].values()))
        for (kind,detail),count in summary['errors'].most_common(3):print(f'  {count}× {kind}: {detail}')
    for warning in data['warnings']:print('ВНИМАНИЕ:',warning)
    print('Объёмы — минимум по доступным снимкам/прогрессу; старые неизвестные байты не считаются нулём. '
          'Завершения файлов — события журналов, включая проверку SHA; скорости подтверждённых байтов исключают служебный трафик и Range.')
