"""Public Nextcloud listings and a separate, durable video upload worker."""
import argparse
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
from threading import Event
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

import download as d
from download import local_path
import network
import storage

VIDEO = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.ts', '.3gp'}
PROPERTIES = b'<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:resourcetype/><d:getcontentlength/><d:getetag/><oc:checksums/></d:prop></d:propfind>'


def error_details(error):
    return dict(error_type=type(error).__name__, http_status=getattr(error, 'code', None),
                error_reason=str(getattr(error, 'reason', error)), error_errno=network.failure(error)['errno'])


def recent_receipt(row):
    # Allow delayed public indexing, but not indefinite trust after remote deletion.
    try:
        written = datetime.fromisoformat(row['time'])
    except (KeyError, TypeError, ValueError):
        return False
    age = (datetime.now().astimezone() - written).total_seconds() if written.tzinfo else -1
    return 0 <= age < 24 * 60 * 60


def normalized(value):
    return unicodedata.normalize('NFC', value).strip()


def remote_path(data, item):
    path = PurePosixPath(item['path'])
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Unsafe source path for Nextcloud')
    year_match = re.search(r'(?<!\d)(20\d\d)(?!\d)', data['title'])
    year = year_match[1] if year_match else None
    title = data['title'].casefold().replace('_',' ')
    parts = list(path.parts[1:])
    season = 'spring' if 'весна' in title or re.search(r'\bspring\b',title) else 'fall' if 'осень' in title or re.search(r'\b(?:autumn|fall)\b',title) else None
    nested = re.fullmatch(r'(?:(?:шад|shad)[ _-]*)?(весна|осень|spring|autumn|fall)(?:[ _-]*(20\d\d))?',parts[0].strip(),re.I) if parts else None
    if nested:
        nested_season = 'spring' if nested[1].casefold() in ('весна','spring') else 'fall'
        if season and season!=nested_season or year and nested[2] and year!=nested[2]:
            raise ValueError('Conflicting Nextcloud year/semester metadata')
        season = nested_season
        year = year or nested[2]
        parts.pop(0)
    if not year or not season or len(parts) < 2:
        raise ValueError('Nextcloud requires a course archive with a known year and semester')
    return year+'-'+season+'/'+normalized('/'.join(parts))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Share:
    def __init__(self, link, timeout=120):
        parsed = urllib.parse.urlsplit(link)
        match = re.fullmatch(r'(.*?)/(?:index\.php/)?s/([A-Za-z0-9_-]+)/?', parsed.path)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password or not match:
            raise ValueError('Expected an HTTPS Nextcloud public share URL')
        self.base = urllib.parse.urlunsplit((parsed.scheme,parsed.netloc,match[1]+'/public.php/dav/files/'+match[2]+'/', '', ''))
        self.subfolder = urllib.parse.parse_qs(parsed.query).get('dir', [''])[0].strip('/')
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler(),NoRedirect(),network.HTTPHandler(),network.HTTPSHandler())
        self.url()

    def url(self, path=''):
        path = '/'.join(p for p in (self.subfolder,path) if p)
        if (PurePosixPath(path).is_absolute() or '..' in PurePosixPath(path).parts
                or any(ord(c)<32 for c in path) or '\\' in path):
            raise ValueError('Unsafe Nextcloud path')
        return self.base+urllib.parse.quote(path,safe='/')

    def request(self, method, path='', data=None, headers=None, uploads=False):
        url=self.url(path)
        if uploads:
            url=self.base.replace('/dav/files/','/dav/uploads/')+urllib.parse.quote(path,safe='/')
        request_headers={'X-Requested-With':'XMLHttpRequest',**(headers or {})}
        if 'Destination' in request_headers:
            # File-drop creates folders from the header before URL decoding; send UTF-8 bytes.
            request_headers['Destination']=urllib.parse.unquote(request_headers['Destination']).encode('utf8').decode('latin1')
        if getattr(self,'nickname',None):
            request_headers['X-NC-Nickname']=urllib.parse.quote(self.nickname,safe='')
        request = urllib.request.Request(url,data=data,method=method,headers=request_headers)
        return self.opener.open(request,timeout=self.timeout)

    def listing(self, folder):
        with self.request('PROPFIND',folder,data=PROPERTIES,
                          headers={'Depth':'1','Content-Type':'application/xml'}) as response:
            if response.status != 207:
                raise RuntimeError('Nextcloud did not return a WebDAV listing')
            tree = ET.fromstring(response.read())
        prefix = urllib.parse.unquote(urllib.parse.urlsplit(self.url()).path).rstrip('/')+'/'
        result = []
        for row in tree.findall('{DAV:}response'):
            href = urllib.parse.urlsplit(row.findtext('{DAV:}href',''))
            path = urllib.parse.unquote(href.path)
            if not path.startswith(prefix):
                continue
            properties = next((p.find('{DAV:}prop') for p in row.findall('{DAV:}propstat')
                               if ' 200 ' in p.findtext('{DAV:}status','')),None)
            if properties is None:
                continue
            length = properties.findtext('{DAV:}getcontentlength')
            result.append(dict(path=normalized(path[len(prefix):].rstrip('/')),
                download_path=path[len(prefix):].rstrip('/'),etag=properties.findtext('{DAV:}getetag'),
                directory=properties.find('{DAV:}resourcetype/{DAV:}collection') is not None,
                size=int(length) if length is not None else None,
                checksums=[c.text or '' for c in properties.findall('.//{http://owncloud.org/ns}checksum')]))
        return result


class Index:
    def __init__(self, root, link, strict=True):
        self.root = Path(root)
        self.share = Share(link,timeout=30)
        self.link = self.share.url()
        self.strict = strict
        self.folders = {}
        self.receipts_stamp = None
        self.receipts = {}

    def contains(self, data, item):
        if PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
            return False
        try:
            relative = remote_path(data,item)
        except ValueError:
            if self.strict:
                raise
            print('NEXTCLOUD PATH UNMAPPED: '+item['path']+'; downloading from source',flush=True)
            return False
        receipts = self.root/'.download/nextcloud-uploads.json'
        try:
            stat = receipts.stat()
            stamp = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        except FileNotFoundError:
            stamp = None
        if stamp != self.receipts_stamp:
            indexed = {}
            for row in json.loads(receipts.read_text()).values() if stamp else ():
                if (row.get('state')=='uploaded' and row.get('variant')=='original'
                        and row.get('read_share')==self.link and row.get('http_status')==201
                        and row.get('sent_bytes')==row.get('size')
                        and row.get('payload_sha256')==row.get('sha256')):
                    key = (row.get('source'), row.get('path'), row.get('size'), row.get('sha256'))
                    indexed.setdefault(key, []).append(row)
            self.receipts = indexed
            self.receipts_stamp = stamp
        uploaded = self.receipts.get((data['source'], item['path'], item['size'], item['sha256']), [])
        candidates = self.candidates(data,item)
        conflicting = False
        for row in candidates:
            hashes = [value.split(':',1)[1].lower() for value in row.get('checksums',[])
                      if value.upper().startswith('SHA256:')]
            if row['size']==item['size'] and item['sha256'].lower() in hashes:
                return True
            # An old upload receipt must not override newer, conflicting WebDAV evidence.
            conflicting |= row['size'] != item['size'] or bool(hashes)
        # A receipt without a visible file is a short-lived eventual-consistency hint.
        return bool(uploaded) and not conflicting and (bool(candidates) or any(map(recent_receipt, uploaded)))

    def candidates(self, data, item):
        relative=remote_path(data,item)
        folder=str(PurePosixPath(relative).parent)
        if folder not in self.folders:
            try:
                self.folders[folder]=self.share.listing(folder)
            except urllib.error.HTTPError as error:
                self.folders[folder]=[] if error.code==404 else error
            except (OSError,RuntimeError,ET.ParseError) as error:
                self.folders[folder]=error
        if isinstance(self.folders[folder],Exception):
            if self.strict:
                raise self.folders[folder]
            print('NEXTCLOUD LOOKUP UNAVAILABLE: '+folder+'; downloading from source',flush=True)
            return []
        return [row for row in self.folders[folder] if not row['directory']
                and normalized(str(PurePosixPath(row['path']).parent))==folder
                and normalized(re.sub(r'^\d{3,} - ','',PurePosixPath(row['path']).name))==PurePosixPath(relative).name]


def saved_remote_files(state, link, source=None, saved=None):
    if not link:
        return {}
    if saved is None:
        path = state/'nextcloud-existing.json'
        if not path.exists():
            return {}
        saved = json.loads(path.read_text())
    if source is None:
        source = json.loads((state/'source.json').read_text())['source']
    if saved.get('source')!=source or saved.get('share')!=Share(link).url():
        return {}
    return saved.get('files', {})


def remote_saved(state, item, link):
    row = saved_remote_files(state, link).get(item['path'], {})
    return (row.get('size'), row.get('sha256')) == (item['size'], item['sha256'])


class Body:
    def __init__(self, stream, size, stopping, path):
        self.stream, self.size, self.stopping, self.path = stream,size,stopping,path
        self.sent = 0
        self.digest = hashlib.sha256()
        self.last_progress=time.monotonic()

    def read(self, amount=-1):
        if self.stopping.is_set():
            raise InterruptedError('Nextcloud upload interrupted')
        storage.check(self.path)
        data = self.stream.read(min(self.size-self.sent,1024**2 if amount<0 else amount))
        self.sent += len(data)
        self.digest.update(data)
        if time.monotonic()-self.last_progress>=30:
            print(f'NEXTCLOUD UPLOAD PROGRESS: {self.path.name}; {self.sent}/{self.size} bytes',flush=True)
            self.last_progress=time.monotonic()
        return data


class Sync:
    def __init__(self, existing, upload, stopping, timeout=120, nickname=None, chunk_size=16*1024**2):
        self.existing, self.share, self.stopping = existing,Share(upload,timeout),stopping
        if nickname and (nickname.startswith('.') or any(ord(c)<32 or c in '/\\' for c in nickname)):
            raise ValueError('Invalid Nextcloud nickname')
        self.share.nickname=nickname
        self.chunk_size=chunk_size
        self.cache = {}

    def read(self, path, default=None):
        if not path.exists():
            return default
        stat = path.stat();stamp=(stat.st_ino,stat.st_size,stat.st_mtime_ns)
        if path not in self.cache or self.cache[path][0]!=stamp:
            value = d.read_events(path) if path.suffix=='.jsonl' else json.loads(path.read_text())
            self.cache[path] = stamp,value
        return self.cache[path][1]

    def run_once(self, roots):
        for root in roots:
            if self.stopping.is_set():
                return
            storage.check(root)
            state = root/'.download'
            data = self.read(state/'source.json')
            if not data:
                continue
            if self.read(state/'catalog.json',{}).get('root'):
                print('NEXTCLOUD ARCHIVE SKIPPED: personal/root catalog',flush=True)
                continue
            index = Index(root,self.existing) if self.existing else None
            events = {e['path']:e for e in self.read(state/'events.jsonl',[])}
            for e in self.read(state/'local-copies.json',[]):
                if e.get('source')==data['source'] and e.get('status')=='valid':
                    events[e['path']] = e
            partial = {e['path']:e for e in self.read(state/'partial-copies.json',[]) if e.get('source')==data['source']}
            entries = {}
            for manifest in sorted(state.glob('*-manifest.json'),key=lambda p:p.stat().st_mtime_ns):
                entries.update({i['path']:i for i in self.read(manifest,[])})
            folders = {}
            for item in entries.values():
                if PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
                    continue
                folders.setdefault(str(PurePosixPath(item['path']).parent),[]).append(item)
            for items in folders.values():
                for ordinal,item in enumerate(sorted(items,key=lambda i:normalized(i['path'])),1):
                    if self.stopping.is_set():
                        return
                    if PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
                        continue
                    event=events.get(item['path'],{})
                    local=Path(event['local']) if event.get('local') else local_path(root,item['path'])
                    variant='original'
                    expected_size=item['size']
                    same=(event.get('size'),event.get('sha256'))==(item['size'],item['sha256'])
                    valid=(same and event.get('status')=='valid' and local.is_file()
                           and (local.stat().st_size,local.stat().st_mtime_ns)==(expected_size,event.get('mtime_ns'))
                           and not Path(str(local)+'.aria2').exists())
                    percent=None
                    if not valid:
                        event=partial.get(item['path'],{})
                        if ((event.get('size'),event.get('sha256'))!=(item['size'],item['sha256'])
                                or (event.get('downloaded_bytes') or 0)*2<=item['size'] or event.get('sha256_mismatch')):
                            continue
                        local=(root/event['recovered_path']).resolve()
                        if not local.is_relative_to(root.resolve()):
                            raise ValueError('Unsafe partial video path')
                        expected_size=event['local_size']
                        if (not local.is_file() or (local.stat().st_size,local.stat().st_mtime_ns)!=(expected_size,event['mtime_ns'])):
                            continue
                        variant='partial'
                        percent=100*event['downloaded_bytes']/item['size']
                    try:
                        if index and index.contains(data,item):
                            print('NEXTCLOUD EXISTS: '+item['path'],flush=True)
                            continue
                        relative=PurePosixPath(remote_path(data,item))
                        stem=relative.stem+(f' (partially available {percent:.2f}%)' if percent is not None else '')
                        name=local_path(Path('/'),f'/{ordinal:03} - {stem}{relative.suffix}').name
                        target=str(relative.with_name(name))
                        self.upload(root,data,item,local,variant,target,expected_size,percent)
                    except (OSError,RuntimeError,ValueError) as error:
                        if storage.disk_error(error):
                            raise
                        details = error_details(error)
                        print(f'NEXTCLOUD ERROR: {item["path"]}; {type(error).__name__}; HTTP={details["http_status"]}; reason={details["error_reason"]!r}; errno={details["error_errno"]}',flush=True)

    def upload(self, root, data, item, local, variant, target, size, percent):
        state=root/'.download/nextcloud-uploads.json'
        ledger=json.loads(state.read_text()) if state.exists() else {}
        stamp=local.stat();fingerprint=[stamp.st_ino,stamp.st_size,stamp.st_mtime_ns]
        key=hashlib.sha256(json.dumps([self.share.url(),self.share.nickname,data['source'],item['path'],item['sha256'],variant,
                                    fingerprint if variant=='partial' else None]).encode()).hexdigest()
        old=ledger.get(key,{})
        journal=state.with_suffix('.jsonl')
        if old and old.get('state')!='uploaded' and journal.exists():
            completed=next((e for e in reversed(self.read(journal,[])) if e.get('key')==key and e.get('state')=='uploaded' and e.get('upload_id')==old.get('upload_id')),None)
            if completed:
                ledger[key]=completed;d.write_json(state,ledger)
                return
        if old.get('state')=='uploaded':
            if recent_receipt(old):
                return
            # Remote file is no longer confirmed; a fresh upload needs a new session.
            old = {}
        row=dict(key=key,source=data['source'],path=item['path'],size=item['size'],sha256=item['sha256'],variant=variant,
                 target=target,upload_share=self.share.url(),read_share=Share(self.existing).url() if self.existing else None,
                 local=str(local),local_stat=fingerprint,payload_size=size,bytes_percent=percent,state='uploading',
                 time=datetime.now().astimezone().isoformat(),nickname=self.share.nickname,
                 upload_id=old.get('upload_id') or uuid.uuid4().hex,
                 chunk_size=old.get('chunk_size',self.chunk_size),
                 next_chunk=0 if old.get('state')=='creating_collection' else old.get('next_chunk',0))
        ledger[key]=row;d.write_json(state,ledger)
        d.append_events(state.with_suffix('.jsonl'),[dict(row)])
        headers={'Destination':self.share.url(target),'OC-Total-Length':str(size)}
        count=(size+row['chunk_size']-1)//row['chunk_size']
        if count>10000:
            raise ValueError('Nextcloud upload needs larger chunks for this video')
        try:
            if row['next_chunk']<count:
                row['state']='creating_collection'
                d.write_json(state,ledger)
                try:
                    # Chunking v2 checks the cached parent before file-drop creates course folders.
                    with self.share.request('MKCOL',row['upload_id'],
                        headers={**headers,'Destination':self.share.url(row['upload_id']+'.upload')},uploads=True):
                        row['next_chunk']=0
                        row['sent_bytes']=0
                except urllib.error.HTTPError as error:
                    if error.code!=405:
                        raise
                row['state']='uploading'
                d.write_json(state,ledger)
            print('NEXTCLOUD UPLOAD START: '+target,flush=True)
            digest=hashlib.sha256()
            with local.open('rb') as stream:
                for number in range(count):
                    if self.stopping.is_set():
                        raise InterruptedError('Nextcloud upload interrupted')
                    storage.check(local)
                    chunk=stream.read(min(row['chunk_size'],size-number*row['chunk_size']))
                    if len(chunk)!=min(row['chunk_size'],size-number*row['chunk_size']):
                        raise RuntimeError('Video truncated during upload')
                    digest.update(chunk)
                    if number<row['next_chunk']:
                        continue
                    import io
                    body=Body(io.BytesIO(chunk),len(chunk),self.stopping,local)
                    chunk_headers={**headers,'Content-Length':str(len(chunk)),
                        'OC-Checksum':'SHA256:'+hashlib.sha256(chunk).hexdigest()}
                    with self.share.request('PUT',row['upload_id']+f'/{number+1:05}',data=body,
                                            headers=chunk_headers,uploads=True) as response:
                        if response.status not in (200,201,204) or body.sent!=len(chunk):
                            raise RuntimeError('Nextcloud did not confirm the complete chunk')
                    after=local.stat()
                    if [after.st_ino,after.st_size,after.st_mtime_ns]!=fingerprint:
                        raise RuntimeError('Video changed during upload')
                    row.update(next_chunk=number+1,sent_bytes=min(size,(number+1)*row['chunk_size']))
                    d.write_json(state,ledger)
                    d.append_events(journal,[dict(row)])
                    print(f'NEXTCLOUD UPLOAD PROGRESS: {target}; {row["sent_bytes"]}/{size} bytes',flush=True)
            after=local.stat()
            if ([after.st_ino,after.st_size,after.st_mtime_ns]!=fingerprint
                    or variant=='original' and digest.hexdigest()!=item['sha256']):
                raise RuntimeError('Video changed during upload or was not sent completely')
            row.update(state='assembling',payload_sha256=digest.hexdigest())
            d.write_json(state,ledger)
            try:
                with self.share.request('MOVE',row['upload_id']+'/.file',
                    headers={**headers,'Overwrite':'F','OC-Checksum':'SHA256:'+digest.hexdigest()},uploads=True) as response:
                    if response.status!=201:
                        raise RuntimeError('Nextcloud did not confirm creation of the assembled video')
                    receipt=dict(http_status=response.status,etag=response.headers.get('ETag'),file_id=response.headers.get('OC-FileId'))
            except urllib.error.HTTPError as error:
                if error.code in (404,412):
                    row['state']='unconfirmed'
                raise
            row.update(state='uploaded',sent_bytes=size,**receipt)
            print('NEXTCLOUD UPLOADED: '+target,flush=True)
        except BaseException as error:
            row.update(state=row['state'] if row['state'] in ('creating_collection','assembling','unconfirmed') else 'pending',
                       **error_details(error))
            raise
        finally:
            row['time']=datetime.now().astimezone().isoformat()
            d.append_events(journal,[dict(row)])
            d.write_json(state,ledger)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    scope=parser.add_mutually_exclusive_group(required=True)
    scope.add_argument('--queue',type=Path,help='Semester queue; uploads from all enabled archives')
    scope.add_argument('--destination',type=Path,help='One downloaded course archive')
    parser.add_argument('--nextcloud-existing',help='Read-only public share used to skip existing originals')
    parser.add_argument('--nextcloud-upload',required=True,help='Public file-drop share for missing originals and partial videos')
    parser.add_argument('--poll-interval',type=float,default=30)
    parser.add_argument('--timeout',type=float,default=120,help='WebDAV socket timeout, seconds')
    parser.add_argument('--nickname',help='File-request sender nickname; defaults to queue mark or yds-download')
    parser.add_argument('--chunk-size-mib',type=int,default=16,help='Resumable upload chunk size, MiB (minimum 5)')
    parser.add_argument('--once',action='store_true',help='Process currently available local videos once')
    args=parser.parse_args()
    if args.poll_interval<=0 or args.timeout<=0 or args.chunk_size_mib<5:
        parser.error('Intervals must be positive')
    os.umask(0o077)
    stopping=Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stopping.set())
    control=args.queue if args.queue else args.destination/'.download/source.json'
    lockpath=control.with_suffix('.nextcloud.lock')
    with lockpath.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        nickname=args.nickname or (json.loads(args.queue.read_text()).get('mark') if args.queue else None) or 'yds-download'
        sync=Sync(args.nextcloud_existing,args.nextcloud_upload,stopping,args.timeout,nickname,args.chunk_size_mib*1024**2)
        while not stopping.is_set():
            storage.check(control)
            roots=[Path(a['destination']) for a in json.loads(args.queue.read_text())['archives'] if a.get('enabled',True)] if args.queue else [args.destination.resolve()]
            sync.run_once(roots)
            if args.once:
                break
            stopping.wait(args.poll_interval)


if __name__=='__main__':
    try:
        raise SystemExit(d.cli(main))
    except OSError as error:
        if not storage.disk_error(error):
            raise
        print('DISK OFFLINE: Nextcloud upload state retained',flush=True)
        raise SystemExit(storage.TEMPFAIL)
