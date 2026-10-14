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
import xml.etree.ElementTree as ET

import download as d
from download import local_path
import network
import storage

VIDEO = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.ts', '.3gp'}
PROPERTIES = b'<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:resourcetype/><d:getcontentlength/><oc:checksums/></d:prop></d:propfind>'


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
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect(),network.HTTPHandler(),network.HTTPSHandler())
        self.url()

    def url(self, path=''):
        path = '/'.join(p for p in (self.subfolder,path) if p)
        if (PurePosixPath(path).is_absolute() or '..' in PurePosixPath(path).parts
                or any(ord(c)<32 for c in path) or '\\' in path):
            raise ValueError('Unsafe Nextcloud path')
        return self.base+urllib.parse.quote(path,safe='/')

    def request(self, method, path='', data=None, headers=None):
        request = urllib.request.Request(self.url(path),data=data,method=method,
            headers={'X-Requested-With':'XMLHttpRequest',**(headers or {})})
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
                directory=properties.find('{DAV:}resourcetype/{DAV:}collection') is not None,
                size=int(length) if length is not None else None,
                checksums=[c.text or '' for c in properties.findall('.//{http://owncloud.org/ns}checksum')]))
        return result


class Index:
    def __init__(self, root, link, strict=True):
        self.root, self.link = Path(root), Share(link).url()
        self.share = Share(link,timeout=30)
        self.strict = strict
        self.folders = {}

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
        if receipts.exists():
            for row in json.loads(receipts.read_text()).values():
                if (row.get('state')=='uploaded' and row.get('variant')=='original'
                        and row.get('read_share')==self.link and row.get('source')==data['source']
                        and row.get('http_status')==201 and row.get('sent_bytes')==item['size']
                        and row.get('payload_sha256')==item['sha256']
                        and (row.get('path'),row.get('size'),row.get('sha256'))==(item['path'],item['size'],item['sha256'])):
                    return True
        folder = str(PurePosixPath(relative).parent)
        if folder not in self.folders:
            try:
                self.folders[folder] = self.share.listing(folder)
            except urllib.error.HTTPError as error:
                self.folders[folder] = [] if error.code==404 else error
            except (OSError,RuntimeError,ET.ParseError) as error:
                self.folders[folder] = error
        if isinstance(self.folders[folder],Exception):
            if self.strict:
                raise self.folders[folder]
            print('NEXTCLOUD LOOKUP UNAVAILABLE: '+folder+'; downloading from source',flush=True)
            return False
        for row in self.folders[folder]:
            path = PurePosixPath(row['path'])
            name = re.sub(r'^\d{3,} - ', '',path.name)
            if (row['directory'] or normalized(str(path.parent))!=folder
                    or normalized(name)!=PurePosixPath(relative).name or row['size']!=item['size']):
                continue
            hashes = [value.split(':',1)[1].lower() for value in row.get('checksums',[])
                      if value.upper().startswith('SHA256:')]
            if hashes and item['sha256'] not in hashes:
                continue
            return True
        return False


def remote_saved(state, item, link):
    path = state/'nextcloud-existing.json'
    if not link or not path.exists():
        return False
    saved = json.loads(path.read_text())
    row = saved.get('files',{}).get(item['path'],{})
    source=json.loads((state/'source.json').read_text())['source']
    return (saved.get('source')==source and saved.get('share')==Share(link).url()
            and (row.get('size'),row.get('sha256'))==(item['size'],item['sha256']))


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
    def __init__(self, existing, upload, stopping, timeout=120):
        self.existing, self.share, self.stopping = existing,Share(upload,timeout),stopping
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
                        print(f'NEXTCLOUD ERROR: {item["path"]}; {type(error).__name__}; HTTP={getattr(error,"code",None)}',flush=True)

    def upload(self, root, data, item, local, variant, target, size, percent):
        state=root/'.download/nextcloud-uploads.json'
        ledger=json.loads(state.read_text()) if state.exists() else {}
        stamp=local.stat();fingerprint=[stamp.st_ino,stamp.st_size,stamp.st_mtime_ns]
        key=hashlib.sha256(json.dumps([self.share.url(),data['source'],item['path'],item['sha256'],variant,
                                    fingerprint if variant=='partial' else None]).encode()).hexdigest()
        old=ledger.get(key,{})
        journal=state.with_suffix('.jsonl')
        if old and old.get('state')!='uploaded' and journal.exists():
            completed=next((e for e in reversed(self.read(journal,[])) if e.get('key')==key and e.get('state')=='uploaded'),None)
            if completed:
                ledger[key]=completed;d.write_json(state,ledger)
                return
        if old.get('state') in ('uploaded','unconfirmed'):
            return
        row=dict(key=key,source=data['source'],path=item['path'],size=item['size'],sha256=item['sha256'],variant=variant,
                 target=target,upload_share=self.share.url(),read_share=Share(self.existing).url() if self.existing else None,
                 local=str(local),local_stat=fingerprint,payload_size=size,bytes_percent=percent,state='uploading',
                 time=datetime.now().astimezone().isoformat())
        ledger[key]=row;d.write_json(state,ledger)
        d.append_events(state.with_suffix('.jsonl'),[dict(row)])
        body=None
        try:
            for parent in list(PurePosixPath(target).parents)[::-1]:
                if str(parent)=='.':
                    continue
                try:
                    with self.share.request('MKCOL',str(parent)):
                        pass
                except urllib.error.HTTPError as error:
                    if error.code!=405:
                        raise
            print('NEXTCLOUD UPLOAD START: '+target,flush=True)
            with local.open('rb') as stream:
                body=Body(stream,size,self.stopping,local)
                headers={'Content-Length':str(size),'Content-Type':'application/octet-stream','If-None-Match':'*'}
                if variant=='original':
                    headers['OC-Checksum']='SHA256:'+item['sha256']
                with self.share.request('PUT',target,data=body,headers=headers) as response:
                    if response.status!=201:
                        raise RuntimeError('Nextcloud did not confirm creation of a new file')
                    receipt=dict(http_status=response.status,etag=response.headers.get('ETag'),file_id=response.headers.get('OC-FileId'))
            after=local.stat()
            if (body.sent!=size or [after.st_ino,after.st_size,after.st_mtime_ns]!=fingerprint
                    or variant=='original' and body.digest.hexdigest()!=item['sha256']):
                raise RuntimeError('Video changed during upload or was not sent completely')
            row.update(state='uploaded',sent_bytes=body.sent,payload_sha256=body.digest.hexdigest(),**receipt)
            print('NEXTCLOUD UPLOADED: '+target,flush=True)
        except BaseException as error:
            unknown_completion=not isinstance(error,urllib.error.HTTPError) and body is not None and body.sent==size
            row.update(state='unconfirmed' if getattr(error,'code',None)==412 or unknown_completion else 'pending',
                       error_type=type(error).__name__,http_status=getattr(error,'code',None),sent_bytes=body.sent if body else 0)
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
    parser.add_argument('--once',action='store_true',help='Process currently available local videos once')
    args=parser.parse_args()
    if args.poll_interval<=0 or args.timeout<=0:
        parser.error('Intervals must be positive')
    os.umask(0o077)
    stopping=Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stopping.set())
    control=args.queue if args.queue else args.destination/'.download/source.json'
    lockpath=control.with_suffix('.nextcloud.lock')
    with lockpath.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        sync=Sync(args.nextcloud_existing,args.nextcloud_upload,stopping,args.timeout)
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
