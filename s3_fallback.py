"""Read-only S3 catalog and HTTP bodies for the existing SHA-verified fallback."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
from threading import Lock
import time
import urllib.error
import urllib.parse

import download as d
from nextcloud import VIDEO, similar_upload_name
import storage

CACHE_ROOT = Path.home()/'.cache/yds-download/s3'


def configuration(server=None, bucket=None, key_file=None):
    if server is None and bucket is None and key_file is None:
        return None
    if not all(isinstance(v, (str, Path)) and str(v).strip() for v in (server, bucket, key_file)):
        raise ValueError('S3 requires server, bucket and key file')
    server, bucket = str(server), str(bucket)
    parsed = urllib.parse.urlsplit(server)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in ('', '/') or parsed.query or parsed.fragment
            or any(c.isspace() for c in server) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket)
            or any(ord(c)<32 for c in str(key_file))):
        raise ValueError('Expected an HTTP(S) S3 origin and a bucket name')
    parsed.port
    return dict(server=server.rstrip('/'), bucket=bucket, key_file=str(Path(key_file).expanduser().resolve()))


def create_client(config):
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise ValueError('S3 fallback requires the optional boto3 package') from None
    credentials = json.loads(Path(config['key_file']).read_text())
    if not all(isinstance(credentials.get(k), str) and credentials[k] for k in ('accessKey', 'secretKey')):
        raise ValueError('S3 key JSON requires nonempty accessKey and secretKey')
    return boto3.client('s3', endpoint_url=config['server'], region_name='us-east-1',
        aws_access_key_id=credentials['accessKey'], aws_secret_access_key=credentials['secretKey'],
        config=Config(proxies={}, connect_timeout=5, read_timeout=30, retries={'max_attempts': 1},
                      s3={'addressing_style': 'path'}))


@contextmanager
def errors():
    from botocore.exceptions import BotoCoreError, ClientError
    try:
        yield
    except ClientError as error:
        code = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 500)
        message = error.response.get('Error', {}).get('Code', 'S3 error')
        raise urllib.error.HTTPError('S3', code, message, {}, None) from None
    except BotoCoreError as error:
        raise urllib.error.URLError(str(error)) from None


class Response:
    def __init__(self, result):
        self.body = result['Body']
        self.status = result['ResponseMetadata']['HTTPStatusCode']
        self.headers = {header: str(result[key]) for key, header in
                        [('ContentLength', 'Content-Length'), ('ContentRange', 'Content-Range'), ('ETag', 'ETag')]
                        if key in result}

    def read(self, amount=-1):
        with errors():
            return self.body.read(amount)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.body.close()


class Share:
    timeout = 30

    def __init__(self, config):
        self.client = create_client(config)
        self.bucket = config['bucket']

    def request(self, method, path, headers=None):
        if method != 'GET':
            raise ValueError('S3 fallback is read-only')
        headers = headers or {}
        arguments = dict(Bucket=self.bucket, Key=path)
        if 'Range' in headers:
            arguments['Range'] = headers['Range']
        if 'If-Range' in headers:
            arguments['IfMatch'] = headers['If-Range']
        try:
            with errors():
                return Response(self.client.get_object(**arguments))
        except urllib.error.HTTPError as error:
            if error.code != 412 or 'IfMatch' not in arguments:
                raise
            error.close()
            # S3 has If-Match, not If-Range; emulate If-Range's full-body fallback.
            with errors():
                return Response(self.client.get_object(Bucket=self.bucket, Key=path))


class S3Index:
    global_candidates = True
    catalogs = {}
    catalogs_lock = Lock()

    def __init__(self, config, stopping):
        self.config = configuration(**config)
        self.share = Share(self.config)
        self.link = self.config['server']+'/'+self.config['bucket']+'/'
        self.stopping, self.folders = stopping, {}
        secret = Path(self.config['key_file'])
        stat = secret.stat()
        identity = [self.link, str(secret), stat.st_ino, stat.st_mtime_ns, stat.st_size]
        key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
        self.cache_file = CACHE_ROOT/(key+'.json')
        with self.catalogs_lock:
            if key not in self.catalogs:
                if len(self.catalogs)>=16:
                    self.catalogs.pop(next(iter(self.catalogs)))
                saved = {}
                try:
                    saved = json.loads(self.cache_file.read_text())
                except (OSError, ValueError) as error:
                    if not isinstance(error, FileNotFoundError):
                        print(f'S3 CACHE WARNING: read; error={type(error).__name__}', flush=True)
                fresh = (isinstance(saved, dict) and saved.get('identity') == identity
                         and isinstance(saved.get('rows'), list) and isinstance(saved.get('heads'), dict)
                         and type(saved.get('time')) in (int, float)
                         and 0 <= time.time()-saved['time'] < 300)
                self.catalogs[key] = dict(rows=saved['rows'] if fresh else None,
                    heads=saved['heads'] if fresh else {}, identity=identity, retry_at=0, error=None,
                    expires=saved['time']+300 if fresh else 0, cache_disabled=False, lock=Lock())
            self.catalog = self.catalogs[key]

    def save(self):
        if self.catalog['cache_disabled']:
            return
        try:
            self.cache_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            d.write_json(self.cache_file, {k:self.catalog[k] for k in ('rows', 'heads', 'identity')} |
                         {'time':self.catalog['expires']-300})
        except OSError as error:
            self.catalog['cache_disabled'] = True
            print(f'S3 CACHE WARNING: persistence disabled; error={type(error).__name__}; errno={error.errno}', flush=True)

    def candidates(self, data, item, *, include_unmatched=False):
        catalog = self.catalog
        with catalog['lock']:
            if time.time() >= catalog['expires'] and time.time() >= catalog['retry_at']:
                try:
                    rows = []
                    with errors():
                        for page in self.share.client.get_paginator('list_objects_v2').paginate(Bucket=self.share.bucket):
                            if self.stopping.is_set():
                                raise InterruptedError('S3 catalog interrupted')
                            rows.extend(dict(path=o['Key'], size=o['Size'], etag=o.get('ETag'))
                                        for o in page.get('Contents', [])
                                        if PurePosixPath(o['Key']).suffix.casefold() in VIDEO)
                    catalog.update(rows=rows, heads={}, error=None, retry_at=0, expires=time.time()+300)
                    self.save()
                except (OSError, ValueError) as error:
                    if isinstance(error, InterruptedError) or storage.disk_error(error):
                        raise
                    catalog.update(error=error.with_traceback(None), retry_at=time.time()+60)
                    print(f'S3 CATALOG DEFERRED: cached={catalog["rows"] is not None}; '
                          f'retry_after_seconds=60; error={type(error).__name__}', flush=True)
            if catalog['rows'] is None:
                raise catalog['error'].with_traceback(None)
            result = []
            changed = False
            try:
                for row in catalog['rows']:
                    if row['size'] != item['size']:
                        continue
                    if self.stopping.is_set():
                        raise InterruptedError('S3 metadata interrupted')
                    key = row['path']
                    metadata = catalog['heads'].get(key)
                    if metadata is None:
                        try:
                            with errors():
                                head = self.share.client.head_object(Bucket=self.share.bucket, Key=key)
                        except urllib.error.HTTPError as error:
                            if error.code != 404:
                                raise
                            error.close()
                            continue
                        # A changed object is discovered by the next complete catalog refresh.
                        if head.get('ETag') != row['etag'] or head['ContentLength'] != row['size']:
                            continue
                        metadata = head.get('Metadata', {})
                        catalog['heads'][key] = metadata
                        changed = True
                    checks = {kind:metadata[kind].lower() for kind, length in
                              [('sha256',64), ('sha1',40), ('md5',32)]
                              if re.fullmatch('[a-fA-F0-9]{'+str(length)+'}', metadata.get(kind, ''))}
                    match = re.search(r'(?:^|/)md5:([a-fA-F0-9]{32})(?:/|$)', metadata.get('s3cmd-attrs', ''))
                    if match:
                        checks.setdefault('md5', match[1].lower())
                    common = next((k for k in ('sha256','sha1','md5') if checks.get(k) and item.get(k)), None)
                    if common:
                        matched = checks[common] == item[common].lower()
                    else:
                        matched = similar_upload_name(PurePosixPath(key).name, PurePosixPath(item['path']).name)
                    if matched or include_unmatched:
                        result.append(dict(row, directory=False, match_rank=(0 if common else 1) if matched else 2,
                                           checksums=[k.upper()+':'+v for k,v in checks.items()]))
            finally:
                if changed:
                    self.save()
            return result
