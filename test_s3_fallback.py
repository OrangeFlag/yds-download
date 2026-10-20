import hashlib
import io
import json
from pathlib import Path
from threading import Event
import unittest
from unittest.mock import patch
try:
    from botocore.exceptions import ClientError
except ImportError:
    raise unittest.SkipTest('Optional S3 tests require boto3')

import cloud_fallback
import download as d
import s3_fallback as s3
import test_cloud_fallback as fixtures


class Server:
    def __init__(self, body):
        self.body = body
        self.key = 'IFLED/0/renamed.mp4'
        self.etag = '"multipart-12"'
        self.lists = self.heads = self.gets = 0
        self.drop = False
        self.fail_listing = False

    def get_paginator(self, name):
        server = self
        class Pages:
            def paginate(self, **kwargs):
                server.lists += 1
                if server.fail_listing:
                    raise TimeoutError('offline')
                yield {'Contents': [dict(Key=server.key, Size=len(server.body), ETag=server.etag)]}
        return Pages()

    def head_object(self, **kwargs):
        self.heads += 1
        return dict(ContentLength=len(self.body), ETag=self.etag,
                    Metadata={'s3cmd-attrs': 'mtime:1/md5:' + hashlib.md5(self.body).hexdigest()})

    def get_object(self, **kwargs):
        self.gets += 1
        offset = int(kwargs.get('Range', 'bytes=0-')[6:-1])
        if offset:
            if kwargs['IfMatch'] != self.etag:
                raise ClientError({'Error': {'Code': 'PreconditionFailed'},
                                   'ResponseMetadata': {'HTTPStatusCode': 412}}, 'GetObject')
        body = self.body[offset:]
        class Interrupted(io.BytesIO):
            def read(stream, size=-1):
                if stream.tell():
                    raise TimeoutError('body interrupted')
                return super().read(4)
        stream = Interrupted(body) if self.drop else io.BytesIO(body)
        self.drop = False
        return dict(Body=stream, ContentLength=len(body), ETag=self.etag,
                    ContentRange=f'bytes {offset}-{len(self.body)-1}/{len(self.body)}',
                    ResponseMetadata={'HTTPStatusCode': 206 if offset else 200})


class S3Checks(unittest.TestCase):
    def setUp(self):
        fixtures.CloudChecks.setUp(self)
        self.item['md5'] = hashlib.md5(self.body).hexdigest()
        d.write_json(self.state/'01-manifest.json', [self.item])
        self.keyfile = self.root/'s3-key.json'
        self.keyfile.write_text(json.dumps(dict(accessKey='access', secretKey='secret')))
        self.config = dict(server='http://s3.example', bucket='vids', key_file=str(self.keyfile))
        self.server = Server(self.body)
        self.addCleanup(patch.stopall)
        patch.object(s3, 'CACHE_ROOT', self.root/'catalog-cache').start()
        patch.object(s3, 'create_client', return_value=self.server).start()
        s3.S3Index.catalogs.clear()

    def cloud(self):
        return cloud_fallback.CloudFallback(self.root, None, Event(), s3=self.config)

    def uploader(self):
        import opencloud
        from test_opencloud import Server as UploadServer
        local = d.target(self.root, self.item, 'Course')
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(self.body)
        d.append_events(self.state/'events.jsonl', [dict(self.item, status='valid',
            mtime_ns=local.stat().st_mtime_ns, local=str(local))])
        server = UploadServer(); server.size = len(self.body)
        uploader = opencloud.Uploader(server, Event(), s3_existing=s3.S3Index(self.config, Event()))
        return uploader, server, local

    def test_opencloud_skips_renamed_s3_original_without_reading_bodies(self):
        uploader, server, local = self.uploader()
        with patch('file_hashes.calculate', side_effect=AssertionError('Unnecessary hash')):
            self.assertEqual(uploader.run_once([self.root], upload=True), 0)
        self.assertFalse(server.calls)
        self.assertEqual((self.server.lists, self.server.heads, self.server.gets), (1, 1, 0))

    def test_opencloud_uses_cached_payload_hashes_to_skip_renamed_s3_copy(self):
        import file_hashes
        self.item.pop('md5'); d.write_json(self.state/'01-manifest.json', [self.item])
        uploader, server, local = self.uploader()
        file_hashes.calculate(local, Event(), root=self.root)
        with patch('file_hashes.calculate', side_effect=AssertionError('Hash cache ignored')), \
                patch.object(d, 'api', side_effect=AssertionError('Original hash cache should suffice')):
            self.assertEqual(uploader.run_once([self.root], upload=True), 0)
        self.assertFalse(server.calls)
        self.assertEqual(self.server.gets, 0)

    def test_opencloud_does_not_skip_a_conflicting_s3_hash_with_same_name_and_size(self):
        self.server.key = 'Other/001 - Lecture.mp4'
        self.server.body = b'x'*len(self.body)
        uploader, server, local = self.uploader()
        self.assertEqual(uploader.run_once([self.root], upload=True), 0)
        self.assertEqual(bytes(server.payload), self.body)
        self.assertEqual(self.server.gets, 0)

    def test_opencloud_resolves_s3_md5_before_trusting_name_and_size(self):
        self.item.pop('md5'); d.write_json(self.state/'01-manifest.json', [self.item])
        self.server.key = 'Other/001 - Lecture.mp4'
        self.server.body = b'x'*len(self.body)
        uploader, server, local = self.uploader()
        self.assertEqual(uploader.run_once([self.root], upload=True), 0)
        self.assertEqual(bytes(server.payload), self.body)
        self.assertEqual(self.server.gets, 0)

    def test_opencloud_skips_by_name_and_size_only_when_s3_has_no_hash(self):
        self.server.key = 'Other/001 - Lecture.mp4'
        head = self.server.head_object
        self.server.head_object = lambda **kwargs: dict(head(**kwargs), Metadata={})
        uploader, server, local = self.uploader()
        with patch('file_hashes.calculate', side_effect=AssertionError('Unnecessary hash')):
            self.assertEqual(uploader.run_once([self.root], upload=True), 0)
        self.assertFalse(server.calls)
        self.assertEqual(self.server.gets, 0)

    def test_opencloud_does_not_skip_a_different_lecture_number_without_hashes(self):
        self.item['path'] = '/Course/Lecture 10.mp4'
        d.write_json(self.state/'01-manifest.json', [self.item])
        self.server.key = 'Other/001 - Lecture 11.mp4'
        head = self.server.head_object
        self.server.head_object = lambda **kwargs: dict(head(**kwargs), Metadata={})
        uploader, server, local = self.uploader()
        self.assertEqual(uploader.run_once([self.root], upload=True), 0)
        self.assertEqual(bytes(server.payload), self.body)

    def test_s3_original_md5_suppresses_partial_and_is_cached_without_original_body(self):
        self.item.pop('md5'); d.write_json(self.state/'01-manifest.json', [self.item])
        uploader, server, local = self.uploader()
        job = list(uploader.jobs([self.root])[0])
        local.write_bytes(b'partial-copy')
        uploader.job_stamps.clear()
        job[5] = 'partial'; job[6] = local.stat().st_size
        job[7] = 71.43; job[10] = local.stat().st_mtime_ns
        metadata = dict(self.item, md5=hashlib.md5(self.body).hexdigest())
        with patch.object(uploader, 'jobs', return_value=[tuple(job)]), patch.object(d, 'api', return_value=metadata) as api:
            self.assertEqual(uploader.run_once([self.root], upload=True), 0)
            self.assertEqual(api.call_count, 1)
        with patch.object(d, 'api', side_effect=AssertionError('Source MD5 cache ignored')):
            self.assertTrue(uploader.original_exists(dict(self.item), self.root, self.data['source']))
        self.assertFalse(server.calls)
        self.assertEqual(self.server.gets, 0)

    def test_s3_original_md5_lookup_failure_defers_partial_without_upload(self):
        self.item.pop('md5'); d.write_json(self.state/'01-manifest.json', [self.item])
        uploader, server, local = self.uploader()
        job = list(uploader.jobs([self.root])[0]); local.write_bytes(b'partial-copy')
        uploader.job_stamps.clear()
        job[5] = 'partial'; job[6] = local.stat().st_size
        job[7] = 71.43; job[10] = local.stat().st_mtime_ns
        with patch.object(uploader, 'jobs', return_value=[tuple(job)]), patch.object(d, 'api', side_effect=TimeoutError('metadata offline')):
            self.assertEqual(uploader.run_once([self.root], upload=True), 1)
        self.assertFalse(server.calls)
        self.assertEqual(self.server.gets, 0)

    def test_conflicting_s3_sha_does_not_require_yandex_md5_lookup(self):
        self.item.pop('md5'); d.write_json(self.state/'01-manifest.json', [self.item])
        head = self.server.head_object
        self.server.head_object = lambda **kwargs: dict(head(**kwargs), Metadata={'sha256':'0'*64, 'md5':'1'*32})
        uploader, server, local = self.uploader()
        uploader.index = []
        with patch.object(d, 'api', side_effect=TimeoutError('metadata offline')) as api:
            self.assertFalse(uploader.original_exists(dict(self.item), self.root, self.data['source']))
            self.assertEqual(uploader.run_once([self.root], upload=True), 0)
            self.assertEqual(api.call_count, 0)
        self.assertEqual(bytes(server.payload), self.body)
        self.assertEqual(self.server.gets, 0)

    def test_lost_original_session_uses_known_payload_md5_without_source_lookup(self):
        import opencloud
        self.item.pop('md5'); d.write_json(self.state/'01-manifest.json', [self.item])
        uploader, server, local = self.uploader(); uploader.index = []
        job = uploader.jobs([self.root])[0]
        sha1 = hashlib.sha1(self.body).hexdigest()
        row = dict(path=self.item['path'], target='ingest/Walterit/2023-fall/Course/001 - Lecture.mp4',
                   payload_sha256=self.item['sha256'], payload_md5=hashlib.md5(self.body).hexdigest(),
                   size=len(self.body), state='pending', sent_bytes=7, session_url='https://cloud.example/lost')
        with patch.object(uploader, 'confirm_publication', side_effect=opencloud.PublicationPending()), \
                patch.object(d, 'api', side_effect=AssertionError('Known original payload hashes should suffice')):
            self.assertIsNone(uploader.recover_session(job, row, 'ingest/stage', sha1, job[10], True))
        self.assertEqual(row['state'], 'existing')
        self.assertFalse(server.calls)
        self.assertEqual(self.server.gets, 0)

    def test_opencloud_defers_upload_when_s3_refresh_fails_despite_cached_listing(self):
        uploader, server, local = self.uploader()
        self.server.body = b'x'*len(self.body)
        self.assertFalse(uploader.s3_exists(self.item))
        uploader.s3_existing.catalog['expires'] = 0
        self.server.fail_listing = True
        self.assertEqual(uploader.run_once([self.root], upload=True), 1)
        self.assertFalse(server.calls)
        self.assertEqual(self.server.gets, 0)

    def test_recovers_renamed_original_by_md5_and_does_not_download_twice(self):
        cloud = self.cloud()
        self.assertEqual(cloud.recover_all(self.item, self.event)['status'], 'valid')
        self.assertFalse(cloud.ready(self.item))
        self.assertIsNone(cloud.recover_all(self.item, self.event))
        self.assertEqual(self.server.gets, 1)
        self.assertEqual(d.target(self.root, self.item, 'Course').read_bytes(), self.body)
        ledger = ''.join(p.read_text() for p in self.state.glob('s3-downloads/*.json'))
        self.assertNotIn('secret', ledger)

    def test_resume_uses_range_and_if_match(self):
        cloud = self.cloud(); self.server.drop = True
        self.assertIsNone(cloud.recover_all(self.item, self.event))
        fallback = cloud.sources[0]
        row = fallback.read(self.item); row['retry_after'] = 0; fallback.save(self.item, row)
        self.assertEqual(cloud.recover_all(self.item, self.event)['status'], 'valid')
        self.assertEqual(self.server.gets, 2)

    def test_conflicting_md5_is_not_matched_by_name(self):
        self.server.key = 'Other/Lecture.mp4'
        self.item['md5'] = '0'*32
        index = self.cloud().sources[0].index
        self.assertEqual(index.candidates(self.data, self.item), [])
        self.assertEqual(self.server.gets, 0)

    def test_known_hash_is_preferred_over_fuzzy_name(self):
        cloud = self.cloud(); index = cloud.sources[0].index
        candidates = index.candidates(self.data, self.item)
        unknown = dict(candidates[0], path='A/001 - Lecture.mp4', checksums=[], match_rank=1)
        with patch.object(index, 'candidates', return_value=[unknown, *candidates]), \
                patch.object(index.share, 'request', wraps=index.share.request) as request:
            self.assertEqual(cloud.recover_all(self.item, self.event)['status'], 'valid')
        self.assertEqual(request.call_args.args[1], self.server.key)

    def test_changed_remote_etag_restarts_only_that_body(self):
        cloud = self.cloud(); self.server.drop = True
        self.assertIsNone(cloud.recover_all(self.item, self.event))
        fallback = cloud.sources[0]
        row = fallback.read(self.item); row['retry_after'] = 0; fallback.save(self.item, row)
        self.server.etag = '"replacement-12"'
        self.assertEqual(cloud.recover_all(self.item, self.event)['status'], 'valid')
        self.assertEqual(self.server.gets, 3)

    def test_first_catalog_failure_retries_after_cooldown(self):
        index = self.cloud().sources[0].index
        self.server.fail_listing = True
        for _ in range(2):
            with self.assertRaises(TimeoutError):
                index.candidates(self.data, self.item)
        self.assertEqual(self.server.lists, 1)
        self.server.fail_listing = False; index.catalog['retry_at'] = 0
        self.assertEqual(len(index.candidates(self.data, self.item)), 1)

    def test_same_size_and_similar_name_without_shared_hash_requires_local_sha(self):
        self.server.key = 'Other/001 - Lecture.mp4'
        self.item.pop('md5')
        self.server.body = b'x'*len(self.body)
        d.write_json(self.state/'01-manifest.json', [self.item])
        result = self.cloud().recover_all(self.item, self.event)
        self.assertEqual(result['status'], 'mismatch')
        self.assertIn('s3.example', result['local'])
        self.assertFalse(d.target(self.root, self.item, 'Course').exists())

    def test_metadata_cache_survives_restart_and_listing_errors_cool_down(self):
        index = self.cloud().sources[0].index
        self.assertEqual(len(index.candidates(self.data, self.item)), 1)
        s3.S3Index.catalogs.clear()
        index = self.cloud().sources[0].index
        self.assertEqual(len(index.candidates(self.data, self.item)), 1)
        self.assertEqual((self.server.lists, self.server.heads), (1, 1))
        index.catalog.update(expires=0)
        self.server.fail_listing = True
        for _ in range(2):
            self.assertEqual(len(index.candidates(self.data, self.item)), 1)
        self.assertEqual(self.server.lists, 2)

    def test_optional_cache_io_errors_do_not_block_video_recovery(self):
        with patch.object(Path, 'read_text', autospec=True, side_effect=PermissionError('cache denied')):
            # Only S3Index reads the optional cache here; credentials use the fake client.
            index = s3.S3Index(self.config, Event())
        with patch.object(index, 'cache_file', self.root/'unwritable'/'cache.json'), \
                patch.object(Path, 'mkdir', side_effect=OSError(28, 'cache full')):
            self.assertEqual(len(index.candidates(self.data, self.item)), 1)
        self.assertEqual(self.server.gets, 0)

    def test_personal_and_unlisted_files_are_not_requested(self):
        cloud = self.cloud()
        self.assertIsNone(cloud.recover_all(dict(self.item, path='/Unknown.mp4'), self.event))
        d.write_json(self.state/'catalog.json', dict(root=True))
        self.assertIsNone(cloud.recover_all(self.item, self.event))
        self.assertEqual(self.server.gets, 0)

    def test_http_config_rejects_credentials_paths_and_partial_flags(self):
        self.assertEqual(s3.configuration(**self.config), self.config)
        for server in ('http://user:pass@host', 'http://host/path', 'http://host/?token=x', 'file:///tmp/x'):
            with self.subTest(server=server), self.assertRaises(ValueError):
                s3.configuration(server, 'vids', self.keyfile)
        with self.assertRaises(ValueError):
            s3.configuration('http://host', 'vids', None)

    def test_binding_to_background_cancels_all_source_work(self):
        cloud = self.cloud(); background_stopping = Event()
        cloud.set_stopping(background_stopping)
        self.assertIs(cloud.sources[0].stopping, background_stopping)
        self.assertIs(cloud.sources[0].index.stopping, background_stopping)
        background_stopping.set()
        with self.assertRaises(InterruptedError):
            cloud.recover_all(self.item, self.event)
        self.assertEqual(self.server.gets, 0)

    def test_global_s3_flag_does_not_bind_a_fallback_to_personal_archives(self):
        import range_queue
        import test_range_queue
        state, queue = test_range_queue.SharedRangeChecks()._finished_archive_with_cached_range(self.root/'personal')
        catalog = json.loads((state/'catalog.json').read_text())
        d.write_json(state/'catalog.json', dict(catalog, root=True))
        stopping = Event()
        with patch.object(stopping, 'wait', side_effect=lambda _: stopping.set()), \
                patch.object(s3, 'create_client') as client:
            range_queue.run([queue], stopping=stopping, s3_fallback=self.config)
        client.assert_not_called()
        self.assertEqual(json.loads((state/'tasks/00.json').read_text())['state'], 'applied')
