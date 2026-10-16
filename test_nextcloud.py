"""Remote copies never become local SHA proofs; upload receipts survive delayed listings."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import ssl
from threading import Barrier, Event, Lock
from types import SimpleNamespace
import unittest
import urllib.error
import urllib.parse
from unittest.mock import patch

import download
import nextcloud


class Response(io.BytesIO):
    status = 201
    headers = {'ETag': 'accepted'}


class NextcloudChecks(unittest.TestCase):
    def test_shared_budget_counts_only_successful_new_chunks(self):
        with nextcloud.SharedUploadBudget(self.root/'budget','nextcloud',2,8) as budget:
            sync=nextcloud.Sync(None,self.upload,Event(),chunk_size=4)
            sync.upload_budget=budget
            failed=False
            def request(share,method,path='',data=None,**kwargs):
                nonlocal failed
                if method=='PUT':
                    data.read()
                    if not failed:
                        failed=True
                        raise urllib.error.URLError(ssl.SSLEOFError(8,'TLS EOF'))
                return Response()
            with patch.object(nextcloud.Share,'request',request):
                sync.run_once([self.root])
            state=json.loads((self.root/'budget/state.json').read_text())
            self.assertEqual(state['controller']['total'],7)
            sync.run_once([self.root])
            self.assertEqual(json.loads((self.root/'budget/state.json').read_text())['controller']['total'],7)

    def test_upload_urlerror_logs_and_saves_underlying_reason(self):
        import errno
        error = urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, 'proxy refused'))
        output = io.StringIO()
        with patch.object(nextcloud.Share, 'listing', return_value=[]), \
                patch.object(nextcloud.Share, 'request', side_effect=error), \
                patch('sys.stdout', output):
            nextcloud.Sync(self.read, self.upload, Event()).run_once([self.root])
        self.assertIn('reason=', output.getvalue())
        self.assertIn('proxy refused', output.getvalue())
        self.assertIn(f'errno={errno.ECONNREFUSED}', output.getvalue())
        row = next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['error_type'], 'URLError')
        self.assertEqual(row['error_reason'], str(error.reason))
        self.assertEqual(row['error_errno'], errno.ECONNREFUSED)
        self.assertEqual(row['state'], 'creating_collection')
        self.assertEqual(download.read_events(self.state/'nextcloud-uploads.jsonl')[-1]['error_errno'], errno.ECONNREFUSED)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / '.download'
        self.state.mkdir()
        self.data = dict(source='https://disk.yandex.ru/d/test', title='ШАД осень 2023')
        self.content = b'lecture'
        self.item = dict(path='/Course/2023-09-15 Лекция.mp4', size=7,
                         sha256=hashlib.sha256(self.content).hexdigest(), file='https://example.invalid/file')
        self.read = 'https://example.invalid/index.php/s/read?dir=/'
        self.upload = 'https://example.invalid/index.php/s/upload'
        download.write_json(self.state / 'source.json', self.data)
        download.write_json(self.state / '01-manifest.json', [{k:self.item[k] for k in ('path','size','sha256')}])
        self.local = download.target(self.root, self.item, 'Course')
        self.local.parent.mkdir()
        self.local.write_bytes(self.content)
        stat = self.local.stat()
        self.proof = dict(self.item, status='valid', course='Course', time='2026-10-14T00:00:00+03:00',
                          mtime_ns=stat.st_mtime_ns, local=str(self.local))
        download.append_events(self.state / 'events.jsonl', [self.proof])

    def test_remote_candidate_matching_preserves_trailing_space_in_course_folder(self):
        item=dict(self.item,path='/Course /video.mp4')
        row=dict(path='2023-fall/Course /001 - video.mp4',size=item['size'],
                 directory=False,checksums=['SHA256:'+item['sha256']])
        with patch.object(nextcloud.Share,'listing',return_value=[row]):
            self.assertTrue(nextcloud.Index(self.root,self.read).contains(self.data,item))

    def test_numbered_names_match_only_the_same_course_size_and_checksum(self):
        index = nextcloud.Index(self.root, self.read)
        rows = [dict(path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4', size=7, directory=False, checksums=[])]
        with patch.object(nextcloud.Share, 'listing', return_value=rows):
            self.assertFalse(index.contains(self.data, self.item))
            self.assertFalse(index.contains(self.data, dict(self.item, size=8)))
            self.assertFalse(index.contains(self.data, dict(self.item, path='/Other/2023-09-15 Лекция.mp4')))
        rows[0]['checksums'] = ['SHA256:'+self.item['sha256']]
        with patch.object(nextcloud.Share, 'listing', return_value=rows):
            self.assertTrue(nextcloud.Index(self.root, self.read).contains(self.data, self.item))
        rows[0]['checksums'] = ['SHA256:'+'0'*64]
        with patch.object(nextcloud.Share, 'listing', return_value=rows):
            self.assertFalse(nextcloud.Index(self.root, self.read).contains(self.data, self.item))

    def test_partial_and_wrong_semester_are_never_full_copies(self):
        rows = [dict(path='2023-fall/Course/001 - 2023-09-15 Лекция (partially available 75.00%).mp4',
                     size=7, directory=False, checksums=[])]
        with patch.object(nextcloud.Share, 'listing', return_value=rows):
            self.assertFalse(nextcloud.Index(self.root, self.read).contains(self.data, self.item))
        with self.assertRaises(ValueError):
            nextcloud.remote_path(dict(title='personal'), self.item)

    def test_remote_copy_skips_transfer_without_claiming_a_local_file(self):
        self.local.unlink()
        (self.state / 'events.jsonl').unlink()
        with patch.object(download, 'list_course', return_value=[self.item]), \
                patch.object(nextcloud.Share, 'listing', return_value=[dict(
                    path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4', size=7, directory=False,
                    checksums=['SHA256:'+self.item['sha256']])]), \
                patch('pipeline.transfer_process', side_effect=AssertionError('downloaded existing remote copy')):
            self.assertEqual(download.download(self.data, ['Course'], self.root, retries=0,
                                               nextcloud_existing=self.read), 0)
        history = json.loads((self.state / 'course-history.json').read_text())['Course']
        self.assertEqual(history['verified'], 0)
        self.assertEqual(history['remote_available'], 1)
        self.assertFalse(self.local.exists())
        self.assertFalse(any(e.get('status')=='valid' for e in download.read_events(self.state/'events.jsonl')))

    def test_same_size_unverified_nextcloud_copy_does_not_skip_download(self):
        self.local.unlink()
        (self.state / 'events.jsonl').unlink()
        transfers = []

        def transfer(*args, **kwargs):
            transfers.append(True)
            self.local.write_bytes(self.content)
            return 0

        with patch.object(download, 'list_course', return_value=[self.item]), \
                patch.object(nextcloud.Share, 'listing', return_value=[dict(
                    path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4', size=7,
                    directory=False, checksums=[])]), \
                patch('pipeline.transfer_process', side_effect=transfer):
            self.assertEqual(download.download(self.data, ['Course'], self.root, retries=0,
                                               nextcloud_existing=self.read), 0)
        self.assertEqual(len(transfers), 1)
        status = json.loads((self.state / 'course-history.json').read_text())['Course']
        self.assertEqual((status['verified'], status['remote_available']), (1, 0))

    def test_successful_upload_is_not_repeated_before_public_listing_catches_up(self):
        sent = []
        def request(share, method, path='', data=None, headers=None,uploads=False):
            if method == 'PUT':
                sent.append((urllib.parse.unquote(headers['Destination'].split('/files/upload/',1)[1]), data.read(), headers))
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]), \
                patch.object(nextcloud.Share,'request',request):
            sync = nextcloud.Sync(self.read, self.upload, Event())
            sync.run_once([self.root])
            nextcloud.Sync(self.read, self.upload, Event()).run_once([self.root])
        self.assertEqual(len(sent),1)
        self.assertEqual(sent[0][0], '2023-fall/Course/001 - 2023-09-15 Лекция.mp4')
        self.assertEqual(sent[0][1],self.content)
        self.assertEqual(sent[0][2]['OC-Total-Length'],'7')
        journal=download.read_events(self.state/'nextcloud-uploads.jsonl')
        self.assertEqual(len([row for row in journal if row['state']=='uploaded']),1)
        self.assertEqual(journal[-1]['sent_bytes'],7)
        self.assertEqual(journal[-1]['payload_sha256'],self.item['sha256'])
        with patch.object(nextcloud.Share,'listing',return_value=[]):
            self.assertTrue(nextcloud.Index(self.root,self.read).contains(self.data,self.item))

    def test_old_upload_receipt_cannot_override_a_conflicting_remote_checksum(self):
        download.write_json(self.state / 'nextcloud-uploads.json', {'receipt': dict(
            state='uploaded', variant='original', read_share=nextcloud.Share(self.read).url(),
            source=self.data['source'], http_status=201, sent_bytes=self.item['size'],
            payload_sha256=self.item['sha256'], path=self.item['path'],
            size=self.item['size'], sha256=self.item['sha256'])})
        row = dict(path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4',
                   size=self.item['size'], directory=False, checksums=['SHA256:'+'0'*64])
        with patch.object(nextcloud.Share, 'listing', return_value=[row]):
            self.assertFalse(nextcloud.Index(self.root, self.read).contains(self.data, self.item))

    def test_index_caches_upload_receipts_until_ledger_changes(self):
        from datetime import datetime, timezone
        ledger = self.state / 'nextcloud-uploads.json'
        receipt = dict(state='uploaded', variant='original',
                       read_share=nextcloud.Share(self.read).url(), source=self.data['source'],
                       http_status=201, sent_bytes=self.item['size'],
                       payload_sha256=self.item['sha256'], path=self.item['path'],
                       size=self.item['size'], sha256=self.item['sha256'],
                       time=datetime.now(timezone.utc).isoformat())
        download.write_json(ledger, {'one': receipt})
        index = nextcloud.Index(self.root, self.read)
        original = Path.read_text
        reads = []
        def read(path, *args, **kwargs):
            if path == ledger:
                reads.append(path)
            return original(path, *args, **kwargs)
        with patch.object(nextcloud.Share, 'listing', return_value=[]), \
                patch.object(Path, 'read_text', read):
            for _ in range(4):
                self.assertTrue(index.contains(self.data, self.item))
            self.assertEqual(len(reads), 1)
            download.write_json(ledger, {})
            self.assertFalse(index.contains(self.data, self.item))
            self.assertEqual(len(reads), 2)

    def test_old_upload_receipt_survives_absent_remote_listing(self):
        from datetime import datetime, timedelta, timezone
        receipt = dict(state='uploaded', variant='original',
                       read_share=nextcloud.Share(self.read).url(),
                       source=self.data['source'], http_status=201,
                       sent_bytes=self.item['size'], payload_sha256=self.item['sha256'],
                       path=self.item['path'], size=self.item['size'], sha256=self.item['sha256'],
                       time=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat())
        download.write_json(self.state / 'nextcloud-uploads.json', {'old': receipt})
        with patch.object(nextcloud.Share, 'listing', return_value=[]):
            self.assertTrue(nextcloud.Index(self.root, self.read).contains(self.data, self.item))

    def test_old_upload_receipt_does_not_start_a_new_session(self):
        from datetime import datetime, timedelta, timezone
        sent = []
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method == 'PUT':
                sent.append((path, data.read()))
            return Response()
        with patch.object(nextcloud.Share, 'request', request):
            nextcloud.Sync(None, self.upload, Event()).run_once([self.root])
            ledger = self.state / 'nextcloud-uploads.json'
            rows = json.loads(ledger.read_text())
            first_upload_id = next(iter(rows.values()))['upload_id']
            for row in rows.values():
                row['time'] = (datetime.now(timezone.utc)-timedelta(days=7)).isoformat()
            download.write_json(ledger, rows)
            nextcloud.Sync(None, self.upload, Event()).run_once([self.root])
        receipts = json.loads(ledger.read_text())
        self.assertEqual(len(sent), 1)
        self.assertEqual(first_upload_id, next(iter(receipts.values()))['upload_id'])

    def test_uploader_never_repairs_another_process_unfinished_event(self):
        sync=nextcloud.Sync(None,self.upload,Event())
        journal=self.state/'events.jsonl'
        payload=json.dumps(dict(self.proof,status='invalid')).encode()
        prefix=payload[:len(payload)//2]
        with journal.open('ab') as stream:stream.write(prefix)
        before=journal.read_bytes()
        self.assertEqual(len(sync.jobs([self.root])),1)
        self.assertEqual(journal.read_bytes(),before)
        with journal.open('ab') as stream:stream.write(payload[len(prefix):]+b'\n')
        self.assertEqual(sync.jobs([self.root]),[])

    def test_uploader_waits_for_newline_before_consuming_a_tail_proof(self):
        sync=nextcloud.Sync(None,self.upload,Event())
        journal=self.state/'events.jsonl'
        payload=json.dumps(dict(self.proof,status='invalid')).encode()
        with journal.open('ab') as stream:stream.write(payload)
        before=journal.read_bytes()
        self.assertEqual(len(sync.jobs([self.root])),1)
        self.assertEqual(journal.read_bytes(),before)
        with journal.open('ab') as stream:stream.write(b'\n')
        self.assertEqual(sync.jobs([self.root]),[])

    def test_uploader_reuses_compact_proof_index_across_polls(self):
        download.append_events(self.state/'events.jsonl',
            [dict(self.proof,range_attempts=['history'*1000]*50)])
        sync=nextcloud.Sync(None,self.upload,Event())
        first=sync.jobs([self.root])
        self.assertEqual(len(first),1)
        with patch.object(download,'iter_events',
                          side_effect=AssertionError('Full proof journal rescanned')):
            second=sync.jobs([self.root])
        self.assertEqual(len(second),1)
        index=sync.event_indexes[self.root]
        self.assertNotIn('range_attempts',index.events[self.item['path']])
        self.assertEqual(index.events[self.item['path']]['mtime_ns'],self.proof['mtime_ns'])

    def test_sync_streams_source_events_without_retaining_bulk_jsonl_history(self):
        sync = nextcloud.Sync(self.read, self.upload, Event())
        journal = self.state / 'events.jsonl'
        with patch.object(nextcloud.Share, 'listing', return_value=[]), \
                patch.object(sync, 'upload') as upload, \
                patch.object(download, 'read_events',
                             side_effect=AssertionError('Entire event history loaded')):
            sync.run_once([self.root])
        upload.assert_called_once()
        self.assertNotIn(journal, sync.cache)

    def test_upload_journal_recovery_streams_without_caching_full_history(self):
        sent = []
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method == 'PUT':
                sent.append(path)
                data.read()
            return Response()
        ledger = self.state / 'nextcloud-uploads.json'
        with patch.object(nextcloud.Share, 'listing', return_value=[]), \
                patch.object(nextcloud.Share, 'request', request):
            sync = nextcloud.Sync(self.read, self.upload, Event())
            sync.run_once([self.root])
            rows = json.loads(ledger.read_text())
            entry = next(iter(rows.values()))
            entry['state'] = 'pending'
            download.write_json(ledger, rows)
            journal = ledger.with_suffix('.jsonl')
            download.append_events(journal, [dict(key='unrelated', state='uploaded') for _ in range(100)])
            with patch.object(download, 'read_events',
                              side_effect=AssertionError('Upload journal materialized in RAM')):
                sync.run_once([self.root])
            self.assertEqual(next(iter(json.loads(ledger.read_text()).values()))['state'], 'uploaded')
            self.assertEqual(len(sent), 1)
            self.assertNotIn(journal, sync.cache)

    def test_success_journal_cancels_an_accidental_duplicate_session(self):
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method == 'PUT':
                data.read()
            return Response()
        ledger = self.state / 'nextcloud-uploads.json'
        with patch.object(nextcloud.Share, 'request', request):
            nextcloud.Sync(None, self.upload, Event()).run_once([self.root])
        rows = json.loads(ledger.read_text())
        original_id = next(iter(rows.values()))['upload_id']
        for row in rows.values():
            row.update(state='pending', upload_id='accidental-duplicate-session', next_chunk=0, sent_bytes=0)
        download.write_json(ledger, rows)
        with patch.object(nextcloud.Share, 'request', side_effect=AssertionError('Repeated a completed upload')):
            nextcloud.Sync(None, self.upload, Event()).run_once([self.root])
        restored = next(iter(json.loads(ledger.read_text()).values()))
        self.assertEqual(restored['state'], 'uploaded')
        self.assertEqual(restored['upload_id'], original_id)

    def test_failed_or_ambiguous_upload_is_not_a_success_receipt(self):
        def request(share, method, path='', data=None, headers=None,uploads=False):
            if method=='PUT':data.read()
            if method=='MOVE':
                raise urllib.error.HTTPError('https://example.invalid/file',412,'exists',{},None)
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
        with patch.object(nextcloud.Share,'listing',return_value=[]):
            self.assertFalse(nextcloud.Index(self.root,self.read).contains(self.data,self.item))
        receipts=json.loads((self.state/'nextcloud-uploads.json').read_text())
        self.assertEqual(next(iter(receipts.values()))['state'],'unconfirmed')

    def test_changed_file_and_unverified_original_are_not_uploaded(self):
        self.local.write_bytes(b'changed')
        with patch.object(nextcloud.Share,'listing',return_value=[]), \
                patch.object(nextcloud.Share,'request',side_effect=AssertionError('unverified file uploaded')):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])

    def test_personal_root_catalog_never_sends_its_videos(self):
        download.write_json(self.state/'catalog.json',dict(self.data,root=True))
        with patch.object(nextcloud.Share,'request',side_effect=AssertionError('personal file sent')):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])

    def test_partial_upload_name_has_bytes_percent_and_original_number(self):
        partial=self.local.with_name('2023-09-15 Лекция (partially available).mp4')
        partial.write_bytes(b'partial')
        (self.state/'events.jsonl').unlink()
        download.write_json(self.state/'partial-copies.json',[dict(self.item,source=self.data['source'],
            recovered_path=str(partial.relative_to(self.root)),local_size=7,mtime_ns=partial.stat().st_mtime_ns,
            downloaded_bytes=5,downloaded_percent=71.428571)])
        sent=[]
        def request(share,method,path='',data=None,headers=None,uploads=False):
            if method=='PUT':sent.append((urllib.parse.unquote(headers['Destination'].split('/files/upload/',1)[1]),data.read()))
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
        self.assertEqual(sent,[('2023-fall/Course/001 - 2023-09-15 Лекция (partially available 71.43%).mp4',b'partial')])
        with patch.object(nextcloud.Share,'listing',return_value=[]):
            self.assertFalse(nextcloud.Index(self.root,self.read).contains(self.data,self.item))

    def test_share_paths_reject_escape_and_preserve_subfolder(self):
        share=nextcloud.Share('https://example.invalid/index.php/s/token?dir=/sub')
        self.assertEqual(share.url('Course/video.mp4'),'https://example.invalid/public.php/dav/files/token/sub/Course/video.mp4')
        for path in ('../other','Course/../video.mp4','file\n.mp4'):
            with self.assertRaises(ValueError):share.url(path)

    def test_file_request_nickname_is_sent_with_write_requests(self):
        sync=nextcloud.Sync(self.read,self.upload,Event(),nickname='Walterit')
        requests=[]
        def open_request(request,**kwargs):
            requests.append(request)
            return Response()
        with patch.object(sync.share.opener,'open',open_request):
            with sync.share.request('MKCOL','2024-spring'):pass
        self.assertEqual(requests[0].get_header('X-nc-nickname'),'Walterit')

    def test_unicode_move_destination_uses_utf8_header_bytes(self):
        share=nextcloud.Share(self.upload)
        requests=[]
        def open_request(request,**kwargs):
            requests.append(request)
            return Response()
        target='2023-fall/Курс/001 - Лекция (partially available 71.43%).mp4'
        with patch.object(share.opener,'open',open_request):
            with share.request('MOVE','upload/.file',headers={'Destination':share.url(target)},uploads=True):pass
        wire=requests[0].get_header('Destination').encode('latin1')
        self.assertEqual(wire.decode('utf8'),urllib.parse.unquote(share.url(target)))

    def test_session_stages_in_share_root_then_moves_to_course_folder(self):
        destinations={}
        def request(share,method,path='',data=None,headers=None,uploads=False):
            destinations[method]=headers['Destination']
            if method=='PUT':data.read()
            if method=='MOVE':self.assertNotIn('Content-Length',headers)
            response=Response()
            if method=='PUT':response.status=200
            return response
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
        base=nextcloud.Share(self.upload).url()
        self.assertNotIn('/',destinations['MKCOL'][len(base):])
        self.assertEqual(destinations['PUT'],destinations['MOVE'])
        self.assertIn('/2023-fall/Course/',destinations['MOVE'])

    def test_old_year_archive_uses_nested_semester_without_duplicate_folder(self):
        item=dict(self.item,path='/Весна 2021/Course/video.mp4')
        for title in ('ШАД 2021','ШАД весна 2021'):
            self.assertEqual(nextcloud.remote_path(dict(title=title),item),'2021-spring/Course/video.mp4')
        for title,prefix,expected in (
                ('ШАД 2021','Шад осень 2021','2021-fall/Course/video.mp4'),
                ('ШАД 2016',' Shad_autumn_2016','2016-fall/Course/video.mp4'),
                ('ШАД 2015','Shad_spring_2015','2015-spring/Course/video.mp4')):
            self.assertEqual(nextcloud.remote_path(dict(title=title),dict(self.item,path='/'+prefix+'/Course/video.mp4')),expected)

    def test_unavailable_catalog_neither_stops_download_nor_allows_blind_upload(self):
        self.local.unlink();(self.state/'events.jsonl').unlink()
        def transfer(*args,**kwargs):
            self.local.write_bytes(self.content)
            return 0
        with patch.object(download,'list_course',return_value=[self.item]), \
                patch.object(nextcloud.Share,'listing',side_effect=TimeoutError()),patch('pipeline.transfer_process',transfer):
            self.assertEqual(download.download(self.data,['Course'],self.root,retries=0,nextcloud_existing=self.read),0)
        with patch.object(nextcloud.Share,'listing',side_effect=TimeoutError()), \
                patch.object(nextcloud.Share,'request',side_effect=AssertionError('blind upload')):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])

    def test_http_503_after_sending_body_is_retried_immediately(self):
        sent=[]
        def request(share,method,path='',data=None,headers=None,uploads=False):
            if method=='PUT':
                sent.append(data.read())
                if len(sent)==1:raise urllib.error.HTTPError('https://example.invalid/file',503,'unavailable',{},None)
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            sync=nextcloud.Sync(self.read,self.upload,Event())
            sync.run_once([self.root])
        self.assertEqual(sent,[self.content,self.content])
        self.assertEqual(next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))['state'],'uploaded')

    def test_tls_eof_retries_current_block_before_next_file(self):
        second = dict(self.item, path='/Course/Z-last.mp4')
        local = download.target(self.root, second, 'Course')
        local.write_bytes(self.content)
        download.write_json(self.state/'01-manifest.json', [self.item, second])
        download.append_events(self.state/'events.jsonl', [dict(self.proof, **second, local=str(local), mtime_ns=local.stat().st_mtime_ns)])
        sent = []
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method == 'PUT':
                sent.append((path, data.read()))
                if len(sent) == 2:
                    raise urllib.error.URLError(ssl.SSLEOFError(8, 'TLS EOF'))
            return Response()
        with patch.object(nextcloud.Share, 'request', request):
            nextcloud.Sync(None, self.upload, Event(), chunk_size=4, transfers=1).run_once([self.root])
        self.assertEqual([body for _, body in sent], [b'lect', b'ure', b'ure', b'lect', b'ure'])
        self.assertEqual(sent[1][0], sent[2][0])
        self.assertTrue(all(r['state']=='uploaded' for r in json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))

    def test_started_upload_in_later_archive_precedes_new_files(self):
        other = self.root/'other-archive'
        state = other/'.download'
        state.mkdir(parents=True)
        source = dict(self.data, source='https://disk.yandex.ru/d/other', title='ШАД осень 2024')
        local = download.target(other, self.item, 'Course')
        local.parent.mkdir(); local.write_bytes(self.content)
        download.write_json(state/'source.json', source)
        download.write_json(state/'01-manifest.json', [self.item])
        download.append_events(state/'events.jsonl', [dict(self.proof, local=str(local), mtime_ns=local.stat().st_mtime_ns)])
        sent = []
        fail = True
        def request(share, method, path='', data=None, headers=None, uploads=False):
            nonlocal fail
            if method == 'PROPFIND':
                response = Response(b'<d:multistatus xmlns:d="DAV:"><d:response><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>')
                response.status = 207
                return response
            if method == 'PUT':
                sent.append(data.read())
                if len(sent)==2 and fail:
                    fail=False
                    raise TimeoutError()
            return Response()
        with patch.object(nextcloud.Share, 'request', request):
            with patch.object(nextcloud.network.RequestRetry, 'retry', return_value=False):
                nextcloud.Sync(None, self.upload, Event(), chunk_size=4, transfers=1).run_once([other])
            sent.clear()
            nextcloud.Sync(None, self.upload, Event(), chunk_size=4, transfers=1).run_once([self.root, other])
        self.assertEqual(sent, [b'ure', b'lect', b'ure'])

    def test_chunk_retry_is_bounded_and_preserves_progress(self):
        for error in (urllib.error.URLError(ssl.SSLEOFError(8, 'TLS EOF')),
                      nextcloud.http.client.BadStatusLine('broken status')):
            calls = []
            def request(share, method, path='', data=None, headers=None, uploads=False):
                if method == 'PUT':
                    calls.append(data.read())
                    raise error
                return Response()
            with self.subTest(error=type(error).__name__), patch.object(nextcloud.Share, 'request', request):
                nextcloud.Sync(None, self.upload, Event()).run_once([self.root])
                self.assertEqual(calls, [self.content]*3)
                row = next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
                self.assertEqual(row['state'], 'pending')
                self.assertEqual(row['next_chunk'], 0)

    def test_shutdown_interrupts_chunk_retry_without_losing_state(self):
        stopping = Event()
        sent = []
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method == 'PUT':
                sent.append(data.read())
                stopping.set()
                raise urllib.error.URLError(ssl.SSLEOFError(8, 'TLS EOF'))
            return Response()
        with patch.object(nextcloud.Share, 'request', request):
            nextcloud.Sync(None, self.upload, stopping).run_once([self.root])
        self.assertEqual(sent, [self.content])
        row = next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'], 'pending')
        self.assertEqual(row['next_chunk'], 0)

    def test_confirmed_chunks_are_not_sent_again_after_restart(self):
        sent=[]
        created=False
        fail=True
        def request(share,method,path='',data=None,headers=None,uploads=False):
            nonlocal created,fail
            if method=='PROPFIND':
                if not created:raise urllib.error.HTTPError('https://example.invalid',404,'expired',{},None)
                response=Response(b'<d:multistatus xmlns:d="DAV:"><d:response><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>')
                response.status=207
                return response
            if method=='MKCOL':
                if created:raise urllib.error.HTTPError('https://example.invalid',405,'exists',{},None)
                created=True
            if method=='PUT':
                sent.append((path,data.read()))
                if path.endswith('/00002') and fail:
                    fail=False
                    raise TimeoutError()
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
        self.assertEqual([body for _,body in sent],[b'lect',b'ure',b'ure'])
        self.assertEqual(sent[1][0],sent[2][0])

    def test_tls_failure_checking_upload_session_preserves_confirmed_chunks(self):
        sent = []
        created = False
        checks = 0
        def request(share, method, path='', data=None, headers=None, uploads=False):
            nonlocal created, checks
            if method in ('MKCOL', 'PROPFIND'):
                if created:
                    checks += 1
                    if checks == 1:
                        raise urllib.error.URLError(ssl.SSLEOFError(8, 'TLS EOF'))
                    if method == 'MKCOL':
                        raise urllib.error.HTTPError('https://example.invalid',405,'exists',{},None)
                    response = Response(b'<d:multistatus xmlns:d="DAV:"><d:response><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>')
                    response.status = 207
                    return response
                created = True
            if method == 'PUT':
                sent.append((path, data.read()))
                if len(sent) == 2:
                    raise TimeoutError('Interrupt second chunk')
            return Response()
        with patch.object(nextcloud.Share, 'listing', return_value=[]), patch.object(nextcloud.Share, 'request', request):
            with patch.object(nextcloud.network.RequestRetry, 'retry', return_value=False):
                nextcloud.Sync(self.read, self.upload, Event(), chunk_size=4).run_once([self.root])
            row = next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
            self.assertEqual(row['next_chunk'],1)
            self.assertEqual(row['sent_bytes'],4)
            nextcloud.Sync(self.read, self.upload, Event(), chunk_size=4).run_once([self.root])
        self.assertEqual([body for _,body in sent], [b'lect',b'ure',b'ure'])
        self.assertEqual(sent[1][0],sent[2][0])
        self.assertEqual(checks,2)
        row = next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'],'uploaded')

    def test_expired_session_lost_creation_response_does_not_skip_missing_chunks(self):
        sent=[]
        created=False
        expire=False
        def request(share,method,path='',data=None,headers=None,uploads=False):
            nonlocal created
            if method=='PROPFIND':
                raise urllib.error.HTTPError('https://example.invalid',404,'expired',{},None)
            if method=='MKCOL':
                if created:raise urllib.error.HTTPError('https://example.invalid',405,'exists',{},None)
                created=True
                if expire:raise urllib.error.URLError(ssl.SSLEOFError(8,'Creation response lost'))
            if method=='PUT':
                sent.append(data.read())
                if len(sent)==2:raise TimeoutError('Second chunk interrupted')
            return Response()
        with patch.object(nextcloud.network.RequestRetry,'retry',return_value=False), patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
            created=False;expire=True
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
            row=next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
            self.assertEqual(row['next_chunk'],0)
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
        self.assertEqual(sent,[b'lect',b'ure',b'lect',b'ure'])
        row=next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'],'uploaded')

    def test_lost_move_response_does_not_create_a_second_upload(self):
        sent=[]
        moves=[]
        def request(share,method,path='',data=None,headers=None,uploads=False):
            if method=='PUT':sent.append(data.read())
            if method=='MOVE':
                moves.append(path)
                if len(moves)==1:raise TimeoutError()
                raise urllib.error.HTTPError('https://example.invalid',404,'upload already moved',{},None)
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
        self.assertEqual(sent,[self.content])
        self.assertEqual(moves[0],moves[1])
        row=next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'],'unconfirmed')

    def test_expired_collection_reset_survives_disk_failure(self):
        import errno
        sent=[]
        created=False
        crash=False
        def request(share,method,path='',data=None,headers=None,uploads=False):
            nonlocal created
            if method=='PROPFIND':
                if not created:raise urllib.error.HTTPError('https://example.invalid',404,'expired',{},None)
                response=Response(b'<d:multistatus xmlns:d="DAV:"><d:response><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>')
                response.status=207
                return response
            if method=='MKCOL':
                if created:raise urllib.error.HTTPError('https://example.invalid',405,'exists',{},None)
                created=True
            if method=='PUT':
                sent.append(data.read())
                if len(sent)==2:raise TimeoutError()
            return Response()
        original=download.write_json
        def write_json(path,value):
            if crash and path.name=='nextcloud-uploads.json' and any(
                    r.get('state')=='uploading' and r.get('next_chunk')==0 for r in value.values()):
                raise OSError(errno.EIO,'disk disconnected after MKCOL')
            return original(path,value)
        with patch.object(nextcloud.network.RequestRetry,'retry',return_value=False), patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
            created=False;crash=True
            with patch.object(download,'write_json',write_json),self.assertRaises(OSError):
                nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
            crash=False
            nextcloud.Sync(self.read,self.upload,Event(),chunk_size=4).run_once([self.root])
        self.assertEqual(sent,[b'lect',b'ure',b'lect',b'ure'])

    def test_durable_success_journal_prevents_reupload_after_ledger_write_failure(self):
        import errno
        sent=[]
        def request(share,method,path='',data=None,headers=None,uploads=False):
            if method=='PUT':sent.append(data.read())
            return Response()
        original=download.write_json
        def write_json(path,value):
            if path.name=='nextcloud-uploads.json' and any(r.get('state')=='uploaded' for r in value.values()):
                raise OSError(errno.EIO,'disk disconnected after server confirmation')
            return original(path,value)
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            with patch.object(download,'write_json',write_json),self.assertRaises(OSError):
                nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
            nextcloud.Sync(self.read,self.upload,Event()).run_once([self.root])
        self.assertEqual(sent,[self.content])
        rows=json.loads((self.state/'nextcloud-uploads.json').read_text())
        self.assertEqual(next(iter(rows.values()))['state'],'uploaded')

    def test_stale_remote_proof_is_removed_before_failed_transfer_handoff(self):
        self.local.unlink();(self.state/'events.jsonl').unlink()
        download.write_json(self.state/'nextcloud-existing.json',dict(source=self.data['source'],
            share=nextcloud.Share(self.read).url(),files={self.item['path']:dict(size=7,sha256=self.item['sha256'])}))
        def failed_transfer(*args,**kwargs):
            self.assertFalse(nextcloud.remote_saved(self.state,self.item,self.read))
            return 1
        with patch.object(download,'list_course',return_value=[self.item]), \
                patch.object(nextcloud.Share,'listing',return_value=[]),patch('pipeline.transfer_process',failed_transfer):
            download.download(self.data,['Course'],self.root,retries=0,defer_range=True,nextcloud_existing=self.read)

    def test_remote_snapshot_can_be_validated_without_rereading_state(self):
        remote = nextcloud.Share(self.read).url()
        saved = dict(source=self.data['source'], share=remote,
                     files={self.item['path']: dict(size=self.item['size'], sha256=self.item['sha256'])})
        with patch.object(Path, 'read_text', side_effect=AssertionError('Unexpected disk reread')):
            self.assertEqual(nextcloud.saved_remote_files(self.state, self.read,
                              source=self.data['source'], saved=saved), saved['files'])
            self.assertEqual(nextcloud.saved_remote_files(self.state, self.read,
                              source='different', saved=saved), {})
            self.assertEqual(nextcloud.saved_remote_files(self.state, self.upload,
                              source=self.data['source'], saved=saved), {})

    def test_range_summary_reads_remote_index_once_for_many_files(self):
        import range_queue
        self.local.unlink()
        entries = [dict(self.item, path=f'/Course/lesson-{i}.mp4') for i in range(12)]
        download.write_json(self.state/'courses.json', ['Course'])
        download.write_json(self.state/'course-history.json', {'Course': dict(state='recovering')})
        download.write_json(self.state/'settings.json', dict(nextcloud_existing=self.read))
        download.write_json(self.state/'01-manifest.json',
                            [{k: item[k] for k in ('path', 'size', 'sha256')} for item in entries])
        download.write_json(self.state/'nextcloud-existing.json', dict(
            source=self.data['source'], share=nextcloud.Share(self.read).url(),
            files={item['path']: dict(size=item['size'], sha256=item['sha256']) for item in entries}))
        (self.state/'tasks').mkdir()
        bg = SimpleNamespace(root=self.root, state=self.state, source=self.data['source'],
                             directory=self.state/'tasks')
        originals = Path.read_text
        reads = []
        def track(path, *args, **kwargs):
            if path.name in ('nextcloud-existing.json', 'source.json'):
                reads.append(path.name)
            return originals(path, *args, **kwargs)
        with patch.object(Path, 'read_text', track):
            range_queue.finish_courses(bg, {})
        self.assertLessEqual(reads.count('nextcloud-existing.json'), 1)
        self.assertLessEqual(reads.count('source.json'), 1)
        row = json.loads((self.state/'course-history.json').read_text())['Course']
        self.assertEqual((row['verified'], row['remote_available'], row['failed']), (0, 12, 0))

    def test_range_summary_counts_remote_even_if_old_local_proof_is_stale(self):
        import range_queue
        self.local.unlink()
        (self.state/'tasks').mkdir()
        download.write_json(self.state/'courses.json',['Course'])
        download.write_json(self.state/'course-history.json',{'Course':dict(state='recovering')})
        download.write_json(self.state/'settings.json',dict(nextcloud_existing=self.read))
        download.write_json(self.state/'nextcloud-existing.json',dict(source=self.data['source'],
            share=nextcloud.Share(self.read).url(),files={self.item['path']:dict(size=7,sha256=self.item['sha256'])}))
        range_queue.finish_courses(SimpleNamespace(root=self.root,state=self.state,source=self.data['source'],directory=self.state/'tasks'),{})
        row=json.loads((self.state/'course-history.json').read_text())['Course']
        self.assertEqual((row['verified'],row['remote_available'],row['failed']),(0,1,0))





class ParallelUploadChecks(unittest.TestCase):
    setUp = NextcloudChecks.setUp

    def videos(self, count):
        items = [dict(self.item, path=f'/Course/{n:02}.mp4') for n in range(count)]
        for item in items:
            local = download.target(self.root, item, 'Course')
            local.write_bytes(self.content)
            download.append_events(self.state/'events.jsonl', [dict(self.proof, **item,
                local=str(local), mtime_ns=local.stat().st_mtime_ns)])
        download.write_json(self.state/'01-manifest.json', items)
        return items

    def test_four_uploads_overlap_without_losing_ledger_rows(self):
        self.videos(5)
        gate, lock = Barrier(4), Lock()
        sessions, active, peak = set(), 0, 0
        def request(share, method, path='', data=None, headers=None, uploads=False):
            nonlocal active, peak
            if method == 'PUT':
                with lock:
                    active += 1; peak = max(peak, active)
                    sessions.add(path.split('/')[0])
                    first_wave = len(sessions)<=4 and path.endswith('/00001')
                try:
                    data.read()
                    if first_wave: gate.wait(3)
                finally:
                    with lock: active -= 1
            return Response()
        with patch.object(nextcloud.Share, 'request', request):
            nextcloud.Sync(None, self.upload, Event(), chunk_size=4, transfers=4).run_once([self.root])
        self.assertEqual(peak, 4)
        rows = json.loads((self.state/'nextcloud-uploads.json').read_text())
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(r['state']=='uploaded' and r['next_chunk']==2 for r in rows.values()))
        successes = [r for r in download.iter_events(self.state/'nextcloud-uploads.jsonl') if r['state']=='uploaded']
        self.assertEqual({r['key'] for r in successes}, set(rows))

    def test_started_files_fill_the_first_slots_and_keep_original_numbers(self):
        items = self.videos(6)
        started = items[2:]
        sync = nextcloud.Sync(None, self.upload, Event(), transfers=4)
        download.write_json(self.state/'nextcloud-uploads.json', {str(n):dict(
            item, state='pending', next_chunk=1, source=self.data['source'],
            upload_share=sync.share.url(), nickname=None) for n,item in enumerate(started)})
        gate, lock = Barrier(4), Lock()
        seen = []
        def upload(root, data, item, local, variant, target, size, percent):
            with lock:
                seen.append((item['path'],target)); first_wave=len(seen)<=4
            if first_wave:gate.wait(3)
        with patch.object(sync, 'upload', side_effect=upload):
            sync.run_once([self.root])
        self.assertEqual({path for path,_ in seen[:4]}, {item['path'] for item in started})
        for path,target in seen:
            number=int(Path(path).stem)+1
            self.assertTrue(Path(target).name.startswith(f'{number:03} - '))

    def test_disk_failure_stops_active_and_queued_uploads(self):
        import errno
        self.videos(3)
        stopping, gate, lock = Event(), Barrier(2), Lock()
        calls = []
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method == 'PUT':
                data.read()
                with lock:
                    calls.append(path); first=len(calls)==1
                gate.wait(3)
                if first:raise OSError(errno.EIO, 'disk disconnected')
                if not stopping.wait(3):raise AssertionError('Other worker was not stopped')
                raise InterruptedError('Stopped')
            return Response()
        with patch.object(nextcloud.Share, 'request', request), self.assertRaises(OSError):
            nextcloud.Sync(None, self.upload, stopping, transfers=2).run_once([self.root])
        self.assertTrue(stopping.is_set())
        self.assertEqual(len(calls), 2)
        rows = json.loads((self.state/'nextcloud-uploads.json').read_text())
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r['next_chunk']==0 for r in rows.values()))

    def test_invalid_concurrency_is_rejected(self):
        with self.assertRaises(ValueError):
            nextcloud.Sync(None, self.upload, Event(), transfers=0)

    def test_disk_error_is_not_hidden_by_a_simultaneous_stop(self):
        import errno
        stopping = Event()
        sync = nextcloud.Sync(None, self.upload, stopping)
        real_wait = nextcloud.wait
        calls = 0
        def fail(job):
            stopping.set()
            raise OSError(errno.EIO, 'disk disconnected')
        def wait(futures, **kwargs):
            nonlocal calls
            result = real_wait(futures, **kwargs)
            calls += 1
            return SimpleNamespace(done=set()) if calls==1 else result
        with patch.object(sync, 'transfer', side_effect=fail), patch.object(nextcloud, 'wait', wait), self.assertRaises(OSError):
            sync.run_once([self.root])


class ExpiredMetadataChecks(unittest.TestCase):
    setUp = NextcloudChecks.setUp

    def error(self, message='Missing metadata for chunked upload. The distributed cache does not hold the information of previous requests.'):
        xml=('<d:error xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns">'
             '<s:exception>Sabre\\DAV\\Exception\\PreconditionFailed</s:exception>'
             '<s:message>'+message+'</s:message></d:error>').encode()
        return urllib.error.HTTPError('https://example.invalid',412,'Precondition failed',{},io.BytesIO(xml))

    def test_missing_metadata_restarts_only_the_failed_upload(self):
        sent, sessions = [], []
        def request(share, method, path='', data=None, headers=None, uploads=False):
            if method=='MKCOL':sessions.append(path)
            if method=='PUT':
                sent.append((path,data.read()))
                if len(sent)==2:raise self.error()
            return Response()
        with patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(None,self.upload,Event(),chunk_size=4,transfers=1).run_once([self.root])
        self.assertEqual([b for _,b in sent],[b'lect',b'ure',b'lect',b'ure'])
        self.assertEqual(len(sessions),2)
        self.assertNotEqual(sessions[0],sessions[1])
        rows=json.loads((self.state/'nextcloud-uploads.json').read_text())
        self.assertEqual(len(rows),1)
        self.assertEqual(next(iter(rows.values()))['state'],'uploaded')
        self.assertEqual(next(iter(rows.values()))['payload_sha256'],self.item['sha256'])

    def test_unrelated_412_does_not_reset_the_session(self):
        methods=[]
        def request(share, method, path='', data=None, headers=None, uploads=False):
            methods.append(method)
            if method=='PUT':
                data.read();raise self.error('Checksum mismatch')
            return Response()
        with patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(None,self.upload,Event(),transfers=1).run_once([self.root])
        self.assertEqual(methods.count('MKCOL'),1)
        self.assertEqual(methods.count('PUT'),1)
        row=next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'],'pending')
        self.assertEqual(row['error_reason'],'Checksum mismatch')

    def test_412_during_move_never_restarts_the_payload(self):
        methods=[]
        def request(share, method, path='', data=None, headers=None, uploads=False):
            methods.append(method)
            if method=='PUT':data.read()
            if method=='MOVE':raise self.error()
            return Response()
        with patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(None,self.upload,Event(),transfers=1).run_once([self.root])
        self.assertEqual(methods.count('MKCOL'),1)
        self.assertEqual(methods.count('PUT'),1)
        row=next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'],'unconfirmed')
        self.assertEqual(row['sent_bytes'],7)

    def test_repeated_metadata_loss_is_bounded(self):
        methods=[]
        def request(share, method, path='', data=None, headers=None, uploads=False):
            methods.append(method)
            if method=='PUT':
                data.read();raise self.error()
            return Response()
        with patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(None,self.upload,Event(),transfers=1).run_once([self.root])
        self.assertEqual(methods.count('MKCOL'),2)
        self.assertEqual(methods.count('PUT'),2)
        row=next(iter(json.loads((self.state/'nextcloud-uploads.json').read_text()).values()))
        self.assertEqual(row['state'],'pending')
        self.assertEqual(row['next_chunk'],0)


class AdaptiveUploadChecks(unittest.TestCase):
    def setUp(self):
        clock = patch.object(nextcloud.time, 'monotonic', return_value=0)
        self.clock = clock.start()
        self.addCleanup(clock.stop)
        self.control = nextcloud.UploadConcurrency(2, 8)

    def sample(self, rate, active=None):
        active = self.control.limit if active is None else active
        self.control.update(active)
        self.clock.return_value += 90
        self.control.confirmed(int(rate*90))
        return self.control.update(active)

    def test_grows_only_with_measured_gain_and_never_above_eight(self):
        self.assertEqual(self.control.limit, 2)
        self.assertEqual(self.sample(100), 3)
        for limit in range(4, 9):
            self.assertEqual(self.sample(100*2**(limit-3)), limit)
        self.assertEqual(self.sample(6400), 8)

    def test_failed_increase_rolls_back_and_later_tries_fewer_files(self):
        self.sample(100)
        self.assertEqual(self.sample(105), 2)
        self.assertEqual(self.sample(100), 2)
        self.clock.return_value += 300
        self.assertEqual(self.sample(100), 3)
        self.assertEqual(self.sample(200), 4)
        self.assertEqual(self.sample(200), 3)
        self.clock.return_value += 300
        self.assertEqual(self.sample(200), 2)
        self.assertEqual(self.sample(200), 2)

    def test_insufficient_work_and_draining_do_not_change_the_limit(self):
        self.assertEqual(self.sample(100, active=1), 2)
        self.sample(100)
        self.sample(100)
        self.assertEqual(self.control.limit, 2)
        self.assertEqual(self.sample(0, active=3), 2)
        self.assertIsNone(self.control.window)

    def test_idle_time_is_excluded_and_zero_speed_never_adds_connections(self):
        self.assertEqual(self.sample(0), 2)
        self.control.update(0)
        self.clock.return_value += 3600
        self.assertEqual(self.sample(100), 3)

    def test_bad_limits_are_rejected(self):
        for bounds in ((0,8),(3,2)):
            with self.assertRaises(ValueError):
                nextcloud.UploadConcurrency(*bounds)

    def test_incomplete_trial_is_abandoned_when_work_runs_out(self):
        self.sample(100)
        self.assertEqual(self.control.trial,(2,100))
        self.assertEqual(self.control.update(0),2)
        self.assertIsNone(self.control.trial)
        self.clock.return_value += 3600
        self.assertEqual(self.sample(20),3)
        self.assertEqual(self.control.trial,(2,20))

    def test_exact_ten_percent_gain_is_accepted(self):
        self.sample(100)
        self.assertEqual(self.sample(110),4)

    def test_downward_trial_refreshes_baseline_while_old_files_drain(self):
        self.control.limit=2
        self.control.trial=(3,100)
        self.control.direction=-1
        self.assertEqual(self.sample(200,active=3),2)
        self.assertEqual(self.control.trial,(3,200))
        self.assertEqual(self.sample(190),2)
        self.assertIsNone(self.control.trial)


class AdaptiveUploadIntegrationChecks(unittest.TestCase):
    setUp = NextcloudChecks.setUp
    videos = ParallelUploadChecks.videos

    def test_freed_slots_are_refilled_before_sampling(self):
        self.videos(8)
        sync = nextcloud.Sync(None,self.upload,Event(),transfers=8,auto_transfers=True)
        observed=[]
        real_update=sync.concurrency.update
        def update(active):
            observed.append(active)
            return real_update(active)
        real_wait=nextcloud.wait
        def wait(futures, **kwargs):
            return real_wait(futures)
        with patch.object(sync,'transfer'),patch.object(sync.concurrency,'update',update),patch.object(nextcloud,'wait',wait):
            sync.run_once([self.root])
        self.assertEqual(observed[:4],[2,2,2,2])
        self.assertEqual(observed[-1],0)

    def test_auto_starts_at_two_and_counts_only_confirmed_blocks(self):
        self.videos(5)
        sync = nextcloud.Sync(None,self.upload,Event(),chunk_size=4,transfers=8,auto_transfers=True)
        gate, lock = Barrier(2), Lock()
        sessions, failed, active, peak = set(), False, 0, 0
        def request(share, method, path='', data=None, headers=None, uploads=False):
            nonlocal failed, active, peak
            if method=='PUT':
                with lock:
                    active += 1; peak = max(peak,active)
                    first = path.split('/')[0] not in sessions and len(sessions)<2
                    sessions.add(path.split('/')[0])
                try:
                    data.read()
                    if first:gate.wait(3)
                    with lock:
                        if not failed:
                            failed=True
                            raise urllib.error.URLError(ssl.SSLEOFError(8,'TLS EOF'))
                finally:
                    with lock:active -= 1
            return Response()
        with patch.object(nextcloud.Share,'request',request):
            sync.run_once([self.root])
        self.assertEqual(peak,2)
        self.assertEqual(sync.concurrency.total,35)
        # Permanent receipts and resumed prefixes add no newly confirmed bytes.
        with patch.object(nextcloud.Share,'request',side_effect=AssertionError('Duplicate upload')):
            sync.run_once([self.root])
        self.assertEqual(sync.concurrency.total,35)

    def test_lower_limit_drains_started_files_without_starting_extras(self):
        self.videos(5)
        sync = nextcloud.Sync(None,self.upload,Event(),transfers=8,auto_transfers=True)
        sync.concurrency.limit=3
        gate, release, lock = Barrier(3), Event(), Lock()
        started, finished = [], []
        def transfer(job):
            with lock:
                started.append(job[3]['path']); number=len(started)
            if number<=3:
                gate.wait(3)
                if number<3 and not release.wait(3):
                    raise AssertionError('Scheduler did not drain active uploads')
            with lock:finished.append(job[3]['path'])
        real_wait = nextcloud.wait
        reduced = False
        def wait(futures, **kwargs):
            nonlocal reduced
            if reduced and not release.is_set():
                self.assertEqual(len(started),3)
                self.assertEqual(len(futures),2)
                release.set()
            result=real_wait(futures,**kwargs)
            if result.done and not reduced:
                sync.concurrency.limit=2
                reduced=True
            return result
        with patch.object(sync,'transfer',side_effect=transfer),patch.object(nextcloud,'wait',wait):
            sync.run_once([self.root])
        self.assertTrue(reduced)
        self.assertCountEqual(started,finished)
        self.assertEqual(len(finished),5)


class ListingRetryChecks(unittest.TestCase):
    setUp = NextcloudChecks.setUp

    def index(self, strict=True):
        stopping=Event()
        waiter=patch.object(stopping,'wait',return_value=False)
        wait=waiter.start();self.addCleanup(waiter.stop)
        jitter=patch.object(nextcloud.random,'uniform',return_value=1)
        jitter.start();self.addCleanup(jitter.stop)
        return nextcloud.Index(self.root,self.read,strict=strict,stopping=stopping),wait

    def test_tls_failure_retries_and_caches_only_successful_listing(self):
        index,wait=self.index()
        error=urllib.error.URLError(ssl.SSLEOFError(8,'TLS EOF'))
        with patch.object(nextcloud.Share,'listing',side_effect=[error,error,[]]) as listing:
            self.assertEqual(index.candidates(self.data,self.item),[])
            self.assertEqual(index.candidates(self.data,self.item),[])
        self.assertEqual(listing.call_count,3)
        self.assertEqual([call.args[0] for call in wait.call_args_list],[1,2])

    def test_failed_folder_can_be_read_again_after_cooldown(self):
        index,wait=self.index()
        with patch.object(nextcloud.time,'monotonic',return_value=100) as clock, \
                patch.object(nextcloud.Share,'listing',side_effect=TimeoutError()) as listing:
            with self.assertRaises(TimeoutError):index.candidates(self.data,self.item)
            self.assertEqual(listing.call_count,3)
            with self.assertRaises(TimeoutError):index.candidates(self.data,self.item)
            self.assertEqual(listing.call_count,3)
            clock.return_value=161
            listing.side_effect=None;listing.return_value=[]
            self.assertEqual(index.candidates(self.data,self.item),[])
            self.assertEqual(listing.call_count,4)

    def test_backoff_has_jitter_and_honors_short_retry_after(self):
        index,wait=self.index()
        with patch.object(nextcloud.random,'uniform',side_effect=[.75,1.25]), \
                patch.object(nextcloud.Share,'listing',side_effect=[TimeoutError(),TimeoutError(),[]]):
            index.candidates(self.data,self.item)
        self.assertEqual([call.args[0] for call in wait.call_args_list],[.75,2.5])
        index,wait=self.index()
        error=urllib.error.HTTPError('hidden',503,'Unavailable',{'Retry-After':'3'},None)
        with patch.object(nextcloud.Share,'listing',side_effect=[error,[]]):
            index.candidates(self.data,self.item)
        self.assertEqual(wait.call_args.args[0],3)

    def test_uploader_retries_only_the_read_share_and_then_uploads(self):
        stopping=Event()
        error=urllib.error.URLError(ssl.SSLEOFError(8,'TLS EOF'))
        seen=[]
        def listing(share,folder):
            self.assertEqual(share.url(),nextcloud.Share(self.read).url())
            seen.append(folder)
            if len(seen)==1:raise error
            return []
        def request(share,method,path='',data=None,headers=None,uploads=False):
            self.assertEqual(share.url(),nextcloud.Share(self.upload).url())
            if method=='PUT':data.read()
            return Response()
        with patch.object(stopping,'wait',return_value=False), \
                patch.object(nextcloud.Share,'listing',listing),patch.object(nextcloud.Share,'request',request):
            nextcloud.Sync(self.read,self.upload,stopping,transfers=1).run_once([self.root])
        self.assertEqual(seen,['2023-fall/Course']*2)
        rows=json.loads((self.state/'nextcloud-uploads.json').read_text())
        self.assertEqual(next(iter(rows.values()))['state'],'uploaded')

    def test_uploader_keeps_retry_after_between_passes(self):
        stopping=Event();sync=nextcloud.Sync(self.read,self.upload,stopping,transfers=1)
        error=urllib.error.HTTPError('hidden',429,'Slow down',{'Retry-After':'120'},None)
        with patch.object(nextcloud.time,'monotonic',return_value=100) as clock, \
                patch.object(nextcloud.Share,'listing',side_effect=error) as listing, \
                patch.object(sync,'upload') as upload:
            sync.run_once([self.root])
            clock.return_value=130
            sync.run_once([self.root])
            self.assertEqual(listing.call_count,1)
            upload.assert_not_called()
            clock.return_value=221;listing.side_effect=None;listing.return_value=[]
            sync.run_once([self.root])
            self.assertEqual(listing.call_count,2)
            upload.assert_called_once()

    def test_successful_catalog_is_refreshed_on_next_uploader_pass(self):
        sync=nextcloud.Sync(self.read,self.upload,Event(),transfers=1)
        rows=[dict(path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4',size=7,
                   directory=False,checksums=['SHA256:'+self.item['sha256']])]
        with patch.object(nextcloud.Share,'listing',side_effect=[rows,[]]) as listing,patch.object(sync,'upload') as upload:
            sync.run_once([self.root]);upload.assert_not_called()
            sync.run_once([self.root]);upload.assert_called_once()
        self.assertEqual(listing.call_count,2)

    def test_malformed_catalog_does_not_stop_other_course_uploads(self):
        other=dict(self.item,path='/Other/video.mp4')
        local=download.target(self.root,other,'Other');local.parent.mkdir();local.write_bytes(self.content)
        download.write_json(self.state/'01-manifest.json',[self.item,other])
        download.append_events(self.state/'events.jsonl',[dict(self.proof,**other,local=str(local),mtime_ns=local.stat().st_mtime_ns)])
        stopping=Event();sync=nextcloud.Sync(self.read,self.upload,stopping,transfers=1)
        def listing(share,folder):
            if folder.endswith('/Course'):raise nextcloud.ET.ParseError('truncated XML')
            return []
        with patch.object(stopping,'wait',return_value=False),patch.object(nextcloud.Share,'listing',listing), \
                patch.object(sync,'upload') as upload:
            sync.run_once([self.root])
        self.assertFalse(stopping.is_set())
        upload.assert_called_once()
        self.assertEqual(upload.call_args.args[2]['path'],other['path'])

    def test_retry_covers_truncated_response_and_transient_http_errors(self):
        import http.client
        for error in (http.client.IncompleteRead(b'partial'),nextcloud.ET.ParseError('truncated XML'),
                      urllib.error.HTTPError('hidden',500,'Server error',{},None)):
            index,wait=self.index()
            with patch.object(nextcloud.Share,'listing',side_effect=[error,[]]) as listing:
                self.assertEqual(index.candidates(self.data,self.item),[])
            self.assertEqual(listing.call_count,2)
            self.assertEqual(wait.call_count,1)

    def test_forbidden_and_missing_folders_are_not_retried(self):
        for status in (401,403,404):
            index,wait=self.index()
            with patch.object(nextcloud.Share,'listing',side_effect=urllib.error.HTTPError('hidden',status,'error',{},None)) as listing:
                for _ in range(2):
                    if status==404:self.assertEqual(index.candidates(self.data,self.item),[])
                    else:
                        with self.assertRaises(urllib.error.HTTPError):index.candidates(self.data,self.item)
            self.assertEqual(listing.call_count,1)
            self.assertEqual(wait.call_count,0)

    def test_retry_after_defers_long_wait_without_blocking_other_folders(self):
        index,wait=self.index()
        error=urllib.error.HTTPError('hidden',429,'Slow down',{'Retry-After':'120'},None)
        with patch.object(nextcloud.time,'monotonic',return_value=100), \
                patch.object(nextcloud.Share,'listing',side_effect=[error,[]]) as listing:
            with self.assertRaises(urllib.error.HTTPError):index.candidates(self.data,self.item)
            self.assertEqual(index.retry_at['2023-fall/Course'],220)
            self.assertEqual(index.candidates(self.data,dict(self.item,path='/Other/video.mp4')),[])
        self.assertEqual(wait.call_count,0)
        self.assertEqual(listing.call_count,2)

    def test_permission_error_also_honors_retry_after(self):
        index,wait=self.index()
        error=urllib.error.HTTPError('hidden',403,'Forbidden',{'Retry-After':'120'},None)
        with patch.object(nextcloud.time,'monotonic',return_value=100), \
                patch.object(nextcloud.Share,'listing',side_effect=error) as listing:
            with self.assertRaises(urllib.error.HTTPError):index.candidates(self.data,self.item)
        self.assertEqual(index.retry_at['2023-fall/Course'],220)
        self.assertEqual(listing.call_count,1)
        wait.assert_not_called()

    def test_shutdown_interrupts_retry_and_disk_error_is_not_cached(self):
        import errno
        index,wait=self.index()
        wait.side_effect=lambda _: index.stopping.set() or True
        with patch.object(nextcloud.Share,'listing',side_effect=TimeoutError()) as listing:
            with self.assertRaises(InterruptedError):index.candidates(self.data,self.item)
        self.assertEqual(listing.call_count,1)
        self.assertNotIn('2023-fall/Course',index.folders)
        index.stopping.clear();wait.side_effect=None
        with patch.object(nextcloud.Share,'listing',side_effect=OSError(errno.EIO,'disk failed')) as listing:
            with self.assertRaises(OSError):index.candidates(self.data,self.item)
        self.assertEqual(listing.call_count,1)
        self.assertNotIn('2023-fall/Course',index.folders)


if __name__ == '__main__':
    unittest.main()
