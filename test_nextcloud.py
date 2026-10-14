"""Remote copies never become local SHA proofs; upload receipts survive delayed listings."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
from threading import Event
from types import SimpleNamespace
import unittest
import urllib.error
from unittest.mock import patch

import download
import nextcloud


class Response(io.BytesIO):
    status = 201
    headers = {'ETag': 'accepted'}


class NextcloudChecks(unittest.TestCase):
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

    def test_numbered_names_match_only_the_same_course_size_and_checksum(self):
        index = nextcloud.Index(self.root, self.read)
        rows = [dict(path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4', size=7, directory=False, checksums=[])]
        with patch.object(nextcloud.Share, 'listing', return_value=rows):
            self.assertTrue(index.contains(self.data, self.item))
            self.assertFalse(index.contains(self.data, dict(self.item, size=8)))
            self.assertFalse(index.contains(self.data, dict(self.item, path='/Other/2023-09-15 Лекция.mp4')))
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
                    path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4', size=7, directory=False, checksums=[])]), \
                patch('pipeline.transfer_process', side_effect=AssertionError('downloaded existing remote copy')):
            self.assertEqual(download.download(self.data, ['Course'], self.root, retries=0,
                                               nextcloud_existing=self.read), 0)
        history = json.loads((self.state / 'course-history.json').read_text())['Course']
        self.assertEqual(history['verified'], 0)
        self.assertEqual(history['remote_available'], 1)
        self.assertFalse(self.local.exists())
        self.assertFalse(any(e.get('status')=='valid' for e in download.read_events(self.state/'events.jsonl')))

    def test_successful_upload_is_not_repeated_before_public_listing_catches_up(self):
        sent = []
        def request(share, method, path='', data=None, headers=None):
            if method == 'PUT':
                sent.append((path, data.read(), headers))
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]), \
                patch.object(nextcloud.Share,'request',request):
            sync = nextcloud.Sync(self.read, self.upload, Event())
            sync.run_once([self.root])
            nextcloud.Sync(self.read, self.upload, Event()).run_once([self.root])
        self.assertEqual(len(sent),1)
        self.assertEqual(sent[0][0], '2023-fall/Course/001 - 2023-09-15 Лекция.mp4')
        self.assertEqual(sent[0][1],self.content)
        self.assertEqual(sent[0][2]['If-None-Match'],'*')
        journal=download.read_events(self.state/'nextcloud-uploads.jsonl')
        self.assertEqual(len([row for row in journal if row['state']=='uploaded']),1)
        self.assertEqual(journal[-1]['sent_bytes'],7)
        self.assertEqual(journal[-1]['payload_sha256'],self.item['sha256'])
        with patch.object(nextcloud.Share,'listing',return_value=[]):
            self.assertTrue(nextcloud.Index(self.root,self.read).contains(self.data,self.item))

    def test_failed_or_ambiguous_upload_is_not_a_success_receipt(self):
        def request(share, method, path='', data=None, headers=None):
            if method=='PUT':
                data.read()
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
        def request(share,method,path='',data=None,headers=None):
            if method=='PUT':sent.append((path,data.read()))
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

    def test_old_year_archive_uses_nested_semester_without_duplicate_folder(self):
        item=dict(self.item,path='/Весна 2021/Course/video.mp4')
        for title in ('ШАД 2021','ШАД весна 2021'):
            self.assertEqual(nextcloud.remote_path(dict(title=title),item),'2021-spring/Course/video.mp4')

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

    def test_http_503_after_sending_body_is_retried_on_next_cycle(self):
        sent=[]
        def request(share,method,path='',data=None,headers=None):
            if method=='PUT':
                sent.append(data.read())
                if len(sent)==1:raise urllib.error.HTTPError('https://example.invalid/file',503,'unavailable',{},None)
            return Response()
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(nextcloud.Share,'request',request):
            sync=nextcloud.Sync(self.read,self.upload,Event())
            sync.run_once([self.root]);sync.run_once([self.root])
        self.assertEqual(sent,[self.content,self.content])

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


if __name__=='__main__':unittest.main()
