import json
import os
from pathlib import Path
from threading import Event
import unittest
from unittest.mock import patch

import download as d
import file_hashes
import pipeline
import recovery
import test_cloud_fallback as fixtures


class CtimeProofTests(unittest.TestCase):
    def setUp(self):
        fixtures.CloudChecks.setUp(self)
        self.local=d.target(self.root,self.item,'Course')
        self.local.parent.mkdir()
        self.local.write_bytes(self.body)

    def change_preserving_mtime(self, path):
        before=path.stat()
        path.write_bytes(b'x'*before.st_size)
        os.utime(path,ns=(before.st_atime_ns,before.st_mtime_ns))
        self.assertEqual(path.stat().st_mtime_ns,before.st_mtime_ns)
        self.assertNotEqual(path.stat().st_ctime_ns,before.st_ctime_ns)

    def cloud(self):
        return fixtures.CloudChecks.cloud(self)

    def test_inspect_rejects_post_hash_change_with_restored_mtime(self):
        original=d.sha256_file
        def change(path, stopping=None):
            actual=original(path,stopping)
            self.change_preserving_mtime(path)
            return actual
        with patch.object(d,'sha256_file',side_effect=change):
            self.assertEqual(d.inspect(self.root,self.item,'Course')[0],'invalid')

    def test_record_rejects_change_after_inspect_with_restored_mtime(self):
        stamp=pipeline.Background.stat(self.local)
        self.change_preserving_mtime(self.local)
        proof=d.record(self.state,self.item,'Course',1,'valid','checked',verified_stat=stamp)
        self.assertEqual(proof['status'],'invalid')

    def test_recovered_result_does_not_trust_changed_file(self):
        event=dict(self.event,range_recovered_complete=True,recovered_stat=pipeline.Background.stat(self.local))
        self.change_preserving_mtime(self.local)
        self.assertFalse(recovery.range_result_current(self.root,self.item,event))

    def test_partial_copy_records_ctime_and_preserves_a_changed_copy(self):
        archive=self.root/'corrupted/test/Course/Lecture.mp4'
        archive.parent.mkdir(parents=True)
        archive.write_bytes(self.body[:10])
        d.write_json(Path(str(archive)+'.ranges.json'),dict(size=self.item['size'],sha256=self.item['sha256'],ranges=[[0,10]]))
        event=dict(self.event,corrupted_path=str(archive.relative_to(self.root)))
        self.assertTrue(recovery.promote_partial(self.root,event))
        proof=json.loads((self.state/'partial-copies.json').read_text())[0]
        partial=self.root/proof['recovered_path']
        self.assertEqual(proof['ctime_ns'],partial.stat().st_ctime_ns)
        self.change_preserving_mtime(partial)
        with self.assertRaisesRegex(FileExistsError,'changed locally'):
            recovery.promote_partial(self.root,event)
        self.assertEqual(partial.read_bytes(),b'x'*10)

    def test_cloud_memory_cache_notices_restored_mtime(self):
        d.record(self.state,self.item,'Course',1,'valid','checked',local=self.local)
        cloud=self.cloud()
        self.assertIsNotNone(cloud._local(self.item))
        self.change_preserving_mtime(self.local)
        self.assertIsNone(cloud._local(self.item))

    def test_legacy_proof_is_verified_lazily_once_then_persisted(self):
        d.append_events(self.state/'events.jsonl',[dict(self.item,status='valid',local=str(self.local),
                        source=self.data['source'],mtime_ns=self.local.stat().st_mtime_ns)])
        cloud=self.cloud()
        with patch.object(d,'sha256_file',wraps=d.sha256_file) as sha:
            self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'valid')
            self.assertEqual(sha.call_count,1)
        with patch.object(d,'sha256_file',side_effect=AssertionError('Proof already upgraded')):
            self.assertFalse(self.cloud().ready(self.item))

    def test_legacy_proof_reuses_a_current_full_hash_cache(self):
        d.sha256_file(self.local)
        d.append_events(self.state/'events.jsonl',[dict(self.item,status='valid',local=str(self.local),
                        source=self.data['source'],mtime_ns=self.local.stat().st_mtime_ns)])
        with patch.object(d,'sha256_file',side_effect=AssertionError('Cached SHA reread')):
            self.assertIsNotNone(self.cloud()._local(self.item))

    def test_current_hash_cache_outlives_a_stale_event_timestamp(self):
        proof=dict(self.item,mtime_ns=self.local.stat().st_mtime_ns)
        os.utime(self.local,ns=(1,proof['mtime_ns']+1000000))
        d.sha256_file(self.local)
        self.assertTrue(file_hashes.proof_current(self.local,proof,root=self.root))

    def test_fallback_verified_copy_is_rechecked_after_ctime_change(self):
        fallback=self.cloud().sources[0]
        row=dict(source=self.data['source'],path=self.item['path'],state='verified',local=str(self.local),
                 local_stat=fallback.stamp(self.local))
        fallback.directory.mkdir()
        fallback.save(self.item,row)
        self.change_preserving_mtime(self.local)
        self.assertTrue(fallback.ready(self.item))


if __name__=='__main__':unittest.main()
