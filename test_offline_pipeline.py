"""Offline originals are retained copies, not files on the current disk."""
import json
from pathlib import Path
from unittest.mock import patch
import unittest

import download as d
import pipeline
import report
import volume_catalog as v
import test_pipeline as fixtures

OLD='088d300e-87f5-4293-999d-80726a0b2250'
NEW='4b45c6a0-122e-4dc1-9175-04fc6c765330'

class OfflinePipelineChecks(unittest.TestCase):
    setUp=fixtures.PipelineChecks.setUp
    transfer=fixtures.PipelineChecks.transfer
    run_download=fixtures.PipelineChecks.run_download

    def catalog(self, verified=True):
        host=self.root/'host';host.mkdir(exist_ok=True)
        item=self.items['First']
        payload=dict(schema=1,volumes={OLD:dict(updated_ns=1,label='old',archives={'Archive':dict(files={item['path']:dict(size=item['size'],sha256=item['sha256'],status='verified' if verified else 'expected',local='Archive/First/video.mp4')})})})
        d.write_json(host/v.CATALOG,dict(payload=payload,sha256=v._digest(payload)))
        return host

    def test_offline_originals_skip_transfer_without_creating_local_sha_proofs(self):
        host=self.catalog()
        with patch.dict('os.environ',dict(YDS_VOLUME_CATALOG=str(host),YDS_VOLUME_UUID=NEW)):
            self.assertEqual(self.run_download(),0)
        self.assertFalse(self.calls)
        self.assertFalse((self.root/'First/video.mp4').exists())
        summary=json.loads((self.root/'.download/summary.json').read_text())
        self.assertEqual(summary['verified'],0)
        self.assertEqual(summary['offline_available'],2)
        self.assertTrue(summary['complete'])
        events=d.read_events(self.root/'.download/events.jsonl')
        self.assertTrue(all(e['status']=='stored_offline' for e in events))

    def test_offline_restart_reuses_course_metadata_but_removed_proof_downloads(self):
        host=self.catalog()
        with patch.dict('os.environ',dict(YDS_VOLUME_CATALOG=str(host),YDS_VOLUME_UUID=NEW)):
            self.assertEqual(self.run_download(),0)
            with patch.object(d,'list_course',side_effect=AssertionError('Unchanged offline course relisted')):
                self.assertEqual(d.download(self.data,['First','Second'],self.root,retries=0),0)
            self.catalog(verified=False)
            self.assertEqual(self.run_download(),0)
        self.assertEqual(self.calls,['First','Second'])
        summary=json.loads((self.root/'.download/summary.json').read_text())
        self.assertEqual(summary['verified'],2)
        self.assertEqual(summary['offline_available'],0)

    def test_offline_original_does_not_resume_interrupted_sha_or_touch_raw_body(self):
        import settings
        host=self.catalog();state=self.root/'.download';state.mkdir()
        item=self.items['First'];local=self.root/'First/video.mp4'
        local.parent.mkdir();local.write_bytes(b'badbody')
        for status in ('pending','running','done'):
            with self.subTest(status=status):
                bg=pipeline.Background(self.root,self.data['source'],0,settings.resolve())
                bg.checkpoint(item,'First',1,'verifying')
                d.write_json(bg.directory/'interrupted.json',dict(source=self.data['source'],kind='sha',
                    item=item,course='First',attempt=1,state=status))
                bg.close()
                with patch.dict('os.environ',dict(YDS_VOLUME_CATALOG=str(host),YDS_VOLUME_UUID=NEW)), \
                        patch.object(pipeline.Background,'submit',side_effect=AssertionError('Offline SHA resumed')):
                    self.assertEqual(self.run_download(),0)
                self.assertEqual(local.read_bytes(),b'badbody')
        self.assertFalse(self.calls)

    def test_report_keeps_owner_mark_for_offline_original(self):
        host=self.catalog()
        repo=self.root/'backup';(repo/'courses').mkdir(parents=True)
        card=repo/'courses/first.md'
        card.write_text('- Год: 2026\n- Источник: [disk]('+self.data['source']+')\n- Путь: `First`\n**Статус:** копия есть\n| Файл | Размер | Копия 1 | Копия 2 | Копия 3 |\n|---|---|---|---|---|\n| `video.mp4` | 7 | Walterit | Other | |\n')
        with patch.dict('os.environ',dict(YDS_VOLUME_CATALOG=str(host),YDS_VOLUME_UUID=NEW)):
            self.assertEqual(self.run_download(),0)
            report.update_report(self.root,repo,'Walterit')
        self.assertIn('| Walterit | Other |',card.read_text())
        self.assertFalse((self.root/'First/video.mp4').exists())

    def test_range_summary_does_not_hide_offline_copy_behind_stale_remote_identity(self):
        import range_queue
        import settings
        host=self.catalog()
        with patch.dict('os.environ',dict(YDS_VOLUME_CATALOG=str(host),YDS_VOLUME_UUID=NEW)):
            self.assertEqual(self.run_download(),0)
            bg=pipeline.Background(self.root,self.data['source'],0,settings.resolve(),defer_range=True)
            self.addCleanup(bg.close)
            with patch('nextcloud.saved_remote_files',return_value={self.items['First']['path']:dict(size=7,sha256='0'*64)}):
                range_queue.finish_courses(bg,{},['First'])
        summary=json.loads((self.root/'.download/summary.json').read_text())
        self.assertTrue(summary['complete'])
        self.assertEqual(summary['offline_available'],2)
