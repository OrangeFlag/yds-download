"""Personal files reserve shared slots without idling them when no personal work waits."""
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from upload_concurrency import SharedUploadBudget


class DownloadPriorityChecks(unittest.TestCase):
    def setUp(self):
        temp=TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.personal=SharedUploadBudget(self.root,'yandex',4,4,priority=True)
        self.addCleanup(self.personal.close)
        self.courses=SharedUploadBudget(self.root,'yandex',4,4)
        self.addCleanup(self.courses.close)

    def waiting(self,budget):
        with budget.state() as state:
            state['members'][budget.key]['waiting_slots'].append('waiting-file')

    def blocked(self,budget):
        stop=Event()
        with patch.object(stop,'wait',side_effect=lambda _:stop.set() or True):
            with self.assertRaises(InterruptedError):
                with budget.slot(stop):self.fail('Borrowed a reserved slot')

    def test_personal_waiters_reserve_three_of_four_slots(self):
        self.waiting(self.personal)
        with self.courses.slot(Event()):
            self.blocked(self.courses)
            with ExitStack() as stack:
                for _ in range(3):stack.enter_context(self.personal.slot(Event()))

    def test_idle_personal_queue_does_not_hold_unused_slots(self):
        with ExitStack() as stack:
            for _ in range(4):stack.enter_context(self.courses.slot(Event()))

    def test_multiple_course_sources_cannot_deadlock_personal_admission(self):
        with SharedUploadBudget(self.root,'nextcloud',4,4) as nc, SharedUploadBudget(self.root,'s3',4,4) as s3:
            self.waiting(self.personal);self.waiting(nc);self.waiting(s3)
            with self.courses.slot(Event()),ExitStack() as stack:
                for _ in range(3):stack.enter_context(self.personal.slot(Event()))

    def test_courses_keep_a_slot_when_personal_files_wait(self):
        self.waiting(self.courses)
        with ExitStack() as stack:
            for _ in range(3):stack.enter_context(self.personal.slot(Event()))
            self.blocked(self.personal)
            with self.courses.slot(Event()):pass


if __name__=='__main__':unittest.main()
