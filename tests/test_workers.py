from multiprocessing import JoinableQueue
from unittest.mock import MagicMock

from mock import create_autospec, patch

import monitor.workers.AgilentWorker
from rest.stasis import StasisClient


class _TestAgilentWorker(object):

    @patch('requests.post', autospec=True)
    def test_run(self):
        zip_q = JoinableQueue()
        zip_q.get = MagicMock(return_value='zipping')
        zip_q.task_done = MagicMock(return_value='done zipping')

        cnv_q = JoinableQueue()
        cnv_q.get = MagicMock(return_value='converting')
        cnv_q.task_done = MagicMock(return_value='done converting')

        st_mock = create_autospec(StasisClient)
        st_mock.set_tracking = MagicMock()

        agw = monitor.workers.AgilentWorker(st_mock, zip_q, cnv_q)

        target = agw.run()

        assert target != ""

        assert zip_q.get.called
        assert cnv_q.get.called
        print(st_mock.method_calls)
