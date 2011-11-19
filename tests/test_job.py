import thoonk
import unittest
from ConfigParser import ConfigParser
import threading


class TestJob(unittest.TestCase):

    def setUp(self, *args, **kwargs):
        conf = ConfigParser()
        conf.read('test.cfg')
        if conf.sections() == ['Test']:
            self.ps = thoonk.Thoonk(host=conf.get('Test', 'host'),
                                    port=conf.getint('Test', 'port'),
                                    db=conf.getint('Test', 'db'))
            self.ps.redis.flushdb()
        else:
            print 'No test configuration found in test.cfg'
            exit()

    def tearDown(self):
        self.ps.close()

    def test_10_basic_job(self):
        """Test job publish, retrieve, finish flow"""
        #publisher
        testjob = self.ps.job("testjob")
        self.assertEqual(testjob.get_ids(), [])
        
        id = testjob.put('9.0')
        
        #worker
        id_worker, job_content = testjob.get(timeout=3)
        self.assertEqual(job_content, '9.0')
        self.assertEqual(testjob.get_failure_count(id), 0)
        self.assertEqual(id_worker, id)
        testjob.finish(id_worker)
        
        self.assertEqual(testjob.get_ids(), [])
    
    def test_20_cancel_job(self):
        """Test cancelling a job"""
        j = self.ps.job("testjob")
        #publisher
        id = j.put('9.0')
        #worker claims
        id, job_content = j.get()
        self.assertEqual(job_content, '9.0')
        self.assertEqual(j.get_failure_count(id), 0)
        #publisher or worker cancels
        j.cancel(id)
        id2, job_content2 = j.get()
        self.assertEqual(j.get_failure_count(id), 1)
        self.assertEqual(job_content2, '9.0')
        self.assertEqual(id, id2)
        #cancel the work again
        j.cancel(id)
        # check the cancelled increment again
        id3, job_content3 = j.get()
        self.assertEqual(j.get_failure_count(id), 2)
        self.assertEqual(job_content3, '9.0')
        self.assertEqual(id, id3)
        #cleanup -- remove the job from the queue
        j.retract(id)
        self.assertEqual(j.get_ids(), [])

    def test_25_stall_job(self):
        """Test stalling a job"""
        testjob = self.ps.job("testjob")
        self.assertEqual(testjob.get_ids(), [])
        
        # put
        id = testjob.put('9.0')
        self.assertEqual(testjob.get_ids(), [id])
        
        # invalid stall
        self.assertRaises(thoonk.exceptions.JobNotClaimed, testjob.stall, id)
        
        # get
        id_worker, job_content = testjob.get(timeout=3)
        self.assertEqual(id_worker, id)
        self.assertEqual(job_content, '9.0')
        self.assertEqual(testjob.get_failure_count(id), 0)

        # invalid retry
        self.assertRaises(thoonk.exceptions.JobNotStalled, testjob.retry, id)
        
        # stall
        testjob.stall(id)
        self.assertEqual(testjob.get_ids(), [id])
        self.assertRaises(thoonk.exceptions.Empty, testjob.get, timeout=1)
        
        # retry
        testjob.retry(id)
        self.assertEqual(testjob.get_ids(), [id])
        
        # get
        id_worker, job_content = testjob.get(timeout=3)
        self.assertEqual(id_worker, id)
        self.assertEqual(job_content, '9.0')
        self.assertEqual(testjob.get_failure_count(id), 0)
        
        # finish
        testjob.finish(id_worker)
        self.assertEqual(testjob.get_ids(), [])
    
    def test_27_retract_job(self):
        """Test retracting a job"""
        testjob = self.ps.job("testjob")
        self.assertEqual(testjob.get_ids(), [])
        
        # put
        id = testjob.put('9.0')
        self.assertEqual(testjob.get_ids(), [id])
        
        # retract
        testjob.retract(id)
        self.assertEqual(testjob.get_ids(), [])
        
        # invalid retract
        self.assertRaises(thoonk.exceptions.ItemDoesNotExist, testjob.retract, id)
    
    def test_30_no_job(self):
        """Test exception raise when job.get times out"""
        j = self.ps.job("testjob")
        self.assertEqual(j.get_ids(), [])
        self.assertRaises(thoonk.exceptions.Empty, j.get, timeout=1)

class TestJobResult(unittest.TestCase):

    def setUp(self, *args, **kwargs):
        conf = ConfigParser()
        conf.read('test.cfg')
        if conf.sections() == ['Test']:
            self.ps = thoonk.Thoonk(host=conf.get('Test', 'host'),
                                    port=conf.getint('Test', 'port'),
                                    db=conf.getint('Test', 'db'),
                                    listen=True)
            self.ps.redis.flushdb()
        else:
            print 'No test configuration found in test.cfg'
            exit()

    def tearDown(self):
        self.ps.close()
    
    def test_10_job_result(self):
        """Test job result published"""

        create_event = threading.Event()
        def create_handler(name):
            self.assertEqual(name, "testjobresult")
            create_event.set()
        self.ps.register_handler("create", create_handler)

        #publisher
        testjob = self.ps.job("testjobresult")
        self.assertEqual(testjob.get_ids(), [])
        
        # Wait until the create event has been received by the ThoonkListener
        create_event.wait()
        
        id = testjob.put('9.0')
        
        #worker
        id_worker, job_content = testjob.get(timeout=3)
        self.assertEqual(job_content, '9.0')
        self.assertEqual(testjob.get_failure_count(id), 0)
        self.assertEqual(id_worker, id)
        
        result_event = threading.Event()
        def result_handler(name, id, result):
            self.assertEqual(name, "testjobresult")
            self.assertEqual(id, id_worker)
            self.assertEqual(result, "myresult")
            result_event.set()
        
        self.ps.register_handler("finish", result_handler)
        testjob.finish(id_worker, "myresult")
        result_event.wait(1)
        self.assertTrue(result_event.isSet(), "No result received!")
        self.assertEqual(testjob.get_ids(), [])
        self.ps.remove_handler("result", result_handler)
        
#suite = unittest.TestLoader().loadTestsFromTestCase(TestJob)

