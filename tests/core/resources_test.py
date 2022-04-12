"""
Test script for resources
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, cmp_dict

import logging
logger = logging.getLogger('resources')

import os
import sys
import tempfile
import shutil
import time

import unittest
try:
    import cPickle as pickle
except:
    import pickle

try:
    import psutil
except ImportError:
    psutil = False

from iceprod.core import to_log
import iceprod.core.resources


class resources_test(unittest.TestCase):
    def setUp(self):        
        super(resources_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        base_env = dict(os.environ)
        def reset_env():
            for k in set(os.environ).difference(base_env):
                del os.environ[k]
            for k in base_env:
                os.environ[k] = base_env[k]
        self.addCleanup(reset_env)

    @unittest_reporter(name='Resources(cls)')
    def test_000_Resources(self):
        r = iceprod.core.resources.Resources
        for t in ('cpu','gpu','memory','disk','time'):
            self.assertIn(t,r.defaults)

    @unittest_reporter(name='Resources.__init__()')
    def test_001_Resources_init(self):
        r = iceprod.core.resources.Resources()
        for t in ('cpu','gpu','memory','disk','time'):
            self.assertIn(t,r.total)
            self.assertIn(t,r.available)
            self.assertEqual(r.total[t], r.available[t])

        raw = {'cpu':10}
        r2 = iceprod.core.resources.Resources(raw=raw, debug=True)
        self.assertEqual(r2.total['cpu'], 10)
        for t in ('gpu','memory','disk','time'):
            self.assertAlmostEqual(r2.total[t],r.total[t], delta=.1)

        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.4, 'disk':18.21,
               'time':12, 'blah':{1,2,3}}
        r2 = iceprod.core.resources.Resources(raw=raw, debug=True)
        for t in ('cpu','gpu','memory','disk'):
            self.assertEqual(r2.total[t],raw[t])
        self.assertAlmostEqual(r2.total['time'], raw['time'], delta=1)
        self.assertNotIn('blah', r2.total)

    @unittest_reporter(name='Resources.claim()')
    def test_010_Resources_claim(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)

        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)
        self.assertIn(task_id, r.claimed)
        for k in ('cpu','memory','disk','time'):
            self.assertEqual(c[k], reqs[k])
        self.assertEquals(c['gpu'], r.total['gpu'][:1])

        for k in ('cpu','memory','disk'):
            self.assertEquals(r.available[k], r.total[k]-c[k])
        self.assertAlmostEqual(r.available['time'], r.total['time'], delta=0.01)
        self.assertEquals(r.available['gpu'], r.total['gpu'][1:])

        task_id2 = 'bar'
        reqs2 = {'cpu':4, 'gpu':1, 'memory':0.7, 'disk':3.4, 'time': 9}
        c2 = r.claim(task_id2, reqs2)
        self.assertEquals(c2['gpu'], r.total['gpu'][1:2])
        for k in ('cpu','memory','disk'):
            self.assertEquals(r.available[k], r.total[k]-c[k]-c2[k])
        self.assertAlmostEqual(r.available['time'], r.total['time'], delta=0.01)
        self.assertEquals(r.available['gpu'], r.total['gpu'][2:])

        with self.assertRaisesRegexp(Exception, "resources available"):
            r.claim('baz', reqs2)

        with self.assertRaisesRegexp(Exception, "resources available"):
            r.claim('baz', {'memory':10})
            
        with self.assertRaisesRegexp(Exception, "bad resource"):
            r.claim('baz', {'foo':'bar'})

    @unittest_reporter(name='Resources.claim() - all')
    def test_011_Resources_claim(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)

        task_id = 'foo'
        reqs = None
        c = r.claim(task_id, reqs)
        self.assertIn(task_id, r.claimed)
        for k in ('cpu','memory','disk'):
            self.assertEqual(c[k], raw[k])
        self.assertAlmostEqual(c['time'], raw['time'], delta=.2)
        self.assertEquals(c['gpu'], r.total['gpu'])

    @unittest_reporter(name='Resources.release()')
    def test_020_Resources_release(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)

        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)
        r.release(task_id)
        self.assertCountEqualRecursive(r.total, r.available, skip=['time'])

        r.release('baz')
        self.assertCountEqualRecursive(r.total, r.available, skip=['time'])

    @unittest_reporter(name='Resources.register_process()')
    def test_030_Resources_register_process(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)

        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)

        proc = 'the process'
        tmpdir = '/the/tmp/dir'
        r.register_process(task_id, proc, tmpdir)
        self.assertEqual(r.claimed[task_id]['process'], proc)
        self.assertEqual(r.claimed[task_id]['tmpdir'], tmpdir)

        # nothing should happen
        cc = dict(r.claimed)
        r.register_process('baz', proc, tmpdir)
        self.assertCountEqualRecursive(cc, r.claimed)

    @unittest_reporter(name='Resources.get_usage()', skip=not psutil)
    def test_040_Resources_get_usage(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw, debug=True)
        r.lookup_intervals['memory'] = 0.1

        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':0, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)
        proc = psutil.Process()
        tmpdir = self.test_dir
        r.register_process(task_id, proc, tmpdir)

        usage = r.get_usage(task_id)
        logger.info('usage: %r', usage)

        # make more memory and disk
        open(os.path.join(tmpdir,'blah'),'w').write(''.join(map(str,range(10000))))

        # second lookup should be cached, except time
        usage2 = r.get_usage(task_id)
        logger.info('usage: %r', usage)
        for k in ('cpu','memory','disk'):
            self.assertEqual(usage[k], usage2[k])
        self.assertGreater(usage2['time'], usage['time'])

        time.sleep(0.2)
        
        # third lookup should update memory, not disk
        usage2 = r.get_usage(task_id)
        self.assertGreater(usage2['memory'], usage['memory'])
        self.assertEqual(usage2['disk'], usage['disk'])
        self.assertGreater(usage2['time'], usage['time'])
        
        # force disk lookup
        usage2 = r.get_usage(task_id, force=True)
        self.assertGreater(usage2['memory'], usage['memory'])
        self.assertGreater(usage2['disk'], usage['disk'])
        self.assertGreater(usage2['time'], usage['time'])

    @unittest_reporter(name='Resources.get_usage() - errors', skip=not psutil)
    def test_041_Resources_get_usage(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw, debug=True)
        task_id = 'foo'

        with self.assertRaisesRegexp(Exception, "unknown claim"):
            r.get_usage(task_id)

        reqs = {'cpu':1, 'gpu':1, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)

        with self.assertRaisesRegexp(Exception, "no process"):
            r.get_usage(task_id)

        proc = psutil.Process()
        tmpdir = None
        r.register_process(task_id, proc, tmpdir)

        with self.assertRaisesRegexp(Exception, "no tmpdir"):
            r.get_usage(task_id)

        proc = psutil.Process()
        tmpdir = self.test_dir
        r.register_process(task_id, proc, tmpdir)

        iceprod.core.resources.psutil = None
        with self.assertRaisesRegexp(Exception, "psutil not available"):
            r.get_usage(task_id)
        iceprod.core.resources.psutil = psutil

    @unittest_reporter(name='Resources.check_claims()')
    def test_050_Resources_check_claims(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':.4, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)
        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':.25, 'disk':3.4, 'time': 1}
        c = r.claim(task_id, reqs)
        proc = psutil.Process()
        tmpdir = self.test_dir
        r.register_process(task_id, proc, tmpdir)

        ret = r.check_claims()
        self.assertEqual(ret, {})

    @unittest_reporter(name='Resources.check_claims() - time overuse')
    def test_051_Resources_check_claims(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':.4, 'disk':20,
               'time':.1001}
        r = iceprod.core.resources.Resources(raw=raw)
        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':.25, 'disk':3.4, 'time': .000001}
        c = r.claim(task_id, reqs)
        proc = psutil.Process()
        tmpdir = self.test_dir
        r.register_process(task_id, proc, tmpdir)

        ret = r.check_claims()
        self.assertEqual(ret, {})

        # test managable overusage
        time.sleep(0.1)
        ret = r.check_claims(force=True)
        logger.info('check_claims ret: %r',ret)
        self.assertEqual(ret, {})

        # test overusage above total
        time.sleep(0.4)
        ret = r.check_claims(force=True)
        logger.info('check_claims ret: %r',ret)
        logger.info('avail: %r',r.available)
        logger.info('used: %r',r.used)
        self.assertIn(task_id, ret)

    @unittest_reporter(name='Resources.check_claims() - memory overuse')
    def test_052_Resources_check_claims(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':.4, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)
        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':.25, 'disk':3.4, 'time': 1}
        c = r.claim(task_id, reqs)
        proc = psutil.Process()
        tmpdir = self.test_dir
        r.register_process(task_id, proc, tmpdir)

        ret = r.check_claims()
        self.assertEqual(ret, {})

        # test managable overusage
        import array
        blah = array.array('L',[0])
        blah *= 2**26 # 256 MB
        ret = r.check_claims(force=True)
        logger.info('check_claims ret: %r',ret)
        self.assertEqual(ret, {})

        # test overusage above total
        blah *= 2 # 512 MB
        ret = r.check_claims(force=True)
        logger.info('check_claims ret: %r',ret)
        self.assertIn(task_id, ret)

    @unittest_reporter(name='Resources.get_peak()')
    def test_080_Resources_get_peak(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20, 'time':12}
        r = iceprod.core.resources.Resources(raw=raw)
        used = {
            'cpu':{'max':8.,'cnt':1,'avg':8.},
            'gpu':{'max':2.,'cnt':1,'avg':2.},
            'memory':{'max':3.5,'cnt':1,'avg':3.5},
            'disk':{'max':20.,'cnt':1,'avg':20.},
            'time':{'max':12.,'cnt':1,'avg':12.},
        }
        r.used['foo'] = used
        self.assertEqual(r.get_peak('foo'),
            {'cpu':8, 'gpu':2, 'memory':3.5, 'disk':20, 'time':12})

        self.assertFalse(r.get_peak('baz'))

    @unittest_reporter(name='Resources.set_env()')
    def test_090_Resources_set_env(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)

        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':0, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)

        r.set_env(c)
        self.assertEqual(os.environ['CUDA_VISIBLE_DEVICES'],'9999')
        self.assertEqual(os.environ['GPU_DEVICE_ORDINAL'],'9999')

    @unittest_reporter(name='Resources.set_env() - gpus')
    def test_091_Resources_set_env(self):
        raw = {'cpu':8, 'gpu':['0','1'], 'memory':3.5, 'disk':20,
               'time':12}
        r = iceprod.core.resources.Resources(raw=raw)

        task_id = 'foo'
        reqs = {'cpu':1, 'gpu':1, 'memory':2.1, 'disk':3.4, 'time': 9}
        c = r.claim(task_id, reqs)

        r.set_env(c)
        self.assertEqual(os.environ['CUDA_VISIBLE_DEVICES'],','.join(c['gpu']))
        self.assertEqual(os.environ['GPU_DEVICE_ORDINAL'],','.join(c['gpu']))

    @unittest_reporter
    def test_100_get_cpus(self):
        ret = iceprod.core.resources.get_cpus()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['cpu'])

        os.environ['NUM_CPUS'] = '13'
        ret = iceprod.core.resources.get_cpus()
        self.assertEqual(ret, 13)

        os.environ['NUM_CPUS'] = '2.4'
        ret = iceprod.core.resources.get_cpus()
        self.assertEqual(ret, 2)

        os.environ['NUM_CPUS'] = 'blah'
        ret = iceprod.core.resources.get_cpus()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['cpu'])
        
        with open('.machine.ad', 'w') as f:
            f.write("""
                totalcpus = 12
            """)
        ret = iceprod.core.resources.get_cpus()
        self.assertEqual(ret, 12)

    @unittest_reporter
    def test_110_get_gpus(self):
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['gpu'])

        os.environ['NUM_GPUS'] = '2'
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, ['0','1'])

        os.environ['NUM_GPUS'] = 'blah'
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['gpu'])
        del os.environ['NUM_GPUS']

        os.environ['CUDA_VISIBLE_DEVICES'] = 'CUDA0'
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, ['CUDA0'])

        os.environ['CUDA_VISIBLE_DEVICES'] = 'CUDA4,CUDA6'
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, ['CUDA4','CUDA6'])
        del os.environ['CUDA_VISIBLE_DEVICES']

        os.environ['GPU_DEVICE_ORDINAL'] = 'OCL2,OCL5'
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, ['OCL2','OCL5'])
        del os.environ['GPU_DEVICE_ORDINAL']

        os.environ['_CONDOR_AssignedGPUs'] = '1,2'
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, ['1','2'])
        
        with open('.machine.ad', 'w') as f:
            f.write("""
                assignedgpus = "foo,bar"
            """)
        ret = iceprod.core.resources.get_gpus()
        self.assertEqual(ret, ['foo','bar'])

    @unittest_reporter
    def test_120_get_memory(self):
        ret = iceprod.core.resources.get_memory()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['memory'])

        os.environ['NUM_MEMORY'] = '13'
        ret = iceprod.core.resources.get_memory()
        self.assertEqual(ret, 13.0)

        os.environ['NUM_MEMORY'] = '2.4'
        ret = iceprod.core.resources.get_memory()
        self.assertEqual(ret, 2.4)

        os.environ['NUM_MEMORY'] = 'blah'
        ret = iceprod.core.resources.get_memory()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['memory'])
        
        with open('.machine.ad', 'w') as f:
            f.write("""
                totalmemory = 123000
            """)
        ret = iceprod.core.resources.get_memory()
        self.assertAlmostEqual(ret, 123.0)

    @unittest_reporter
    def test_130_get_disk(self):
        ret = iceprod.core.resources.get_disk()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['disk'])

        os.environ['NUM_DISK'] = '13'
        ret = iceprod.core.resources.get_disk()
        self.assertEqual(ret, 13.0)

        os.environ['NUM_DISK'] = '2.4'
        ret = iceprod.core.resources.get_disk()
        self.assertEqual(ret, 2.4)

        os.environ['NUM_DISK'] = 'blah'
        ret = iceprod.core.resources.get_disk()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['disk'])
        
        with open('.machine.ad', 'w') as f:
            f.write("""
                totaldisk = 123000000
            """)
        ret = iceprod.core.resources.get_disk()
        self.assertAlmostEqual(ret, 123.0)

    @unittest_reporter
    def test_140_get_time(self):
        ret = iceprod.core.resources.get_time()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['time'])

        os.environ['NUM_TIME'] = '13'
        ret = iceprod.core.resources.get_time()
        self.assertEqual(ret, 13.0)

        os.environ['NUM_TIME'] = '2.4'
        ret = iceprod.core.resources.get_time()
        self.assertEqual(ret, 2.4)

        os.environ['NUM_TIME'] = 'blah'
        ret = iceprod.core.resources.get_time()
        self.assertEqual(ret, iceprod.core.resources.Resources.defaults['time'])

        with open('.machine.ad', 'w') as f:
            f.write("""
                TimeToLive = 10800
            """)
        ret = iceprod.core.resources.get_time()
        self.assertAlmostEqual(ret, 3.0)

        with open('.machine.ad', 'w') as f:
            f.write("""
                Glidein_max_walltime = 14400
            """)
        ret = iceprod.core.resources.get_time()
        self.assertAlmostEqual(ret, 4.0)

        with open('.machine.ad', 'w') as f:
            f.write("""
                pyglidein_time_to_live = 14400 - claim_time
                claim_time = 3600
            """)
        ret = iceprod.core.resources.get_time()
        try:
            import classad
        except ImportError:
            self.assertAlmostEqual(ret, 1.0)
        else:
            self.assertAlmostEqual(ret, 3.0)
        
    @unittest_reporter
    def test_230_du(self):
        du_dir = os.path.join(self.test_dir,'test')
        os.mkdir(du_dir)
        for f in ('a','b','c'):
            path = os.path.join(du_dir,f)
            open(path,'w').write('a'*100)
        self.assertEqual(iceprod.core.resources.du(du_dir), 300)

    @unittest_reporter(name='du() symlink')
    def test_231_du_symlink(self):
        du_dir = os.path.join(self.test_dir,'test')
        os.mkdir(du_dir)
        for f in ('a','b','c'):
            path = os.path.join(du_dir,f)
            open(path,'w').write('a'*100)
        os.symlink(os.path.join(du_dir,'a'), os.path.join(du_dir,'l'))
        self.assertEqual(iceprod.core.resources.du(du_dir), 300)

    @unittest_reporter(name='du() dir + symlink')
    def test_232_du_dir_symlink(self):
        du_dir = os.path.join(self.test_dir,'test')
        os.mkdir(du_dir)
        for f in ('a','b','c'):
            path = os.path.join(du_dir,f)
            open(path,'w').write('a'*100)
        os.symlink(os.path.join(du_dir,'a'), os.path.join(du_dir,'l'))
        os.mkdir(os.path.join(du_dir,'subdir'))
        for f in ('a','b','c'):
            path = os.path.join(du_dir,'subdir',f)
            open(path,'w').write('a'*100)
        os.symlink(os.path.join(du_dir,'subdir'), os.path.join(du_dir,'s2'))
        os.symlink(os.path.join(du_dir,'subdir','a'), os.path.join(du_dir,'subdir','s3'))
        self.assertEqual(iceprod.core.resources.du(du_dir), 600)

    @unittest_reporter(name='group_hasher() - defaults')
    def test_300_group_hasher(self):
        r = iceprod.core.resources.Resources.defaults
        h = iceprod.core.resources.group_hasher(r)

    @unittest_reporter(name='group_hasher() - mem range')
    def test_301_group_hasher(self):
        r = iceprod.core.resources.Resources.defaults.copy()

        hashes = set()
        for i in range(1,100):
            r['memory'] = i
            hashes.add(iceprod.core.resources.group_hasher(r))
        logger.info('hashes: %r', hashes)
        self.assertLess(len(hashes),15)

    @unittest_reporter
    def test_400_sanitized_requirements(self):
        r = {'cpu':2,'gpu':1}
        ret = iceprod.core.resources.sanitized_requirements(r)
        self.assertEqual(r['cpu'], ret['cpu'])
        self.assertEqual(r['gpu'], ret['gpu'])
        self.assertFalse('memory' in ret)

        r = {'os':['RHEL_7_x86_64'], 'site':'foo'}
        ret = iceprod.core.resources.sanitized_requirements(r)
        self.assertEqual(r['os'], ret['os'])
        self.assertEqual(r['site'], ret['site'])
        self.assertFalse('gpu' in ret)
        self.assertFalse('memory' in ret)

    @unittest_reporter(name='sanitized_requirements() - defaults')
    def test_401_sanitized_requirements(self):
        r = {'cpu':2,'gpu':1}
        ret = iceprod.core.resources.sanitized_requirements(r, use_defaults=True)
        self.assertEqual(r['cpu'], ret['cpu'])
        self.assertEqual(r['gpu'], ret['gpu'])
        self.assertEqual(iceprod.core.resources.Resources.defaults['memory'], ret['memory'])

        r = {'os':['RHEL_7_x86_64'], 'site':'foo'}
        ret = iceprod.core.resources.sanitized_requirements(r, use_defaults=True)
        self.assertEqual(r['os'], ret['os'])
        self.assertEqual(r['site'], ret['site'])
        self.assertFalse('gpu' in ret)
        self.assertEqual(iceprod.core.resources.Resources.defaults['memory'], ret['memory'])
        
        

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(resources_test))
    suite.addTests(loader.loadTestsFromNames(alltests,resources_test))
    return suite
