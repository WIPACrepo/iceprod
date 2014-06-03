#! /usr/bin/env python
"""
  Test script for server init scripts

  copyright (c) 2011 the icecube collaboration  
"""

from __future__ import print_function
try:
    from server_tester import printer, glob_tests, logger
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    import logging
    logging.basicConfig()
    logger = logging.getLogger('server_tester')

import os, sys, time
import shutil
import random
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.server


class init_test(unittest.TestCase):
    def setUp(self):
        super(init_test,self).setUp()
        
        # make test dir
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(init_test,self).tearDown()
    
    def test_01_getconfig(self):
        """Test getconfig"""
        try:
            # create empty file
            filename = os.path.join(self.test_dir,'test.cfg')
            with open(filename,'w') as f:
                f.write('')
            
            try:
                cfg = iceprod.server.getconfig(filename)
            except Exception as e:
                logger.error('getconfig returned exception: %r',e,exc_info=True)
                raise Exception('empty cfgfile returned exception')
            
            # create incorrect file
            with open(filename,'w') as f:
                f.write('[server_modules]\n  queue=testing\n')
            
            try:
                cfg = iceprod.server.getconfig(filename)
            except Exception as e:
                logger.info('getconfig returned exception: %r',e,exc_info=True)
            else:
                raise Exception('incorrect cfgfile did not return exception')
            
        except Exception as e:
            logger.error('Error running getconfig test - %s',str(e))
            printer('Test getconfig',False)
            raise
        else:
            printer('Test getconfig')
    
    def test_02_saveconfig(self):
        """Test saveconfig"""
        try:
            # create empty file
            filename = os.path.join(self.test_dir,'test.cfg')
            with open(filename,'w') as f:
                f.write('')
            
            cfg = iceprod.server.getconfig(filename)
            
            # save config
            try:
                iceprod.server.saveconfig(filename,cfg)
            except Exception as e:
                logger.error('saveconfig returned exception: %r',e,exc_info=True)
                raise Exception('empty cfgfile returned exception')
            
            # try with no file
            filename2 = os.path.join(self.test_dir,'test2.cfg')
            
            try:
                iceprod.server.saveconfig(filename2,cfg)
            except Exception as e:
                logger.error('saveconfig returned exception: %r',e,exc_info=True)
                raise Exception('no cfgfile returned exception')
            
            if not os.path.exists(filename2):
                raise Exception('empty file not created')

            with open(filename) as f, open(filename2) as f2:
                if f.read() != f2.read():
                    raise Exception('empty file config not set')
            
        except Exception as e:
            logger.error('Error running saveconfig test - %s',str(e))
            printer('Test saveconfig',False)
            raise
        else:
            printer('Test saveconfig')
    
    def test_10_GlobalID_int2char(self):
        """Test GlobalID_int2char"""
        try:
            inputs = [0,26,61,62,100,124,126,59428801]
            outputs = ['a','A','9','aa','aM','ba','bc','david']
            for i,input in enumerate(inputs):
                output = iceprod.server.GlobalID.int2char(input)
                if outputs[i] != output:
                    raise Exception('input=%d, given output=%s, correct output=%s'%(input,str(output),outputs[i]))
            
        except Exception as e:
            logger.error('Error running GlobalID.int2char test - %s',str(e))
            printer('Test GlobalID.int2char',False)
            raise
        else:
            printer('Test GlobalID.int2char')

    def test_11_GlobalID_char2int(self):
        """Test GlobalID_char2int"""
        try:
            inputs = ['a','A','9','aa','aM','ba','bc','david']
            outputs = [0,26,61,62,100,124,126,59428801]
            for i,input in enumerate(inputs):
                output = iceprod.server.GlobalID.char2int(input)
                if outputs[i] != output:
                    raise Exception('input=%s, given output=%s, correct output=%d'%(input,str(output),outputs[i]))
            
        except Exception as e:
            logger.error('Error running GlobalID.char2int test - %s',str(e))
            printer('Test GlobalID.char2int',False)
            raise
        else:
            printer('Test GlobalID.char2int')

    def test_12_GlobalID_siteID_gen(self):
        """Test GlobalID_siteID_gen"""
        try:
            for i in xrange(1000):
                ret = iceprod.server.GlobalID.siteID_gen()
                ret2 = iceprod.server.GlobalID.char2int(ret)
                if ret2 < 0 or ret2 >= iceprod.server.GlobalID.MAXSITEID:
                    raise Exception('outsite permissible range: %s : %d'%(ret,ret2))
            
        except Exception as e:
            logger.error('Error running GlobalID.siteID_gen test - %s',str(e))
            printer('Test GlobalID.siteID_gen',False)
            raise
        else:
            printer('Test GlobalID.siteID_gen')

    def test_13_GlobalID_globalID_gen(self):
        """Test GlobalID_globalID_gen"""
        try:
            for i in xrange(1000):
                ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
                ret = iceprod.server.GlobalID.globalID_gen(ran,iceprod.server.GlobalID.siteID_gen())
                ret2 = iceprod.server.GlobalID.char2int(ret)
                if ret2 < ran or ret2 >= (iceprod.server.GlobalID.MAXSITEID*iceprod.server.GlobalID.MAXLOCALID):
                    raise Exception('outsite permissible range: (0<x<%d)  %d = %s'%((iceprod.server.GlobalID.MAXSITEID*iceprod.server.GlobalID.MAXLOCALID),ret2,ret))
            
        except Exception as e:
            logger.error('Error running GlobalID.globalID_gen test - %s',str(e))
            printer('Test GlobalID.globalID_gen',False)
            raise
        else:
            printer('Test GlobalID.globalID_gen')

    def test_14_GlobalID_localID_ret(self):
        """Test GlobalID_localID_ret"""
        try:
            for i in xrange(1000):
                ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
                ret = iceprod.server.GlobalID.globalID_gen(ran,iceprod.server.GlobalID.siteID_gen())
                ret2 = iceprod.server.GlobalID.localID_ret(ret,type='int')
                if ret2 != ran:
                    raise Exception('returned local id does not match initial id: %d != %d'%(ran,ret2))
            
        except Exception as e:
            logger.error('Error running GlobalID.localID_ret test - %s',str(e))
            printer('Test GlobalID.localID_ret',False)
            raise
        else:
            printer('Test GlobalID.localID_ret')

    def test_15_GlobalID_siteID_ret(self):
        """Test GlobalID_siteID_ret"""
        try:
            for i in xrange(1000):
                ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
                ran2 = iceprod.server.GlobalID.siteID_gen()
                ret = iceprod.server.GlobalID.globalID_gen(ran,ran2)
                ret2 = iceprod.server.GlobalID.siteID_ret(ret)
                if ret2 != ran2:
                    raise Exception('returned site id does not match initial id: %d != %d'%(ran,ret2))
            
        except Exception as e:
            logger.error('Error running GlobalID.siteID_ret test - %s',str(e))
            printer('Test GlobalID.siteID_ret',False)
            raise
        else:
            printer('Test GlobalID.siteID_ret')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(init_test))
    suite.addTests(loader.loadTestsFromNames(alltests,init_test))
    return suite
