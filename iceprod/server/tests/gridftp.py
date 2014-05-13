#! /usr/bin/env python
"""
  Test script for gridftp tornado integration

  copyright (c) 2012 the icecube collaboration
"""

from __future__ import print_function
try:
    from server_tester import printer,glob_tests
    import logging
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
logger = logging.getLogger('gridftp')

import os, sys, time
import shutil
import random
import string
import subprocess
from threading import Event

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import tornado.ioloop

import iceprod.server.gridftp


class gridftp_test(unittest.TestCase):
    def setUp(self):
        self._timeout = 120
        self.test_dir = os.path.join(os.getcwd(),'test')
        self.server_test_dir = os.path.join('gsiftp://gridftp.icecube.wisc.edu/data/sim/sim-new/tmp/test',
                                            str(random.randint(0,2**32)))
        try:
            iceprod.server.gridftp.GridFTP.mkdir(self.server_test_dir,
                                               parents=True)
        except:
            pass
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        super(gridftp_test,self).setUp()
    
    def tearDown(self):
        try:
            iceprod.server.gridftp.GridFTP.rmtree(self.server_test_dir)
        except:
            pass
        shutil.rmtree(self.test_dir)
        super(gridftp_test,self).tearDown()
        
    def test_01_supported_address(self):
        """Test supported_address"""
        try:
            bad_addresses = ['test','file:/test','gsiftp:test','gsiftp:/test',
                             'ftp:test','http://x2100.icecube.wisc.edu',
                             'ftp:/test']
            good_addresses = ['gsiftp://data.icecube.wisc.edu','ftp://gnu.org',
                              'gsiftp://gridftp-rr.icecube.wisc.edu/data/sim/sim-new']
            
            for i in xrange(0,10):
                for a in bad_addresses:
                    ret = iceprod.server.gridftp.GridFTP.supported_address(a)
                    if ret is True:
                        raise Exception('Bad address %s was called good'%a)
                for a in good_addresses:
                    ret = iceprod.server.gridftp.GridFTP.supported_address(a)
                    if ret is not True:
                        raise Exception('Good address %s was called bad'%a)
            
        except Exception, e:
            logger.error('Error running supported_address test: %s',str(e))
            printer('Test gridftp.supported_address()',False)
            raise
        else:
            printer('Test gridftp.supported_address()')
    
    def test_02_address_split(self):
        """Test address_split"""
        try:
            good_addresses = {'gsiftp://data.icecube.wisc.edu':('gsiftp://data.icecube.wisc.edu','/'),
                              'ftp://gnu.org':('ftp://gnu.org','/'),
                              'gsiftp://gridftp-rr.icecube.wisc.edu/data/sim/sim-new':('gsiftp://gridftp-rr.icecube.wisc.edu','/data/sim/sim-new')}
            
            for i in xrange(0,10):
                for a in good_addresses:
                    pieces = iceprod.server.gridftp.GridFTP.address_split(a)
                    if pieces != good_addresses[a]:
                        raise Exception('Address %s was not split properly'%a)
            
        except Exception, e:
            logger.error('Error running address_split test: %s',str(e))
            printer('Test gridftp.address_split()',False)
            raise
        else:
            printer('Test gridftp.address_split()')

    def test_100_put_str(self):
        """Test put with a str - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running put with str test: %s',str(e))
            printer('Test gridftp.put() with str',False)
            raise
        else:
            printer('Test gridftp.put() with str')
    
    def test_101_put_file(self):
        """Test put with a file - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            # make temp file
            filename = os.path.join(self.test_dir,'test')
            filecontents = 'this is a test'
            with open(filename,'w') as f:
                f.write(filecontents)
            
            try:
                # put file
                ret = iceprod.server.gridftp.GridFTP.put(address,filename=filename)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running put with file test: %s',str(e))
            printer('Test gridftp.put() with file',False)
            raise
        else:
            printer('Test gridftp.put() with file')

    def test_102_put_func(self):
        """Test put with a function - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            def contents():
                # give every 10 chars
                for i in xrange(0,len(filecontents),10):
                    yield filecontents[i:i+10]
            
            try:
                # put from function
                ret = iceprod.server.gridftp.GridFTP.put(address,streaming_callback=contents().next)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running put with func test: %s',str(e))
            printer('Test gridftp.put() with func',False)
            raise
        else:
            printer('Test gridftp.put() with func')

    def test_110_get_str(self):
        """Test get with a str - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                # get str
                ret = iceprod.server.gridftp.GridFTP.get(address)
                if ret is False:
                    raise Exception('get failed: ret=%r'%ret)
                if ret != filecontents:
                    logger.warning('contents should be: %s',filecontents)
                    logger.warning('contents is actually: %s',ret)
                    raise Exception('contents is incorrect')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running get with str test: %s',str(e))
            printer('Test gridftp.get() with str',False)
            raise
        else:
            printer('Test gridftp.get() with str')
    
    def test_111_get_file(self):
        """Test get with a file - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            # make temp file
            filename = os.path.join(self.test_dir,'test')
            filename2 = os.path.join(self.test_dir,'test2')
            filecontents = 'this is a test'
            with open(filename,'w') as f:
                f.write(filecontents)
            
            try:
                # put file
                ret = iceprod.server.gridftp.GridFTP.put(address,filename=filename)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                # get file
                ret = iceprod.server.gridftp.GridFTP.get(address,filename=filename2)
                if ret is False:
                    raise Exception('get failed: ret=%r'%ret)
                if not os.path.exists(filename2):
                    raise Exception('dest file does not exist')
                with open(filename2) as f:
                    newcontents = f.read()
                    if filecontents != newcontents:
                        logger.warning('contents should be: %s',filecontents)
                        logger.warning('contents is actually: %s',newcontents)
                        raise Exception('file contents is incorrect')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running get with file test: %s',str(e))
            printer('Test gridftp.get() with file',False)
            raise
        else:
            printer('Test gridftp.get() with file')

    def test_112_get_func(self):
        """Test get with a function - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            def contents():
                # give every 10 chars
                for i in xrange(0,len(filecontents),10):
                    yield filecontents[i:i+10]
            def contents2(data):
                contents2.data += data
            contents2.data = ''
            
            try:
                # put from function
                ret = iceprod.server.gridftp.GridFTP.put(address,streaming_callback=contents().next)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                
                # get strGridFTP
                ret = iceprod.server.gridftp.GridFTP.get(address,streaming_callback=contents2)
                if ret is False:
                    raise Exception('get failed: ret=%r'%ret)
                if contents2.data != filecontents:
                    logger.warning('contents should be: %s',filecontents)
                    logger.warning('contents is actually: %s',contents2.data)
                    raise Exception('contents is incorrect')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running get with func test: %s',str(e))
            printer('Test gridftp.get() with func',False)
            raise
        else:
            printer('Test gridftp.get() with func')

    def test_120_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != []:
                    logger.info('expected: []')
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir)',False)
            raise
        else:
            printer('Test gridftp.list(dir)')

    def test_121_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != ['test_file']:
                    logger.info("expected: ['test_file']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
        
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file)',False)
            raise
        else:
            printer('Test gridftp.list(file)')

    def test_122_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address,dotfiles=True)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != ['.','..']:
                    logger.info("expected: ['.','..']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(dir,dotfiles)')

    def test_123_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address,dotfiles=True)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != ['test_file']:
                    logger.info('expected: [\'test_file\']')
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(file,dotfiles)')
    
    def test_124_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address,details=True)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != []:
                    logger.info('expected: []')
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir,details)',False)
            raise
        else:
            printer('Test gridftp.list(dir,details)')

    def test_125_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address,details=True)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if len(ret) != 1 or ret[0].directory:
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
        
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file,details)',False)
            raise
        else:
            printer('Test gridftp.list(file,details)')

    def test_126_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address,details=True,
                                                        dotfiles=True)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if (len(ret) != 2 or not any([x.name == '.' for x in ret])
                    or not any([x.name == '..' for x in ret])):
                    logger.info("expected: ['..','.']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir,details,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(dir,details,dotfiles)')

    def test_127_list(self):
        """Test list - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                ret = iceprod.server.gridftp.GridFTP.list(address,details=True,
                                                        dotfiles=True)
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if len(ret) != 1 or ret[0].name != 'test_file':
                    logger.info("expected: ['test_file']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file,details,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(file,details,dotfiles)')

    def test_130_delete(self):
        """Test delete - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
                
            ret = iceprod.server.gridftp.GridFTP.delete(address)
            if ret is not True:
                raise Exception('delete failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running delete test: %s',str(e))
            printer('Test gridftp.delete()',False)
            raise
        else:
            printer('Test gridftp.delete()')

    def test_140_rmtree(self):
        """Test rmtree - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'file_test')
            filecontents = 'this is a test'
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
            
            ret = iceprod.server.gridftp.GridFTP.rmtree(address)
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(file)',False)
            raise
        else:
            printer('Test gridftp.rmtree(file)')

    def test_141_rmtree(self):
        """Test rmtree - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            # mkdir
            ret = iceprod.server.gridftp.GridFTP.mkdir(address)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
                
            ret = iceprod.server.gridftp.GridFTP.rmtree(address)
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(empty dir)',False)
            raise
        else:
            printer('Test gridftp.rmtree(empty dir)')

    def test_142_rmtree(self):
        """Test rmtree - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            # mkdir
            ret = iceprod.server.gridftp.GridFTP.mkdir(address)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
                
            address2 = os.path.join(self.server_test_dir,'test','file_test')
            filecontents = 'this is a test'
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address2,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
            
            ret = iceprod.server.gridftp.GridFTP.rmtree(address)
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(dir + file)',False)
            raise
        else:
            printer('Test gridftp.rmtree(dir + file)')

    def test_143_rmtree(self):
        """Test rmtree - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            # mkdir
            ret = iceprod.server.gridftp.GridFTP.mkdir(address)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
            
            # mkdir
            address2 = os.path.join(self.server_test_dir,'test','test2')
            ret = iceprod.server.gridftp.GridFTP.mkdir(address2)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
                
            address3 = os.path.join(self.server_test_dir,'test','test2','file_test')
            filecontents = 'this is a test'
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address3,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
            
            ret = iceprod.server.gridftp.GridFTP.rmtree(address)
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(dir + dir + file)',False)
            raise
        else:
            printer('Test gridftp.rmtree(dir + dir + file)')

    def test_160_exists(self):
        """Test exists - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            try:
                ret = iceprod.server.gridftp.GridFTP.exists(address)
                if ret is True:
                    raise Exception('exists succeeded when it should have failed')
                
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                ret = iceprod.server.gridftp.GridFTP.exists(address)
                if ret is not True:
                    raise Exception('exists failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running exists test: %s',str(e))
            printer('Test gridftp.exists()',False)
            raise
        else:
            printer('Test gridftp.exists()')

    def test_170_move(self):
        """Test move - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            address2 = os.path.join(self.server_test_dir,'test2')
            filecontents = 'this is a test'
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                
                ret = iceprod.server.gridftp.GridFTP.exists(address2)
                if ret is True:
                    raise Exception('exists succeeded before move')
                
                ret = iceprod.server.gridftp.GridFTP.move(address,address2)
                if ret is not True:
                    raise Exception('move failed: ret=%r'%ret)
                
                ret = iceprod.server.gridftp.GridFTP.exists(address)
                if ret is True:
                    raise Exception('exists succeeded on old address')
                ret = iceprod.server.gridftp.GridFTP.exists(address2)
                if ret is not True:
                    raise Exception('exists failed on new address')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address2)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running move test: %s',str(e))
            printer('Test gridftp.move()',False)
            raise
        else:
            printer('Test gridftp.move()')
    
    def test_180_checksum(self):
        """Test checksums - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            import hashlib
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                ret = iceprod.server.gridftp.GridFTP.md5sum(address)
                correct = hashlib.md5(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('md5sum failed: ret=%r and correct=%r'%(ret,correct))
                    
                ret = iceprod.server.gridftp.GridFTP.sha1sum(address)
                correct = hashlib.sha1(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('sha1sum failed: ret=%r and correct=%r'%(ret,correct))
                    
                ret = iceprod.server.gridftp.GridFTP.sha256sum(address)
                correct = hashlib.sha256(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('sha256sum failed: ret=%r and correct=%r'%(ret,correct))
                    
                ret = iceprod.server.gridftp.GridFTP.sha512sum(address)
                correct = hashlib.sha512(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('sha512sum failed: ret=%r and correct=%r'%(ret,correct))
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running checksum test: %s',str(e))
            printer('Test gridftp.checksum()',False)
            raise
        else:
            printer('Test gridftp.checksum()')
    
    def test_190_size(self):
        """Test size - synchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                ret = iceprod.server.gridftp.GridFTP.size(address)
                correct = len(filecontents)
                if ret != correct:
                    raise Exception('size failed: ret=%r and correct=%r'%(ret,correct))
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running size test: %s',str(e))
            printer('Test gridftp.size()',False)
            raise
        else:
            printer('Test gridftp.size()')
    
    def test_200_put_str(self):
        """Test put with a str - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                # put str
                iceprod.server.gridftp.GridFTP.put(address,data=filecontents,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running put with str test: %s',str(e))
            printer('Test gridftp.put() with str',False)
            raise
        else:
            printer('Test gridftp.put() with str')
    
    def test_201_put_file(self):
        """Test put with a file - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            # make temp file
            filename = os.path.join(self.test_dir,'test')
            filecontents = 'this is a test'
            with open(filename,'w') as f:
                f.write(filecontents)
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()   
            cb.ret = False
            
            try:
                # put file
                iceprod.server.gridftp.GridFTP.put(address,filename=filename,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running put with file test: %s',str(e))
            printer('Test gridftp.put() with file',False)
            raise
        else:
            printer('Test gridftp.put() with file')

    def test_202_put_func(self):
        """Test put with a function - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            def contents():
                # give every 10 chars
                for i in xrange(0,len(filecontents),10):
                    yield filecontents[i:i+10]
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                # put from function
                iceprod.server.gridftp.GridFTP.put(address,streaming_callback=contents().next,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running put with func test: %s',str(e))
            printer('Test gridftp.put() with func',False)
            raise
        else:
            printer('Test gridftp.put() with func')

    def test_210_get_str(self):
        """Test get with a str - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()  
            cb.ret = False
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                # get str
                iceprod.server.gridftp.GridFTP.get(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('get failed: ret=%r'%ret)
                if ret != filecontents:
                    logger.warning('contents should be: %s',filecontents)
                    logger.warning('contents is actually: %s',ret)
                    raise Exception('contents is incorrect')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running get with str test: %s',str(e))
            printer('Test gridftp.get() with str',False)
            raise
        else:
            printer('Test gridftp.get() with str')
    
    def test_211_get_file(self):
        """Test get with a file - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            # make temp file
            filename = os.path.join(self.test_dir,'test')
            filename2 = os.path.join(self.test_dir,'test2')
            filecontents = 'this is a test'
            with open(filename,'w') as f:
                f.write(filecontents)
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                # put file
                ret = iceprod.server.gridftp.GridFTP.put(address,filename=filename)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                # get file
                iceprod.server.gridftp.GridFTP.get(address,filename=filename2,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('get failed: ret=%r'%ret)
                if not os.path.exists(filename2):
                    raise Exception('dest file does not exist')
                with open(filename2) as f:
                    newcontents = f.read()
                    if filecontents != newcontents:
                        logger.warning('contents should be: %s',filecontents)
                        logger.warning('contents is actually: %s',newcontents)
                        raise Exception('file contents is incorrect')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running get with file test: %s',str(e))
            printer('Test gridftp.get() with file',False)
            raise
        else:
            printer('Test gridftp.get() with file')

    def test_212_get_func(self):
        """Test get with a function - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            def contents():
                # give every 10 chars
                for i in xrange(0,len(filecontents),10):
                    yield filecontents[i:i+10]
            def contents2(data):
                contents2.data += data
            contents2.data = ''
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()   
            cb.ret = False
            
            try:
                # put from function
                ret = iceprod.server.gridftp.GridFTP.put(address,streaming_callback=contents().next)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                
                # get strGridFTP
                iceprod.server.gridftp.GridFTP.get(address,streaming_callback=contents2,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('get failed: ret=%r'%ret)
                if contents2.data != filecontents:
                    logger.warning('contents should be: %s',filecontents)
                    logger.warning('contents is actually: %s',contents2.data)
                    raise Exception('contents is incorrect')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running get with func test: %s',str(e))
            printer('Test gridftp.get() with func',False)
            raise
        else:
            printer('Test gridftp.get() with func')

    def test_220_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != []:
                    logger.info('expected: []')
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir)',False)
            raise
        else:
            printer('Test gridftp.list(dir)')

    def test_221_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != ['test_file']:
                    logger.info("expected: ['test_file']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
        
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file)',False)
            raise
        else:
            printer('Test gridftp.list(file)')

    def test_222_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,dotfiles=True,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != ['.','..']:
                    logger.info("expected: ['.','..']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(dir,dotfiles)')

    def test_223_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,dotfiles=True,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != ['test_file']:
                    logger.info('expected: [\'test_file\']')
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(file,dotfiles)')
    
    def test_224_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,details=True,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if ret != []:
                    logger.info('expected: []')
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir,details)',False)
            raise
        else:
            printer('Test gridftp.list(dir,details)')

    def test_225_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,details=True,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if len(ret) != 1 or ret[0].directory:
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
        
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file,details)',False)
            raise
        else:
            printer('Test gridftp.list(file,details)')

    def test_226_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.mkdir(address)
                if ret is False:
                    raise Exception('mkdir failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,details=True,
                                                  dotfiles=True,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if (len(ret) != 2 or not any([x.name == '.' for x in ret])
                    or not any([x.name == '..' for x in ret])):
                    logger.info("expected: ['..','.']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.rmdir(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(dir,details,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(dir,details,dotfiles)')

    def test_227_list(self):
        """Test list - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test_file')
            data = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                ret = iceprod.server.gridftp.GridFTP.put(address,data=data)
                if ret is False:
                    raise Exception('put failed')
                
                # get listing
                iceprod.server.gridftp.GridFTP.list(address,details=True,
                                                  dotfiles=True,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is False:
                    raise Exception('list failed: ret=%r'%ret)
                
                if len(ret) != 1 or ret[0].name != 'test_file':
                    logger.info("expected: ['test_file']")
                    logger.info('actual: %r',ret)
                    raise Exception('list did not return expected results')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running list test: %s',str(e))
            printer('Test gridftp.list(file,details,dotfiles)',False)
            raise
        else:
            printer('Test gridftp.list(file,details,dotfiles)')

    def test_230_delete(self):
        """Test delete - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
                
            iceprod.server.gridftp.GridFTP.delete(address,callback=cb)
            tornado.ioloop.IOLoop.instance().start()
            ret = cb.ret
            
            if ret is not True:
                raise Exception('delete failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running delete test: %s',str(e))
            printer('Test gridftp.delete()',False)
            raise
        else:
            printer('Test gridftp.delete()')

    def test_240_rmtree(self):
        """Test rmtree - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'file_test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
            
            iceprod.server.gridftp.GridFTP.rmtree(address,callback=cb)
            tornado.ioloop.IOLoop.instance().start()
            ret = cb.ret
            
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(file)',False)
            raise
        else:
            printer('Test gridftp.rmtree(file)')

    def test_241_rmtree(self):
        """Test rmtree - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            # mkdir
            ret = iceprod.server.gridftp.GridFTP.mkdir(address)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
                
            iceprod.server.gridftp.GridFTP.rmtree(address,callback=cb)
            tornado.ioloop.IOLoop.instance().start()
            ret = cb.ret
            
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(empty dir)',False)
            raise
        else:
            printer('Test gridftp.rmtree(empty dir)')

    def test_242_rmtree(self):
        """Test rmtree - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            # mkdir
            ret = iceprod.server.gridftp.GridFTP.mkdir(address)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
                
            address2 = os.path.join(self.server_test_dir,'test','file_test')
            filecontents = 'this is a test'
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address2,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
            
            iceprod.server.gridftp.GridFTP.rmtree(address,callback=cb)
            tornado.ioloop.IOLoop.instance().start()
            ret = cb.ret
            
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(dir + file)',False)
            raise
        else:
            printer('Test gridftp.rmtree(dir + file)')

    def test_243_rmtree(self):
        """Test rmtree - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            # mkdir
            ret = iceprod.server.gridftp.GridFTP.mkdir(address)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
            
            # mkdir
            address2 = os.path.join(self.server_test_dir,'test','test2')
            ret = iceprod.server.gridftp.GridFTP.mkdir(address2)
            if ret is not True:
                raise Exception('mkdir failed: ret=%r'%ret)
                
            address3 = os.path.join(self.server_test_dir,'test','test2','file_test')
            filecontents = 'this is a test'
            
            # put str
            ret = iceprod.server.gridftp.GridFTP.put(address3,data=filecontents)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
            
            iceprod.server.gridftp.GridFTP.rmtree(address,callback=cb)
            tornado.ioloop.IOLoop.instance().start()
            ret = cb.ret
            
            if ret is not True:
                raise Exception('rmtree failed: ret=%r'%ret)
            
        except Exception, e:
            logger.error('Error running rmtree test: %s',str(e))
            printer('Test gridftp.rmtree(dir + dir + file)',False)
            raise
        else:
            printer('Test gridftp.rmtree(dir + dir + file)')

    def test_260_exists(self):
        """Test exists - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()  
            cb.ret = False
            cb.event = Event()
            cb.event.clear()
            
            try:
                ret = iceprod.server.gridftp.GridFTP.exists(address)
                if ret is True:
                    raise Exception('exists succeeded when it should have failed')
                
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                iceprod.server.gridftp.GridFTP.exists(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('exists failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running exists test: %s',str(e))
            printer('Test gridftp.exists()',False)
            raise
        else:
            printer('Test gridftp.exists()')

    def test_270_move(self):
        """Test move - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            address2 = os.path.join(self.server_test_dir,'test2')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()   
            cb.ret = False
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                
                ret = iceprod.server.gridftp.GridFTP.exists(address2)
                if ret is True:
                    raise Exception('exists succeeded before move')
                
                iceprod.server.gridftp.GridFTP.move(address,address2,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('move failed: ret=%r'%ret)
                
                ret = iceprod.server.gridftp.GridFTP.exists(address)
                if ret is True:
                    raise Exception('exists succeeded on old address')
                ret = iceprod.server.gridftp.GridFTP.exists(address2)
                if ret is not True:
                    raise Exception('exists failed on new address')
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address2)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running move test: %s',str(e))
            printer('Test gridftp.move()',False)
            raise
        else:
            printer('Test gridftp.move()')
    
    def test_280_checksum(self):
        """Test checksums - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()   
            cb.ret = False
            
            import hashlib
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                iceprod.server.gridftp.GridFTP.md5sum(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                correct = hashlib.md5(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('md5sum failed: ret=%r and correct=%r'%(ret,correct))
                    
                iceprod.server.gridftp.GridFTP.sha1sum(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                correct = hashlib.sha1(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('sha1sum failed: ret=%r and correct=%r'%(ret,correct))
                    
                iceprod.server.gridftp.GridFTP.sha256sum(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                correct = hashlib.sha256(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('sha256sum failed: ret=%r and correct=%r'%(ret,correct))
                    
                iceprod.server.gridftp.GridFTP.sha512sum(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                correct = hashlib.sha512(filecontents).hexdigest()
                if ret != correct:
                    raise Exception('sha512sum failed: ret=%r and correct=%r'%(ret,correct))
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running checksum test: %s',str(e))
            printer('Test gridftp.checksum()',False)
            raise
        else:
            printer('Test gridftp.checksum()')
    
    def test_290_size(self):
        """Test size - asynchronous"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()   
            cb.ret = False
            
            try:
                # put str
                ret = iceprod.server.gridftp.GridFTP.put(address,data=filecontents)
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
                    
                iceprod.server.gridftp.GridFTP.size(address,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                correct = len(filecontents)
                if ret != correct:
                    raise Exception('size failed: ret=%r and correct=%r'%(ret,correct))
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address)
                except:
                    pass
            
        except Exception, e:
            logger.error('Error running size test: %s',str(e))
            printer('Test gridftp.size()',False)
            raise
        else:
            printer('Test gridftp.size()')
    
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(gridftp_test))
    suite.addTests(loader.loadTestsFromNames(alltests,gridftp_test))
    return suite
