"""
Common functions
"""

from __future__ import absolute_import, division, print_function

import sys
import os
import re
import shutil
import time
import logging
import socket
import subprocess
import tarfile
import urllib
import tempfile
import hashlib
from functools import partial

try:
    import cPickle as pickle
except:
    import pickle

from iceprod.core import util
from iceprod.core.gridftp import GridFTP
from iceprod.core.jsonUtil import json_encode,json_decode

logger = logging.getLogger('functions')


### Compression Functions ###
_compress_suffixes = ('.tgz','.gz','.tbz2','.tbz','.bz2','.bz',
                     '.lzma2','.lzma','.lz','.xz')
_tar_suffixes = ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tbz',
                '.tar.lzma', '.tar.xz', '.tlz', '.txz')

def uncompress(infile):
    """Uncompress a file, if possible"""
    files = []
    logger.info('uncompressing %s',infile)
    if istarred(infile):
        # handle tarfile
        output = subprocess.check_output(['tar','-atf',infile])
        files = [x for x in output.split('\n') if x.strip() and x[-1] != '/']
        if not files:
            raise Exception('no files inside tarfile')
        for f in files:
            if os.path.exists(f):
                break
        else:
            subprocess.call(['tar','-axf',infile])
    else:
        if infile.endswith('.gz'):
            cmd = 'gzip'
        elif any(infile.endswith(s) for s in ('.bz','.bz2')):
            cmd = 'bzip2'
        elif any(infile.endswith(s) for s in ('.xz','.lzma')):
            cmd = 'xz'
        else:
            logger.info('unknown format: %s',infile)
            raise Exception('unknown format')
        subprocess.call([cmd,'-kdf',infile])
        files.append(infile.rsplit('.',1)[0])

    logger.info('files: %r', files)
    if len(files) == 1:
        return files[0]
    else:
        return files

def compress(infile,compression='lzma'):
    """Compress a file or directory.
       The compression argument is used as the new file extension"""
    if not istarred('.'+compression) and os.path.isdir(infile):
        outfile = infile+'.tar.'+compression
    else:
        outfile = infile+'.'+compression
    if istarred(outfile):
        dirname, filename = os.path.split(infile)
        subprocess.call(['tar','-acf',outfile,'-C',dirname,filename])
    else:
        if outfile.endswith('.gz'):
            cmd = ['gzip']
        elif any(outfile.endswith(s) for s in ('.bz','.bz2')):
            cmd = ['bzip2']
        elif outfile.endswith('.xz'):
            cmd = ['xz']
        elif outfile.endswith('.lzma'):
            cmd = ['xz','-F','lzma']
        else:
            logger.info('unknown format: %s',infile)
            raise Exception('unknown format')
        subprocess.call(cmd+['-kf',infile])
    return outfile

def iscompressed(infile):
    """Check if a file is a compressed file, based on file name"""
    return any(infile.endswith(s) for s in _compress_suffixes)

def istarred(infile):
    """Check if a file is a tarred file, based on file name"""
    return any(infile.endswith(s) for s in _tar_suffixes)

def cksm(filename,type,buffersize=16384,file=True):
    """Return checksum of file using algorithm specified"""
    if type not in ('md5','sha1','sha256','sha512'):
        raise Exception('cannot get checksum for type %r',type)

    try:
        digest = getattr(hashlib,type)()
    except:
        raise Exception('cannot get checksum for type %r',type)

    if file and os.path.exists(filename):
        # checksum file contents
        filed = open(filename)
        buffer = filed.read(buffersize)
        while buffer:
            digest.update(buffer)
            buffer = filed.read(buffersize)
        filed.close()
    else:
        # just checksum the contents of the first argument
        digest.update(filename)
    return digest.hexdigest()

def md5sum(filename,buffersize=16384):
    """Return md5 digest of file"""
    return cksm(filename,'md5',buffersize)

def sha1sum(filename,buffersize=16384):
    """Return sha1 digest of file"""
    return cksm(filename,'sha1',buffersize)

def sha256sum(filename,buffersize=16384):
    """Return sha256 digest of file"""
    return cksm(filename,'sha256',buffersize)

def sha512sum(filename,buffersize=16384):
    """Return sha512 digest of file"""
    return cksm(filename,'sha512',buffersize)


def check_cksm(file,type,sum):
    """Check a checksum of a file"""
    if not os.path.exists(file):
        return False
    # get checksum from file
    file_cksm = cksm(file,type)
    # load sum
    if os.path.isfile(sum):
        sum_cksm = ''
        for l in open(sum,'r'):
            if os.path.basename(file) in l:
                sum_cksm, name = l.strip('\n').split()
                break
    else:
        sum_cksm = sum
    # check sum
    return (file_cksm == sum_cksm)

def check_md5sum(file,sum):
    """Check an md5sum of a file"""
    return check_cksm(file,'md5',sum)

def check_sha1sum(file,sum):
    """Check an sha1sum of a file"""
    return check_cksm(file,'sha1',sum)

def check_sha256sum(file,sum):
    """Check an sha256sum of a file"""
    return check_cksm(file,'sha256',sum)

def check_sha512sum(file,sum):
    """Check an sha512sum of a file"""
    return check_cksm(file,'sha512',sum)


### File and Directory Manipulation Functions ###

def removedirs(path):
    try:
        if os.path.isdir(path):
            shutil.rmtree(path,True)
        else:
            os.remove(path)
    except Exception:
        pass

def copy(src,dest):
    parent_dir = os.path.dirname(dest)
    if not os.path.exists(parent_dir):
        logger.info('attempting to make parent dest dir %s',parent_dir)
        try:
            os.makedirs(parent_dir)
        except:
            logger.error('failed to make dest directory for copy',exc_info=True)
            raise
    if os.path.isdir(src):
        logger.info('dircopy: %s to %s',src,dest)
        shutil.copytree(src,dest,symlinks=True)
    else:
        logger.info('filecopy: %s to %s',src,dest)
        shutil.copy2(src,dest)


### Network Functions ###

def getInterfaces(legacy=False,newkernel=False):
    """Get a list of available interfaces"""
    interfaces = []

    if legacy is False and os.path.exists('/sbin/ip'):
        # test for new kernel
        kernel_version = re.split('[\.-]',os.uname()[2])[:3]
        kernel = all(map(lambda b,c:int(b)>=int(c),kernel_version,[2,6,35]))
        # get results of ip
        try:
            ret = subprocess.Popen('/sbin/ip addr',shell=True,stdout=subprocess.PIPE).communicate()[0]
            if kernel is True or newkernel is not False:
                ret2 = subprocess.Popen('/sbin/ip -s link',shell=True,stdout=subprocess.PIPE).communicate()[0]
            else:
                ret2 = subprocess.Popen('cat /proc/net/dev',shell=True,stdout=subprocess.PIPE).communicate()[0]
        except OSError, e:
            logger.error('Error calling /sbin/ip: %s'%(str(e)))
            return getInterfaces(True)

        # parse results
        groups = [[]]
        i=0
        for line in ret.split('\n'):
            if line is None or line.strip() == '':
                continue
            if line[0] != ' ' and len(groups[i]) != 0:
                i += 1
                groups.append([])
            words = line.split()
            if line[0] != ' ':
                words.pop(0)
            groups[i].extend(words)
        if len(groups[i]) == 0:
            del groups[i]

        for g in groups:
            if 'UP' not in g[1] or 'DOWN' in g[1]:
                continue
            iface = util.IFace()
            iface.name = g.pop(0)[:-1]
            link = {}
            nextis = None
            for w in g:
                if nextis == 'mac':
                    iface.mac = w
                    nextis = None
                elif nextis == 'ip':
                    if '/' in w:
                        link['ip'] = w.split('/')[0]
                        link['Mask'] = '/'+w.split('/')[1]
                    else:
                       link['ip'] = w
                    nextis = None
                elif nextis == 'brd':
                    link['Bcast'] = w
                    nextis = None
                else:
                    if w.startswith('link') and '/' in w:
                        iface.encap = w.split('/')[1]
                        nextis = 'mac'
                    elif w == 'brd' and link != {}:
                        next = 'brd'
                    elif w.startswith('inet'):
                        if link != {}:
                            iface.link.append(link)
                        link = {}
                        if w == 'inet':
                            link['type'] = 'ipv4'
                        elif w == 'inet6':
                            link['type'] = 'ipv6'
                        else:
                            link['type'] = w
                        nextis = 'ip'
            if link != {}:
                iface.link.append(link)

            interfaces.append(iface)

        # get bandwidth stats
        groups = [[]]
        i = 0
        if kernel is True or newkernel is not False:
            for line in ret2.split('\n'):
                if line is None or line.strip() == '':
                    continue
                if line[0] != ' ' and len(groups[i]) != 0:
                    i += 1
                    groups.append([])
                words = line.split()
                if line[0] != ' ':
                    words.pop(0)
                groups[i].append(words)
            if len(groups[i]) == 0:
                del groups[i]

            for g in groups:
                name = g[0][0][:-1]
                for iface in interfaces:
                    if name == iface.name:
                        # add stats to iface
                        iface.rx_bytes = long(g[3][0])
                        iface.rx_packets = long(g[3][1])
                        iface.tx_bytes = long(g[5][0])
                        iface.tx_packets = long(g[5][1])
        else:
            for line in ret2.split('\n'):
                if line is None or line.strip() == '':
                    continue
                if i >= 2:
                    groups[i-2].extend(line.split())
                    groups.append([])
                i += 1
            if len(groups[i-2]) == 0:
                del groups[i-2]

            for g in groups:
                name = g[0].split(':')[0]
                if g[0].split(':')[1] != '':
                    g[0] = g[0].split(':')[1]
                else:
                    g.pop(0)
                for iface in interfaces:
                    if name == iface.name:
                        # add stats to iface
                        iface.rx_bytes = long(g[0])
                        iface.rx_packets = long(g[1])
                        iface.tx_bytes = long(g[8])
                        iface.tx_packets = long(g[9])

    else:
        # get results of ifconfig
        from math import log10 as log
        try:
            ret = subprocess.Popen('/sbin/ifconfig',shell=True,stdout=subprocess.PIPE).communicate()[0]
        except OSError, e:
            logger.error('Error calling /sbin/ifconfig: %s'%(str(e)))
            return []
        # parse results
        groups = [[]]
        i=0
        for line in ret.split('\n'):
            words = line.split()
            if len(words) == 0:
                if len(groups[i]) != 0:
                    i += 1
                    groups.append([])
                continue
            groups[i].extend(words)
        if len(groups[i]) == 0:
            del groups[i]

        for g in groups:
            try:
                iface = util.IFace()
                iface.name = g.pop(0)
                link = {}
                nextis = None
                for w in g:
                    if nextis == 'mac':
                        iface.mac = w
                        nextis = None
                    elif nextis == 'ip':
                        if '/' in w:
                            link['ip'] = w.split('/')[0]
                            link['Mask'] = '/'+w.split('/')[1]
                        else:
                            link['ip'] = w
                        nextis = None
                    elif nextis == 'rx':
                        tmp = w.split(':')
                        if tmp[0] == 'packets':
                            iface.rx_packets = long(tmp[1])
                        elif tmp[0] == 'bytes':
                            iface.rx_bytes = long(tmp[1])
                        else:
                            logger.warning('error when nextis=rx')
                        nextis = None
                    elif nextis == 'tx':
                        tmp = w.split(':')
                        if tmp[0] == 'packets':
                            iface.tx_packets = long(tmp[1])
                        elif tmp[0] == 'bytes':
                            iface.tx_bytes = long(tmp[1])
                        else:
                            logger.warning('error when nextis=tx')
                        nextis = None
                    else:
                        if w.startswith('encap'):
                            iface.encap = w.split(':')[1]
                        elif w == 'HWaddr':
                            nextis = 'mac'
                        elif w.startswith('addr'):
                            ip = w.split(':')[1]
                            if ip == '':
                                nextis = 'ip'
                            else:
                                link['ip'] = ip
                        elif w.startswith('Bcast'):
                            link['Bcast'] = w.split(':')[1]
                        elif w.startswith('Mask'):
                            link['Mask'] = w.split(':')[1]
                            if link['Mask'][0] != '/' and '.' in link['Mask']:
                                link['Mask'] = '/'+str(int(32-log(reduce(lambda a,b:a*256+(255-int(b)),link['Mask'].split('.'),0)+1)/log(2)))
                        elif w.startswith('inet'):
                            if link != {}:
                                iface.link.append(link)
                            link = {}
                            if w == 'inet':
                                link['type'] = 'ipv4'
                            elif w == 'inet6':
                                link['type'] = 'ipv6'
                            else:
                                link['type'] = w
                        elif w == 'RX':
                            nextis = 'rx'
                        elif w == 'TX':
                            nextis = 'tx'
                if link != {}:
                    iface.link.append(link)
                interfaces.append(iface)
            except Exception:
                logger.info('nic error',exc_info=True)

    return interfaces

def get_local_ip_address():
    return socket.gethostbyname( socket.getfqdn() )

def gethostname():
    """Get host names of this computer as a set"""
    hostnames = set()
    for iface in getInterfaces():
        if iface.encap.lower() in ('local','loopback'): # ignore loopback interface
            continue
        for link in iface.link:
            if not 'ip' in link: continue
            try:
                hn = socket.gethostbyaddr(link['ip'])
            except socket.herror:
                continue # likely unknown host
            try:
                name,alias,ip = hn
                hostnames.add(name)
            except ValueError:
                pass
    if not hostnames:
        # method of last resort
        try:
            name = socket.gethostname()
        except Exception:
            pass
        else:
            if name != 'localhost':
                hostnames.add(name)
    if len(hostnames) < 1:
        return None
    elif len(hostnames) == 1:
        return hostnames.pop()
    else:
        return hostnames

def download(url,local,cache=False,proxy=False,options={}):
    """Download a file, checksumming if possible"""
    trials = 5  # repeat wget 5 times if failed
    success = False
    while (not success) and trials > 0:
        trials -= 1
        # get file
        success_file = wget(url,local,cache,proxy,options)
        if not success_file:
            logger.warn('error from wget, %d trials left',trials)
            continue
        # get checksum
        try:
            success_checksum,checksum_type = wget_checksum(checksum_url,cache=cache,proxy=proxy,options=options)
        except Exception:
            success_checksum = False
        if success_checksum:
            # check checksum
            logger.info('checking checksum for %s against %s',success_file,success_checksum)
            success = check_cksm(success_file,checksum_type,success_checksum)
        else:
            success = True
    if not success:
        logger.warn('failed to download %s to %s',url,local)
        raise Exception('download failed')

def upload(local,remote,proxy=False,options={}):
    """Upload a file, checksumming if possible"""
    if not os.path.exists(local):
        logger.warn('upload: local path, %s, does not exist' % local)
        return False
    trials = 5  # repeat wput 5 times if failed
    success = False
    while (not success) and trials > 0:
        trials -= 1
        # put file
        try:
            success = wput(local,remote,proxy,options)
        except Exception as e:
            logger.error('error uploading file %s to %s \n%r',
                         local,remote,e,exc_info=True)
            success = 1
        if success: # expect unix return code 0==OK
            logger.warn('error from wput, %d trials left',trials)
            success = False
        else:
            success = True
    if not success:
        raise Exception('upload failed')

pycurl_handle = util.PycURL()
def wget(url,dest='./',cache=False,proxy=False,options={}):
    """wrapper for downloading from multiple protocols"""
    dest = os.path.expandvars(dest)
    url  = os.path.expandvars(url)
    if not isurl(url):
        if os.path.exists(url):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)
    dest_path = dest
    if dest_path[:5] == 'file:':
        dest_path = dest_path[5:]
    if os.path.isdir(dest_path):
        dest_path = os.path.join(dest_path, os.path.basename(url))
        dest = "file:"+dest_path
    if not dest[:5] == "file:":
        dest = "file:" + dest

    logger.warn('wget(): src: %s, dest: %s',url,dest)

    if cache:
        # test for cache hit
        cache_match = incache(url,options)
        if cache_match:
            # file is cached, so get from cache
            logger.info('cache hit for %s',url)
            copy(cache_match,dest_path)
            return dest_path

    if proxy:
        # is this a full proxy?
        if proxy == 'match':
            # test url to see if it matches the proxy options
            proxy_expr = '/svn|code.icecube.wisc.edu|.py$'
            if 'proxy_expr' in options and options['proxy_expr']:
                proxy_expr = options['proxy_expr']
            if re.search(proxy_expr,url):
                proxy = True
            else:
                proxy = False
        if (proxy is True or (isinstance(proxy,str) and
            url.startswith(proxy)) or (isinstance(proxy,(list,tuple)) and
            reduce(lambda a,b:a or url.startswith(b),proxy,False))):
            # test for proxy hit
            try:
                ret = inproxy(url,dest_path,options)
            except Exception as e:
                logger.error('error in proxy: %r',e)
            else:
                if ret and cache:
                    # insert in cache
                    insertincache(url,ret,options)
                return ret

    # actually download the file locally
    ret = None
    if url[:5] in ('http:','https','ftp:/','ftps:'):
        # use pycurl
        logger.info('curl from %s to %s', url, dest_path)
        global pycurl_handle
        for i in xrange(0,2):
            try:
                kwargs = {}
                if 'username' in options:
                    kwargs['username'] = options['username']
                if 'password' in options:
                    kwargs['password'] = options['password']
                if 'sslcert' in options:
                    kwargs['ssl_cert'] = options['sslcert']
                if 'sslkey' in options:
                    kwargs['ssl_key'] = options['sslkey']
                if 'cacert' in options:
                    kwargs['cacert'] = options['cacert']
                # do regular get
                pycurl_handle.fetch(url,dest_path,**kwargs)
            except util.NoncriticalError as e:
                ee = e.value
                try:
                    if 'HTTP error code' in ee:
                        if int(ee.split(':')[-1]) == 405 and post == 1:
                            # need to use get
                            post = 0
                            continue
                except:
                    pass
                raise e
            except Exception as e:
                if i == 0:
                    # try regenerating the pycurl handle
                    logger.info('regenerating pycurl handle because of error')
                    pycurl_handle = util.PycURL()
                else:
                    logger.error('error fetching url %s : %s',url,e)
                    ret = None
            else:
                if os.path.exists(dest_path):
                    ret = dest_path
                break
    elif url[:5] == 'file:':
        # use copy command
        logger.info('copy from %s to %s', url[5:], dest_path)
        if os.path.exists(url[5:]):
            copy(url[5:],dest_path)
            ret = dest_path
    elif url[:7] == 'gsiftp:':
        logger.info('gsiftp from %s to %s', url, dest_path)
        try:
            ret = GridFTP.get(url,filename=dest_path)
        except Exception as e:
            logger.error('error fetching url %s', url, exc_info=True)
            ret = None
        else:
            if ret is not True or not os.path.exists(dest_path):
                logger.error('error fetching url %s',url)
                ret = None
            else:
                ret = dest_path
    else:
        # command line programs
        if url.startswith("lfn:"):
            cmd = "lcg-cp --vo icecube -v %s %s" % (url,dest)
        else:
            raise Exception("unsupported protocol %s" % url)
        # add options
        if 'cmd_line_opts' in options:
            cmd += options['cmd_line_opts']
        cmd += ' >/dev/null 2>&1'
        logging.info(cmd)
        if not subprocess.call(cmd,shell=True):
            ret = dest_path

    if ret is None:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        raise Exception('download failed - ret is None')
    elif cache:
        # insert in cache
        insertincache(url,ret,options)
    return ret

def wget_checksum(url,cache=False,proxy=False,options={}):
    """wrapper for getting checksum from multiple protocols"""
    url  = os.path.expandvars(url)
    if not isurl(url):
        if os.path.exists(url):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)

    if cache:
        # test for cache hit
        cache_match = incache_checksum(url,options)
        if cache_match:
            # file is cached
            logger.info('cache checksum hit for %s',url)
            return cache_match

    if proxy:
        # is this a full proxy?
        if proxy == 'match':
            # test url to see if it matches the proxy options
            proxy_expr = '/svn|code.icecube.wisc.edu|.py$|.py.md5sum$'
            if 'proxy_expr' in options and options['proxy_expr']:
                proxy_expr = options['proxy_expr']
            if re.search(proxy_expr,url):
                proxy = True
            else:
                proxy = False
        if (proxy is True or (isinstance(proxy,str) and
            url.startswith(proxy)) or (isinstance(proxy,(list,tuple)) and
            reduce(lambda a,b:a or url.startswith(b),proxy,False))):
            # test for proxy hit
            try:
                ret = inproxy_ckechsum(url,options)
            except Exception as e:
                logger.error('error in proxy: %r',e)
            else:
                if ret and cache:
                    # insert in cache
                    insertincache_checksum(url,ret[0],ret[1],options)
                return ret

    # actually get the checksum directly
    ret = None
    if url[:5] in ('http:','https','ftp:/','ftps:'):
        # use pycurl
        global pycurl_handle
        for i in xrange(0,2):
            try:
                kwargs = {}
                if 'username' in options:
                    kwargs['username'] = options['username']
                if 'password' in options:
                    kwargs['password'] = options['password']
                if 'sslcert' in options:
                    kwargs['ssl_cert'] = options['sslcert']
                if 'sslkey' in options:
                    kwargs['ssl_key'] = options['sslkey']
                if 'cacert' in options:
                    kwargs['cacert'] = options['cacert']
                def cb(data):
                    cb.data += data
                cb.data = ''
                # do regular get
                # try every extension type for checksums
                for type in ('sha512','sha256','sha1','md5'):
                    try:
                        url2 = url+type+'sum'
                        pycurl_handle.post(url2,cb,**kwargs)
                    except util.NoncriticalError:
                        continue
                    else:
                        break
                if cb.data:
                    ret = (cb.data,type)
            except util.NoncriticalError as e:
                ee = e.value
                try:
                    if 'HTTP error code' in ee:
                        if int(ee.split(':')[-1]) == 405 and post == 1:
                            # need to use get
                            post = 0
                            continue
                except:
                    pass
                raise e
            except Exception as e:
                if i == 0:
                    # try regenerating the pycurl handle
                    logger.warn('regenerating pycurl handle because of error')
                    pycurl_handle = util.PycURL()
                else:
                    logger.error('error fetching url %s : %s',url,e)
                    ret = None
            else:
                break
    elif url[:5] == 'file:':
        # generate md5sum right now
        try:
            ret = (sha512sum(url[5:]),'sha512')
        except:
            ret = None
    elif url[:7] == 'gsiftp:':
        try:
            ret = GridFTP.sha512sum(url)
        except Exception as e:
            logger.error('error checksumming url %s : %r',url,e)
            ret = None
        else:
            if ret:
                ret = (ret,'sha512')
            else:
                logger.error('error checksumming url %s',url)
                ret = None
    else:
        # command line programs
        for type in ('sha512','sha256','sha1','md5'):
            try:
                url2 = url+type+'sum'
                (f,dest) = tempfile.mkstemp()
                dest2 = 'file:'+dest
                f.close()
                if url2.startswith("lfn:"):
                    cmd = "lcg-cp --vo icecube -v %s %s" % (url2,dest2)
                else:
                    raise Exception("unsupported protocol %s" % url)
                # add options
                if 'cmd_line_opts' in options:
                    cmd += options['cmd_line_opts']
                cmd += ' >/dev/null 2>&1'
                logging.info(cmd)
                if not subprocess.call(cmd,shell=True):
                    for l in open(dest,'r'):
                        if os.path.basename(url) in l:
                            ret, name = l.strip('\n').split()
                            break
                if os.path.exists(dest):
                    os.remove(dest)
            except:
                pass
            if ret:
                ret = (ret,type)
                break

    if ret and cache:
        # insert in cache
        insertincache_checksum(url,ret[0],ret[1],options)
    return ret

def wput(source,url,proxy=False,options={}):
    """wrapper for uploading using multiple protocols

       options
         :proxy_addr: main address of an iceprod server
         :key: proxy key
         :http_username: for http basic auth, the username
         :http_password: for http basic auth, the password
         :ssl_cert: for https, the cert
         :ssl_key: for https, the key
         :ssl_cacert: for https, the CA cert

       If using the command line, options should be name,value pairs
       that can be mapped to --name=value arguments.
    """
    global pycurl_handle
    source = os.path.expandvars(source)
    url  = os.path.expandvars(url)
    if not isurl(url):
        if os.path.exists(os.path.dirname(url)):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)

    source_path = source
    if source_path[:5] == 'file:':
        source_path = source_path[5:]
    if os.path.exists(source_path):
        source_path = os.path.abspath(source_path)
    if os.path.isdir(source_path):
        compress(source_path,'tar')
        source_path += '.tar'
        source = "file:"+source_path
    if not source[:5] == "file:":
        source = "file:" + source

    logger.warn('wput(): src: %s, dest: %s',source,url)

    chksum = sha512sum(source_path)
    chksum_type = 'sha512'

    if (proxy is True or (isinstance(proxy,str) and url.startswith(proxy)) or
        (isinstance(proxy,(list,tuple)) and
        reduce(lambda a,b:a or url.startswith(b),proxy,False))):
        # upload to proxy first
        if 'proxy_addr' not in options:
            raise Exception('proxy_addr not in options, so cannot locate proxy')
        if 'key' not in options:
            raise Exception('auth key not in options, so cannot communicate with server')
        proxy_addr = options['proxy_addr']

        try:
            kwargs = {}
            if 'username' in options:
                kwargs['username'] = options['username']
            if 'password' in options:
                kwargs['password'] = options['password']
            if 'sslcert' in options:
                kwargs['ssl_cert'] = options['sslcert']
            if 'sslkey' in options:
                kwargs['ssl_key'] = options['sslkey']
            if 'cacert' in options:
                kwargs['cacert'] = options['cacert']

            # initial json request request
            def reply(data):
                reply.data += data
            reply.data = ''
            for i in range(0,2):
                try:
                    body = json_encode({'type':'upload',
                                        'url':url,
                                        'size':os.path.getsize(source_path),
                                        'checksum':chksum,
                                        'checksum_type':chksum_type,
                                        'key':options['key']
                                       })
                    pycurl_handle.post(proxy_addr+'/upload',reply,
                                       postbody=body,**kwargs)
                except Exception as e:
                    if i == 0:
                        # try regenerating the pycurl handle
                        logger.warn('regenerating pycurl handle because of error')
                        pycurl_handle = util.PycURL()
                    else:
                        logger.error('error uploading to url %s : %r',url,e)
                        raise
                else:
                    break
            if not reply.data:
                raise Exception('Unknown error contacting proxy server')
            data = json_decode(reply.data)
            if data['type'] != 'upload' or data['url'] != url:
                raise Exception('Received bad response from proxy')
            upload_url = data['upload']

            if upload_url:
                # actual upload
                for i in xrange(0,2):
                    try:
                        pycurl_handle.put(proxy_addr+upload_url,
                                          source_path, **kwargs)
                    except Exception as e:
                        if i == 0:
                            # try regenerating the pycurl handle
                            logger.warn('regenerating pycurl handle because of error')
                            pycurl_handle = util.PycURL()
                        else:
                            logger.error('error uploading to url %s : %r',url,e)
                            raise
                    else:
                        break
            # else: we've already uploaded this file, so just check the status

            # check that upload was successful
            for _ in xrange(100):
                reply.data = ''
                for i in xrange(0,2):
                    try:
                        body = json_encode({'type':'check',
                                            'url':url,
                                            'key':options['key']
                                           })
                        pycurl_handle.post(proxy_addr+'/upload',reply,
                                           postbody=body,**kwargs)
                    except Exception as e:
                        if i == 0:
                            # try regenerating the pycurl handle
                            logger.warn('regenerating pycurl handle because of error')
                            pycurl_handle = util.PycURL()
                        else:
                            logger.error('error uploading to url %s : %r',url,e)
                            raise
                    else:
                        break
                if not reply.data:
                    raise Exception('Unknown error contacting proxy server')
                data = json_decode(reply.data)
                if data['type'] != 'check' or data['url'] != url:
                    raise Exception('Received bad response from proxy')
                if data['result'] == 'still uploading':
                    time.sleep(10)
                    continue
                elif not data['result']:
                    raise Exception('Recieved an error when checking the upload')
                else:
                    # success
                    break

        except Exception as e:
            logger.error('error uploading using proxy to url %s : %r',url,e)
            return 1

        return 0

    # actually upload the file
    ret = None
    urlprefix = url[:5]
    if urlprefix in ('http:','https','ftp:/','ftps:'):
        # use pycurl
        for i in range(0,2):
            try:
                kwargs = {}
                if 'http_username' in options:
                    kwargs['username'] = options['http_username']
                if 'http_password' in options:
                    kwargs['password'] = options['http_password']
                if 'ssl_cert' in options:
                    kwargs['sslcert'] = options['ssl_cert']
                if 'ssl_key' in options:
                    kwargs['sslkey'] = options['ssl_key']
                if 'ssl_cacert' in options:
                    kwargs['cacert'] = options['ssl_cacert']
                pycurl_handle.put(url,source_path,**kwargs)
            except Exception as e:
                if i == 0:
                    # try regenerating the pycurl handle
                    logger.warn('regenerating pycurl handle because of error')
                    pycurl_handle = util.PycURL()
                else:
                    logger.error('error uploading to url %s', url, exc_info=True)
                    raise
            else:
                break
    elif urlprefix == 'file:':
        # use copy command
        url = url[5:]
        if os.path.exists(url):
            logger.warn('url already exists. overwriting!')
            os.remove(url)
        copy(source_path,url)
    elif url[:7] == 'gsiftp:':
        try:
            ret = GridFTP.put(url,filename=source_path)
        except Exception as e:
            logger.error('error putting url %s : %r',url,e)
            ret = 1
        else:
            if ret is not True:
                logger.error('error putting url %s',url)
                ret = 1
            else:
                try:
                    ret = GridFTP.sha512sum(url)
                except Exception as e:
                    logger.error('error checksumming url %s', url, exc_info=True)
                    ret = 1
                else:
                    if ret and ret == chksum:
                        ret = 0
                    else:
                        logger.error('error checksumming url %s',url)
                        ret = 1
    else:
        # command line programs
        if url.startswith("lfn:"):
            cmd = "lcg-cp --vo icecube -v %s %s" % (source,url)
        else:
            raise Exception("unsupported protocol %s" % url)
        # add options
        if 'cmd_line_opts' in options:
            cmd += options['cmd_line_opts']
        cmd += ' >/dev/null 2>&1'
        logging.info(cmd)
        if not subprocess.call(cmd,shell=True):
            raise('failed to run cmd: %s'%cmd)

    return ret

def isurl(url):
    """Determine if this is a supported protocol"""
    prefixes = ('file:','http:','https:','ftp:','ftps:','gsiftp:','srm:','lfn:')
    try:
        return url.startswith(prefixes)
    except:
        try:
            return reduce(lambda a,b: a or url.startswith(b), prefixes, False)
        except:
            return False

def _getcachedir(options={}):
    # get cache directory
    cache_dir = 'cache'
    if 'cache_dir' in options and options['cache_dir']:
        cache_dir = os.path.expandvars(options['cache_dir'])
    if not cache_dir.startswith('/'):
        cache_dir = os.path.join(os.getcwd(),cache_dir)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    return cache_dir

def insertincache(url,file,options={}):
    """Copy file to cache"""
    if 'debug' in options:
        return None

    # check url type
    search_expr = '/svn|code.icecube.wisc.edu|.py$'
    if 'cache_script_expr' in options and options['cache_script_expr']:
        search_expr = options['cache_script_expr']
    if re.search(search_expr,url):
        # this is a script
        # check if this is a release / candidate / tag
        if not re.search('release|candidate|tag',url):
            # don't cache svn files under active development
            return

    # get cache directory
    cache_dir = _getcachedir(options)

    # get hash of url
    hash = cksm(url,'sha512',file=False)

    # copy file
    logger.info('insertincache(%s) = %s',url,hash)
    match = os.path.join(cache_dir,hash)
    copy(file,match)

def insertincache_checksum(url,checksum,type='sha512',options={}):
    """Copy checksum to cache"""
    if 'debug' in options:
        return None

    # check url type
    search_expr = '/svn|code.icecube.wisc.edu|.py$'
    if 'cache_script_expr' in options and options['cache_script_expr']:
        search_expr = options['cache_script_expr']
    if re.search(search_expr,url):
        # this is a script
        # check if this is a release / candidate / tag
        if not re.search('release|candidate|tag',url):
            # don't cache svn files under active development
            return

    # get cache directory
    cache_dir = _getcachedir(options)

    # get hash of url
    hash = cksm(url,'sha512',file=False)

    # copy file
    logger.info('insertincache(%s) = %s',url,hash)
    match = os.path.join(cache_dir,hash)+'_cksm'
    pickle.dump({'cksm':checksum,'type':type},open(match,'w'))

def incache(url,options={}):
    """Test if the url is in the cache, and return it if available"""
    # test for debug variable, skipping cache if debug is enabled
    if 'debug' in options:
        return None
    logger.info('incache(%s)',url)

    # get cache directory
    cache_dir = _getcachedir(options)

    # get hash of url
    hash = cksm(url,'sha512',file=False)

    # check cache dir for url
    match = os.path.join(cache_dir,hash)
    if os.path.exists(match):
        # check url type
        search_expr = '/svn|code.icecube.wisc.edu|.py$'
        if 'cache_script_expr' in options and options['cache_script_expr']:
            search_expr = options['cache_script_expr']
        if re.search(search_expr,url):
            # this is a script
            # check if this is a release / candidate / tag
            if re.search('release|candidate|tag',url):
                # use cached value
                return match
        else:
            # this is not a script
            # check date
            cache_age = '100800' # 4 weeks default
            if 'cache_age' in options and options['cache_age']:
                cache_age = options['cache_age']
            try:
                cache_age = float(cache_age)
            except:
                cache_age = 100800.0
                logger.error('error converting cache_age to float: %s',str(cache_age))
            if os.path.getmtime(match) >= (time.time()-cache_age):
                return match

    # no matches in cache
    return None

def incache_checksum(url,options={}):
    """Test if the checksum is in the cache, and return it if available"""
    # test for debug variable, skipping cache if debug is enabled
    if 'debug' in options:
        return None
    logger.info('incache(%s)',url)

    # get cache directory
    cache_dir = _getcachedir(options)

    # get hash of url
    hash = cksm(url,'sha512',file=False)

    # check cache dir for url
    match = os.path.join(cache_dir,hash)+'_cksm'
    if os.path.exists(match):
        # check url type
        search_expr = '/svn|code.icecube.wisc.edu|.py$'
        if 'cache_script_expr' in options and options['cache_script_expr']:
            search_expr = options['cache_script_expr']
        if re.search(search_expr,url):
            # this is a script
            # check if this is a release / candidate / tag
            if re.search('release|candidate|tag',url):
                # use cached value
                cache = pickle.load(open(match))
                return (cache['cksm'],cache['type'])
        else:
            # this is not a script
            # check date
            cache_age = '100800' # 4 weeks default
            if 'cache_age' in options and options['cache_age']:
                cache_age = options['cache_age']
            try:
                cache_age = float(cache_age)
            except:
                cache_age = 100800.0
                logger.error('error converting cache_age to float: %s',str(cache_age))
            if os.path.getmtime(match) >= (time.time()-cache_age):
                cache = pickle.load(open(match))
                return (cache['cksm'],cache['type'])

    # no matches in cache
    return None

def inproxy(url,dest_path,options):
    """Ask the proxy to download the file"""
    if 'proxy_addr' not in options:
        raise Exception('proxy_addr not in options, so cannot locate proxy')
    proxy_addr = options['proxy_addr']
    if 'key' not in options:
        raise Exception('auth key not in options, so cannot communicate with server')

    # make request body
    body = {'url':url,'key':options['key']}

    # use pycurl
    global pycurl_handle
    with open(dest_path,'w') as f:
        for i in xrange(0,2):
            try:
                kwargs = {}
                if 'http_username' in options:
                    kwargs['username'] = options['http_username']
                if 'http_password' in options:
                    kwargs['password'] = options['http_password']
                if 'ssl_cert' in options:
                    kwargs['sslcert'] = options['ssl_cert']
                if 'ssl_key' in options:
                    kwargs['sslkey'] = options['ssl_key']
                if 'ssl_cacert' in options:
                    kwargs['cacert'] = options['ssl_cacert']
                kwargs['postbody'] = json_encode(body)
                pycurl_handle.post(proxy_addr,f.write,**kwargs)
            except Exception as e:
                if i == 0:
                    # try regenerating the pycurl handle
                    logger.warn('regenerating pycurl handle because of error')
                    pycurl_handle = util.PycURL()
                else:
                    logger.error('error fetching url %s : %s',url,e)
                    ret = None
            else:
                if os.path.exists(dest_path):
                    ret = dest_path
                break
    return ret

def inproxy_checksum(url,dest_path,options):
    """Ask the proxy to download the checksum"""
    if 'proxy_addr' not in options:
        raise Exception('proxy_addr not in options, so cannot locate proxy')
    proxy_addr = options['proxy_addr']
    if 'key' not in options:
        raise Exception('auth key not in options, so cannot communicate with server')

    # make request body
    body = {'url':url,'key':options['key'],'type':'checksum'}

    ret = None

    def cb(data):
        cb.data += data
    cb.data = ''

    # use pycurl
    global pycurl_handle
    for i in xrange(0,2):
        try:
            kwargs = {}
            if 'http_username' in options:
                kwargs['username'] = options['http_username']
            if 'http_password' in options:
                kwargs['password'] = options['http_password']
            if 'ssl_cert' in options:
                kwargs['sslcert'] = options['ssl_cert']
            if 'ssl_key' in options:
                kwargs['sslkey'] = options['ssl_key']
            if 'ssl_cacert' in options:
                kwargs['cacert'] = options['ssl_cacert']
            kwargs['postbody'] = json_encode(body)
            pycurl_handle.post(proxy_addr,cb,**kwargs)
        except Exception as e:
            if i == 0:
                # try regenerating the pycurl handle
                logger.warn('regenerating pycurl handle because of error')
                pycurl_handle = util.PycURL()
            else:
                logger.error('error fetching url %s : %s',url,e)
                ret = None
        else:
            if cb.data:
                ret = cb.data
            break

    return ret

def sendMail(cfg,subject,msg):
    """Send email"""
    smtpuser = "%s@%s" % (os.getlogin(),os.uname()[1].split(".")[0])
    proto = 'sendmail'
    if cfg.has_option('monitoring','smtphost'):
       try:
          import smtplib
       except ImportError, e:
          logger.error(e)
       else:
          proto = 'smtp'

    if proto == 'stmp':
       smtphost  = cfg.get('monitoring','smtphost')
       smtpuser  = cfg.get('monitoring','smtpuser')

       from_addr = "From: " + smtpuser
       to_addrs  = cfg.get('monitoring','smtpnotify').split(',')
       subject   = "Subject: %s\n" % subject

       server = smtplib.SMTP(smtphost)
       server.sendmail(from_addr, to_addrs, subject + msg)
       server.quit()

    else:
       sendmail_location = "/usr/sbin/sendmail" # sendmail location
       p = os.popen("%s -t" % sendmail_location, "w")
       p.write("From: %s\n" % smtpuser)
       p.write("To: %s\n" % smtpuser)
       p.write("Subject: %s\n" % subject)
       p.write("\n")
       p.write(msg)
       status = p.close()
       if status != 0:
          raise Exception("Sendmail exited with status %u" % status)
