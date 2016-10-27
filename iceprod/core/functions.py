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

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

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

def load_cksm(sumfile, base_filename):
    """Load the checksum from a file"""
    for l in open(sumfile,'r'):
        if os.path.basename(base_filename) in l:
            sum_cksm, name = l.strip('\n').split()
            return sum_cksm
    raise Exception('could not find checksum in file')

def check_cksm(file,type,sum):
    """Check a checksum of a file"""
    if not os.path.exists(file):
        return False
    # get checksum from file
    file_cksm = cksm(file,type)
    # load sum
    if os.path.isfile(sum):
        sum_cksm = load_cksm(sum, file)
    else:
        sum_cksm = sum
    # check sum
    logger.debug('file_cksm: %r', file_cksm)
    logger.debug('sum_cksm:  %r', sum_cksm)
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

def download(url, local, cache=False, options={}):
    """Download a file, checksumming if possible"""
    local = os.path.expandvars(local)
    url  = os.path.expandvars(url)
    if not isurl(url):
        if os.path.exists(url):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)
    if local.startswith('file:'):
        local = local[5:]
    if os.path.isdir(local):
        local = os.path.join(local, os.path.basename(url))

    logger.warn('wget(): src: %s, local: %s', url, local)

    # get checksum
    for checksum_type in ('sha512', 'sha256', 'sha1', 'md5'):
        ending = '.'+checksum_type+'sum'
        try:
            _wget(url+ending, local+ending, options)
            break
        except:
            logger.debug('failed to get checksum for %s', url+ending,
                         exc_info=True)
    else:
        checksum_type = None

    if cache and checksum_type:
        try:
            # test for cache hit
            cache_match = incache(url, options=options)
            if cache_match:
                logger.info('checking checksum for %s', cache_match)
                s = load_cksm(local+ending, local)
                if not check_cksm(cache_match, checksum_type, s):
                    raise Exception('checksum failed')
                logger.info('cache hit for %s', url)
                copy(cache_match, local)
                return
            else:
                logger.info('cache miss for %s', url)
        except:
            logger.warn('cache failed, try direct download', exc_info=True)

    # actually download the file locally
    _wget(url, local, options)

    if checksum_type:
        logger.info('checking checksum with type %s for %s', checksum_type,
                    local)
        if not check_cksm(local, checksum_type, local+ending):
            raise Exception('checksum failed')

    if cache:
        insertincache(url, local, options=options)

def upload(local, url, options={}):
    """Upload a file, checksumming if possible"""
    local = os.path.expandvars(local)
    url  = os.path.expandvars(url)
    if not isurl(url):
        if os.path.exists(url):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)
    if local.startswith('file:'):
        local = local[5:]
    if os.path.isdir(local):
        compress(local, 'tar')
        local += '.tar'

    logger.warn('wput(): local: %s, url: %s', local, url)

    if not os.path.exists(local):
        logger.warn('upload: local path, %s, does not exist', local)
        raise Exception('local file does not exist')

    chksum = sha512sum(local)
    chksum_type = 'sha512'

    # actually upload the file
    if url.startswith('http'):
        logger.info('http from %s to %s', local, url)
        with requests.Session() as s:
            if 'username' in options and 'password' in options:
                s.auth = (options['username'], options['password'])
            if 'sslcert' in options:
                if 'sslkey' in options:
                    s.cert = (options['sslcert'], options['sslkey'])
                else:
                    s.cert = options['sslcert']
            if 'cacert' in options:
                s.verify = options['cacert']
            for i in range(5, 0, -1):
                try:
                    with open(local, 'rb') as f:
                        m = MultipartEncoder(
                            fields={'field0': ('filename', f, 'text/plain')}
                            )
                        r = s.post(url, timeout=60, data=m,
                                   headers={'Content-Type': m.content_type})
                        r.raise_for_status()
                    break
                except Exception:
                    if i <= 0:
                        logger.error('error uploading to url %s', url,
                                     exc_info=True)
                        raise
    elif url.startswith('file:'):
        # use copy command
        url = url[5:]
        if os.path.exists(url):
            logger.warn('put: file already exists. overwriting!')
            removedirs(url)
        copy(local, url)
        if sha512sum(url) != chksum:
            raise Exception('file checksum error')
    elif url.startswith('gsiftp:') or url.startswith('ftp:'):
        if not GridFTP.put(url, filename=local):
            raise Exception('gridftp error')
        ret = GridFTP.sha512sum(url)
        if ret != chksum:
            raise Exception('gridftp checksum error')
    else:
        raise Exception("unsupported protocol %s" % url)

def _wget(url, local, options):
    """wrapper for downloading from multiple protocols"""
    if url.startswith('http'):
        logger.info('http from %s to %s', url, local)
        with requests.Session() as s:
            if 'username' in options and 'password' in options:
                s.auth = (options['username'], options['password'])
            if 'sslcert' in options:
                if 'sslkey' in options:
                    s.cert = (options['sslcert'], options['sslkey'])
                else:
                    s.cert = options['sslcert']
            if 'cacert' in options:
                s.verify = options['cacert']
            for i in range(5, 0, -1):
                try:
                    r = s.get(url, stream=True, timeout=60)
                    with open(local, 'wb') as f:
                        for chunk in r.iter_content(65536):
                            f.write(chunk)
                    r.raise_for_status()
                    break
                except Exception:
                    if i <= 0:
                        logger.error('error fetching url %s', url,
                                     exc_info=True)
                        raise
    elif url.startswith('file:'):
        url = url[5:]
        logger.info('copy from %s to %s', url, local)
        if os.path.exists(url):
            copy(url, local)
    elif url.startswith('gsiftp:') or url.startswith('ftp:'):
        logger.info('gsiftp from %s to %s', url, local)
        if not GridFTP.get(url, filename=local):
            raise Exception('gridftp generic failure')
    else:
        raise Exception("unsupported protocol %s" % url)

    if not os.path.exists(local):
        raise Exception('download failed - file does not exist')

def isurl(url):
    """Determine if this is a supported protocol"""
    prefixes = ('file:','http:','https:','ftp:','ftps:','gsiftp:')
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

    # get cache directory
    cache_dir = _getcachedir(options)

    # get hash of url
    hash = cksm(url,'sha512',file=False)

    # copy file
    logger.info('insertincache(%s) = %s',url,hash)
    match = os.path.join(cache_dir,hash)
    copy(file,match)

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
        return match

    # no matches in cache
    return None
