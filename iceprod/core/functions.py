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
except ImportError:
    import pickle

try:
    import psutil
except ImportError:
    psutil = None

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

def uncompress(infile, out_dir=None):
    """Uncompress a file, if possible"""
    files = []
    cur_dir = os.getcwd()
    try:
        if out_dir:
            os.chdir(out_dir)
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
    finally:
        os.chdir(cur_dir)

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

def getInterfaces():
    """
    Get a list of available interfaces.

    Requires `psutil`.

    Returns:
        dict of {nic_name: {type: address}}
    """
    interfaces = {}

    ret = psutil.net_if_addrs()
    for nic_name in ret:
        n = {}
        for snic in ret[nic_name]:
            if not snic.address:
                continue
            if snic.family == socket.AF_INET:
                n['ipv4'] = snic.address
            elif snic.family == socket.AF_INET6:
                n['ipv6'] = snic.address
            elif snic.family == psutil.AF_LINK:
                n['mac'] = snic.address
        interfaces[nic_name] = n
    return interfaces

def get_local_ip_address():
    """Get the local (loopback) ip address"""
    try:
        return socket.gethostbyname('localhost')
    except:
        return socket.gethostbyname( socket.getfqdn() )

def gethostname():
    """Get hostname of this computer."""
    ret = socket.getfqdn()
    try:
        resp = requests.get('http://simprod.icecube.wisc.edu/downloads/getip.php')
        resp.raise_for_status()
        logger.info('getip: %r', resp.text)
        ret2 = resp.text.split(' ')[-1]
        if len(ret2.split('.')) > 1:
            ret = '.'.join(ret.split('.')[:1]+ret2.split('.')[1:])
    except:
        logger.info('error getting global ip', exc_info=True)
    return ret

def download(url, local, cache=False, options={}):
    """Download a file, checksumming if possible"""
    local = os.path.expandvars(local)
    url  = os.path.expandvars(url)
    if not isurl(url):
        if os.path.exists(url):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)

    # strip off query params
    if '?' in url:
        clean_url = url[:url.find('?')]
    elif '#' in url:
        clean_url = url[:url.find('#')]
    else:
        clean_url = url

    # fix local to be a filename to write to
    if local.startswith('file:'):
        local = local[5:]
    if os.path.isdir(local):
        local = os.path.join(local, os.path.basename(clean_url))

    logger.warn('wget(): src: %s, local: %s', url, local)

    # get checksum
    for checksum_type in ('sha512', 'sha256', 'sha1', 'md5'):
        ending = '.'+checksum_type+'sum'
        try:
            _wget(clean_url+ending, local+ending, options)
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
            removedirs(local)
            removedirs(local+ending)
            raise Exception('checksum failed')

    if cache:
        insertincache(url, local, options=options)

    return local

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
            for i in range(4, -1, -1):
                try:
                    r = s.get(url, stream=True, timeout=60)
                    with open(local, 'wb') as f:
                        for chunk in r.iter_content(65536):
                            f.write(chunk)
                    r.raise_for_status()
                    break
                except:
                    if os.path.exists(local):
                        os.remove(local)
                    if i <= 0:
                        logger.error('error fetching url %s', url,
                                     exc_info=True)
                        raise
                    else:
                        logger.info('retrying download')
    elif url.startswith('file:'):
        url = url[5:]
        logger.info('copy from %s to %s', url, local)
        if os.path.exists(url):
            copy(url, local)
    elif url.startswith('gsiftp:') or url.startswith('ftp:'):
        logger.info('gsiftp from %s to %s', url, local)
        if not GridFTP.get(url, filename=local):
            if os.path.exists(local):
                removedirs(local)
            raise Exception('gridftp generic failure')
    else:
        raise Exception("unsupported protocol %s" % url)

    if not os.path.exists(local):
        raise Exception('download failed - file does not exist')
    fail = False
    with open(local) as f:
        if not f.read(10):
            fail = True
    if fail:
        os.remove(local)
        raise Exception('download failed - file is empty')

def delete(url, options={}):
    """Delete a url or file"""
    url = os.path.expandvars(url)
    if (not isurl(url)) and os.path.exists(url):
        url = 'file:'+url

    if url.startswith('http'):
        logger.info('delete http: %s', url)
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
            for i in range(4, -1, -1):
                try:
                    r = s.delete(url, timeout=60)
                    r.raise_for_status()
                    break
                except:
                    if i <= 0:
                        logger.error('error with url %s', url,
                                     exc_info=True)
                        raise
                    else:
                        logger.info('retrying delete')
    elif url.startswith('file:'):
        url = url[5:]
        logger.info('delete file: %r', url)
        if os.path.exists(url):
            removedirs(url)
    elif url.startswith('gsiftp:') or url.startswith('ftp:'):
        logger.info('delete gsiftp: %r', url)
        if not GridFTP.rmtree(url):
            raise Exception('gridftp generic failure')
    else:
        raise Exception("unsupported protocol %s" % url)

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

def platform():
    """Get the platform

    same as $PLATFORM from loader.sh
    returns $ARCH.$OSTYPE.$VER.$PYTHONUNICODE
    """
    uname = os.uname()
    arch = uname[4].replace('i686','i386')
    ostype = uname[0]
    if ostype.lower() == 'linux':
        ver = subprocess.check_output(['/usr/bin/ldd','--version'])
        ver = ver.split('\n')[0].split()[-1]
    else:
        ver = uname[2]
    python_unicode = 'ucs4' if sys.maxunicode == 1114111 else 'ucs2'
    return '%s.%s.%s.%s'%(arch,ostype,ver,python_unicode)
