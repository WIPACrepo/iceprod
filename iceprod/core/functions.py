"""
Common functions
"""

from __future__ import absolute_import, division, print_function

import os
import shutil
import logging
import socket
import subprocess
import hashlib
from functools import partial, reduce
from contextlib import contextmanager
import asyncio

try:
    import psutil
except ImportError:
    psutil = None

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from rest_tools.client import Session, AsyncSession

from iceprod.core.gridftp import GridFTP


# Compression Functions #
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
        logging.info('uncompressing %s',infile)
        if istarred(infile):
            # handle tarfile
            output = subprocess.check_output(['tar','-atf',infile]).decode('utf-8')
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
                logging.info('unknown format: %s',infile)
                raise Exception('unknown format')
            subprocess.call([cmd,'-kdf',infile])
            files.append(infile.rsplit('.',1)[0])
    finally:
        os.chdir(cur_dir)

    logging.info('files: %r', files)
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
            logging.info('unknown format: %s',infile)
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
    except Exception:
        raise Exception('cannot get checksum for type %r',type)

    if file and os.path.exists(filename):
        # checksum file contents
        with open(filename,'rb') as filed:
            buffer = filed.read(buffersize)
            while buffer:
                digest.update(buffer)
                buffer = filed.read(buffersize)
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
    for line in open(sumfile, 'r'):
        if os.path.basename(base_filename) in line:
            sum_cksm, name = line.strip('\n').split()
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
    logging.debug('file_cksm: %r', file_cksm)
    logging.debug('sum_cksm:  %r', sum_cksm)
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


# File and Directory Manipulation Functions #


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
        logging.info('attempting to make parent dest dir %s',parent_dir)
        try:
            os.makedirs(parent_dir)
        except Exception:
            logging.error('failed to make dest directory for copy',exc_info=True)
            raise
    if os.path.isdir(src):
        logging.info('dircopy: %s to %s',src,dest)
        shutil.copytree(src,dest,symlinks=True)
    else:
        logging.info('filecopy: %s to %s',src,dest)
        shutil.copy2(src,dest)


# Network Functions #


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
    except Exception:
        return socket.gethostbyname(socket.getfqdn())


def gethostname():
    """Get hostname of this computer."""
    ret = socket.getfqdn()
    ret2 = socket.gethostname()
    if len(ret2) > len(ret):
        return ret2
    else:
        return ret


@contextmanager
def _http_helper(options={}, sync=True):
    """Set up an http session using requests"""
    if sync:
        session = Session
    else:
        session = AsyncSession
    with session(retries=10, backoff_factor=0.3) as s:
        if 'username' in options and 'password' in options:
            s.auth = (options['username'], options['password'])
        if 'sslcert' in options:
            if 'sslkey' in options:
                s.cert = (options['sslcert'], options['sslkey'])
            else:
                s.cert = options['sslcert']
        if 'cacert' in options:
            s.verify = options['cacert']
        if 'token' in options:
            s.headers.update({'Authorization': f'bearer {options["token"]}'})
        yield s


async def download(url, local, options={}):
    """Download a file, checksumming if possible"""
    local = os.path.expanduser(os.path.expandvars(local))
    url = os.path.expanduser(os.path.expandvars(url))
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

    logging.warning('wget(): src: %s, local: %s', url, local)

    # actually download the file
    try:
        if url.startswith('http'):
            logging.info('http from %s to %s', url, local)
            # http_proxy fix
            for k in os.environ:
                if k.lower() == 'http_proxy' and not os.environ[k].startswith('http'):
                    os.environ[k] = 'http://'+os.environ[k]

            def _d():
                with _http_helper(options) as s:
                    r = s.get(url, stream=True, timeout=300)
                    with open(local, 'wb') as f:
                        for chunk in r.iter_content(65536):
                            f.write(chunk)
                    r.raise_for_status()
            await asyncio.get_event_loop().run_in_executor(None, _d)
        elif url.startswith('file:'):
            url = url[5:]
            logging.info('copy from %s to %s', url, local)
            if os.path.exists(url):
                await asyncio.get_event_loop().run_in_executor(None, partial(copy, url, local))
        elif url.startswith('gsiftp:') or url.startswith('ftp:'):
            logging.info('gsiftp from %s to %s', url, local)
            await asyncio.get_event_loop().run_in_executor(None, partial(GridFTP.get, url, filename=local))
        else:
            raise Exception("unsupported protocol %s" % url)

        if not os.path.exists(local):
            raise Exception('download failed - file does not exist')
    except Exception:
        await asyncio.get_event_loop().run_in_executor(None, removedirs, local)
        raise

    return local


async def upload(local, url, checksum=True, options={}):
    """Upload a file, checksumming if possible"""
    local = os.path.expandvars(local)
    url = os.path.expandvars(url)
    if not isurl(url):
        if url.startswith('/'):
            url = 'file:'+url
        else:
            raise Exception("unsupported protocol %s" % url)
    if local.startswith('file:'):
        local = local[5:]
    if os.path.isdir(local):
        compress(local, 'tar')
        local += '.tar'

    logging.warning('wput(): local: %s, url: %s', local, url)

    if not os.path.exists(local):
        logging.warning('upload: local path, %s, does not exist', local)
        raise Exception('local file does not exist')

    chksum = sha512sum(local)
    chksum_type = 'sha512'
    if not checksum:
        logging.warning(f'not performing checksum {chksum_type}\n{checksum}: {url}')

    # actually upload the file
    if url.startswith('http'):
        logging.info('http from %s to %s', local, url)
        # http_proxy fix
        for k in os.environ:
            if k.lower() == 'http_proxy' and not os.environ[k].startswith('http'):
                os.environ[k] = 'http://'+os.environ[k]

        def _d():
            with _http_helper(options) as s:
                try:
                    with open(local, 'rb') as f:
                        r = s.put(url, timeout=300, data=f)
                    r.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code != 405:
                        raise
                    else:
                        logging.warning('WebDav PUT not allowed, trying multipart upload')
                        with open(local, 'rb') as f:
                            m = MultipartEncoder(
                                fields={'field0': ('filename', f, 'text/plain')}
                            )
                            r = s.post(url, timeout=300, data=m,
                                       headers={'Content-Type': m.content_type})
                            r.raise_for_status()
                if checksum:  # get checksum
                    if 'ETAG' in r.headers:
                        md5 = r.headers['ETAG'].strip('"\'')
                        if md5sum(local) != md5:
                            raise Exception('http checksum error')
                    else:
                        r = s.get(url, stream=True, timeout=300)
                        try:
                            with open(local+'.tmp', 'wb') as f:
                                for chunk in r.iter_content(65536):
                                    f.write(chunk)
                            r.raise_for_status()
                            if sha512sum(local+'.tmp') != chksum:
                                raise Exception('http checksum error')
                        finally:
                            removedirs(local+'.tmp')
        await asyncio.get_event_loop().run_in_executor(None, _d)
    elif url.startswith('file:'):
        # use copy command
        url = url[5:]

        def _c():
            if os.path.exists(url):
                logging.warning('put: file already exists. overwriting!')
                removedirs(url)
            copy(local, url)
            if checksum and sha512sum(url) != chksum:
                raise Exception('file checksum error')
        await asyncio.get_event_loop().run_in_executor(None, _c)
    elif url.startswith('gsiftp:') or url.startswith('ftp:'):
        def _g():
            try:
                GridFTP.put(url, filename=local)
            except Exception:
                # because d-cache doesn't allow overwriting, try deletion
                GridFTP.delete(url)
                GridFTP.put(url, filename=local)
            if checksum and GridFTP.sha512sum(url) != chksum:
                raise Exception('gridftp checksum error')
        await asyncio.get_event_loop().run_in_executor(None, _g)
    else:
        raise Exception("unsupported protocol %s" % url)


def delete(url, options={}):
    """Delete a url or file"""
    url = os.path.expandvars(url)
    if (not isurl(url)) and os.path.exists(url):
        url = 'file:'+url

    if url.startswith('http'):
        logging.info('delete http: %s', url)
        with _http_helper(options) as s:
            r = s.delete(url, timeout=300)
            r.raise_for_status()
    elif url.startswith('file:'):
        url = url[5:]
        logging.info('delete file: %r', url)
        if os.path.exists(url):
            removedirs(url)
    elif url.startswith('gsiftp:') or url.startswith('ftp:'):
        logging.info('delete gsiftp: %r', url)
        GridFTP.rmtree(url)
    else:
        raise Exception("unsupported protocol %s" % url)


def isurl(url):
    """Determine if this is a supported protocol"""
    prefixes = ('file:','http:','https:','ftp:','ftps:','gsiftp:')
    try:
        return url.startswith(prefixes)
    except Exception:
        try:
            return reduce(lambda a,b: a or url.startswith(b), prefixes, False)
        except Exception:
            return False
