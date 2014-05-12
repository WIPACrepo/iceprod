"""
  Common functions

  copyright (c) 2012 the icecube collaboration
"""

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

from iceprod.core import dataclasses
from iceprod.core.gridftp import GridFTP
from iceprod.core.jsonUtil import json_encode,json_decode

logger = logging.getLogger('functions')


### Compression Functions - using p7zip ###
mmt_off = ('xz','lzma','7z')
compress_suffixes = ('.tgz','.gz','.tbz2','.tbz','.bz2','.bz','.rar',
                     '.lzma2','.lzma','.lz','.xz','.7z','.z','.Z')
tar_suffixes = ('.tgz','.tbz2','.tbz')

def _checksuffixes(file,prev,s):
    if prev:
        return prev
    elif file.endswith(s):
        return s[1:]

def uncompress(file):
    """Uncompress a file, if possible"""
    file = file.replace(';`','')
    dir = os.path.dirname(file)
    logger.info('uncompressing %s to dir %s',file,dir)
    # get file listing
    proc = subprocess.Popen('7za l -mmt=off %s'%(file),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output = proc.communicate()[0]
    if proc.returncode:
        raise dataclasses.NoncriticalError('Failed to open archive')
    files = []
    type = ''
    save = False
    for line in output.split('\n'):
        line = line.strip()
        if line[:4] == 'Type':
            type = line.split('=')[1].strip()
        elif line[:4] == '----':
            if save == True:
                save = False
            else:
                save = True
        if save:
            # save 6th col
            cols = line.split()
            if len(cols) >= 6:
                pos = line.find(cols[5])
                if pos != -1:
                    files.append(os.path.join(dir,line[pos:]))
    logger.info('files: %s',str(files))
    
    # check for existence in cache
    exists = True
    for f in files:
        if not os.path.exists(f):
            exists = False
            break
    if not exists or len(files) < 1:
        # uncompressed files do not exist yet, so actually do uncompression
        if type in mmt_off:
            proc = subprocess.Popen('7za x -y -mmt=off -o%s %s'%(dir,file),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        else:
            proc = subprocess.Popen('7za x -y -o%s %s'%(dir,file),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        output = proc.communicate()[0]
        if len(files) < 1:
            # problems getting filenamess from listing, so try getting them from extraction
            for line in output.split('\n'):
                line = line.strip()
                if line[:10] == 'Extracting':
                    cols = line.split()
                    if len(cols) >= 2:
                        pos = line.find(cols[1])
                        if pos != -1:
                            files.append(os.path.join(dir,line[pos:]))
        logger.debug(output)
        logger.info('files: %s',str(files))
    
    if not proc.returncode:
        # if everything went ok, extract tarfile if possible
        if len(files) == 1 and tarfile.is_tarfile(files[0]):
            # get file listing
            proc = subprocess.Popen('7za l -y -mmt=off %s'%(files[0]),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            output = proc.communicate()[0]
            if not proc.returncode:
                files2 = []
                type2 = ''
                save = False
                for line in output.split('\n'):
                    line = line.strip()
                    if line[:4] == 'Type':
                        type2 = line.split('=')[1].strip()
                    elif line[:4] == '----':
                        if save == True:
                            save = False
                        else:
                            save = True
                    if save:
                        # save 6th col
                        cols = line.split()
                        if len(cols) >= 6:
                            pos = line.find(cols[5])
                            if pos != -1:
                                files2.append(os.path.join(dir,line[pos:]))
                logger.info('files: %s',str(files2))
                
                # check for existence in cache
                exists = True
                if file.endswith(('.tgz','.tbz2','.tbz')):
                    exists = False # these archives sometimes don't work with caching
                for f in files2:
                    if not os.path.exists(f):
                        exists = False
                        break
                if not exists or len(files2) < 1:
                    # extracted files do not exist yet, so actually do extraction
                    #if type2 in mmt_off:
                    #    proc = subprocess.Popen('7za x -y -mmt=off -o%s %s'%(dir,files[0]),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    #else:
                    #    proc = subprocess.Popen('7za x -y -mmt=off -o%s %s'%(dir,files[0]),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    ### use tar instead of 7za because 7za doesn't do symlinks correctly ###
                    proc = subprocess.Popen('tar -x -C %s -f %s'%(dir,files[0]),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    output = proc.communicate()[0]
                    logger.info(output)
                
                if not proc.returncode: # if everything went ok
                    if len(files2) == 0:
                        return ''
                    elif len(files2) == 1:
                        return files2[0]
                    else:
                        return files2
        if len(files) == 0:
            return ''
        elif len(files) == 1:
            return files[0]
        else:
            return files
    raise dataclasses.NoncriticalError('Failed to uncompress')

def compress(file,compression='lzma'):
    """Compress a file or directory.
       The compression argument is used as the new file extension"""
    newfile = reduce(lambda a,b:a.replace(b,''),(';','`'),'%s.%s'%(file,compression))
    file = file.replace(';`','')
    if compression in mmt_off:
        proc = subprocess.Popen('7za a -y -mmt=off %s %s'%(newfile,file),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen('7za a -y %s %s'%(newfile,file),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output = proc.communicate()[0]
    logger.info(output)
    if not proc.returncode:
        return newfile
    raise dataclasses.NoncriticalError('Failed to compress')

def iscompressed(file):
    """Check if a file is a compressed file, based on file name"""
    return reduce(partial(_checksuffixes,file), compress_suffixes, None)  

def istarred(file):
    """Check if a file is a tarred file, based on file name"""
    if '.tar' in file:
        return 'tar'
    return reduce(partial(_checksuffixes,file), tar_suffixes, None)  

def tar(tfile,files,workdir=None):
    """tar a list of files"""
    if not workdir:
        workdir = os.getcwd()
    if workdir.endswith('/'):
        workdir = workdir.rstrip('/')
    tar = tarfile.open(tfile, "w")
    if isinstance(files,str):
        if files.startswith(workdir):
            file = files[len(workdir)+1:]
            tar.add(files,arcname=file)
        else:
            tar.add(files)
    else:
        try:
            for name in files:
                if name.startswith(workdir):
                    file = name[len(workdir)+1:]
                    print file
                    tar.add(name,arcname=file)
                else:
                    tar.add(name)
        except Exception:
            logger.warning('cannnot add %s to tar file'%(str(files)))
            raise dataclasses.NoncriticalError('cannot add to tar file')
    tar.close()
    return tfile

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
    if os.path.isdir(path):
        shutil.rmtree(path,True)
    else:
        os.remove(path)

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

def find_glob(pathlist,pattern,filetype=None):
    """
     Return a list of files that match a given glob pattern
     @param pathlist: base directory to start search from
     @param pattern: glob pattern to match
    """
    import glob
    matches = []
    if not isinstance(pathlist,list) and not isinstance(pathlist,dict):
        pathlist = [pathlist]
    for path in pathlist:
        for root in os.path.expandvars(path).split(os.pathsep):
            if not root: continue
            logger.info('glob pattern is %s',os.path.join(root,pattern))
            matches = glob.glob(os.path.join(root,pattern))
    return matches

def find_unix(pathlist,pattern,filetype=None):
    """
     Return a list of files that match a given unix-style pattern
     @param pathlist: base directory to start search from
     @param pattern: unix-style pattern to match
    """
    import fnmatch
    matches = []
    for root in os.path.expandvars(pathlist).split(os.pathsep):
        if not root: continue
        logger.info('finding in path %s',root)
        for path, dirs, files in os.walk(root):
            if (not filetype) or (filetype and filetype.startswith('file')):
                matches.extend(map(lambda a:os.path.join(path,a),fnmatch.filter(files,pattern)))
            if (not filetype) or (filetype and filetype.startswith('dir')):
                matches.extend(map(lambda a:os.path.join(path,a),fnmatch.filter(dirs,pattern)))
            if (not filetype) or (filetype and filetype.startswith('path')):
                if fnmatch.fnmatch(path,pattern):
                    matches.append(path)
    return matches

def find_regex(pathlist,pattern,filetype=None):
    """
     Return a list of files that match a given regex pattern
     @param pathlist: base directory to start search from
     @param pattern: pattern to match
    """
    from itertools import ifilter
    matches = list()
    regex = re.compile(r'%s'%pattern)

    for root in os.path.expandvars(pathlist).split(os.pathsep):
        if not root: continue
        logger.info('finding in path %s',root)
        for path, dirs, files in os.walk(root):
            if (not filetype) or (filetype and filetype.startswith('file')):
                matches.extend(map(lambda a:os.path.join(path,a),ifilter(regex.match,files)))
            if (not filetype) or (filetype and filetype.startswith('dir')):
                matches.extend(map(lambda a:os.path.join(path,a),ifilter(regex.match,dirs)))
            if (not filetype) or (filetype and filetype.startswith('path')):
                if regex.search(path):
                    matches.append(os.path.abspath(path))
    return matches
    
def find(pathlist,pattern,filetype=None):
    return find_regex(pathlist,pattern,filetype)

def tail(logpath,chars=75):
    """
    Get the end of a file.  Good for grabbing tail of logging.
    @param logpath: path to logfile
    @param chars: number of characters end of file
    @return: last n characters in file
    """
    if not os.path.exists(logpath):
        return "no log output found %s" % logpath
    logfile = open(logpath,'r')
    #Find the size of the file and move to the end
    st_results = os.stat(logpath)
    st_size = st_results[6]
    logfile.seek(max(0,st_size-chars))
    # read tail
    tailtxt = logfile.read()
    logfile.close()
    return tailtxt

def freespace(path='$PWD'):
    """Get the free space in bytes for the specified path"""
    try:
        fs = os.statvfs(os.path.expandvars(path))
    except OSError,e:
        logger.warning('Error getting free space for %s: %s',(path,e))
        raise
    return fs.f_bfree*fs.f_frsize    


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
            iface = dataclasses.IFace()
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
            iface = dataclasses.IFace()
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
        
    return interfaces

def gethostname():
    """Get host names of this computer as a set"""
    hostnames = set()
    for iface in getInterfaces():
        if iface.encap.lower() in ('local','loopback'): # ignore loopback interface
            continue
        for link in iface.link:
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
            success = not check_cksm(success_file,checksum_type,success_checksum)
        else:
            success = True
    return success

def upload(local,remote,proxy=False,options={}):
    """Upload a file, checksumming if possible"""
    if not os.path.exists(local):
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
    return success

pycurl_handle = dataclasses.PycURL()
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
        global pycurl_handle
        post = 1
        for i in xrange(0,2):
            try:
                kwargs = {}
                if 'http_username' in options:
                    kwargs['username'] = options['http_username']
                if 'http_password' in options:
                    kwargs['password'] = options['http_password']
                if 'ssl_cert' in options:
                    kwargs['ssl_cert'] = options['ssl_cert']
                if 'ssl_key' in options:
                    kwargs['ssl_key'] = options['ssl_key']
                if 'ssl_cacert' in options:
                    kwargs['cacert'] = options['ssl_cacert']
                if post:
                    # talk the iceprod server language
                    f = open(dest_path,'w')
                    if 'key' not in options:
                        logger.warn('auth key not in options, so cannot communicate using POST with server')
                        post = 0
                        continue
                    body = {'url':url,'key':options['key']}
                    kwargs['postbody'] = json_encode(body)
                    pycurl_handle.post(url,f.write,**kwargs)
                    f.close()
                else:
                    # do regular get
                    pycurl_handle.fetch(url,dest_path,**kwargs)
            except dataclasses.NoncriticalError as e:
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
                    pycurl_handle = dataclasses.PycURL()
                else:
                    logger.error('error fetching url %s : %s',url,e)
                    ret = None
            else:
                if os.path.exists(dest_path):
                    ret = dest_path
                break
    elif url[:5] == 'file:':
        # use copy command
        if os.path.exists(url[5:]):
            copy(url[5:],dest_path)
            ret = dest_path
    elif url[:7] == 'gsiftp:':
        try:
            ret = GridFTP.get(url,filename=dest_path)
        except Exception as e:
            logger.error('error fetching url %s : %r',url,e)
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
    
    if ret == None:
        if os.path.exists(dest_path):
            os.remove(dest_path)
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
        post = 1
        for i in xrange(0,2):
            try:
                kwargs = {}
                if 'http_username' in options:
                    kwargs['username'] = options['http_username']
                if 'http_password' in options:
                    kwargs['password'] = options['http_password']
                if 'ssl_cert' in options:
                    kwargs['ssl_cert'] = options['ssl_cert']
                if 'ssl_key' in options:
                    kwargs['ssl_key'] = options['ssl_key']
                if 'ssl_cacert' in options:
                    kwargs['cacert'] = options['ssl_cacert']
                def cb(data):
                    cb.data += data
                cb.data = ''
                if post:
                    # talk the iceprod server language
                    if 'key' not in options:
                        logger.warn('auth key not in options, so cannot communicate using POST with server')
                        post = 0
                        continue
                    body = {'url':url,'key':options['key'],'type':'checksum'}
                    kwargs['postbody'] = json_encode(body)
                    pycurl_handle.post(url,cb,**kwargs)
                    ret = json_decode(cb.data)
                    ret = (ret['checksum'],ret['type'])
                else:
                    # do regular get
                    # try every extension type for checksums
                    for type in ('sha512','sha256','sha1','md5'):
                        try:
                            url2 = url+type+'sum'
                            pycurl_handle.post(url2,cb,**kwargs)
                        except dataclasses.NoncriticalError:
                            continue
                        else:
                            break
                    if cb.data:
                        ret = (cb.data,type)
            except dataclasses.NoncriticalError as e:
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
                    pycurl_handle = dataclasses.PycURL()
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
    if os.path.isdir(source_path):
        tar(source_path+'.tar',source_path)
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
            
            # initial json request request
            def reply(data):
                reply.data += data
            reply.data = ''
            for i in xrange(0,2):
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
                        pycurl_handle = dataclasses.PycURL()
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
                            pycurl_handle = dataclasses.PycURL()
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
                            pycurl_handle = dataclasses.PycURL()
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
                pycurl_handle.put(url,source_path,**kwargs)
            except Exception as e:
                if i == 0:
                    # try regenerating the pycurl handle
                    logger.warn('regenerating pycurl handle because of error')
                    pycurl_handle = dataclasses.PycURL()
                else:
                    logger.error('error uploading to url %s : %s',url,e)
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
                    logger.error('error checksumming url %s : %r',url,e)
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
        return reduce(lambda a,b: a or url.startswith(b), prefixes, False)

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
                    pycurl_handle = dataclasses.PycURL()
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
                pycurl_handle = dataclasses.PycURL()
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

   
### Other functions ###

def getuser():
    """Get user name of current process owner"""
    import pwd
    return pwd.getpwuid(os.getuid())[0]
    
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

def findjava(searchpath='/usr/java:/usr/local/java'):
    """find the java home and library path"""
    platform = os.uname()[4].replace('i686','i386').replace('x86_64','amd64')
    javaregex = re.compile(r'jre/lib/%s/server'%platform)
    java = None
    for path in find(searchpath,'libjvm.so'):
        if javaregex.search(path):
           javahome = path.split('/jre/lib')[0]
           if not java or os.path.basename(javahome) > os.path.basename(java):
              java = javahome

    if not java: return java

    ldpath  = os.path.join(java,'jre/lib/%s/server'%platform)
    ldpath += ":"+os.path.join(java,'jre/lib/%s'%platform)
    return java,ldpath

def myputenv(name,value):
    """function to set environment variables"""
    if value == None:
        if name in os.environ:
            del os.environ[name]
    else:
        os.environ[name]=value

def getmemusage():
    # Get memory usage info
    stats = {
        'VmLib:' : 0.0, 
        'VmData:': 0.0, 
        'VmExe:' : 0.0, 
        'VmRSS:' : 0.0, 
        'VmSize:': 0.0, 
        'VmLck:' : 0.0, 
        'VmStk:' : 0.0,
    }
    if os.uname()[0] == 'Linux':
       usage = open("/proc/self/status")
       for line in usage.readlines():
          if line.startswith('Vm'):
             name,value,unit = line.split()
             value = float(value.strip())
             if unit == "MB":
                stats[name]  = value*1024
             else:
                stats[name]  = value
    else:
       self.logger.warn("getmemusage: Not a Linux machine")
    return stats

def hoerandel_fluxsum(emin,dslope):
   """
   function to caculate CORSIKA fluxsum 
   FLUXSUM is the integral in energy of the primary cosmic ray between 
   the minimum and the maximum set energy. 

   The cosmic ray energy spectrum is from Hoerandel polygonato model [Astrop.
   Phys. Vol 19, Issue 2, Pages 193-312 (2003)]. The maximum energy is assumed
   to be much higher than the minimum energy (the maximum energy actually is not explicitly 
   used in this calculation). Note : DSLOPE = 0 for unweighted CORSIKA sample and = -1 for weighted
   CORSIKA sample. 
   """
   integral = 0.
   nmax     = 26
   norm = [
          0.0873, 0.0571, 0.00208, 0.000474,0.000895, 
          0.0106, 0.00235, 0.0157, 0.000328, 
          0.0046, 0.000754, 0.00801, 0.00115, 
          0.00796, 0.00027, 0.00229, 0.000294, 
          0.000836, 0.000536, 0.00147, 0.000304, 
          0.00113, 0.000631, 0.00136, 0.00135, 0.0204 ]

   gamma = [
         2.71, 2.64, 2.54, 2.75, 2.95, 2.66, 2.72, 2.68, 2.69, 2.64, 
         2.66, 2.64, 2.66, 2.75, 2.69, 2.55, 2.68, 2.64, 2.65, 2.7, 
         2.64, 2.61, 2.63, 2.67, 2.46, 2.59 ]

   crs = [
         1.00797, 4.0026, 6.939, 9.0122, 10.811, 12.0112, 14.0067, 
         15.9994, 18.9984, 20.183, 22.9898, 24.312, 26.9815, 28.086, 
         30.984, 32.064, 35.453, 39.948, 39.102, 40.08, 44.956, 47.9, 
         50.942, 51.996, 54.938, 55.847]

   for i in range(nmax): 
         prwght     = round(crs[i])
         gamma[i]  += dslope
         integral  += norm[i] * pow( (emin*prwght), (1-gamma[i]) ) / (gamma[i]-1)

   return integral

