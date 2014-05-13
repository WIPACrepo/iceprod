"""
  proxy class

  copyright (c) 2012 the icecube collaboration
"""

import os
import stat
import logging
from functools import partial
import uuid
try:
    import posix
except:
    pass
try:
    import CStringIO as StringIO
except:
    import StringIO

import tornado.httpclient
import tornado.escape

from pyuv_tornado import fs

from iceprod.server.dbclient import DB
from iceprod.server.gridftp import GridFTPTornado


def calc_checksum(filename,buffersize=16384,type='sha512',callback=None):
    """Return checksum of file"""
    if type not in ('md5','sha1','sha256','sha512'):
        callback(Exception('cannot get checksum for type %r',type))
    try: 
        import hashlib
    except ImportError: 
        callback(Exception('cannot import hashlib'))
        return
    else:
        try:
            digest = getattr(hashlib,type)()
        except:
            callback(Exception('cannot get checksum for type %r',type))
            return
    def cb(ret,path,errno,callback=None):
        callback(ret)
    def do_checksum(fh,offset,path,data,errno):
        if errno:
            fs.close(fh,callback=partial(cb,Exception('error reading file'),callback=callback),tornado=True)
        else:
            digest.update(data)
            if len(data) < buffersize:
                fs.close(fh,callback=partial(cb,digest.hexdigest(),callback=callback),tornado=True)
            else:
                offset += buffersize
                fs.read(fh,buffersize,offset,callback=partial(do_checksum,fh,offset),tornado=True)
    def opencb(filename,fh,errno):
        if errno:
            callback(Exception('error opening file'))
        else:
            fs.read(fh,buffersize,0,callback=partial(do_checksum,fh,0),tornado=True)
    fs.open(filename,os.O_RDONLY,0,callback=opencb,tornado=True)

class Proxy(object):
    """Proxy service.  Functions are static and use DB to store info."""
    _httpclient = None
    _httpclient_urlprefix = None
    _gridftpclient = None
    _gridftpclient_urlprefix = None
    _cfg = {
        'username': None,
        'password': None,
        'sslcert': None,
        'sslkey': None,
        'cacert': None,
        'request_timeout': 10000,
        'download_dir': os.path.expandvars('$PWD'),
        'cache_file_stat':(stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)&~stat.S_IXOTH&~stat.S_IXGRP&~stat.S_IXUSR,
    }
    _cfg_types = {
        'username': 'str',
        'password': 'str',
        'sslcert': 'file',
        'sslkey': 'file',
        'cacert': 'file',
        'request_timeout': 'int',
        'download_dir': 'file',
        'cache_file_stat':'stat',
    }

    @classmethod
    def configure(cls, **kwargs):
        # setup cfg variables
        for s in kwargs.keys():
            v = kwargs[s]
            if not isinstance(s,str):
                raise Exception('parameter name %s is not a string'%(str(s)))
            if not s in cls._cfg:
                raise Exception('%s is not a class variable'%s)
            t = cls._cfg_types[s]
            if t in ('str','file'):
                if not isinstance(v,str):
                    raise Exception('%s is not a string'%(str(s)))
                if t == 'file':
                    v = os.path.expanduser(os.path.expandvars(v))
                    try:
                        fs.stat(v)
                    except Exception:
                        raise Exception('parameter %s with filepath %s does not exist'%(s,v))
            elif t == 'int':
                if not isinstance(v,int):
                    raise Exception('%s is not an int'%(str(s)))
            elif t == 'float':
                if not isinstance(v,float):
                    raise Exception('%s is not a float'%(str(s)))
            elif t == 'stat':
                if isinstance(v,str):
                    try:
                        v = int(v)
                    except:
                        raise Exception('%s is not an int, should be octal permissions'%(str(s)))
                if not isinstance(v,int):
                    raise Exception('%s is not an int, should be octal permissions'%(str(s)))
                # convert to stat syntax
                def convert(i,post):
                    ret = 0
                    if i%2:
                        ret |= getattr(stat,'S_IX'+post)
                    if i/2%2:
                        ret |= getattr(stat,'S_IW'+post)
                    if i/4%2:
                        ret |= getattr(stat,'S_IR'+post)
                    return ret
                v = convert(v%10,'OTH')|convert(v/10%10,'GRP')|convert(v/100%10,'USR')
            else:
                raise Exception('%s has an unknown type'%(str(s)))
            cls._cfg[s] = v
        
        # set up kwargs for http fetching
        cls._http_kwargs = {} 
        if cls._cfg['username'] is not None:
            cls._http_kwargs['auth_username'] = cls._cfg['username']
        if cls._cfg['password'] is not None:
            cls._http_kwargs['auth_password'] = cls._cfg['password']
        if cls._cfg['sslcert'] is not None:
            cls._http_kwargs['client_cert'] = cls._cfg['sslcert']
        if cls._cfg['sslkey'] is not None:
            cls._http_kwargs['client_key'] = cls._cfg['sslkey']
        if cls._cfg['cacert'] is not None:
            cls._http_kwargs['ca_certs'] = cls._cfg['cacert']
        if cls._cfg['request_timeout'] is not None:
            cls._http_kwargs['request_timeout'] = cls._cfg['request_timeout']
        
        # set up clients
        cls._httpclient = tornado.httpclient.AsyncHTTPClient(pseudo_chunked="write_callback")
        cls._httpclient_urlprefix = ('http','https')
        cls._gridftpclient = GridFTPTornado
        cls._gridftpclient_urlprefix = ('gsiftp','ftp')
    
    @classmethod
    def getprefix(cls,url):
        x=url.find(':')
        if x < 0:
            return None
        else:
            return url[:x]
    
    @classmethod
    def newfilename(cls):
        return os.path.expandvars(os.path.join(cls._cfg['download_dir'],uuid.uuid4().hex))
    
    @classmethod
    def cache_stream(cls,data,fh=None,writer=None,flusher=None,error=None,callback=None):
        """Write incoming data to writer and fh, then call flusher and callback"""
        if data is None:
            logging.warn('error in Proxy.cache_stream(): no data')
        elif writer is None:
            error('error in Proxy.cache_stream(): no writer or fh')
        else:
            writer(data)
            def cb(p,bytes,errno):
                if errno:
                    logging.info('error writing to cache file %s'%str(p))
                    def cb2(p,e):
                        pass
                    fs.close(fh,callback=cb2)
                if flusher:
                    if callback:
                        flusher(callback=callback)
                    else:
                        flusher()
                elif callback:
                    callback()
            if fh is None:
                cb(None,None,None)
            else:
                fs.write(fh,data,-1,callback=cb,tornado=True)
    
    @classmethod
    def passthrough_stream(cls,data,writer=None,flusher=None,error=None,callback=None):
        """Write incoming data to writer"""
        if data is None:
            logging.warn('error in Proxy.passthrough_stream(): no data')
        elif writer is None:
            error('error in Proxy.passthrough_stream(): no writer')
        else:
            writer(data)
            if flusher:
                if callback:
                    flusher(callback=callback)
                else:
                    flusher()
            elif callback:
                callback()
    
    @classmethod
    def cache_end(cls,ret,fh=None,filename=None,url=None,uid=None,error=None,callback=None):
        """Finish caching passthrough call"""
        def getchecksum(size,checksum):
            if isinstance(checksum,Exception):
                # remove from cache and fail
                def cb2(p,e):
                    pass
                fs.unlink(filename,callback=cb2)
                error('error when opening cached file %s: errno= %s'%(filename,str(errno)))
            else:
                DB.add_to_cache(url,uid,size,checksum,callback=partial(callback,ret.body))
        
        def getsize(path,stat_result,errno):
            if errno:
                # remove from cache and fail
                def cb2(p,e):
                    pass
                fs.unlink(filename,callback=cb2)
                error('error when opening cached file %s: errno= %s'%(filename,str(errno)))
            else:
                try:
                    stat_result2 = posix.stat_result(stat_result)
                    size = stat_result2.st_size
                except:
                    size = stat_result[6]
                calc_checksum(filename,callback=partial(getchecksum,size))
        
        def cbclose(path,errno):
            if uid is None:
                if errno:
                    error('error closing file %r'%filename)
                elif ret is not None and isinstance(ret,tornado.httpclient.HTTPResponse):
                    callback(ret.body)
                else:
                    callback()
            elif ret is not None and isinstance(ret,tornado.httpclient.HTTPResponse):
                if ret.error:
                    # remove from cache and fail
                    def cb2(p,e):
                        pass
                    fs.unlink(filename,callback=cb2)
                    error('error when doing site-to-site download: %s'%str(ret.error))
                else:
                    fs.stat(filename,callback=getsize,tornado=True)
            else:
                def cb2(p,e):
                    pass
                fs.unlink(filename,callback=cb2)
                error('request did not return HTTPResponse object')
        if fh is None:
            error('no filehandle provided')
        else:
            fs.close(fh,callback=cbclose,tornado=True)
    
    @classmethod
    def passthrough_end(cls,ret,error=None,callback=None):
        """Finish passthrough call"""
        if ret is not None and isinstance(ret,tornado.httpclient.HTTPResponse):
            if ret.error:
                error('error when doing site-to-site download: %s'%str(ret.error))
            elif ret.body:
                callback(ret.body)
            else:
                callback()
        else:
            error('request did not return HTTPResponse object')
        
    @classmethod
    def cache_request(cls, url, setheader=None, writer=None,
                      flusher=None, error=None, callback=None):
        """Make a caching passthrough request to the specified url, and save to cache"""
        def opencb(filename,fh,errno):
            uid = os.path.basename(filename)
            prefix = cls.getprefix(url)
            cb = partial(cls.cache_end,fh=fh,filename=filename,url=url,uid=uid,error=error,callback=callback)
            cbstream = partial(cls.cache_stream,fh=fh,writer=writer,flusher=flusher,error=error)
            if prefix in cls._httpclient_urlprefix:
                def cb2(site,key):
                    def cb3(p,e):
                        pass
                    if site is None:
                        fs.close(fh,callback=lambda p,e:fs.unlink(filename,callback=cb3))
                        error('error communicating with other sites: site_id not found')
                    if key is None:
                        fs.close(fh,callback=lambda p,e:fs.unlink(filename,callback=cb3))
                        error('error communicating with other sites: key not found')
                    body = tornado.escape.json_encode({'url':url,
                                                       'site_id':site,
                                                       'key':key})
                    # set up args for httpclient fetch
                    kwargs = {'callback':cb,
                              'streaming_callback':cbstream,
                              'method':'POST',
                              'body':body}
                    if setheader is not None:
                        def h(line):
                            if 'Content-Length' in line or 'Content-Disposition' in line:
                                pieces = line.split(':',1)
                                setheader(pieces[0].strip(),pieces[1].strip())
                        kwargs['header_callback'] = h
                    kwargs.update(cls._http_kwargs)
                    cls._httpclient.fetch(url,**kwargs)
                DB.get_site_auth(callback=cb2)
            elif prefix in cls._gridftpclient_urlprefix:
                def cb2(ret):
                    if ret:
                        req = tornado.httpclient.HTTPRequest('http://test')
                        resp = tornado.httpclient.HTTPResponse(req,200)
                        if ret is not True:
                            resp.buffer = StringIO.StringIO(ret)
                        cb(resp)
                    else:
                        error('error getting %s from gridftp'%url)
                cls._gridftpclient.get(url,
                                       callback=cb2,
                                       streaming_callback=cbstream,
                                       request_timeout=cls._cfg['request_timeout'])
            else:
                error('Protocol not supported or bad url')
                return
        def filenamecheck(filename,stat_result,errno):
            if errno:
                fs.open(filename,os.O_WRONLY|os.O_CREAT,cls._cfg['cache_file_stat'],callback=opencb,tornado=True)
            else:
                fs.stat(cls.newfilename(),callback=filenamecheck,tornado=True)
        fs.stat(cls.newfilename(),callback=filenamecheck,tornado=True)
    
    @classmethod
    def passthrough_request(cls, url, setheader=None, writer=None,
                            flusher=None, error=None, callback=None):
        """Make a passthrough request to the specified url"""
        prefix = cls.getprefix(url)
        cb = partial(cls.passthrough_end,error=error,callback=callback)
        cbstream = partial(cls.passthrough_stream,writer=writer,flusher=flusher,error=error)
        if prefix in cls._httpclient_urlprefix:
            def cb2(site,key):
                if site is None:
                    error('error communicating with other sites: site_id not found')
                if key is None:
                    error('error communicating with other sites: key not found')
                body = tornado.escape.json_encode({'url':url,
                                                   'site_id':site,
                                                   'key':key})
                # set up args for httpclient fetch
                kwargs = {'callback':cb,
                          'streaming_callback':cbstream,
                          'method':'POST',
                          'body':body}
                if setheader is not None:
                    def h(line):
                        if 'Content-Length' in line or 'Content-Disposition' in line:
                            pieces = line.split(':',1)
                            setheader(pieces[0].strip(),pieces[1].strip())
                    kwargs['header_callback'] = h
                kwargs.update(cls._http_kwargs)
                cls._httpclient.fetch(url,**kwargs)
            DB.get_site_auth(callback=cb2)
        elif prefix in cls._gridftpclient_urlprefix:
            def cb2(ret):
                if ret:
                    req = tornado.httpclient.HTTPRequest('http://test')
                    resp = tornado.httpclient.HTTPResponse(req,200)
                    if ret is not True:
                        resp.buffer = StringIO.StringIO(ret)
                    cb(resp)
                else:
                    error('error getting %s from gridftp'%url)
            cls._gridftpclient.get(url,
                                   callback=cb2,
                                   streaming_callback=cbstream,
                                   request_timeout=cls._cfg['request_timeout'])
        else:
            error('Protocol not supported or bad url')
            return
    
    @classmethod
    def passthrough_size_request(cls, url, setheader=None, writer=None,
                                 error=None, callback=None):
        """Make a passthrough size request to the specified url"""
        prefix = cls.getprefix(url)
        cb = partial(cls.passthrough_end,error=error,callback=callback)
        if writer is not None:
            cbstream = partial(cls.passthrough_stream,writer=writer,error=error)
        if prefix in cls._httpclient_urlprefix:
            def cb2(site,key):
                if site is None:
                    error('error communicating with other sites: site_id not found')
                if key is None:
                    error('error communicating with other sites: key not found')
                body = tornado.escape.json_encode({'url':url,
                                                   'site_id':site,
                                                   'key':key,
                                                   'type':'size'})
                kwargs = {'callback':cb,
                          'method':'POST',
                          'body':body}
                if writer is not None:
                    kwargs['streaming_callback'] = cbstream
                if setheader is not None:
                    setheader('Content-Type','application/json')
                    def h(line):
                        if 'Content-Length' in line:
                            setheader('Content-Length',line.split(':',1)[1].strip())
                    kwargs['header_callback'] = h
                kwargs.update(cls._http_kwargs)
                cls._httpclient.fetch(url,**kwargs)
            DB.get_site_auth(callback=cb2)
        elif prefix in cls._gridftpclient_urlprefix:
            def cb2(ret):
                if ret:
                    req = tornado.httpclient.HTTPRequest('http://test')
                    resp = tornado.httpclient.HTTPResponse(req,200)
                    if ret is not True:
                        resp.buffer = StringIO.StringIO(ret)
                    cb(resp)
                else:
                    error('error getting %s from gridftp'%url)
            kwargs = {'callback':cb2,
                      'request_timeout':cls._cfg['request_timeout']}
            if writer is not None:
                    kwargs['streaming_callback'] = cbstream
            cls._gridftpclient.size(url,**kwargs)
        else:
            error('Protocol not supported or bad url')
            return
    
    @classmethod
    def passthrough_checksum_request(cls, url, setheader=None, writer=None,
                                     error=None, callback=None):
        """Make a passthrough checksum request to the specified url"""
        prefix = cls.getprefix(url)
        cb = partial(cls.passthrough_end,error=error,callback=callback)
        if writer is not None:
            cbstream = partial(cls.passthrough_stream,writer=writer,error=error)
        if prefix in cls._httpclient_urlprefix:
            def cb2(site,key):
                if site is None:
                    error('error communicating with other sites: site_id not found')
                if key is None:
                    error('error communicating with other sites: key not found')
                body = tornado.escape.json_encode({'url':url,
                                                   'site_id':site,
                                                   'key':key,
                                                   'type':'checksum'})
                kwargs = {'callback':cb,
                          'method':'POST',
                          'body':body}
                if writer is not None:
                    kwargs['streaming_callback'] = cbstream
                if setheader is not None:
                    setheader('Content-Type','application/json')
                    def h(line):
                        if 'Content-Length' in line:
                            setheader('Content-Length',line.split(':',1)[1].strip())
                    kwargs['header_callback'] = h
                kwargs.update(cls._http_kwargs)
                cls._httpclient.fetch(url,**kwargs)
            DB.get_site_auth(callback=cb2)
        elif prefix in cls._gridftpclient_urlprefix:
            def cb2(ret):
                if ret:
                    req = tornado.httpclient.HTTPRequest('http://test')
                    resp = tornado.httpclient.HTTPResponse(req,200)
                    if ret is not True:
                        resp.buffer = StringIO.StringIO(ret)
                    cb(resp)
                else:
                    error('error getting %s from gridftp'%url)
            kwargs = {'callback':cb2,
                      'request_timeout':cls._cfg['request_timeout']}
            if writer is not None:
                kwargs['streaming_callback'] = cbstream
            cls._gridftpclient.sha512sum(url,**kwargs)
        else:
            error('Protocol not supported or bad url')
            return
    
    @classmethod
    def send_from_cache(cls, filename, writer=None, flusher=None,
                        error=None, callback=None):
        """Send the file from the cache"""
        def readcb(path,data,errno,fh=None):
            if errno:
                try:
                    cls.cache_end(fh=fh)
                except:
                    pass
                error('error reading data from cache file %r'%filename)
            elif not data:
                cls.cache_end(None,fh=fh,error=error,callback=callback)
            else:
                cls.passthrough_stream(data,writer=writer,flusher=flusher,error=error,callback=partial(read,fh=fh))
        def read(fh=None):
            fs.read(fh,16384,-1,callback=partial(readcb,fh=fh),tornado=True)
        
        def opencb(path,fh,errno):
            if errno:
                error('error opening cache file %r'%filename)
            else:
                read(fh)
            
        def filenamecheck(path,stat_result,errno):
            if errno:
                error('Cache file not found: %r'%filename)
            else:
                fs.open(filename,os.O_RDONLY,cls._cfg['cache_file_stat'],callback=opencb,tornado=True)
        fs.stat(filename,callback=filenamecheck,tornado=True)
    
    @classmethod
    def download_request(cls,url,host=None,cache=True,setheader=None,
                         writer=None,flusher=None,error=None,callback=None):
        """A generic download request"""
        try:
            if cache or url.host == host:
                # check in cache, or check ability to cache file
                def download_to_cache(ret=False):
                    if ret is False:
                        # do a pass-through download
                        cls.passthrough_request(url.full_url(),
                                                setheader=setheader,
                                                writer=writer,
                                                flusher=flusher,
                                                error=error,
                                                callback=callback)
                    else:
                        # save to cache and do passthrough
                        cls.cache_request(url.full_url(),
                                          setheader=setheader,
                                          writer=writer,
                                          flusher=flusher,
                                          error=error,
                                          callback=callback)
                def cb(ret):
                    if ret is not None and isinstance(ret,str):
                        # parse JSON
                        try:
                            request = tornado.escape.json_decode(ret)
                        except Exception as e:
                            error('size request from remote site is not valid json')
                            return
                        if 'size' not in request:
                            error('size request json from remote site does not contain size')
                            return
                        if not isinstance(request['size'],int):
                            error('size request json from remote site is not an int')
                            return
                        DB.check_cache_space(cls._cfg['download_dir'],request['size'],5,callback=download_to_cache)
                    else:
                        error('Error parsing size request from remote site')
                def cberror(ret):
                    error('Error obtaining size from remote site: %s'%(str(ret)))
                def cache_decision(incache=False,uid=None):
                    if incache is True and uid is not None:
                        filename = os.path.expandvars(os.path.join(cls._cfg['download_dir'],uid))
                        # send from file
                        def getsize(path,stat_result,errno):
                            if errno:
                                # remove from cache and failover to other branch
                                DB.remove_from_cache(url.full_url(),callback=cache_decision)
                            else:
                                try:
                                    stat_result2 = posix.stat_result(stat_result)
                                    size = stat_result2.st_size
                                except:
                                    size = stat_result[6]
                                if setheader is not None:
                                    setheader('Content-Length',str(size))
                                    setheader('Content-Disposition','attachment; filename='+os.path.basename(url.path))
                                cls.send_from_cache(filename,
                                                    writer=writer,
                                                    flusher=flusher,
                                                    error=error,
                                                    callback=callback)
                        fs.stat(filename,callback=getsize,tornado=True)
                    else:
                        # check if we are the primary url
                        if url.host == host:
                            # We're the primary, but we don't have it
                            # Throw an error message
                            error('File not found',404)
                        else:
                            cls.passthrough_size_request(url.full_url(),
                                                         writer=None,
                                                         error=cberror,
                                                         callback=cb)
                DB.in_cache(url.full_url(),callback=cache_decision)
            else:
                # do a pass-through download
                cls.passthrough_request(url.full_url(),
                                        writer=writer,
                                        flusher=flusher,
                                        error=error,
                                        callback=callback)
        except Exception as e:
            error('Error in Proxy.download_request: %s'%e)
    
    @classmethod
    def size_request(cls,url,host=None,setheader=None,writer=None,error=None,
                     callback=None):
        """A size request"""
        # check for file
        def cache_decision(incache=False,size=None):
            if incache is True and size is not None:
                # return size
                if writer:
                    writer({'url':url.full_url(),'size':size})
                    callback()
                else:
                    callback({'url':url.full_url(),'size':size})
            else:
                # check if we are the primary url
                if url.host == host:
                    # We're the primary, but we don't have it
                    # Throw an error message
                    error('File not found',404)
                else:
                    # try doing size request from source
                    cls.passthrough_size_request(url.full_url(),
                                                 setheader=setheader,
                                                 writer=writer,
                                                 error=error,
                                                 callback=callback)
        DB.get_cache_size(url.full_url(),callback=cache_decision)
    
    @classmethod
    def checksum_request(cls, url, host=None, setheader=None, writer=None,
                       error=None, callback=None):
        """A checksum request"""
        # check for file
        def cache_decision(incache=False,checksum=None,checksum_type='sha512'):
            if incache is True and checksum and checksum_type:
                # return checksum
                if writer:
                    writer({'url':url.full_url(),
                            'checksum':checksum,
                            'checksum_type':checksum_type})
                    callback()
                else:
                    callback({'url':url.full_url(),
                              'checksum':checksum,
                              'checksum_type':checksum_type})
            else:
                # check if we are the primary url
                if url.host == host:
                    # We're the primary, but we don't have it
                    # Throw an error message
                    error('File not found',404)
                else:
                    # try doing checksum request from source
                    cls.passthrough_checksum_request(url.full_url(),
                                                   setheader=setheader,
                                                   writer=writer,
                                                   error=error,
                                                   callback=callback)
        DB.get_cache_checksum(url.full_url(),callback=cache_decision)
