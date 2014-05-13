"""
  gridftp interface

  copyright (c) 2012 the icecube collaboration  
"""
import os
import logging
from threading import Event
from functools import partial
from collections import namedtuple
from datetime import datetime

try:
    import gridftpClient
except ImportError:
    # TODO: fallback to command line?
    class GridFTP(object):
        pass
else:
    class GridFTP(object):
        """Asyncronous GridFTP interface.
           Designed to hide the complex stuff and mimic tornado http downloads.
           
           Example:
               GridFTP.get('gsiftp://data.icecube.wisc.edu/file',
                           callback=endfunc,
                           streaming_callback=streamfunc)
        """
        __timeout = 60 # 1 min default timeout
        __buffersize = 1048576 # 1MB default buffer size
        
        @classmethod
        def supported_address(cls,address):
            """Return False for address types that are not supported"""
            if '://' not in address:
                return False
            addr_type = address.split(':')[0]
            if addr_type not in ('gsiftp','ftp'):
                return False
            return True
        
        @classmethod
        def address_split(cls,address):
            """Split an address into server/path parts"""
            pieces = address.split('://',1)
            if '/' in pieces[1]:
                pieces2 = pieces[1].split('/',1)
                return (pieces[0]+'://'+pieces2[0],'/'+pieces2[1])
            else:
                return (address,'/')
        
        @classmethod
        def get(cls,address,callback=None,streaming_callback=None,filename=None,request_timeout=None):
            """Do a GridFTP get request, asyncronously if a callback is defined.
               streaming_callback or filename may be defined.
               
               streaming_callback should be of type streaming_callback(data)
                  where data is a bytestring
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and either False
                  or the data is returned, or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._get_callback,callback=cb)
            else:
                complete_callback = partial(cls._get_callback,callback=callback)
            
            if streaming_callback is not None:
                # stream data to requester as it comes in
                data_callback = partial(cls._get_data_callback,streaming_callback=streaming_callback)
            elif filename is not None:
                # write to file
                def scb(filehandle,data):
                    filehandle.write(data)
                cc = complete_callback
                def scb2(*args,**kwargs):
                    scb2.file.close()
                    cc(*args,**kwargs)
                scb2.file = open(filename,'wb')
                data_callback = partial(cls._get_data_callback,streaming_callback=partial(scb,scb2.file))
                complete_callback = scb2
            else:
                # store temp state in buffer
                def scb(buffer):
                    scb.buffer += buffer
                scb.buffer = ''
                data_callback = partial(cls._get_data_callback,streaming_callback=scb)
                if callback is not None:
                    # wrap callback to get to buffer
                    def cb(ret):
                        if ret is not False:
                            callback(scb.buffer)
                        else:
                            callback(ret)
                    complete_callback = partial(cls._get_callback,callback=cb)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            b = gridftpClient.Buffer(cls.__buffersize)
            cl.get(address,complete_callback,(cl,b),gridftpClient.OperationAttr())
            cl.register_read(b,data_callback,(cl,b))
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                if streaming_callback is not None or filename is not None:
                    return cb.ret
                else:
                    if cb.ret is False:
                        return False
                    try:
                        return scb.buffer
                    except:
                        return False
        
        @classmethod
        def _get_data_callback(cls,arg,handle,error,buffer,length,offset,eof,streaming_callback=None):
            cl,b = arg
            if error:
                logging.warning('Error in GridFTP._get_data_callback: %s',str(error))
                try:
                    cl.abort()
                except:
                    pass
                return
            if streaming_callback is not None:
                streaming_callback(str(buffer))
            else:
                logging.warning('Error in GridFTP._get_data_callback: streaming_callback is not defined')
                try:
                    cl.abort()
                except:
                    pass
            
            if not eof:
                cl.register_read(b,partial(cls._get_data_callback,streaming_callback=streaming_callback),arg)            
        
        @classmethod
        def _get_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._get_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._get_callback: callback is not defined')
        
        @classmethod
        def put(cls,address,callback=None,streaming_callback=None,data=None,filename=None,request_timeout=None):
            """Do a GridFTP put request, asyncronously if a callback is defined.
               Either streaming_callback, data, or filename must be defined.
               
               streaming_callback should be of type streaming_callback()
                  where a block of data is returned by the function each
                  time it is called
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._put_callback,callback=cb)
            else:
                complete_callback = partial(cls._put_callback,callback=callback)
            
            if streaming_callback is not None:
                # stream data to server
                data_callback = streaming_callback
            elif data is not None:
                # write directly from data
                def scb():
                    for i in xrange(0,len(data),cls.__buffersize):
                        yield data[i:i+cls.__buffersize]
                data_callback = scb().next
            elif filename is not None:
                # write from file
                def scb():
                    with open(filename,'rb') as f:
                        piece = f.read(cls.__buffersize)
                        while len(piece) > 0:
                            yield piece
                            piece = f.read(cls.__buffersize)
                data_callback = scb().next
            else:
                raise Exception('Neither streaming_callback, data, or filename is defined')
            
            # check that the directory exists before we put into it
            dirname = os.path.dirname(address)
            if not cls.supported_address(dirname):
                raise Exception('dirname address type not supported for address %s',
                                dirname)
            
            def cb2(ret):
                if ret:
                    cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
                    cl.put(address,complete_callback,cl,gridftpClient.OperationAttr())
                    cls._put_data(cl,0,streaming_callback=data_callback)
                else:
                    cb(False)
            GridFTP.mkdir(dirname,callback=cb2,parents=True)
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _put_data(cls,cl,offset,streaming_callback=None):
            if streaming_callback is None:
                logging.warning('Error in GridFTP._put_data: streaming_callback is not defined')
                try:
                    cl.abort()
                except:
                    pass
                return
            
            # get some data
            try:
                data = streaming_callback()
            except StopIteration:
                data = None
            except Exception as e:
                logging.warning('Error in GridFTP._put_data when getting data: %s',str(e))
                try:
                    cl.abort()
                except:
                    pass
                return
            if data is None or len(data) == 0:
                eof = 1
                b = gridftpClient.Buffer(1)
                b.size = 0
            else:
                eof = 0
                b = gridftpClient.Buffer(data)
            
            # write to server
            data_callback = partial(cls._put_data_callback,streaming_callback=streaming_callback)
            cl.register_write(b,offset,eof,data_callback,cl)
        
        @classmethod
        def _put_data_callback(cls,arg,handle,error,buffer,length,offset,eof,streaming_callback=None):
            cl = arg
            if eof > 0:
                return
            if error:
                logging.warning('Error in GridFTP._put_data_callback: %s',str(error))
                try:
                    cl.abort()
                except:
                    pass
                return
            if streaming_callback is not None:
                cls._put_data(cl,offset+length,streaming_callback=streaming_callback)
            else:
                logging.warning('Error in GridFTP._put_data_callback: streaming_callback is not defined')
                try:
                    cl.abort()
                except:
                    pass
        
        @classmethod
        def _put_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._put_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._put_callback: callback is not defined')
        
        @classmethod
        def list(cls,address,callback=None,request_timeout=None,details=False,
                 dotfiles=False):
            """Do a GridFTP list request, asyncronously if a callback is defined.
               
               callback should be of type callback(result)
                  where result is a list or False
               
               Result is a list of NamedTuples if detail=True.
               Result includes '.', '..', and other '.' files if dotfiles=True.
               
               If no callback is defined, the function blocks and either False
                  or a list is returned, or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            def listify(lines):
                # turn buffer into list output
                out = []
                if details:
                    File = namedtuple('File', ['directory','perms','subfiles',
                                               'owner','group','size','date',
                                               'name'])
                    months = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
                    for x in lines.split('\n'):
                        if not x.strip():
                            continue
                        pieces = x.split()
                        name = pieces[-1]
                        if name.startswith('.') and not dotfiles:
                            continue
                        d = x[0] == 'd'
                        perms = pieces[0][1:]
                        year = datetime.now().year
                        month = months[pieces[5].lower()]
                        day = int(pieces[6])
                        if ':' in pieces[7]:
                            hour,minute = pieces[7].split(':')
                            dt = datetime(year,month,day,int(hour),int(minute))
                        else:
                            year = int(pieces[7])
                            dt = datetime(year,month,day)
                        out.append(File(d,perms,int(pieces[1]),pieces[2],pieces[3],
                                        int(pieces[4]),dt,name))
                else:
                    for x in lines.split('\n'):
                        if not x.strip():
                            continue
                        f = x.split()[-1]
                        if not f.startswith('.') or dotfiles:
                            out.append(f)
                return out
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._list_callback,callback=cb)
            else:
                complete_callback = partial(cls._list_callback,callback=callback)
            
            # store temp state in buffer
            def scb(buffer):
                scb.buffer += buffer
            scb.buffer = ''
            data_callback = partial(cls._list_data_callback,streaming_callback=scb)
            if callback is not None:
                # wrap callback to get to buffer
                def cb(ret):
                    if ret is not False:
                        try:
                            callback(listify(scb.buffer))
                        except Exception:
                            logging.warn('error in listify',exc_info=True)
                            callback(False)
                    else:
                        callback(ret)
                complete_callback = partial(cls._get_callback,callback=cb)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            b = gridftpClient.Buffer(cls.__buffersize)
            cl.verbose_list(address,complete_callback,(cl,b),gridftpClient.OperationAttr())
            cl.register_read(b,data_callback,(cl,b))
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                if cb.ret is False:
                    return False
                try:
                    return listify(scb.buffer)
                except Exception:
                    logging.warn('error in listify',exc_info=True)
                    return False
        
        @classmethod
        def _list_data_callback(cls,arg,handle,error,buffer,length,offset,eof,streaming_callback=None):
            cl,b = arg
            if error:
                logging.warning('Error in GridFTP._list_data_callback: %s',str(error))
                try:
                    cl.abort()
                except:
                    pass
                return
            if streaming_callback is not None:
                streaming_callback(str(buffer))
            else:
                logging.warning('Error in GridFTP._list_data_callback: streaming_callback is not defined')
                try:
                    cl.abort()
                except Exception:
                    pass
            
            if not eof:
                cl.register_read(b,partial(cls._list_data_callback,streaming_callback=streaming_callback),arg)            
        
        @classmethod
        def _list_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._list_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._list_callback: callback is not defined')
        
        @classmethod
        def popen(cls,address,cmd,args,callback=None,request_timeout=None):
            """Call popen on the ftp server
                  cmd should be the program to execute
                  args should be a list of arguments
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and returns either
                  the cmd output or True/False, or an exception is raised.
            """        
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            if not isinstance(args,list):
                raise Exception('cmd args needs to be a list')
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._get_callback,callback=cb)
            else:
                complete_callback = partial(cls._get_callback,callback=callback)
            
            # store temp state in buffer
            def scb(buffer):
                scb.buffer += buffer
            scb.buffer = ''
            data_callback = partial(cls._get_data_callback,streaming_callback=scb)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            b = gridftpClient.Buffer(cls.__buffersize)
            opattr = gridftpClient.OperationAttr()
            disk_stack='#'.join(["popen:argv=",cmd]+args)
            opattr.set_disk_stack(disk_stack)
            cl.get(address,complete_callback,(cl,b),opattr)
            cl.register_read(b,data_callback,(cl,b))
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                if cb.ret is False:
                    return False
                try:
                    return scb.buffer
                except:
                    return False
        
        @classmethod
        def mkdir(cls,address,callback=None,request_timeout=None,parents=False):
            """Make a directory on the ftp server
               
               parents - no error if existing, make parent directories as needed
               
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
            else:
                cb = callback
            complete_callback = partial(cls._mkdir_callback,callback=cb)
            
            if parents:
                # make parent directories as needed
                logging.info('mkdir parents=True addr=%s',address)
                def cb4(ret):
                    if ret: # try for last time to make this directory
                        logging.info('final mkdir addr=%s',address)
                        cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
                        cl.mkdir(address,complete_callback,0,
                                 gridftpClient.OperationAttr())
                    else:
                        cb(False)
                def cb3(ret):
                    if ret: # made this directory
                        cb(True)
                    else: # make parent directory
                        try:
                            GridFTP.mkdir(os.path.dirname(address),callback=cb4,
                                          request_timeout=request_timeout,
                                          parents=True)
                        except Exception:
                            # hit root and failed?
                            logging.info('make parent Exception to addr=%s',
                                         address,exc_info=True)
                            cb(False)
                def cb2(ret):
                    if ret: # this directory already exists
                        logging.info('already exists addr=%s',address)
                        cb(True)
                    else: # try making this directory
                        logging.info('make addr=%s',address)
                        cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
                        cl.mkdir(address,partial(cls._mkdir_callback,callback=cb3,
                                                 suppress_warn=True),
                                 0,gridftpClient.OperationAttr())
                GridFTP.exists(address,cb2)
            else:
                cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
                cl.mkdir(address,complete_callback,0,gridftpClient.OperationAttr())

            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _mkdir_callback(cls,arg,handle,error,callback=None,suppress_warn=False):
            if callback is not None:
                if error:
                    if suppress_warn:
                        logging.debug('Error in GridFTP._mkdir_callback: %s',str(error))
                    else:
                        logging.warning('Error in GridFTP._mkdir_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._mkdir_callback: callback is not defined')

        @classmethod
        def rmdir(cls,address,callback=None,request_timeout=None):
            """Remove a directory on the ftp server
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._rmdir_callback,callback=cb)
            else:
                complete_callback = partial(cls._rmdir_callback,callback=callback)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            cl.rmdir(address,complete_callback,0,gridftpClient.OperationAttr())

            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _rmdir_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._rmdir_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._rmdir_callback: callback is not defined')

        @classmethod
        def delete(cls,address,callback=None,request_timeout=None):
            """Delete a file on the ftp server
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._delete_callback,callback=cb)
            else:
                complete_callback = partial(cls._delete_callback,callback=callback)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            cl.delete(address,complete_callback,0,gridftpClient.OperationAttr())

            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _delete_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._delete_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._delete_callback: callback is not defined')
        
        @classmethod
        def rmtree(cls,address,callback=None,request_timeout=None):
            """Delete a file or directory on the ftp server (like rm -rf)
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
            else:
                cb = callback
            
            def cb3(ret,totalresults=0):
                # accumulate all results before calling cb
                cb3.results += 1
                cb3.ret = cb3.ret and ret
                if cb3.results == totalresults:
                    if cb3.ret:
                        logging.info('final rmdir %s',address)
                        GridFTP.rmdir(address,callback=cb,
                                      request_timeout=request_timeout)
                    else:
                        cb(False) # an error occurred somewhere
            cb3.results = 0
            cb3.ret = True
            def cb2(ret):
                if not ret:
                    logging.info('%s does not exist',address)
                    cb(True)
                elif (len(ret) == 1 and ret[0].name == os.path.basename(address)
                      and not ret[0].directory):
                    logging.info('Deleting single file at %s',address)
                    GridFTP.delete(address,callback=cb,
                                   request_timeout=request_timeout)
                elif any([x.name == '.' for x in ret]):
                    logging.debug('directory: %s',address)
                    dirs = []
                    files = []
                    for x in ret:
                        if x.name not in ('.','..'):
                            if x.directory:
                                dirs.append(x.name)
                            else:
                                files.append(x.name)
                    total = len(dirs)+len(files)
                    if total < 1:
                        logging.info('rmdir %s',address)
                        GridFTP.rmdir(address,callback=cb,
                                      request_timeout=request_timeout)
                    else:
                        cb4 = partial(cb3,totalresults=total)
                        for d in dirs:
                            GridFTP.rmtree(os.path.join(address,d),callback=cb4,
                                           request_timeout=request_timeout)
                        for f in files:
                            p = os.path.join(address,f)
                            logging.debug('deleting file: %s',p)
                            GridFTP.delete(p,callback=cb4,
                                           request_timeout=request_timeout)
                else:
                    logging.warn('unknown situation. list results: %r',ret)
                    cb(False) # not sure what's going on
            
            GridFTP.list(address,callback=cb2,request_timeout=request_timeout,
                         details=True,dotfiles=True)
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def move(cls,src,dest,callback=None,request_timeout=None):
            """Move a file on the ftp server
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(src):
                raise Exception('address type not supported for address %s'%str(src))
            if not cls.supported_address(dest):
                raise Exception('address type not supported for address %s'%str(dest))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._move_callback,callback=cb)
            else:
                complete_callback = partial(cls._move_callback,callback=callback)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            cl.move(src,dest,complete_callback,0,gridftpClient.OperationAttr())

            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _move_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._move_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._move_callback: callback is not defined')
        
        @classmethod
        def exists(cls,address,callback=None,request_timeout=None):
            """Check if a file exists on the ftp server
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._exists_callback,callback=cb)
            else:
                complete_callback = partial(cls._exists_callback,callback=callback)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            cl.exists(address,complete_callback,0,gridftpClient.OperationAttr())

            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _exists_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._exists_callback: callback is not defined')
        
        @classmethod
        def chmod(cls,address,mode,callback=None,request_timeout=None):
            """Move a file on the ftp server
                 
               callback should be of type callback(result)
                  where result is True/False
               
               If no callback is defined, the function blocks and 
                  returns True/False or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._chmod_callback,callback=cb)
            else:
                complete_callback = partial(cls._chmod_callback,callback=callback)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            cl.chmod(address,mode,complete_callback,0,gridftpClient.OperationAttr())

            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _chmod_callback(cls,arg,handle,error,callback=None):
            if callback is not None:
                if error:
                    logging.warning('Error in GridFTP._chmod_callback: %s',str(error))
                    callback(False)
                else:
                    callback(True)
            else:
                logging.warning('Error in GridFTP._chmod_callback: callback is not defined')
        
        @classmethod
        def size(cls,address,callback=None,request_timeout=None):
            """Get the size of a file on the ftp server
                 
               callback should be of type callback(result)
                  where result is the size or None
               
               If no callback is defined, the function blocks and 
                  returns the size or None or an exception is raised.
            """
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = None
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._size_callback,callback=cb)
            else:
                complete_callback = partial(cls._size_callback,callback=callback)
            
            cl = gridftpClient.FTPClient(gridftpClient.HandleAttr())
            cl.size(address,complete_callback,0,gridftpClient.OperationAttr())
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout
                else:
                    timeout = request_timeout
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    try:
                        cl.abort()
                    except:
                        pass
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _size_callback(cls,ret,arg,handle,error,callback=None):
            if callback is not None:
                if ret is False:
                    callback(None)
                callback(ret)
            else:
                logging.warning('Error in GridFTP._size_callback: callback is not defined')
        
        
        ### Some helper functions for different checksum types ###
        
        @classmethod
        def md5sum(cls,address,callback=None,request_timeout=None):
            """Get the md5sum of a file on an ftp server
                 
               callback should be of type callback(result)
                  where result is the md5sum or False
               
               If no callback is defined, the function blocks and returns either
                  the md5sum or False, or an exception is raised.
            """
            return cls._chksum('md5sum',address,callback=callback,request_timeout=request_timeout)    
        
        @classmethod
        def sha1sum(cls,address,callback=None,request_timeout=None):
            """Get the sha1sum of a file on an ftp server
                 
               callback should be of type callback(result)
                  where result is the sha1sum or False
               
               If no callback is defined, the function blocks and returns either
                  the sha1sum or False, or an exception is raised.
            """
            return cls._chksum('sha1sum',address,callback=callback,request_timeout=request_timeout)    
        
        @classmethod
        def sha256sum(cls,address,callback=None,request_timeout=None):
            """Get the sha256sum of a file on an ftp server
                 
               callback should be of type callback(result)
                  where result is the sha256sum or False
               
               If no callback is defined, the function blocks and returns either
                  the sha256sum or False, or an exception is raised.
            """
            return cls._chksum('sha256sum',address,callback=callback,request_timeout=request_timeout)     
        
        @classmethod
        def sha512sum(cls,address,callback=None,request_timeout=None):
            """Get the sha512sum of a file on an ftp server
                 
               callback should be of type callback(result)
                  where result is the sha512sum or False
               
               If no callback is defined, the function blocks and returns either
                  the sha512sum or False, or an exception is raised.
            """
            return cls._chksum('sha512sum',address,callback=callback,request_timeout=request_timeout)   
        
        @classmethod
        def _chksum(cls,type,address,callback=None,request_timeout=None):
            """The real work of checksums happens here"""
            if not cls.supported_address(address):
                raise Exception('address type not supported for address %s'%str(address))
            
            if callback is None:
                # return like normal function
                def cb(ret):
                    cb.ret = ret
                    cb.event.set()    
                cb.ret = False
                cb.event = Event()
                cb.event.clear()
                complete_callback = partial(cls._chksum_callback,callback=cb)
            else:
                complete_callback = partial(cls._chksum_callback,callback=callback)
            
            server,path = cls.address_split(address)
            cls.popen(server,type,[path],callback=complete_callback,request_timeout=request_timeout)
            
            if callback is None:
                if request_timeout is None:
                    timeout = cls.__timeout+1
                else:
                    timeout = request_timeout+1
                # wait for request to finish
                if cb.event.wait(timeout) is False:
                    # timeout
                    raise Exception('Request timed out: %s'%str(address))
                return cb.ret
        
        @classmethod
        def _chksum_callback(cls,ret,callback=None):
            if callback is not None:
                if ret is False:
                    callback(False)
                # parse chksum output to get just the chksum
                try:
                    callback(ret.split(' ',1)[0])
                except:
                    callback(False)
            else:
                logging.warning('Error in GridFTP._chksum_callback: callback is not defined')

