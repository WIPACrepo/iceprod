"""
File Catalog
============

Interface to communicate with a File Catalog using REST over HTTP.
"""

import logging

import requests

from .jsonUtil import json_encode, json_decode

logger = logging.getLogger('file_catalog')


class FileCatalog(object):
    """
    High level file catalog interface.

    Args:
        url (str): url of the file catalog server
        overwrite (bool): overwrite entries when adding new files
    """
    def __init__(self, url, overwrite=False):
        self.fc = FileCatalogLowLevel(url)
        self.overwrite = overwrite

    def add(self, name, path, checksum, metadata=None, overwrite=None):
        """
        Add a file to the catalog.

        Args:
            name (str): unique name of file
            path (str): url of file
            checksum (str): sha512 checksum of file
            metadata (dict): (optional) additional metadata
            overwrite (bool): overwrite entries when adding new files
        """
        if not metadata:
            metadata = {}
        if overwrite is None:
            overwrite = self.overwrite
        metadata.update({'uid': name, 'checksum': checksum,
                         'locations': [path]})
        try:
            self.fc[name] = metadata
        except requests.exceptions.HTTPError:
            if overwrite:
                del self.fc[name]
                self.fc[name] = metadata
            else:
                raise

    def get(self, name):
        """
        Get a file path and checksum from the catalog.

        Args:
            name (str): unique name of file

        Returns:
            tuple: path, checksum
        """
        m = self.fc[name]
        return (m['locations'][0], m['checksum'])

    def get_metadata(self, name):
        """
        Get all file metadata from the catalog.

        Args:
            name (str): unique name of file

        Returns:
            dict: metadata information
        """
        return self.fc[name]

    def delete(self, name):
        """
        Remove a file from the catalog

        Args:
            name (str): unique name of file
        """
        del self.fc[name]


class FileCatalogLowLevel(object):
    """
    Low level file catalog interface.  Use like a dict::

        fc = FileCatalog('http://file_catalog.com')
        fc['my_new_file'] = {'locations':['/this/is/a/path']}

    Args:
        url (str): url of the file catalog server
        timeout (float): (optional) seconds to wait for a query to finish
    """
    def __init__(self, url, timeout=60):
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()

    def _getfileurl(self, uid):
        for _ in range(5):
            try:
                r = self.session.get(self.url+'/api/files',
                                     params={'query':json_encode({'uid':uid})},
                                     timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            files = json_decode(r.text)['files']
            break
        else:
            raise Exception('server is too busy')
        if len(files) != 1:
            raise KeyError()
        return self.url+files[0]

    def __getitem__(self, uid):
        url = self._getfileurl(uid)
        for _ in range(5):
            try:
                r = self.session.get(url, timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            return json_decode(r.text)
        raise Exception('server is too busy')

    def __setitem__(self, uid, value):
        meta = value.copy()
        meta['uid'] = uid
        data = json_encode(meta)
        try:
            url = self._getfileurl(uid)
        except KeyError:
            # does not exist
            method = self.session.post
            url = self.url+'/api/files'
        else:
            # exists, so update
            method = self.session.put
        for _ in range(5):
            try:
                r = method(url, data=data,
                           timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            return
        raise Exception('server is too busy')

    def __delitem__(self, uid):
        url = self._getfileurl(uid)
        for _ in range(5):
            try:
                r = self.session.delete(url, timeout=self.timeout)
            except requests.exceptions.Timeout:
                continue
            if r.status_code == 429:
                continue
            r.raise_for_status()
            return
        raise Exception('server is too busy')
