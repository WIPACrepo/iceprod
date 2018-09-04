"""
Get a `requests`_ Session that fully retries errors.

.. _requests: http://docs.python-requests.org
"""

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests_futures.sessions import FuturesSession

def AsyncSession(retries=10, backoff_factor=0.3,
            method_whitelist=('HEAD','TRACE','GET','POST','PUT','OPTIONS','DELETE'),
            status_forcelist=(408, 429, 500, 502, 503, 504),
            ):
    """
    Return a Session object with full retry capabilities.

    Args:
        retries (int): number of retries
        backoff_factor (float): speed factor for retries (in seconds)
        method_whitelist (iterable): http methods to retry on
        status_forcelist (iterable): http status codes to retry on

    Returns:
        :py:class:`requests.Session`: session object
    """
    session = FuturesSession()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        redirect=retries,
        status=retries,
        method_whitelist=method_whitelist,
        status_forcelist=status_forcelist,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def Session(retries=10, backoff_factor=0.3,
            method_whitelist=('HEAD','TRACE','GET','POST','PUT','OPTIONS','DELETE'),
            status_forcelist=(408, 429, 500, 502, 503, 504),
            ):
    """
    Return a Session object with full retry capabilities.

    Args:
        retries (int): number of retries
        backoff_factor (float): speed factor for retries (in seconds)
        method_whitelist (iterable): http methods to retry on
        status_forcelist (iterable): http status codes to retry on

    Returns:
        :py:class:`requests.Session`: session object
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        redirect=retries,
        status=retries,
        method_whitelist=method_whitelist,
        status_forcelist=status_forcelist,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session