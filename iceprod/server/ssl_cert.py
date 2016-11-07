"""
Functions relating to OpenSSL certificates.
"""

import os
import time
import socket
import subprocess
import hashlib
import logging
from datetime import datetime
import uuid

from OpenSSL import SSL,crypto
from pyasn1.type import univ
from pyasn1.codec.der import encoder

from iceprod.core import functions

logger = logging.getLogger('ssl_cert')


def create_ca(cert_filename,key_filename,days=365,hostname=None):
    """Make a certificate authority and key pair"""
    cert_filename = os.path.abspath(os.path.expandvars(cert_filename))
    key_filename = os.path.abspath(os.path.expandvars(key_filename))
    logger.warn('making CA cert at %s',cert_filename)

    if not (os.path.exists(cert_filename)
            and os.path.exists(key_filename)):
        if hostname is None:
            # get hostname
            hostname = functions.gethostname()
            if hostname is None:
                raise Exception('Cannot get hostname')

        # create a key pair
        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, 2048)

        # create a self-signed cert
        cert = crypto.X509()
        cert.set_version(2) # version 3, since count starts at 0
        cert.get_subject().C = "US"
        cert.get_subject().ST = "Wisconsin"
        cert.get_subject().L = "Madison"
        cert.get_subject().O = "University of Wisconsin-Madison"
        cert.get_subject().OU = "IceCube IceProd Root CA"
        cert.get_subject().CN = hostname
        cert.set_serial_number(1)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(days*24*60*60)
        cert.set_issuer(cert.get_subject())
        cert.set_pubkey(k)

        # get integer public key for cert
        # pyOpenSSL doesn't provide a good access method,
        # so do it the hard way by extracting from key output
        pubkey = crypto.dump_privatekey(crypto.FILETYPE_TEXT, cert.get_pubkey())
        pubkey = [x.strip() for x in pubkey.split('\n') if len(x)>0 and x[0]==' ']
        pubkey = int(''.join(pubkey).replace(':',''),16)

        # make asn1 DER encoding
        seq = univ.Sequence()
        seq.setComponentByPosition(0,univ.Integer(pubkey))
        seq.setComponentByPosition(1,univ.Integer(65537))
        enc = encoder.encode(seq)

        # get hash of DER
        hash = hashlib.sha1(enc).hexdigest()

        # add extensions
        cert.add_extensions([
            crypto.X509Extension("basicConstraints", True,
                                 "CA:TRUE, pathlen:1"),
            crypto.X509Extension("keyUsage", True,
                                 "keyCertSign, cRLSign"),
            crypto.X509Extension("subjectKeyIdentifier", False, hash,
                                 subject=cert),
            crypto.X509Extension("subjectAltName", False, 'DNS:'+hostname)
            ])
        cert.sign(k, 'sha1')

        open(cert_filename, "w").write(
            crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
        open(key_filename, "w").write(
            crypto.dump_privatekey(crypto.FILETYPE_PEM, k))


def create_cert(cert_filename,key_filename,days=365,hostname=None,
        cacert=None,cakey=None,allow_resign=False):
    """Make a certificate and key pair"""
    cert_filename = os.path.abspath(os.path.expandvars(cert_filename))
    key_filename = os.path.abspath(os.path.expandvars(key_filename))
    logger.warn('making cert at %s',cert_filename)
    if cacert:
        cacert = os.path.abspath(os.path.expandvars(cacert))
        cakey = os.path.abspath(os.path.expandvars(cakey))
        logger.warn('with CA %s',cacert)
        if not (os.path.exists(cacert) and os.path.exists(cakey)):
            raise Exception('CA cert does not exist')

    if not (os.path.exists(cert_filename)
            and os.path.exists(key_filename)):
        if hostname is None:
            # get hostname
            hostname = functions.gethostname()
            if hostname is None:
                raise Exception('Cannot get hostname')

        # create a key pair
        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, 2048)

        if cacert is None or cakey is None:
            # self-signed
            cert = crypto.X509()
        else:
            # certificate request
            cert = crypto.X509Req()

        if cacert is None or cakey is None:
            # self-sign
            cert.get_subject().CN = hostname

            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(days*24*60*60)
            cert.set_serial_number(uuid.uuid4().int)
            cert.set_issuer(cert.get_subject())
            cert.set_pubkey(k)

            # add extensions
            exts = [crypto.X509Extension("subjectAltName", False, 'DNS:'+hostname)]
            if allow_resign:
                exts.extend([
                    crypto.X509Extension("basicConstraints", True,
                                         "CA:TRUE, pathlen:0"),
                    #crypto.X509Extension("keyUsage", True,
                    #                     "keyCertSign, cRLSign"),
                    #crypto.X509Extension("subjectKeyIdentifier", False, hash,
                    #                     subject=cert),
                    ])
            cert.add_extensions(exts)
            cert.sign(k, 'sha1')

        else:
            cert.get_subject().C = "US"
            cert.get_subject().ST = "Wisconsin"
            cert.get_subject().L = "Madison"
            cert.get_subject().O = "University of Wisconsin-Madison"
            cert.get_subject().OU = "IceCube IceProd"
            cert.get_subject().CN = hostname

            # finish cert req
            cert.set_pubkey(k)
            cert.sign(k, 'sha1')

            # load CA
            cacert = os.path.abspath(os.path.expandvars(cacert))
            cakey = os.path.abspath(os.path.expandvars(cakey))
            ca_cert = crypto.load_certificate(crypto.FILETYPE_PEM,open(cacert).read())
            ca_key = crypto.load_privatekey(crypto.FILETYPE_PEM,open(cakey).read())

            # make actual cert and sign with CA
            cert2 = crypto.X509()
            cert2.set_subject(cert.get_subject())
            cert2.set_serial_number(1)
            cert2.gmtime_adj_notBefore(0)
            cert2.gmtime_adj_notAfter(days*24*60*60)
            cert2.set_issuer(ca_cert.get_subject())
            cert2.set_pubkey(cert.get_pubkey())

            # add extensions
            exts = [crypto.X509Extension("subjectAltName", False, 'DNS:'+hostname)]
            if allow_resign:
                exts.extend([
                    crypto.X509Extension("basicConstraints", True,
                                         "CA:TRUE, pathlen:0"),
                    #crypto.X509Extension("keyUsage", True,
                    #                     "keyCertSign, cRLSign"),
                    #crypto.X509Extension("subjectKeyIdentifier", False, hash,
                    #                     subject=cert),
                    ])
            cert2.add_extensions(exts)
            cert2.sign(ca_key, 'sha1')

            # overwrite cert req with real cert
            cert = cert2

        open(cert_filename, "w").write(
            crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
        open(key_filename, "w").write(
            crypto.dump_privatekey(crypto.FILETYPE_PEM, k))

def verify_cert(cert_filename,key_filename):
    """Verify if cert and key match.
       Return False for failure, True for success.
    """
    cert_filename = os.path.abspath(os.path.expandvars(cert_filename))
    key_filename = os.path.abspath(os.path.expandvars(key_filename))

    cert = crypto.load_certificate(crypto.FILETYPE_PEM,open(cert_filename).read())
    key = crypto.load_privatekey(crypto.FILETYPE_PEM,open(key_filename).read())

    # check date
    begin = cert.get_notBefore()
    logger.debug('begin: %r',begin)
    if datetime.strptime(begin, "%Y%m%d%H%M%SZ") > datetime.utcnow():
        logger.error('cert only valid in future')
        return False
    end = cert.get_notAfter()
    logger.debug('end: %r',end)
    if datetime.strptime(end, "%Y%m%d%H%M%SZ") < datetime.utcnow():
        logger.warn('cert has expired')
        return False

    # check matching cert and key
    ctx = SSL.Context(SSL.TLSv1_METHOD)
    ctx.use_privatekey(key)
    ctx.use_certificate(cert)
    try:
        ctx.check_privatekey()
    except SSL.Error:
        logger.warn('cert and key do not match')
        return False
    else:
        return True


