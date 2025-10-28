#!/usr/bin/env python3

"""
This is a custom HTCondor file transfer plugin for GridFTP.
In this example, it transfers files described by a gridftp://path/to/file URL
by copying them from the path indicated to a job's working directory.
"""

import glob
import os
import sys
import subprocess
import time

DEFAULT_TIMEOUT = 300
PLUGIN_VERSION = '1.0.0'

EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_AUTHENTICATION_REFRESH = 2

try:
    from classad import ClassAd, parseAds  # type: ignore
except ImportError:
    import re
    import json

    class ClassAd(dict):  # type: ignore
        def printOld(self):
            ret = []
            for k,v in self.items():
                if isinstance(v, str):
                    v = '"{0}"'.format(v)
                ret.append('{0} = {1}'.format(k, v))
            return '[\n' + ';\n'.join(ret) + '\n]\n'

    def parseAds(data):
        ret = []
        for ad in re.findall(r'\s*?\[([\w\W]*?)\]', data):
            ad_ret = {}
            for k,v in re.findall(r'\s*?(\w+) += +([^;\n]+);?', ad):
                ad_ret[k] = json.loads(v)
            ret.append(ad_ret)
        return ret


def print_help(stream=sys.stderr):
    help_msg = '''Usage: {0} -infile <input-filename> -outfile <output-filename>
       {0} -classad

Options:
  -classad                    Print a ClassAd containing the capablities of this
                              file transfer plugin.
  -infile <input-filename>    Input ClassAd file
  -outfile <output-filename>  Output ClassAd file
  -upload                     Indicates this transfer is an upload (default is
                              download)
'''
    stream.write(help_msg.format(sys.argv[0]))


def print_capabilities():
    capabilities = {
        'MultipleFileSupport': True,
        'PluginType': 'FileTransfer',
        # SupportedMethods indicates which URL methods/types this plugin supports
        'SupportedMethods': 'gsiftp',
        'Version': PLUGIN_VERSION,
    }
    sys.stdout.write(ClassAd(capabilities).printOld())


def parse_args():

    # The only argument lists that are acceptable are
    # <this> -classad
    # <this> -infile <input-filename> -outfile <output-filename>
    # <this> -outfile <output-filename> -infile <input-filename>
    if len(sys.argv) not in [2, 5, 6]:
        print_help()
        sys.exit(EXIT_FAILURE)

    # If -classad, print the capabilities of the plugin and exit early
    if (len(sys.argv) == 2) and (sys.argv[1] == '-classad'):
        print_capabilities()
        sys.exit(EXIT_SUCCESS)

    # If -upload, set is_upload to True and remove it from the args list
    is_upload = False
    if '-upload' in sys.argv[1:]:
        is_upload = True
        sys.argv.remove('-upload')

    # -infile and -outfile must be in the first and third position
    if not (
            ('-infile' in sys.argv[1:]) and
            ('-outfile' in sys.argv[1:]) and
            (sys.argv[1] in ['-infile', '-outfile']) and
            (sys.argv[3] in ['-infile', '-outfile']) and
            (len(sys.argv) == 5)):
        print_help()
        sys.exit(1)
    infile = None
    outfile = None
    try:
        for i, arg in enumerate(sys.argv):
            if i == 0:
                continue
            elif arg == '-infile':
                infile = sys.argv[i+1]
            elif arg == '-outfile':
                outfile = sys.argv[i+1]
    except IndexError:
        print_help()
        sys.exit(EXIT_FAILURE)

    return {'infile': infile, 'outfile': outfile, 'upload': is_upload}


def format_error(error):
    return '{0}: {1}'.format(type(error).__name__, str(error))


def get_error_dict(error, url=''):
    error_string = format_error(error)
    error_dict = {
        'TransferSuccess': False,
        'TransferError': error_string,
        'TransferUrl': url,
    }

    return error_dict


class GridftpPlugin:

    def setup_env(self):
        # if a proxy exists, use it
        proxies = glob.glob(os.path.join(os.getcwd(), 'x509up_*'))
        if proxies:
            os.environ['X509_USER_PROXY'] = proxies[0]
        else:
            raise RuntimeError('X509_USER_PROXY does not exist')

        if not os.path.exists('/cvmfs/icecube.opensciencegrid.org/iceprod/v2.7.1/env-shell.sh'):
            raise RuntimeError('CVMFS does not exist')

    def _do_transfer(self, inpath, outpath):
        try:
            subprocess.check_output([
                '/cvmfs/icecube.opensciencegrid.org/iceprod/v2.7.1/env-shell.sh',
                'globus-url-copy',
                '-cd',
                '-rst',
                inpath,
                outpath,
            ], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            if e.output:
                output = e.output.decode('utf-8')
                for line in output.split('\n'):
                    if line.lower().startswith('error'):
                        raise RuntimeError('globus-url-copy failed: '+line)
                raise RuntimeError('Generic subprocess failure: '+output)
            raise

    def download_file(self, url, local_file_path):

        start_time = time.time()

        # Download transfer logic goes here
        self._do_transfer(url, 'file://'+os.path.abspath(local_file_path))
        file_size = os.stat(local_file_path).st_size

        end_time = time.time()

        # Get transfer statistics
        transfer_stats = {
            'TransferSuccess': True,
            'TransferProtocol': 'gsiftp',
            'TransferType': 'upload',
            'TransferFileName': local_file_path,
            'TransferFileBytes': file_size,
            'TransferTotalBytes': file_size,
            'TransferStartTime': int(start_time),
            'TransferEndTime': int(end_time),
            'ConnectionTimeSeconds': end_time - start_time,
            'TransferUrl': url,
        }

        return transfer_stats

    def upload_file(self, url, local_file_path):

        start_time = time.time()

        # Upload transfer logic goes here
        self._do_transfer('file://'+os.path.abspath(local_file_path), url)
        file_size = os.stat(local_file_path).st_size

        end_time = time.time()

        # Get transfer statistics
        transfer_stats = {
            'TransferSuccess': True,
            'TransferProtocol': 'gsiftp',
            'TransferType': 'upload',
            'TransferFileName': local_file_path,
            'TransferFileBytes': file_size,
            'TransferTotalBytes': file_size,
            'TransferStartTime': int(start_time),
            'TransferEndTime': int(end_time),
            'ConnectionTimeSeconds': end_time - start_time,
            'TransferUrl': url,
        }

        return transfer_stats


if __name__ == '__main__':

    # Start by parsing input arguments
    try:
        args = parse_args()
    except Exception:
        sys.exit(EXIT_FAILURE)

    gridftp_plugin = GridftpPlugin()

    # Parse in the classads stored in the input file.
    # Each ad represents a single file to be transferred.
    try:
        infile_ads = parseAds(open(args['infile'], 'r'))
    except Exception as err:
        try:
            with open(args['outfile'], 'w') as outfile:
                outfile_dict = get_error_dict(err)
                outfile.write(str(ClassAd(outfile_dict)))
        except Exception:
            pass
        sys.exit(EXIT_FAILURE)

    # Now iterate over the list of classads and perform the transfers.
    try:
        with open(args['outfile'], 'w') as outfile:
            for ad in infile_ads:
                try:
                    gridftp_plugin.setup_env()
                    if not args['upload']:
                        outfile_dict = gridftp_plugin.download_file(ad['Url'], ad['LocalFileName'])
                    else:
                        outfile_dict = gridftp_plugin.upload_file(ad['Url'], ad['LocalFileName'])

                    outfile.write(str(ClassAd(outfile_dict)))

                except Exception as err:
                    try:
                        outfile_dict = get_error_dict(err, url=ad['Url'])
                        outfile.write(str(ClassAd(outfile_dict)))
                    except Exception:
                        pass
                    sys.exit(EXIT_FAILURE)

    except Exception:
        sys.exit(EXIT_FAILURE)
