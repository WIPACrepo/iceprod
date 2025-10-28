from importlib.metadata import version, PackageNotFoundError

try:
    VERSION_STRING = version('iceprod')
except PackageNotFoundError:
    # package is not installed
    VERSION_STRING = 'dev'
