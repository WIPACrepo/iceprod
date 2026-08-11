from importlib.metadata import PackageNotFoundError, version

try:
    VERSION_STRING = version('iceprod')
except PackageNotFoundError:
    # package is not installed
    VERSION_STRING = 'dev'
