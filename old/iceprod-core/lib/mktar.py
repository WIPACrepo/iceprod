import os,sys
import iceprod
from os.path import *

if __name__ == '__main__':
	libdir  = abspath(join(dirname(sys.argv[0]),'iceprod'))
	zipoutpath = abspath(join(libdir,"..","..","shared",iceprod.zipfile()))
	iceprod.mktar(libdir,outpath)
	iceprod.mktar(libdir,'iceprod/__init__.py',zipoutpath)
	iceprod.mktar(libdir,'iceprod/core',zipoutpath,'a')
	iceprod.mktar(libdir,'iceprod/modules',zipoutpath,'a')
