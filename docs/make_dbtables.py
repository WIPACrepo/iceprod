"""
Make DB tables in rst format from
iceprod.server.modules.db.DBAPI.tables
"""
from __future__ import absolute_import, division, print_function

#import inspect
import sys
from functools import partial

from iceprod.server.modules.db import DBAPI

def main():
    filename = sys.argv[1]
    
    with open(filename,'w') as f:
        p = partial(print,file=f)
        p('.. _dbtables:')
        p('')
        p('DB Tables')
        p('=========')
        p('')
        #for line in inspect.getsource(DBAPI).split('\n'):
        for table in DBAPI.tables:
            p('*',table)
            p('')
            for k in DBAPI.tables[table]:
                v = DBAPI.tables[table][k].__name__
                p('  *',k,':',v)
            p('')

if __name__ == '__main__':
    main()