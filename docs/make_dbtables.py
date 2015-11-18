"""
Make DB tables in rst format from
iceprod.server.modules.db.DBAPI.tables
"""
from __future__ import absolute_import, division, print_function

import sys
from functools import partial
import json
from collections import OrderedDict

from iceprod.server import get_pkgdata_filename

def main():
    filename = sys.argv[1]
    table_filename = get_pkgdata_filename('iceprod.server','data/etc/db_config.json')
    db_tables = json.load(open(table_filename),object_pairs_hook=OrderedDict)

    with open(filename,'w') as f:
        p = partial(print,file=f)
        p('.. role:: gray')
        p('.. raw:: html')
        p('')
        p('   <style> .gray {color:gray} </style>')
        p('')
        p('.. _dbconfig:')
        p('')
        p('DB Config')
        p('=========')
        p('')
        p('.. _dbtables:')
        p('')
        p('Tables')
        p('------')
        p('')
        for n in db_tables['tables']:
            p('**'+n+'**')
            p('')
            for row in db_tables['tables'][n]:
                if row[0] == 'status':
                    row.append('possible statuses: '+', '.join(db_tables['status_options'][n]))
                if len(row) > 2:
                    p('*',row[0],':',row[1],' :gray:`#',row[2]+'`')
                else:
                    p('*',row[0],':',row[1])
            p('')
        p('Archive Tables')
        p('--------------')
        for n in db_tables['archive_tables']:
            p('*',n)
        p('')
        p('Key')
        p('---')
        for n in db_tables['values']:
            p('*',n,':',db_tables['values'][n])
        p('')

if __name__ == '__main__':
    main()
