from __future__ import absolute_import, division, print_function

import sys
import logging

def printer(input,passed=True):
    numcols = 60
    padding = 4
    while len(input) > numcols:
        # wrap longer strings
        pos = input.rfind(' ',0,numcols)
        if pos < 0:
            break
        tmp_str = input[0:pos]
        input = '     '+input[pos+1:]
        print(tmp_str)
    # print string aligned left, and passed or failed
    final_str = input
    for i in xrange(len(input),numcols+padding):
        final_str += ' '
    
    if passed:
        logging.error(final_str+'passed')
        final_str += '\033[32m'+'passed'+'\033[0m'
    else:
        logging.error(final_str+'failed')
        final_str += '\033[31m'+'failed'+'\033[0m'
    print(final_str)

def glob_tests(x):
    """glob the tests that were requested"""    
    glob_func_str = '*'
    if len(sys.argv) > 2:
        glob_func_str = sys.argv[2]
    import fnmatch
    return fnmatch.filter(x,glob_func_str)
