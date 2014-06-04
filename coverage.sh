#!/bin/sh
export LD_LIBRARY_PATH=$PWD/resource_libs:$LD_LIBRARY_PATH
coverage run --source iceprod -m tests $@;
coverage combine;
echo generating html coverage in 'htmlcov';
coverage html -i --omit=\*test\*;
