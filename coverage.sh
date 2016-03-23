#!/bin/sh
export LD_LIBRARY_PATH=$PWD/resource_libs:$LD_LIBRARY_PATH
rm -f .coverage.sh*;
coverage erase;
coverage run --source iceprod,bin --branch -m tests $@;
if [ $? = 0 ]; then
    coverage combine;
    echo generating html coverage in 'htmlcov';
    coverage html -i;
fi
