#!/bin/sh
export LD_LIBRARY_PATH=$PWD/resource_libs:$LD_LIBRARY_PATH
rm -f .coverage*;
coverage erase;
coverage run --source iceprod,bin --parallel-mode --branch -m tests $@;
if [ $? = 0 ]; then
    coverage combine;
    echo generating html coverage in 'htmlcov';
    coverage html -i;
fi
