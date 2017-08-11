#!/bin/sh

COVERAGE="python3 `which coverage`"

export LD_LIBRARY_PATH=$PWD/resource_libs:$LD_LIBRARY_PATH
rm -f .coverage*;
$COVERAGE erase;
$COVERAGE run --source iceprod,bin --parallel-mode --branch -m tests $@;
#if [ $? = 0 ]; then
    $COVERAGE combine;
    echo generating html coverage in 'htmlcov';
    $COVERAGE html -i;
#fi
