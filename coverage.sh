#!/bin/sh
export LD_LIBRARY_PATH=$PWD/resource_libs:$LD_LIBRARY_PATH
coverage erase;
coverage run --source iceprod,bin --branch -m tests $@;
coverage combine;
echo generating html coverage in 'htmlcov';
coverage html -i;
