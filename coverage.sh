#!/bin/sh

COVERAGE="python3 `which coverage`"

rm -f .coverage*;
$COVERAGE erase;
$COVERAGE run --source iceprod,bin --parallel-mode --branch -m pytest --tb=short tests $@;
ERR=$?
$COVERAGE combine;
echo generating html coverage in 'htmlcov';
$COVERAGE html -i;
exit $ERR
