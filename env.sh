#!/bin/sh
if [ "x$PYTHONPATH" = "x" ]; then
    export PYTHONPATH=$PWD
else
    export PYTHONPATH=$PWD:$PYTHONPATH
fi
if [ "x$PATH" = "x" ]; then
    export PATH=$PWD/bin
else
    export PATH=$PWD/bin:$PATH
fi
exec "$@"
