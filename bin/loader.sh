#!/bin/sh

echo "iceprod loader starting at $PWD"

# usage info
usage()
{
cat << EOF
usage: $0 options passthrough-args

Iceprod core framework starter script.

OPTIONS:
 -h        Show this message
 -s <arg>  Software repository
 -c <arg>  Cache/proxy for Parrot/CVMFS
 -m <arg>  Platform for Parrot
 -e <arg>  IceProd env path
 -x <arg>  x509 proxy filename

EOF
}

# clear variables that we use
unset PROXY
unset SROOT
unset PLATFORM
unset ENV
unset X509

# get args
INC=0
while getopts ":hd:c:s:m:e:x:" opt; do
    case $opt in
        h)
            usage
            exit
            ;;
        c)
            PROXY=$OPTARG
            ;;
        s)
            SROOT=$OPTARG
            ;;
        m)
            PLATFORM=$OPTARG
            ;;
        e)
            ENV=$OPTARG
            ;;
        x)
            X509=$OPTARG
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            echo "Option parsing halted early."
            echo "Passing all other arguments on to i3exec.py"
            break
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
    esac
    INC=$((INC+2))
done
shift $INC
echo " "

# calculate platform
OSTYPE=`uname`
if [ $OSTYPE = 'Linux' ]; then
    VER=`ldd --version|awk 'NR>1{exit};{print $(NF)}'`
    HASH=hash
else
    VER=`uname -r`
    HASH=type
fi
if [ -z $PLATFORM ]; then
    ARCH=`uname -m | sed -e 's/Power Macintosh/ppc/ ; s/i686/i386/'`
    PLATFORM="$ARCH.$OSTYPE.$VER"
fi
echo "Platform: $PLATFORM"
export PLATFORM

if [ -z $SROOT ]; then
    SROOT="/cvmfs/icecube.opensciencegrid.org/iceprod/latest"
fi
if [ -z $PROXY ]; then
    PROXY="squid.icecube.wisc.edu:3128"
fi
#export http_proxy=$PROXY

PYBIN=python
if [ -d $PWD/iceprod ]; then
    # local iceprod available, assume env is good
    # (this is mostly for testing)
    if [ -z $PYTHONPATH ]; then
        export PYTHONPATH=$PWD
    else
        export PYTHONPATH=$PWD:$PYTHONPATH
    fi
else
    if [ ! -d $SROOT ]; then
        # first, try parrot
        TEST_PARROT="parrot_run -p $PROXY ls $SROOT"
        if ( $HASH parrot_run ) && $TEST_PARROT >/dev/null 2>/dev/null; then
            PYBIN="parrot_run -p $PROXY $SROOT/env-shell.sh python"
        else
            echo "No contact with CVMFS"
            exit 1
        fi
    else
        echo "Using software at $SROOT"
        if [ -f $SROOT/setup.sh ]; then
            echo "Evaluating setup.sh"
            eval `$SROOT/setup.sh`
        else
            echo "Manually setting environment"
            export PATH=$SROOT/bin:$PATH
            export LD_LIBRARY_PATH=$SROOT/lib:$SROOT/lib64:$LD_LIBRARY_PATH
            export DYLD_LIBRARY_PATH=$SROOT/lib:$SROOT/lib64:$DYLD_LIBRARY_PATH
            export PYTHONPATH=$SROOT/lib/python`python -c 'import sys;print(".".join([str(x) for x in sys.version_info[:2]]))'`/site-packages:$SROOT/lib:$PYTHONPATH
        fi
    fi
fi

if [ ! -z $ENV ]; then
    # load iceprod into the environment
    if [ -z $PYTHONPATH ]; then
        export PYTHONPATH=$ENV
    else
        export PYTHONPATH=$ENV:$PYTHONPATH
    fi
fi

# create resource_libs directory
if [ ! -d $PWD/resource_libs ]; then
    mkdir resource_libs
fi
export LD_LIBRARY_PATH=$PWD/resource_libs:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$PWD/resource_libs:$DYLD_LIBRARY_PATH

# set proxy
if [ ! -z $X509 ]; then
    export X509_USER_PROXY=$PWD/$X509
elif [ -z $X509_USER_PROXY ]; then
    export X509_USER_PROXY=$PWD/x509up
fi

# run i3exec
cmd="$PYBIN -m iceprod.core.i3exec $@"
echo $cmd
exec $cmd

# clean up after ourselves
rm -Rf resource_libs $ENV env
