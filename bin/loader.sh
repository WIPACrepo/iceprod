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
 -e <arg>  IceProd env path
 -x <arg>  x509 proxy filename

EOF
}

# clear variables that we use
unset PROXY
unset SROOT
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

# touch the log, out, err files so they exist
# this prevents HTCondor from complaining
touch iceprod_log
touch iceprod_out
touch iceprod_err
touch iceprod_log.gz
touch iceprod_out.gz
touch iceprod_err.gz

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
        echo "No contact with CVMFS"
        sleep 1200 # block site so black hole doesn't kill all jobs
        exit 1
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

# make sure we never use a user's home directory
export PYTHONNOUSERSITE=1

starttime=`date +%s `

# run i3exec
cmd="$PYBIN -m iceprod.core.i3exec $@"
echo $cmd
$cmd
err=$?

# clean up after ourselves
rm -Rf resource_libs $ENV env

# black hold protection: sleep if we fail and finish fast
endtime=`date +%s `
totaltime=$(( endtime - starttime ))
echo "IceProd job took $totaltime secs"
undertime=$(( totaltime < 600 ))
if [ $err != 0 ] && [ $undertime = 1 ]; then
    sleeptime=$(( 600 - totaltime ))
    echo "sleeping up to the 5 minute min time: $sleeptime secs"
    sleep $sleeptime
    echo "done sleeping"
fi

exit $err