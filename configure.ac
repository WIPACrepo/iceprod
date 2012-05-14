AC_INIT([iceprod], m4_esyscmd_s([cat VERSION]),[simprod@icecube.wisc.edu],m4_esyscmd([echo iceprod-|tr -d '\n';tr -d '\n' < VERSION]),[http://x2100.icecube.wisc.edu/downloads/iceprod/])
echo "Starting IceProd 2.0 Build..."
echo " "

AC_PROG_CC
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_SED
AC_PROG_GREP

AC_PROG_MAKE_SET

AC_CHECK_TOOLS(www,curl wget fetch,no)
if test $www == curl; then
    AC_SUBST(wwwflags,"--retry 5 --max-time 300 -f --silent -o")
else
    if test $www == wget; then
        AC_SUBST(wwwflags,"-r --tries=5 --timeout=300 --quiet -O")
    else
        if test $www == fetch; then
            AC_SUBST(wwwflags,"-a -T 300 -q -o")
        else
            AC_MSG_ERROR("curl, wget, or fetch required for downloading packages")
        fi
    fi
fi

arch=`uname -m | sed -e 's/Power Macintosh/ppc/ ; s/i686/i386/'`
ostype=`uname`
if test $ostype == Linux; then
    ver=`ldd --version|awk 'NR>1{exit};{print $(NF)}'`
else
    ver=`uname -r`
fi
AC_SUBST(platform,"$arch.$ostype.$ver")

AC_SUBST(tarball_name,["$PACKAGE_TARNAME-$platform.tar.gz"])
cmd=m4_joinall([],["$www $wwwflags"],["$tarball_name "],["$PACKAGE_URL"],["$tarball_name"])
if $cmd; then
    echo Pre-Built Binary Found
    AC_SUBST(prebuilt,[prebuilt])
else
    echo Must build from source, no binary found
    AC_SUBST(prebuilt,[build])
fi

AC_CHECK_FILE([Makefile.hist],[rm Makefile.hist])


AC_PREFIX_DEFAULT("$HOME/iceprod")

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([env-shell.sh],[chmod +x env-shell.sh])
AC_OUTPUT()
