AC_INIT([iceprod], m4_esyscmd_s([cat VERSION]),[simprod@icecube.wisc.edu],m4_esyscmd([echo iceprod-|tr -d '\n';tr -d '\n' < VERSION]),[http://x2100.icecube.wisc.edu/downloads/iceprod/])
echo "Starting IceProd 2.0 Build..."
echo " "

AC_PROG_CC
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_SED
AC_PROG_GREP


dnl We use a path for perl so the #! line in autoscan will work.	
AC_DEFUN([AX_PROG_PERL],[
	AC_PATH_PROG([PERL], perl, no)
	AC_SUBST([PERL])dnl

	if test "$PERL" = no; then
		AC_MSG_ERROR([perl is not found])
	fi
	AC_MSG_CHECKING(for perl version >= $1)
	if `$PERL -e 'require $1;' >> /dev/null 2>&1` ; then
		AC_MSG_RESULT(yes)
	else
		AC_MSG_ERROR([Perl $1 or better is required])
	fi
	AC_PROVIDE([$0])
])
AX_PROG_PERL([5.8.0])

AC_CHECK_TOOLS(www,curl wget fetch,no)
if test $www == curl; then
    AC_SUBST(wwwflags,"--retry 5 --max-time 30 -f --silent -o")
else
    if test $www == wget; then
        AC_SUBST(wwwflags,"-r --tries=5 --timeout=30 --quiet -O")
    else
        if test $www == fetch; then
            AC_SUBST(wwwflags,"-a -T 30 -q -o")
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


AC_PREFIX_DEFAULT("$HOME/iceprod")

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([env-shell.sh],[chmod +x env-shell.sh])
AC_OUTPUT()
