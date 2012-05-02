AC_INIT(iceprod, 2.0)
echo "Starting IceProd 2.0 Build..."
echo " "

AC_PROG_CC
AC_PROG_CXX

AC_CHECK_TOOLS(www,curl wget fetch,no)
if test $www == curl; then
    AC_SUBST(wwwflags,"--retry 5 --max-time 30 -f -o")
else
    if test $www == wget; then
        AC_SUBST(wwwflags,"-r --tries=5 --timeout=30 -O")
    else
        if test $www == fetch; then
            AC_SUBST(wwwflags,"-a -T 30 -o")
        else
            AC_MSG_ERROR("curl, wget, or fetch required for downloading packages")
        fi
    fi
fi


AC_OUTPUT(Makefile)
AC_OUTPUT(env-shell.sh)
