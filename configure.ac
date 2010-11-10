#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(spatialite-tools, 2.4.0, a.furieri@lqt.it)
AC_LANG(C)

AM_INIT_AUTOMAKE
AM_MAINTAINER_MODE

# Checks for header files.
AC_CHECK_HEADERS(stdlib.h,, [AC_MSG_ERROR([cannot find stdlib.h, bailing out])])
AC_CHECK_HEADERS(stdio.h,, [AC_MSG_ERROR([cannot find stdio.h, bailing out])])
AC_CHECK_HEADERS(string.h,, [AC_MSG_ERROR([cannot find string.h, bailing out])])
AC_CHECK_HEADERS(memory.h,, [AC_MSG_ERROR([cannot find memory.h, bailing out])])
AC_CHECK_HEADERS(math.h,, [AC_MSG_ERROR([cannot find math.h, bailing out])])
AC_CHECK_HEADERS(float.h,, [AC_MSG_ERROR([cannot find float.h, bailing out])])
AC_CHECK_HEADERS(fcntl.h,, [AC_MSG_ERROR([cannot find fcntl.h, bailing out])])
AC_CHECK_HEADERS(inttypes.h,, [AC_MSG_ERROR([cannot find inttypes.h, bailing out])])
AC_CHECK_HEADERS(stddef.h,, [AC_MSG_ERROR([cannot find stddef.h, bailing out])])
AC_CHECK_HEADERS(stdint.h,, [AC_MSG_ERROR([cannot find stdint.h, bailing out])])
AC_CHECK_HEADERS(sys/time.h,, [AC_MSG_ERROR([cannot find sys/time.h, bailing out])])
AC_CHECK_HEADERS(unistd.h,, [AC_MSG_ERROR([cannot find unistd.h, bailing out])])
AC_CHECK_HEADERS(spatialite/gaiaexif.h,, [AC_MSG_ERROR([cannot find spatialite/gaiaexif.h, bailing out])])
AC_CHECK_HEADERS(spatialite/gaiaaux.h,, [AC_MSG_ERROR([cannot find spatialite/gaiaaux.h, bailing out])])
AC_CHECK_HEADERS(expat.h,, [AC_MSG_ERROR([cannot find expat.h, bailing out])])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_LIBTOOL_WIN32_DLL
AC_PROG_LIBTOOL

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM
AC_C_VOLATILE

# Checks for library functions.
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MEMCMP
AC_FUNC_STAT
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([memset sqrt strcasecmp strerror strncasecmp strstr fdatasync ftruncate getcwd gettimeofday localtime_r memmove strerror])

AC_CONFIG_FILES([Makefile])

#-----------------------------------------------------------------------
#   --enable-readline
#
AC_ARG_ENABLE(readline, [AS_HELP_STRING(
  [--enable-readline],
  [use readline in shell tool (yes, no) [default=yes]])],
  [], [enable_readline=yes])
if test x"$enable_readline" != xno ; then
  sLIBS=$LIBS
  LIBS=""
  AC_SEARCH_LIBS(tgetent, curses, [], [])
  AC_SEARCH_LIBS(readline, readline, [], [enable_readline=no])
  AC_CHECK_FUNCS(readline, [], [])
  READLINE_LIBS=$LIBS
  LIBS=$sLIBS
fi
AC_SUBST(READLINE_LIBS)

AC_CHECK_LIB(expat,XML_ParserCreate,,AC_MSG_ERROR(['expat' is required but it doesn't seems to be installed on this system.]))
AC_CHECK_LIB(proj,pj_init_plus,,AC_MSG_ERROR(['libproj' is required but it doesn't seems to be installed on this system.]),-lm)
AC_CHECK_LIB(geos_c,GEOSTopologyPreserveSimplify,,AC_MSG_ERROR(['libgeos_c' is required but it doesn't seems to be installed on this system.]),-lm -lgeos)
AC_CHECK_LIB(spatialite,gaiaRingGetPoint,,AC_MSG_ERROR(['spatialite' is required but it doesn't seems to be installed on this system.]))

#-----------------------------------------------------------------------
#   --with-amalgamation
#
AC_ARG_ENABLE(amalgamation, [AS_HELP_STRING(
  [--with-amalgamation], [supporting SpatiaLite-Amalgamation [default=no]])],
  [], [amalgamation=no])
if test x"$with_amalgamation" != "xyes"; then
  AMALGAMATION_FLAGS=
  AC_CHECK_HEADERS(sqlite3.h,, [AC_MSG_ERROR([cannot find sqlite3.h, bailing out])])
  AC_SEARCH_LIBS(sqlite3_backup_init,sqlite3,,AC_MSG_ERROR(['libsqlite3' (> v.3.6.11) is required but it doesn't seems to be installed on this system.]),-lm)
else
  AMALGAMATION_FLAGS=-DSPATIALITE_AMALGAMATION
  AC_CHECK_HEADERS(spatialite/sqlite3.h,, [AC_MSG_ERROR([cannot find sqlite3.h, bailing out])])
  AC_SEARCH_LIBS(SPLite3_backup_init,spatialite,,AC_MSG_ERROR(['libspatialite' doesn't seems to support SQLite internally.]),-lm)
fi
AC_SUBST(AMALGAMATION_FLAGS)

# Checking for MinGW
AM_CONDITIONAL([MINGW], [test "$target_alias" = "mingw32"])
# Checking for Mac OsX
AM_CONDITIONAL([MACOSX], [test "$target_alias" = "macosx"])

AC_OUTPUT
