/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_CONFIG_URI 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_CACHE_HIT 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_CACHE_MISS 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_CACHE_USED 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_CACHE_WRITE 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_LOOKASIDE_HIT 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_LOOKASIDE_USED 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_SCHEMA_USED 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_DBSTATUS_STMT_USED 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_FCNTL_VFSNAME 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_STMTSTATUS_AUTOINDEX 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_STMTSTATUS_FULLSCAN_STEP 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_STMTSTATUS_SORT 1

/* depending on SQLite library version. */
#define HAVE_DECL_SQLITE_TESTCTRL_EXPLAIN_STMT 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the <expat.h> header file. */
#define HAVE_EXPAT_H 1

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the `fdatasync' function. */
#define HAVE_FDATASYNC 1

/* Define to 1 if you have the <float.h> header file. */
#define HAVE_FLOAT_H 1

/* Define to 1 if you have the `ftruncate' function. */
#define HAVE_FTRUNCATE 1

/* Define to 1 if you have the `getcwd' function. */
#define HAVE_GETCWD 1

/* Define to 1 if you have the `gettimeofday' function. */
#define HAVE_GETTIMEOFDAY 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `expat' library (-lexpat). */
#define HAVE_LIBEXPAT 1

/* Define to 1 if you have the `sqlite3' library (-lsqlite3). */
#define HAVE_LIBSQLITE3 1

/* Define to 1 if you have the `localtime_r' function. */
#define HAVE_LOCALTIME_R 1

/* Define to 1 if `lstat' has the bug that it succeeds when given the
   zero-length file name argument. */
/* #undef HAVE_LSTAT_EMPTY_STRING_BUG */

/* Define to 1 if you have the <math.h> header file. */
#define HAVE_MATH_H 1

/* Define to 1 if you have the `memmove' function. */
#define HAVE_MEMMOVE 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `memset' function. */
#define HAVE_MEMSET 1

/* Define to 1 if you have the `readline' function. */
/* #undef HAVE_READLINE */

/* Define to 1 if you have the <sqlite3ext.h> header file. */
#define HAVE_SQLITE3EXT_H 1

/* Define to 1 if you have the <sqlite3.h> header file. */
#define HAVE_SQLITE3_H 1

/* Define to 1 if you have the `sqrt' function. */
/* #undef HAVE_SQRT */

/* Define to 1 if `stat' has the bug that it succeeds when given the
   zero-length file name argument. */
/* #undef HAVE_STAT_EMPTY_STRING_BUG */

/* Define to 1 if you have the <stddef.h> header file. */
#define HAVE_STDDEF_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdio.h> header file. */
#define HAVE_STDIO_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strcasecmp' function. */
#define HAVE_STRCASECMP 1

/* Define to 1 if you have the `strerror' function. */
#define HAVE_STRERROR 1

/* Define to 1 if you have the `strftime' function. */
#define HAVE_STRFTIME 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strncasecmp' function. */
#define HAVE_STRNCASECMP 1

/* Define to 1 if you have the `strstr' function. */
#define HAVE_STRSTR 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if `lstat' dereferences a symlink specified with a trailing
   slash. */
#define LSTAT_FOLLOWS_SLASHED_SYMLINK 1

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
#define LT_OBJDIR ".libs/"

/* Should be defined in order to disable ReadOSM support. */
/* #undef OMIT_READOSM */

/* Name of package */
#define PACKAGE "spatialite-tools"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "a.furieri@lqt.it"

/* Define to the full name of this package. */
#define PACKAGE_NAME "spatialite-tools"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "spatialite-tools 5.1.0-devel"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "spatialite-tools"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "5.1.0-devel"

/* must be defined when using libspatialite-amalgamation */
/* #undef SPATIALITE_AMALGAMATION */

/* Define to 1 if all of the C90 standard headers exist (not just the ones
   required in a freestanding environment). This macro is provided for
   backward compatibility; new code need not use it. */
#define STDC_HEADERS 1

/* Define to 1 if your <sys/time.h> declares `struct tm'. */
/* #undef TM_IN_SYS_TIME */

/* Version number of package */
#define VERSION "5.1.0-devel"

/* Must be =64 in order to enable huge-file support. */
#define _FILE_OFFSET_BITS 64

/* Must be defined in order to enable huge-file support. */
#define _LARGEFILE_SOURCE 1

/* Must be defined in order to enable huge-file support. */
#define _LARGE_FILE 1

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to `long int' if <sys/types.h> does not define. */
/* #undef off_t */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */

/* Define to empty if the keyword `volatile' does not work. Warning: valid
   code using `volatile' can become incorrect without. Disable with care. */
/* #undef volatile */
