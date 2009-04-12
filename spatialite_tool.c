/* 
/ spatialite_tool
/
/ an utility CLI tool for Shapefile import / export
/
/ version 1.0, 2008 Decmber 11
/
/ Author: Sandro Furieri a.furieri@lqt.it
/
/ Copyright (C) 2008  Alessandro Furieri
/
/    This program is free software: you can redistribute it and/or modify
/    it under the terms of the GNU General Public License as published by
/    the Free Software Foundation, either version 3 of the License, or
/    (at your option) any later version.
/
/    This program is distributed in the hope that it will be useful,
/    but WITHOUT ANY WARRANTY; without even the implied warranty of
/    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/    GNU General Public License for more details.
/
/    You should have received a copy of the GNU General Public License
/    along with this program.  If not, see <http://www.gnu.org/licenses/>.
/
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <spatialite/sqlite3.h>
#include <spatialite/gaiaaux.h>
#include <spatialite/gaiageo.h>
#include <spatialite.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	stricmp
#endif /* not WIN32 */

#define ARG_NONE		0
#define ARG_CMD			1
#define ARG_SQL			2
#define ARG_SHP			3
#define ARG_DB			4
#define ARG_TABLE		5
#define ARG_COL			6
#define ARG_CS			7
#define ARG_SRID		8
#define ARG_TYPE		9

static char *
read_sql_line (FILE * in, int *len, int *eof)
{
/* reading an SQL script line */
    int c;
    int size = 4096;
    char *line = (char *) malloc (size);
    int off = 0;
    *eof = 0;
    while ((c = getc (in)) != EOF)
      {
	  /* consuming input one chat at each time */
	  if (off == size)
	    {
		/* buffer overflow; reallocating a bigger one */
		/* presumably this is because there is some BLOB */
		/* so we'll grow by 1MB at each time */
		size += 1024 * 1024;
		line = (char *) realloc (line, size);
	    }
	  *(line + off) = c;
	  off++;
	  if (c == '\n')
	    {
		/* end of line marker */
		*(line + off) = '\0';
		*len = off;
		return line;
	    }
	  if (c == ';')
	    {
		/* end of SQL statement marker */
		*(line + off) = '\0';
		*len = off;
		return line;
	    }
      }
/* EOF reached */
    *len = off;
    *eof = 1;
    return line;
}

static int
do_execute_sql (sqlite3 * handle, char *sql, int row_no)
{
/* executes an SQL statement from the SQL script */
    int ret;
    char *errMsg = NULL;
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ERROR [row=%d]: %s\n", row_no, errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    return 1;
}

static void
do_rollback (sqlite3 * handle)
{
/* performing a ROLLBACK */
    sqlite3_exec (handle, "ROLLBACK", NULL, NULL, NULL);
}

static void
do_execute (char *db_path, char *sql_script, char *charset)
{
/* executing some SQL script */
    void *utf8cvt = NULL;
    char *line = NULL;
    char *statement = NULL;
    int stmt_len = 0;
    char *prev_stmt;
    int prev_len;
    int eof;
    int row_no = 1;
    int stmt = 0;
    int len;
    char *utf8stmt = NULL;
    int cvt_err;
    int ret;
    sqlite3 *handle;
    char *sys_err;
    FILE *in = fopen (sql_script, "rb");
    if (!in)
      {
	  sys_err = strerror (errno);
	  fprintf (stderr, "can't open \"%s\": %s\n", sql_script, sys_err);
	  return;
      }
/* initializing SpatiaLite */
    spatialite_init (0);
/* showing the SQLite version */
    fprintf (stderr, "SQLite version: %s\n", sqlite3_libversion ());
/* showing the SpatiaLite version */
    fprintf (stderr, "SpatiaLite version: %s\n", spatialite_version ());
/* trying to connect the SpatiaLite DB  */
    ret =
	sqlite3_open_v2 (db_path, &handle,
			 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", db_path,
		   sqlite3_errmsg (handle));
	  sqlite3_close (handle);
	  goto end;
      }
/* creating the UTF8 converter */
    utf8cvt = gaiaCreateUTF8Converter (charset);
    if (!utf8cvt)
      {
	  fprintf (stderr,
		   "*** charset ERROR *** cannot convert from '%s' to 'UTF-8'\n",
		   charset);
	  goto abort;
      }
    fprintf (stderr, "executing SQL script: %s\n\n", sql_script);
    while (1)
      {
	  /* reading the SQL script lines */
	  line = read_sql_line (in, &len, &eof);
	  if (len > 0)
	    {
		if (statement == NULL)
		  {
		      statement = line;
		      stmt_len = len;
		  }
		else
		  {
		      /* appending line to SQL statement */
		      prev_stmt = statement;
		      prev_len = stmt_len;
		      stmt_len = prev_len + len;
		      statement = (char *) malloc (stmt_len + 1);
		      memcpy (statement, prev_stmt, prev_len);
		      memcpy (statement + prev_len, line, len);
		      *(statement + stmt_len) = '\0';
		      free (prev_stmt);
		      free (line);
		      line = NULL;
		  }
	    }
	  else
	    {
		free (line);
		line = NULL;
	    }
	  if (statement)
	    {
		if (sqlite3_complete (statement))
		  {
		      /* executing the SQL statement */
		      utf8stmt =
			  gaiaConvertToUTF8 (utf8cvt, statement,
					     stmt_len, &cvt_err);
		      free (statement);
		      statement = NULL;
		      stmt_len = 0;
		      if (cvt_err || !utf8stmt)
			{
			    do_rollback (handle);
			    fprintf (stderr,
				     "SQL Script abnormal termination\nillegal character sequence\n\n");
			    fprintf (stderr,
				     "ROLLBACK was automatically performed\n");
			    goto abort;
			}
		      if (!do_execute_sql (handle, utf8stmt, row_no))
			{
			    do_rollback (handle);
			    fprintf (stderr,
				     "SQL Script abnormal termination\nan error occurred\n\n");
			    fprintf (stderr,
				     "ROLLBACK was automatically performed\n");
			    goto abort;
			}
		      else
			{
			    stmt++;
			    free (utf8stmt);
			    utf8stmt = NULL;
			}
		  }
	    }
	  row_no++;
	  if (eof)
	      break;
      }
    fprintf (stderr, "OK\nread %d lines\nprocessed %d SQL statements\n", row_no,
	     stmt);
  abort:
/* disconnecting the SpatiaLite DB */
    ret = sqlite3_close (handle);
    if (ret != SQLITE_OK)
	fprintf (stderr, "sqlite3_close() error: %s\n",
		 sqlite3_errmsg (handle));
  end:
    fclose (in);
    if (utf8cvt)
	gaiaFreeUTF8Converter (utf8cvt);
}

static void
do_import (char *db_path, char *shp_path, char *table, char *charset, int srid,
	   char *column)
{
/* importing some SHP */
    int ret;
    int rows;
    sqlite3 *handle;
/* initializing SpatiaLite */
    spatialite_init (0);
/* showing the SQLite version */
    fprintf (stderr, "SQLite version: %s\n", sqlite3_libversion ());
/* showing the SpatiaLite version */
    fprintf (stderr, "SpatiaLite version: %s\n", spatialite_version ());
/* trying to connect the SpatiaLite DB  */
    ret =
	sqlite3_open_v2 (db_path, &handle,
			 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", db_path,
		   sqlite3_errmsg (handle));
	  sqlite3_close (handle);
	  return;
      }
    if (load_shapefile
	(handle, shp_path, table, charset, srid, column, 0, &rows))
	fprintf (stderr, "Inserted %d rows into '%s' form '%s.shp'\n", rows,
		 table, shp_path);
    else
	fprintf (stderr, "Some ERROR occurred\n");
/* disconnecting the SpatiaLite DB */
    ret = sqlite3_close (handle);
    if (ret != SQLITE_OK)
	fprintf (stderr, "sqlite3_close() error: %s\n",
		 sqlite3_errmsg (handle));
}

static void
do_export (char *db_path, char *shp_path, char *table, char *column,
	   char *charset, char *type)
{
/* exporting some SHP */
    int ret;
    int rows;
    sqlite3 *handle;
/* initializing SpatiaLite */
    spatialite_init (0);
/* showing the SQLite version */
    fprintf (stderr, "SQLite version: %s\n", sqlite3_libversion ());
/* showing the SpatiaLite version */
    fprintf (stderr, "SpatiaLite version: %s\n", spatialite_version ());
/* trying to connect the SpatiaLite DB  */
    ret = sqlite3_open_v2 (db_path, &handle, SQLITE_OPEN_READONLY, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", db_path,
		   sqlite3_errmsg (handle));
	  sqlite3_close (handle);
	  return;
      }
    if (dump_shapefile
	(handle, table, column, shp_path, charset, type, 0, &rows))
	fprintf (stderr, "Exported %d rows into '%s.shp' form '%s'\n", rows,
		 shp_path, table);
    else
	fprintf (stderr, "Some ERROR occurred\n");
/* disconnecting the SpatiaLite DB */
    ret = sqlite3_close (handle);
    if (ret != SQLITE_OK)
	fprintf (stderr, "sqlite3_close() error: %s\n",
		 sqlite3_errmsg (handle));
}

static void
do_help ()
{
/* printing the argument list */
    fprintf (stderr, "\n\nusage: spatitalite_tool CMD ARGLIST\n");
    fprintf (stderr,
	     "==============================================================\n");
    fprintf (stderr, "CMD has to be one of the followings:\n");
    fprintf (stderr, "------------------------------------\n");
    fprintf (stderr,
	     "-h or --help                      print this help message\n");
    fprintf (stderr,
	     "-i or --import-shp                importing some shapefile\n");
    fprintf (stderr,
	     "-e or --export-shp                exporting some shapefile\n");
    fprintf (stderr,
	     "-x or --execute-sql               executing some sql script\n");
    fprintf (stderr, "\nsupported ARGs are:\n");
    fprintf (stderr, "-------------------\n");
    fprintf (stderr, "-sql or --sql-script pathname     the SQL script path\n");
    fprintf (stderr,
	     "-shp or --shapefile pathname      the shapefile path [NO SUFFIX]\n");
    fprintf (stderr,
	     "-d or --db-path pathname          the SpatiaLite db path\n");
    fprintf (stderr, "-t or --table table_name          the db geotable\n");
    fprintf (stderr, "-g or --geometry-column col_name  the Geometry column\n");
    fprintf (stderr, "-c or --charset charset_name      a charset name\n");
    fprintf (stderr, "-s or --srid SRID                 the SRID\n");
    fprintf (stderr,
	     "--type         [POINT | LINESTRING | POLYGON | MULTIPOINT]\n");
    fprintf (stderr, "\nexamples:\n");
    fprintf (stderr, "---------\n");
    fprintf (stderr, "spatialite_tool -x -sql script.sql -c ASCII\n");
    fprintf (stderr,
	     "spatialite_tool -i -shp abc -d db.sqlite -t tbl -c CP1252 [-s 4326] [-g geom]\n");
    fprintf (stderr,
	     "spatialite_tool -e -shp abc -d db.sqlite -t tbl -g geom -c CP1252 [--type POINT]\n");
}

int
main (int argc, char *argv[])
{
/* the MAIN function simply perform arguments checking */
    int i;
    int next_arg = ARG_NONE;
    char *sql_path = NULL;
    char *shp_path = NULL;
    char *db_path = NULL;
    char *table = NULL;
    char *column = NULL;
    char *charset = NULL;
    char *type = NULL;
    int srid = -1;
    int execute = 0;
    int import = 0;
    int export = 0;
    int error = 0;
    for (i = 1; i < argc; i++)
      {
	  /* parsing the invocation arguments */
	  if (next_arg != ARG_NONE)
	    {
		switch (next_arg)
		  {
		  case ARG_SQL:
		      sql_path = argv[i];
		      break;
		  case ARG_SHP:
		      shp_path = argv[i];
		      break;
		  case ARG_DB:
		      db_path = argv[i];
		      break;
		  case ARG_TABLE:
		      table = argv[i];
		      break;
		  case ARG_COL:
		      column = argv[i];
		      break;
		  case ARG_CS:
		      charset = argv[i];
		      break;
		  case ARG_SRID:
		      srid = atoi (argv[i]);
		      break;
		  case ARG_TYPE:
		      type = argv[i];
		      break;
		  };
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "--help") == 0
	      || strcmp (argv[i], "-h") == 0)
	    {
		do_help ();
		return -1;
	    }
	  if (strcasecmp (argv[i], "--sql-script") == 0)
	    {
		next_arg = ARG_SQL;
		continue;
	    }
	  if (strcmp (argv[i], "-sql") == 0)
	    {
		next_arg = ARG_SQL;
		continue;
	    }
	  if (strcasecmp (argv[i], "--shapefile") == 0)
	    {
		next_arg = ARG_SHP;
		continue;
	    }
	  if (strcmp (argv[i], "-shp") == 0)
	    {
		next_arg = ARG_SHP;
		continue;
	    }
	  if (strcasecmp (argv[i], "--db-path") == 0)
	    {
		next_arg = ARG_DB;
		continue;
	    }
	  if (strcmp (argv[i], "-d") == 0)
	    {
		next_arg = ARG_DB;
		continue;
	    }
	  if (strcasecmp (argv[i], "--table") == 0)
	    {
		next_arg = ARG_TABLE;
		continue;
	    }
	  if (strcmp (argv[i], "-t") == 0)
	    {
		next_arg = ARG_TABLE;
		continue;
	    }
	  if (strcasecmp (argv[i], "--geometry-column") == 0)
	    {
		next_arg = ARG_COL;
		continue;
	    }
	  if (strcmp (argv[i], "-g") == 0)
	    {
		next_arg = ARG_COL;
		continue;
	    }
	  if (strcasecmp (argv[i], "--charset") == 0)
	    {
		next_arg = ARG_CS;
		continue;
	    }
	  if (strcasecmp (argv[i], "-c") == 0)
	    {
		next_arg = ARG_CS;
		continue;
	    }
	  if (strcasecmp (argv[i], "--srid") == 0)
	    {
		next_arg = ARG_SRID;
		continue;
	    }
	  if (strcasecmp (argv[i], "-s") == 0)
	    {
		next_arg = ARG_SRID;
		continue;
	    }
	  if (strcasecmp (argv[i], "--type") == 0)
	    {
		next_arg = ARG_TYPE;
		continue;
	    }
	  if (strcasecmp (argv[i], "--import-shp") == 0 ||
	      strcasecmp (argv[i], "-i") == 0)
	    {
		import = 1;
		continue;
	    }
	  if (strcasecmp (argv[i], "--export-shp") == 0 ||
	      strcasecmp (argv[i], "-e") == 0)
	    {
		export = 1;
		continue;
	    }
	  if (strcasecmp (argv[i], "--execute-sql") == 0 ||
	      strcasecmp (argv[i], "-x") == 0)
	    {
		execute = 1;
		continue;
	    }
	  fprintf (stderr, "unknown argument: %s\n", argv[i]);
	  error = 1;
      }
    if ((execute + import + export) != 1)
      {
	  fprintf (stderr, "undefined CMD\n");
	  error = 1;
      }
    if (error)
      {
	  do_help ();
	  return -1;
      }
/* checking the arguments */
    if (execute)
      {
	  /* execute SQL */
	  if (!db_path)
	    {
		fprintf (stderr,
			 "did you forget setting the --db-path argument ?\n");
		error = 1;
	    }
	  if (!sql_path)
	    {
		fprintf (stderr,
			 "did you forget setting the --sql-script argument ?\n");
		error = 1;
	    }
	  if (!charset)
	    {
		fprintf (stderr,
			 "did you forget setting the --charset argument ?\n");
		error = 1;
	    }
      }
    if (import)
      {
	  /* import SHP */
	  if (!db_path)
	    {
		fprintf (stderr,
			 "did you forget setting the --db-path argument ?\n");
		error = 1;
	    }
	  if (!shp_path)
	    {
		fprintf (stderr,
			 "did you forget setting the --shapefile argument ?\n");
		error = 1;
	    }
	  if (!table)
	    {
		fprintf (stderr,
			 "did you forget setting the --table argument ?\n");
		error = 1;
	    }
	  if (!charset)
	    {
		fprintf (stderr,
			 "did you forget setting the --charset argument ?\n");
		error = 1;
	    }
      }
    if (export)
      {
	  /* export SHP */
	  if (!db_path)
	    {
		fprintf (stderr,
			 "did you forget setting the --db-path argument ?\n");
		error = 1;
	    }
	  if (!shp_path)
	    {
		fprintf (stderr,
			 "did you forget setting the --shapefile argument ?\n");
		error = 1;
	    }
	  if (!table)
	    {
		fprintf (stderr,
			 "did you forget setting the --table argument ?\n");
		error = 1;
	    }
	  if (!column)
	    {
		fprintf (stderr,
			 "did you forget setting the --geometry-column argument ?\n");
		error = 1;
	    }
	  if (!charset)
	    {
		fprintf (stderr,
			 "did you forget setting the --charset argument ?\n");
		error = 1;
	    }
      }
    if (error)
      {
	  do_help ();
	  return -1;
      }
    if (execute)
	do_execute (db_path, sql_path, charset);
    if (import)
	do_import (db_path, shp_path, table, charset, srid, column);
    if (export)
	do_export (db_path, shp_path, table, column, charset, type);
    return 0;
}
