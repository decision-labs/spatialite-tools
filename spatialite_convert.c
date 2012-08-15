/* 
/ spatialite_convert
/
/ a tool converting a DB between different versions of SpatiaLite
/
/ version 1.0, 2012 August 5
/
/ Author: Sandro Furieri a.furieri@lqt.it
/
/ Copyright (C) 2012  Alessandro Furieri
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

#if defined(_WIN32) && !defined(__MINGW32__)
/* MSVC strictly requires this include [off_t] */
#include <sys/types.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "config.h"

#ifdef SPATIALITE_AMALGAMATION
#include <spatialite/sqlite3.h>
#else
#include <sqlite3.h>
#endif

#include <spatialite/gaiageo.h>
#include <spatialite.h>

#ifdef _WIN32
#define strcasecmp	_stricmp
#endif /* not WIN32 */

#define ARG_NONE		0
#define ARG_DB_PATH		1
#define ARG_VERSION		2

struct spatial_index_str
{
/* a struct to implement a linked list of spatial-indexes */
    char ValidRtree;
    char ValidCache;
    char *TableName;
    char *ColumnName;
    struct spatial_index_str *Next;
};

static int
exists_spatialite_history (sqlite3 * handle)
{
/* testing if SPATIALITE_HISTORY exists */
    int event_id = 0;
    int table_name = 0;
    int geometry_column = 0;
    int event = 0;
    int timestamp = 0;
    int ver_sqlite = 0;
    int ver_splite = 0;
    char sql[1024];
    int ret;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;
/* checking the SPATIALITE_HISTORY table */
    strcpy (sql, "PRAGMA table_info(spatialite_history)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "event_id") == 0)
		    event_id = 1;
		if (strcasecmp (name, "table_name") == 0)
		    table_name = 1;
		if (strcasecmp (name, "geometry_column") == 0)
		    geometry_column = 1;
		if (strcasecmp (name, "event") == 0)
		    event = 1;
		if (strcasecmp (name, "timestamp") == 0)
		    timestamp = 1;
		if (strcasecmp (name, "ver_sqlite") == 0)
		    ver_sqlite = 1;
		if (strcasecmp (name, "ver_splite") == 0)
		    ver_splite = 1;
	    }
      }
    sqlite3_free_table (results);
    if (event_id && table_name && geometry_column && event && timestamp
	&& ver_sqlite && ver_splite)
	return 1;
    return 0;
}

static int
update_history (sqlite3 * handle, int in_version, int version)
{
/* updating SPATIALITE_HISTORY (if possible) */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    sqlite3_stmt *stmt = NULL;
    const char *table = "Whole Database";

    if (version == 2 && !exists_spatialite_history (handle))
	return 1;

/* creating the SPATIALITE_HISTORY table ... just in case ... */
    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "spatialite_history (\n");
    strcat (sql, "event_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n");
    strcat (sql, "table_name TEXT NOT NULL,\n");
    strcat (sql, "geometry_column TEXT,\n");
    strcat (sql, "event TEXT NOT NULL,\n");
    strcat (sql, "timestamp TEXT NOT NULL,\n");
    strcat (sql, "ver_sqlite TEXT NOT NULL,\n");
    strcat (sql, "ver_splite TEXT NOT NULL)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE SPATIALITE_HISTORY error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* updating SPATIALITE HISTORY */
    strcpy (sql, "INSERT INTO spatialite_history ");
    strcat (sql, "(event_id, table_name, geometry_column, event, timestamp, ");
    strcat (sql, "ver_sqlite, ver_splite) ");
    strcat (sql,
	    "VALUES (NULL, ?, ?, ?, DateTime('now'), sqlite_version(), spatialite_version())");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE \"spatialite_history\" error: %s\n%s\n", sql,
		   sqlite3_errmsg (handle));
	  ret = 0;
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_null (stmt, 2);
    sprintf (sql,
	     "Converted by \"spatialite_convert\" from Version=%d to Version=%d",
	     in_version, version);
    sqlite3_bind_text (stmt, 3, sql, strlen (sql), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
      {
	  ret = 1;
	  goto stop;
      }
    fprintf (stderr, "UPDATE \"spatialite_history\"  error: %s\n",
	     sqlite3_errmsg (handle));
    ret = 0;

  stop:
    if (stmt)
	sqlite3_finalize (stmt);
    return 1;
}

static void
clean_sql_string (char *buf)
{
/* well-formatting a string to be used as an SQL string-value */
    char tmp[1024];
    char *in = tmp;
    char *out = buf;
    strcpy (tmp, buf);
    while (*in != '\0')
      {
	  if (*in == '\'')
	      *out++ = '\'';
	  *out++ = *in++;
      }
    *out = '\0';
}

static void
double_quoted_sql (char *buf)
{
/* well-formatting a string to be used as an SQL name */
    char tmp[1024];
    char *in = tmp;
    char *out = buf;
    strcpy (tmp, buf);
    *out++ = '"';
    while (*in != '\0')
      {
	  if (*in == '"')
	      *out++ = '"';
	  *out++ = *in++;
      }
    *out++ = '"';
    *out = '\0';
}

static int
update_triggers (sqlite3 * sqlite, const char *table,
		 const char *column, int version)
{
/* updates triggers for some Spatial Column */
    char sql[256];
    char trigger[4096];
    char **results;
    int ret;
    int rows;
    int columns;
    int i;
    char tblname[256];
    char colname[256];
    char col_index[32];
    char col_dims[64];
    int index;
    int cached;
    int dims;
    char *txt_dims;
    int len;
    char *errMsg = NULL;
    char dummy[512];
    char sqltable[1024];
    char sqlcolumn[1024];
    char xname[1024];
    char xcolname[1024];
    char xtable[1024];
    char xindex[1024];
    struct spatial_index_str *first_idx = NULL;
    struct spatial_index_str *last_idx = NULL;
    struct spatial_index_str *curr_idx;
    struct spatial_index_str *next_idx;
    strcpy (sqltable, (char *) table);
    clean_sql_string (sqltable);
    strcpy (sqlcolumn, (char *) column);
    clean_sql_string (sqlcolumn);
    if (version == 4)
      {
	  /* current metadata style */
	  sprintf (sql,
		   "SELECT f_table_name, f_geometry_column, spatial_index_enabled "
		   "FROM geometry_columns WHERE Upper(f_table_name) = Upper('%s') "
		   "AND Upper(f_geometry_column) = Upper('%s')", sqltable,
		   sqlcolumn);
      }
    else
      {
	  /* legacy metadata style */
	  sprintf (sql,
		   "SELECT f_table_name, f_geometry_column, spatial_index_enabled, coord_dimension "
		   "FROM geometry_columns WHERE Upper(f_table_name) = Upper('%s') "
		   "AND Upper(f_geometry_column) = Upper('%s')", sqltable,
		   sqlcolumn);
      }
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "update_triggers: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    for (i = 1; i <= rows; i++)
      {
	  /* preparing the triggers */
	  strcpy (tblname, results[(i * columns)]);
	  strcpy (colname, results[(i * columns) + 1]);
	  strcpy (col_index, results[(i * columns) + 2]);
	  if (version == 3)
	    {
		/* legacy metadata style */
		strcpy (col_dims, results[(i * columns) + 3]);
		dims = GAIA_XY;
		if (strcasecmp (col_dims, "XYZ") == 0)
		    dims = GAIA_XY_Z;
		if (strcasecmp (col_dims, "XYM") == 0)
		    dims = GAIA_XY_M;
		if (strcasecmp (col_dims, "XYZM") == 0)
		    dims = GAIA_XY_Z_M;
		switch (dims)
		  {
		  case GAIA_XY_Z:
		      txt_dims = "XYZ";
		      break;
		  case GAIA_XY_M:
		      txt_dims = "XYM";
		      break;
		  case GAIA_XY_Z_M:
		      txt_dims = "XYZM";
		      break;
		  default:
		      txt_dims = "XY";
		      break;
		  };
	    }
	  if (atoi (col_index) == 1)
	      index = 1;
	  else
	      index = 0;
	  if (atoi (col_index) == 2)
	      cached = 1;
	  else
	      cached = 0;

	  if (version == 4)
	    {
		/* current: creating anyway timestamp Triggers */
		strcpy (sqltable, (char *) tblname);
		clean_sql_string (sqltable);
		strcpy (sqlcolumn, (char *) colname);
		clean_sql_string (sqlcolumn);
		sprintf (xname, "tmi_%s_%s", tblname, colname);
		double_quoted_sql (xname);
		strcpy (xtable, tblname);
		double_quoted_sql (xtable);
		sprintf (trigger,
			 "CREATE TRIGGER IF NOT EXISTS %s AFTER INSERT ON %s ",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		strcat (trigger,
			"UPDATE geometry_columns_time SET last_insert = datetime('now')\n");
		sprintf (dummy, "WHERE Upper(f_table_name) = Upper('%s') AND ",
			 sqltable);
		strcat (trigger, dummy);
		sprintf (dummy, "Upper(f_geometry_column) = Upper('%s');\nEND",
			 sqlcolumn);
		strcat (trigger, dummy);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
		sprintf (xname, "tmu_%s_%s", tblname, colname);
		double_quoted_sql (xname);
		sprintf (trigger,
			 "CREATE TRIGGER IF NOT EXISTS %s AFTER UPDATE ON %s ",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		strcat (trigger,
			"UPDATE geometry_columns_time SET last_update = datetime('now')\n");
		sprintf (dummy, "WHERE Upper(f_table_name) = Upper('%s') AND ",
			 sqltable);
		strcat (trigger, dummy);
		sprintf (dummy, "Upper(f_geometry_column) = Upper('%s');\nEND",
			 sqlcolumn);
		strcat (trigger, dummy);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
		sprintf (xname, "tmd_%s_%s", tblname, colname);
		double_quoted_sql (xname);
		sprintf (trigger,
			 "CREATE TRIGGER IF NOT EXISTS %s AFTER DELETE ON %s ",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		strcat (trigger,
			"UPDATE geometry_columns_time SET last_delete = datetime('now')\n");
		sprintf (dummy, "WHERE Upper(f_table_name) = Upper('%s') AND ",
			 sqltable);
		strcat (trigger, dummy);
		sprintf (dummy, "Upper(f_geometry_column) = Upper('%s');\nEND",
			 sqlcolumn);
		strcat (trigger, dummy);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  else
	    {
		/* legacy: deleting anyway timestamp Triggers */
		sprintf (xname, "tmi_%s_%s", tblname, colname);
		double_quoted_sql (xname);
		sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
		sprintf (xname, "tmu_%s_%s", tblname, colname);
		double_quoted_sql (xname);
		sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
		sprintf (xname, "tmd_%s_%s", tblname, colname);
		double_quoted_sql (xname);
		sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }

	  /* trying to delete old versions [v2.0, v2.2] triggers[if any] */
	  strcpy (sqltable, (char *) tblname);
	  clean_sql_string (sqltable);
	  strcpy (sqlcolumn, (char *) colname);
	  clean_sql_string (sqlcolumn);
	  sprintf (xname, "gti_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  sprintf (xname, "gtu_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  sprintf (xname, "gsi_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  sprintf (xname, "gsu_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* end deletion old versions [v2.0, v2.2] triggers[if any] */

	  /* deleting the old INSERT trigger TYPE [if any] */
	  sprintf (xname, "ggi_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* inserting the new INSERT trigger TYPE */
	  strcpy (xtable, tblname);
	  double_quoted_sql (xtable);
	  strcpy (xcolname, colname);
	  double_quoted_sql (xcolname);
	  sprintf (trigger, "CREATE TRIGGER %s BEFORE INSERT ON %s\n", xname,
		   xtable);
	  strcat (trigger, "FOR EACH ROW BEGIN\n");
	  sprintf (dummy,
		   "SELECT RAISE(ROLLBACK, '%s.%s violates Geometry constraint [geom-type or SRID not allowed]')\n",
		   sqltable, sqlcolumn);
	  strcat (trigger, dummy);
	  if (version == 4)
	    {
		/* current metadata style */
		strcat (trigger,
			"WHERE (SELECT geometry_type FROM geometry_columns\n");
		sprintf (dummy, "WHERE Upper(f_table_name) = Upper('%s') AND ",
			 sqltable);
		strcat (trigger, dummy);
		sprintf (dummy, "Upper(f_geometry_column) = Upper('%s')\n",
			 sqlcolumn);
		strcat (trigger, dummy);
	    }
	  else
	    {
		/* legacy metadata style */
		strcat (trigger, "WHERE (SELECT type FROM geometry_columns\n");
		sprintf (dummy,
			 "WHERE f_table_name = '%s' AND f_geometry_column = '%s'\n",
			 sqltable, sqlcolumn);
		strcat (trigger, dummy);
	    }
	  if (version == 4)
	    {
		/* current metadata style  */
		sprintf (dummy,
			 "AND GeometryConstraints(NEW.%s, geometry_type, srid) = 1) IS NULL;\n",
			 xcolname);
	    }
	  else if (version == 3)
	    {
		/* legacy metadata style V=3 */
		sprintf (dummy,
			 "AND GeometryConstraints(NEW.%s, type, srid, '%s') = 1) IS NULL;\n",
			 xcolname, txt_dims);
	    }
	  else
	    {
		/* legacy metadata style V=2 */
		sprintf (dummy,
			 "AND GeometryConstraints(NEW.%s, type, srid) = 1) IS NULL;\n",
			 xcolname);
	    }
	  strcat (trigger, dummy);
	  strcat (trigger, "END;");
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* deleting the old UPDATE trigger TYPE [if any] */
	  sprintf (xname, "ggu_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* inserting the new UPDATE trigger TYPE */
	  sprintf (trigger, "CREATE TRIGGER %s BEFORE UPDATE ON %s\n", xname,
		   xtable);
	  strcat (trigger, "FOR EACH ROW BEGIN\n");
	  sprintf (dummy,
		   "SELECT RAISE(ROLLBACK, '%s.%s violates Geometry constraint [geom-type or SRID not allowed]')\n",
		   sqltable, sqlcolumn);
	  strcat (trigger, dummy);
	  if (version == 4)
	    {
		/* current metadata style */
		strcat (trigger,
			"WHERE (SELECT geometry_type FROM geometry_columns\n");
		sprintf (dummy, "WHERE Upper(f_table_name) = Upper('%s') AND ",
			 sqltable);
		strcat (trigger, dummy);
		sprintf (dummy, "Upper(f_geometry_column) = Upper('%s')\n",
			 sqlcolumn);
		strcat (trigger, dummy);
	    }
	  else
	    {
		/* legacy metadata style */
		strcat (trigger, "WHERE (SELECT type FROM geometry_columns\n");
		sprintf (dummy,
			 "WHERE f_table_name = '%s' AND f_geometry_column = '%s'\n",
			 sqltable, sqlcolumn);
		strcat (trigger, dummy);
	    }
	  if (version == 4)
	    {
		/* current metadata style */
		sprintf (dummy,
			 "AND GeometryConstraints(NEW.%s, geometry_type, srid) = 1) IS NULL;\n",
			 xcolname);
	    }
	  else if (version == 3)
	    {
		/* legacy metadata style V=3 */
		sprintf (dummy,
			 "AND GeometryConstraints(NEW.%s, type, srid, '%s') = 1) IS NULL;\n",
			 xcolname, txt_dims);
	    }
	  else
	    {
		/* legacy metadata style V=2 */
		sprintf (dummy,
			 "AND GeometryConstraints(NEW.%s, type, srid) = 1) IS NULL;\n",
			 xcolname);
	    }
	  strcat (trigger, dummy);
	  strcat (trigger, "END;");
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* inserting SpatialIndex information into the linked list */
	  curr_idx = malloc (sizeof (struct spatial_index_str));
	  len = strlen (tblname);
	  curr_idx->TableName = malloc (len + 1);
	  strcpy (curr_idx->TableName, tblname);
	  len = strlen ((char *) colname);
	  curr_idx->ColumnName = malloc (len + 1);
	  strcpy (curr_idx->ColumnName, (char *) colname);
	  curr_idx->ValidRtree = (char) index;
	  curr_idx->ValidCache = (char) cached;
	  curr_idx->Next = NULL;
	  if (!first_idx)
	      first_idx = curr_idx;
	  if (last_idx)
	      last_idx->Next = curr_idx;
	  last_idx = curr_idx;
	  /* deleting the old INSERT trigger SPATIAL_INDEX [if any] */
	  sprintf (xname, "gii_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (index)
	    {
		/* inserting the new INSERT trigger SRID */
		sprintf (xindex, "idx_%s_%s", tblname, colname);
		double_quoted_sql (xindex);
		sprintf (trigger, "CREATE TRIGGER %s AFTER INSERT ON %s\n",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		if (version == 2)
		  {
		      /* version 2 */
		      sprintf (dummy,
			       "INSERT INTO %s (pkid, xmin, xmax, ymin, ymax) VALUES (NEW.ROWID,\n",
			       xindex);
		      strcat (trigger, dummy);
		      sprintf (dummy,
			       "MbrMinX(NEW.%s), MbrMaxX(NEW.%s), MbrMinY(NEW.%s), MbrMaxY(NEW.%s));\n",
			       xcolname, xcolname, xcolname, xcolname);
		      strcat (trigger, dummy);
		  }
		else
		  {
		      /* version 3 - 4 */
		      sprintf (dummy, "DELETE FROM %s WHERE pkid=NEW.ROWID;\n",
			       xindex);
		      strcat (trigger, dummy);
		      sprintf (xindex, "idx_%s_%s", tblname, colname);
		      clean_sql_string (xindex);
		      sprintf (dummy,
			       "SELECT RTreeAlign('%s', NEW.ROWID, NEW.%s);",
			       xindex, xcolname);
		      strcat (trigger, dummy);
		  }
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger SPATIAL_INDEX [if any] */
	  sprintf (xname, "giu_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (index)
	    {
		/* inserting the new UPDATE trigger SRID */
		sprintf (xindex, "idx_%s_%s", tblname, colname);
		double_quoted_sql (xindex);
		sprintf (trigger, "CREATE TRIGGER %s AFTER UPDATE ON %s\n",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		if (version == 2)
		  {
		      /* version 2 */
		      sprintf (dummy, "UPDATE %s SET ", xindex);
		      strcat (trigger, dummy);
		      sprintf (dummy,
			       "\"xmin\" = MbrMinX(NEW.%s), \"xmax\" = MbrMaxX(NEW.%s), ",
			       xcolname, xcolname);
		      strcat (trigger, dummy);
		      sprintf (dummy,
			       "\"ymin\" = MbrMinY(NEW.%s), \"ymax\" = MbrMaxY(NEW.%s)\n",
			       xcolname, xcolname);
		      strcat (trigger, dummy);
		      strcat (trigger, "WHERE \"pkid\" = NEW.ROWID;\n");
		  }
		else
		  {
		      /* version 3 - 4 */
		      sprintf (dummy, "DELETE FROM %s WHERE pkid=NEW.ROWID;\n",
			       xindex);
		      strcat (trigger, dummy);
		      sprintf (xindex, "idx_%s_%s", tblname, colname);
		      clean_sql_string (xindex);
		      sprintf (dummy,
			       "SELECT RTreeAlign('%s', NEW.ROWID, NEW.%s);",
			       xindex, xcolname);
		      strcat (trigger, dummy);
		  }
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger SPATIAL_INDEX [if any] */
	  sprintf (xname, "gid_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (index)
	    {
		/* inserting the new DELETE trigger SRID */
		sprintf (xindex, "idx_%s_%s", tblname, colname);
		double_quoted_sql (xindex);
		sprintf (trigger, "CREATE TRIGGER %s AFTER DELETE ON %s\n",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy, "DELETE FROM %s WHERE pkid = OLD.ROWID;\n",
			 xindex);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old INSERT trigger MBR_CACHE [if any] */
	  sprintf (xname, "gci_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (cached)
	    {
		/* inserting the new INSERT trigger SRID */
		sprintf (xindex, "cache_%s_%s", tblname, colname);
		double_quoted_sql (xindex);
		sprintf (trigger, "CREATE TRIGGER %s AFTER INSERT ON %s\n",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy,
			 "INSERT INTO %s (rowid, mbr) VALUES (NEW.ROWID,\nBuildMbrFilter(",
			 xindex);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinX(NEW.%s), ", xcolname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinY(NEW.%s), ", xcolname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxX(NEW.%s), ", xcolname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxY(NEW.%s)));\n", xcolname);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger MBR_CACHE [if any] */
	  sprintf (xname, "gcu_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (cached)
	    {
		/* inserting the new UPDATE trigger SRID */
		sprintf (xindex, "cache_%s_%s", tblname, colname);
		double_quoted_sql (xindex);
		sprintf (trigger, "CREATE TRIGGER %s AFTER UPDATE ON %s\n",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy, "UPDATE %s SET ", xindex);
		strcat (trigger, dummy);
		sprintf (dummy, "mbr = BuildMbrFilter(MbrMinX(NEW.%s), ",
			 xcolname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinY(NEW.%s), ", xcolname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxX(NEW.%s), ", xcolname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxY(NEW.%s))\n", xcolname);
		strcat (trigger, dummy);
		strcat (trigger, "WHERE rowid = NEW.ROWID;\n");
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger MBR_CACHE [if any] */
	  sprintf (xname, "gcd_%s_%s", tblname, colname);
	  double_quoted_sql (xname);
	  sprintf (trigger, "DROP TRIGGER IF EXISTS %s", xname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (cached)
	    {
		/* inserting the new DELETE trigger SRID */
		sprintf (xindex, "cache_%s_%s", tblname, colname);
		double_quoted_sql (xindex);
		sprintf (trigger, "CREATE TRIGGER %s AFTER DELETE ON %s\n",
			 xname, xtable);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy, "DELETE FROM %s WHERE rowid = OLD.ROWID;\n",
			 xindex);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
      }
    sqlite3_free_table (results);
    ret = 1;
    goto index_cleanup;
  error:
    fprintf (stderr, "update_triggers: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    ret = 0;
  index_cleanup:
    curr_idx = first_idx;
    while (curr_idx)
      {
	  next_idx = curr_idx->Next;
	  if (curr_idx->TableName)
	      free (curr_idx->TableName);
	  if (curr_idx->ColumnName)
	      free (curr_idx->ColumnName);
	  free (curr_idx);
	  curr_idx = next_idx;
      }
    return ret;
}

static int
cvt_triggers (sqlite3 * handle, int version)
{
/* updating triggers */
    int ret;
    char sql[1024];
    const char *table;
    const char *geom;
    int i;
    char **results;
    int rows;
    int columns;

/* retrieving any already registered Geometry table */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column FROM geometry_columns");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		table = results[(i * columns) + 0];
		geom = results[(i * columns) + 1];
		if (!update_triggers (handle, table, geom, version))
		  {
		      sqlite3_free_table (results);
		      return 0;
		  }
	    }
      }
    sqlite3_free_table (results);
    return 1;
  unknown:
    return 0;
}

static int
prepare_input (sqlite3 * handle, const char *table)
{
/* dropping the input table */
    int ret;
    char *sql_err = NULL;
    char sql[8192];

/* disabling Foreign Key constraints */
    ret =
	sqlite3_exec (handle, "PRAGMA foreign_keys = 0", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "Disabling Foreign Keys error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* creating the shadow temporary table */
    sprintf (sql,
	     "CREATE TEMPORARY TABLE \"cvt-input tmp-cvt\" AS SELECT * FROM %s",
	     table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TEMPORART TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* dropppint the input table */
    sprintf (sql, "DROP TABLE %s", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* re-enabling Foreign Key constraints */
    ret =
	sqlite3_exec (handle, "PRAGMA foreign_keys = 1", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "Re-enabling Foreign Keys error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    return 1;
}

static int
drop_input_table (sqlite3 * handle)
{
/* dropping the temporary input table */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP TABLE \"cvt-input tmp-cvt\"", NULL, NULL,
		      &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    return 1;
}

static void
drop_virts_geometry_columns (sqlite3 * handle)
{
/* dropping anyway VIRTS_GEOMETRY_COLUMNS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS virts_geometry_columns",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static int
cvt_spatial_index (sqlite3 * handle, int version)
{
/* adjusting VirtualSpatialIndex */
    int ret;
    char *sql_err = NULL;

/* dropping anyway SpatialIndex ... just in case ... */
    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS SpatialIndex", NULL, NULL,
		      &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP VirtualSpatialIndex error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    ret = 1;
    if (version == 3 || version == 4)
      {
	  /* creating SpatialIndex */
	  ret =
	      sqlite3_exec (handle,
			    "CREATE VIRTUAL TABLE SpatialIndex USING VirtualSpatialIndex()",
			    NULL, NULL, &sql_err);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "Create VirtulSpatialIndex error: %s\n",
			 sql_err);
		sqlite3_free (sql_err);
		ret = 0;
	    }
	  else
	      ret = 1;
      }
    if (!ret)
	return 0;
    return 1;
}

static int
create_views_geometry_columns_3 (sqlite3 * handle)
{
/* creating VIEWS_GEOMETRY_COLUMNS Version=3 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];

/* creating the VIEWS_GEOMETRY_COLUMNS table */
    strcpy (sql, "CREATE TABLE views_geometry_columns (\n");
    strcat (sql, "view_name TEXT NOT NULL,\n");
    strcat (sql, "view_geometry TEXT NOT NULL,\n");
    strcat (sql, "view_rowid TEXT NOT NULL,\n");
    strcat (sql, "f_table_name VARCHAR(256) NOT NULL,\n");
    strcat (sql, "f_geometry_column VARCHAR(256) NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_geom_cols_views PRIMARY KEY ");
    strcat (sql, "(view_name, view_geometry),\n");
    strcat (sql, "CONSTRAINT fk_views_geom_cols FOREIGN KEY ");
    strcat (sql, "(f_table_name, f_geometry_column) REFERENCES ");
    strcat (sql, "geometry_columns (f_table_name, f_geometry_column) ");
    strcat (sql, "ON DELETE CASCADE)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE VIEWS_GEOMETRY_COLUMN error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
/* creating an INDEX supporting the GEOMETRY_COLUMNS FK */
    strcpy (sql, "CREATE INDEX IF NOT EXISTS ");
    strcat (sql, "idx_viewsjoin ON views_geometry_columns\n");
    strcat (sql, "(f_table_name, f_geometry_column)");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_VIEWSJOIN error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_views_geometry_columns_4 (sqlite3 * handle)
{
/* creating VIEWS_GEOMETRY_COLUMNS Version=4 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];

/* creating the VIEWS_GEOMETRY_COLUMNS table */
    strcpy (sql, "CREATE TABLE views_geometry_columns (\n");
    strcat (sql, "view_name TEXT NOT NULL,\n");
    strcat (sql, "view_geometry TEXT NOT NULL,\n");
    strcat (sql, "view_rowid TEXT NOT NULL,\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql, "read_only INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_geom_cols_views ");
    strcat (sql, "PRIMARY KEY (view_name, view_geometry),\n");
    strcat (sql, "CONSTRAINT fk_views_geom_cols FOREIGN KEY ");
    strcat (sql, "(f_table_name, f_geometry_column) REFERENCES ");
    strcat (sql, "geometry_columns (f_table_name, f_geometry_column) ");
    strcat (sql, "ON DELETE CASCADE,\n");
    strcat (sql, "CONSTRAINT ck_vw_rdonly CHECK (read_only IN (0,1)))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE VIEWS_GEOMETRY_COLUMN error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
/* creating an INDEX supporting the GEOMETRY_COLUMNS FK */
    strcpy (sql, "CREATE INDEX IF NOT EXISTS ");
    strcat (sql, "idx_viewsjoin ON views_geometry_columns\n");
    strcat (sql, "(f_table_name, f_geometry_column)");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_VIEWSJOIN error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static void
drop_views_geometry_columns (sqlite3 * handle)
{
/* dropping anyway VIEWS_GEOMETRY_COLUMNS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS views_geometry_columns",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static int
copy_views_geometry_columns_3_4 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=3 to Version=4 */
    char sql[8192];
    int ret;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT view_name, view_geometry, view_rowid, f_table_name, f_geometry_column ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, ");
    strcat (sql,
	    "f_table_name, f_geometry_column, read_only) VALUES (?, ?, ?, ?, ?, 1)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 2);
		sqlite3_bind_text (stmt_out, 3, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 3);
		sqlite3_bind_text (stmt_out, 4, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_views_geometry_columns_4_3 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=4 to Version=3 */
    char sql[8192];
    int ret;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT view_name, view_geometry, view_rowid, f_table_name, f_geometry_column ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, ");
    strcat (sql, "f_table_name, f_geometry_column) VALUES (?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 2);
		sqlite3_bind_text (stmt_out, 3, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 3);
		sqlite3_bind_text (stmt_out, 4, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
cvt_views_geometry_columns (sqlite3 * handle, int in_version, int version)
{
/* converting VIEWS_GEOMETRY_COLUMNS */
    int ret;

    if (in_version != 2)
      {
	  /* preparing the input table */
	  if (!prepare_input (handle, "views_geometry_columns"))
	      return 0;
      }
    else
      {
	  /* if version=2 we'll simply create the table and exit */
	  if (version == 3)
	      ret = create_views_geometry_columns_3 (handle);
	  if (version == 4)
	      ret = create_views_geometry_columns_4 (handle);
	  if (!ret)
	      return 0;
      }
    drop_views_geometry_columns (handle);

/* creating the output table */
    ret = 0;
    if (version == 2)
	ret = 1;
    if (version == 3)
	ret = create_views_geometry_columns_3 (handle);
    if (version == 4)
	ret = create_views_geometry_columns_4 (handle);
    if (!ret)
	return 0;

/* copying any row */
    ret = 0;
    if (in_version == 2)
	ret = 1;
    if (in_version == 3)
      {
	  if (version == 2)
	      ret = 1;
	  if (version == 4)
	      ret = copy_views_geometry_columns_3_4 (handle);
      }
    if (in_version == 4)
      {
	  if (version == 2)
	      ret = 1;
	  if (version == 3)
	      ret = copy_views_geometry_columns_4_3 (handle);
      }
    if (!ret)
	return 0;

    if (in_version != 2)
      {
	  /* dropping the temporary input table */
	  if (!drop_input_table (handle))
	      return 0;
      }

    return 1;
}

static void
drop_vector_layers_auth (sqlite3 * handle)
{
/* dropping anyway VECTOR_LAYERS_AUTH ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP VIEW IF EXISTS vector_layers_auth",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP VIEW error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_vector_layers_field_infos (sqlite3 * handle)
{
/* dropping anyway VECTOR_LAYERS_FIELD_INFOS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP VIEW IF EXISTS vector_layers_field_infos",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP VIEW error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_vector_layers_statistics (sqlite3 * handle)
{
/* dropping anyway VECTOR_LAYERS_STATISTICS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP VIEW IF EXISTS vector_layers_statistics",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP VIEW error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_views_geometry_columns_auth (sqlite3 * handle)
{
/* dropping anyway VIEWS_GEOMETRY_COLUMNS_AUTH ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS views_geometry_columns_auth", NULL,
		      NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_views_geometry_columns_field_infos (sqlite3 * handle)
{
/* dropping anyway VIEWS_GEOMETRY_COLUMNS_FIELD_INFOS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS views_geometry_columns_field_infos",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_views_geometry_columns_statistics (sqlite3 * handle)
{
/* dropping anyway VIEWS_GEOMETRY_COLUMNS_STATISTICS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS views_geometry_columns_statistics",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_virts_geometry_columns_auth (sqlite3 * handle)
{
/* dropping anyway VIRTS_GEOMETRY_COLUMNS_AUTH ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS virts_geometry_columns_auth", NULL,
		      NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_virts_geometry_columns_field_infos (sqlite3 * handle)
{
/* dropping anyway VIRTS_GEOMETRY_COLUMNS_FIELD_INFOS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS virts_geometry_columns_field_infos",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_virts_geometry_columns_statistics (sqlite3 * handle)
{
/* dropping anyway VIRTS_GEOMETRY_COLUMNS_STATISTICS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS virts_geometry_columns_statistics",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_geometry_columns_auth (sqlite3 * handle)
{
/* dropping anyway GEOMETRY_COLUMNS_AUTH ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS geometry_columns_auth",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_geometry_columns_field_infos (sqlite3 * handle)
{
/* dropping anyway GEOMETRY_COLUMNS_FIELD_INFOS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS geometry_columns_field_infos", NULL,
		      NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_geometry_columns_statistics (sqlite3 * handle)
{
/* dropping anyway GEOMETRY_COLUMNS_STATISTICS ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle,
		      "DROP TABLE IF EXISTS geometry_columns_statistics", NULL,
		      NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_layer_statistics (sqlite3 * handle)
{
/* dropping anyway LAYER_STATISTICS and friends ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS layer_statistics",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS views_layer_statistics",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS virts_layer_statistics",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
drop_geometry_columns_time (sqlite3 * handle)
{
/* dropping anyway GEOMETRY_COLUMNS_TIME ... just in case ... */
    int ret;
    char *sql_err = NULL;

    ret =
	sqlite3_exec (handle, "DROP TABLE IF EXISTS geometry_columns_time",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_times (sqlite3 * handle)
{
/* feeding GEOMETRY_COLUMNS_TIME ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "geometry_columns_time (\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql,
	    "last_insert TIMESTAMP NOT NULL DEFAULT '0000-01-01 00:00:00',\n");
    strcat (sql,
	    "last_update TIMESTAMP NOT NULL DEFAULT '0000-01-01 00:00:00',\n");
    strcat (sql,
	    "last_delete TIMESTAMP NOT NULL DEFAULT '0000-01-01 00:00:00',\n");
    strcat (sql, "CONSTRAINT pk_gc_time PRIMARY KEY ");
    strcat (sql, "(f_table_name, f_geometry_column),\n");
    strcat (sql, "CONSTRAINT fk_gc_time FOREIGN KEY ");
    strcat (sql, "(f_table_name, f_geometry_column) ");
    strcat (sql, "REFERENCES geometry_columns ");
    strcat (sql, "(f_table_name, f_geometry_column) ");
    strcat (sql, "ON DELETE CASCADE)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO geometry_columns_time ");
    strcat (sql, "(f_table_name, f_geometry_column) ");
    strcat (sql, "SELECT f_table_name, f_geometry_column ");
    strcat (sql, "FROM geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_auths (sqlite3 * handle)
{
/* creating and feeding GEOMETRY_COLUMNS_AUTH ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "geometry_columns_auth (\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql, "read_only INTEGER NOT NULL,\n");
    strcat (sql, "hidden INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_gc_auth PRIMARY KEY ");
    strcat (sql, "(f_table_name, f_geometry_column),\n");
    strcat (sql, "CONSTRAINT fk_gc_auth FOREIGN KEY ");
    strcat (sql, "(f_table_name, f_geometry_column) ");
    strcat (sql, "REFERENCES geometry_columns ");
    strcat (sql, "(f_table_name, f_geometry_column) ");
    strcat (sql, "ON DELETE CASCADE,\n");
    strcat (sql, "CONSTRAINT ck_gc_ronly CHECK (read_only IN ");
    strcat (sql, "(0,1)),\n");
    strcat (sql, "CONSTRAINT ck_gc_hidden CHECK (hidden IN ");
    strcat (sql, "(0,1)))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO geometry_columns_auth ");
    strcat (sql, "(f_table_name, f_geometry_column, read_only, hidden) ");
    strcat (sql, "SELECT f_table_name, f_geometry_column, 0, 0 ");
    strcat (sql, "FROM geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_views_auths (sqlite3 * handle)
{
/* feeding VIEWS_GEOMETRY_COLUMNS_AUTH ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "views_geometry_columns_auth (\n");
    strcat (sql, "view_name TEXT NOT NULL,\n");
    strcat (sql, "view_geometry TEXT NOT NULL,\n");
    strcat (sql, "hidden INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_vwgc_auth PRIMARY KEY ");
    strcat (sql, "(view_name, view_geometry),\n");
    strcat (sql, "CONSTRAINT fk_vwgc_auth FOREIGN KEY ");
    strcat (sql, "(view_name, view_geometry) ");
    strcat (sql, "REFERENCES views_geometry_columns ");
    strcat (sql, "(view_name, view_geometry) ");
    strcat (sql, "ON DELETE CASCADE,\n");
    strcat (sql, "CONSTRAINT ck_vwgc_hidden CHECK (hidden IN ");
    strcat (sql, "(0,1)))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO views_geometry_columns_auth ");
    strcat (sql, "(view_name, view_geometry, hidden) ");
    strcat (sql, "SELECT view_name, view_geometry, 0 ");
    strcat (sql, "FROM views_geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_virts_auths (sqlite3 * handle)
{
/* feeding VIRTS_GEOMETRY_COLUMNS_AUTH ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "virts_geometry_columns_auth (\n");
    strcat (sql, "virt_name TEXT NOT NULL,\n");
    strcat (sql, "virt_geometry TEXT NOT NULL,\n");
    strcat (sql, "hidden INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_vrtgc_auth PRIMARY KEY ");
    strcat (sql, "(virt_name, virt_geometry),\n");
    strcat (sql, "CONSTRAINT fk_vrtgc_auth FOREIGN KEY ");
    strcat (sql, "(virt_name, virt_geometry) ");
    strcat (sql, "REFERENCES virts_geometry_columns ");
    strcat (sql, "(virt_name, virt_geometry) ");
    strcat (sql, "ON DELETE CASCADE,\n");
    strcat (sql, "CONSTRAINT ck_vrtgc_hidden CHECK (hidden IN ");
    strcat (sql, "(0,1)))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO virts_geometry_columns_auth ");
    strcat (sql, "(virt_name, virt_geometry, hidden) ");
    strcat (sql, "SELECT virt_name, virt_geometry, 0 ");
    strcat (sql, "FROM virts_geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_statistics (sqlite3 * handle)
{
/* feeding GEOMETRY_COLUMNS_STATISTICS ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "geometry_columns_statistics (\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql, "last_verified TIMESTAMP,\n");
    strcat (sql, "row_count INTEGER,\n");
    strcat (sql, "extent_min_x DOUBLE,\n");
    strcat (sql, "extent_min_y DOUBLE,\n");
    strcat (sql, "extent_max_x DOUBLE,\n");
    strcat (sql, "extent_max_y DOUBLE,\n");
    strcat (sql, "CONSTRAINT pk_gc_statistics PRIMARY KEY ");
    strcat (sql, "(f_table_name, f_geometry_column),\n");
    strcat (sql, "CONSTRAINT fk_gc_statistics FOREIGN KEY ");
    strcat (sql, "(f_table_name, f_geometry_column) REFERENCES ");
    strcat (sql, "geometry_columns (f_table_name, f_geometry_column) ");
    strcat (sql, "ON DELETE CASCADE)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "geometry_columns_field_infos (\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql, "ordinal INTEGER NOT NULL,\n");
    strcat (sql, "column_name TEXT NOT NULL,\n");
    strcat (sql, "null_values INTEGER NOT NULL,\n");
    strcat (sql, "integer_values INTEGER NOT NULL,\n");
    strcat (sql, "double_values INTEGER NOT NULL,\n");
    strcat (sql, "text_values INTEGER NOT NULL,\n");
    strcat (sql, "blob_values INTEGER NOT NULL,\n");
    strcat (sql, "max_size INTEGER,\n");
    strcat (sql, "CONSTRAINT pk_gcfld_infos PRIMARY KEY ");
    strcat (sql, "(f_table_name, f_geometry_column, ordinal, column_name),\n");
    strcat (sql, "CONSTRAINT fk_gcfld_infos FOREIGN KEY ");
    strcat (sql, "(f_table_name, f_geometry_column) REFERENCES ");
    strcat (sql, "geometry_columns (f_table_name, f_geometry_column) ");
    strcat (sql, "ON DELETE CASCADE)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO geometry_columns_statistics ");
    strcat (sql, "(f_table_name, f_geometry_column) ");
    strcat (sql, "SELECT f_table_name, f_geometry_column ");
    strcat (sql, "FROM geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_views_statistics (sqlite3 * handle)
{
/* feeding VIEWS_GEOMETRY_COLUMNS_STATISTICS ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "views_geometry_columns_statistics (\n");
    strcat (sql, "view_name TEXT NOT NULL,\n");
    strcat (sql, "view_geometry TEXT NOT NULL,\n");
    strcat (sql, "last_verified TIMESTAMP,\n");
    strcat (sql, "row_count INTEGER,\n");
    strcat (sql, "extent_min_x DOUBLE,\n");
    strcat (sql, "extent_min_y DOUBLE,\n");
    strcat (sql, "extent_max_x DOUBLE,\n");
    strcat (sql, "extent_max_y DOUBLE,\n");
    strcat (sql, "CONSTRAINT pk_vwgc_statistics PRIMARY KEY ");
    strcat (sql, "(view_name, view_geometry),\n");
    strcat (sql, "CONSTRAINT fk_vwgc_statistics FOREIGN KEY ");
    strcat (sql, "(view_name, view_geometry) REFERENCES ");
    strcat (sql, "views_geometry_columns (view_name, view_geometry) ");
    strcat (sql, "ON DELETE CASCADE)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "views_geometry_columns_field_infos (\n");
    strcat (sql, "view_name TEXT NOT NULL,\n");
    strcat (sql, "view_geometry TEXT NOT NULL,\n");
    strcat (sql, "ordinal INTEGER NOT NULL,\n");
    strcat (sql, "column_name TEXT NOT NULL,\n");
    strcat (sql, "null_values INTEGER NOT NULL,\n");
    strcat (sql, "integer_values INTEGER NOT NULL,\n");
    strcat (sql, "double_values INTEGER NOT NULL,\n");
    strcat (sql, "text_values INTEGER NOT NULL,\n");
    strcat (sql, "blob_values INTEGER NOT NULL,\n");
    strcat (sql, "max_size INTEGER,\n");
    strcat (sql, "CONSTRAINT pk_vwgcfld_infos PRIMARY KEY ");
    strcat (sql, "(view_name, view_geometry, ordinal, column_name),\n");
    strcat (sql, "CONSTRAINT fk_vwgcfld_infos FOREIGN KEY ");
    strcat (sql, "(view_name, view_geometry) REFERENCES ");
    strcat (sql, "views_geometry_columns (view_name, view_geometry) ");
    strcat (sql, "ON DELETE CASCADE)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO views_geometry_columns_statistics ");
    strcat (sql, "(view_name, view_geometry) ");
    strcat (sql, "SELECT view_name, view_geometry ");
    strcat (sql, "FROM views_geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
feed_virts_statistics (sqlite3 * handle)
{
/* feeding VIRTS_GEOMETRY_COLUMNS_STATISTICS ... just in case ... */
    int ret;
    char sql[8192];
    char *sql_err = NULL;

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "virts_geometry_columns_statistics (\n");
    strcat (sql, "virt_name TEXT NOT NULL,\n");
    strcat (sql, "virt_geometry TEXT NOT NULL,\n");
    strcat (sql, "last_verified TIMESTAMP,\n");
    strcat (sql, "row_count INTEGER,\n");
    strcat (sql, "extent_min_x DOUBLE,\n");
    strcat (sql, "extent_min_y DOUBLE,\n");
    strcat (sql, "extent_max_x DOUBLE,\n");
    strcat (sql, "extent_max_y DOUBLE,\n");
    strcat (sql, "CONSTRAINT pk_vrtgc_statistics PRIMARY KEY ");
    strcat (sql, "(virt_name, virt_geometry),\n");
    strcat (sql, "CONSTRAINT fk_vrtgc_statistics FOREIGN KEY ");
    strcat (sql, "(virt_name, virt_geometry) REFERENCES ");
    strcat (sql, "virts_geometry_columns (virt_name, virt_geometry) ");
    strcat (sql, "ON DELETE CASCADE)");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "virts_geometry_columns_field_infos (\n");
    strcat (sql, "virt_name TEXT NOT NULL,\n");
    strcat (sql, "virt_geometry TEXT NOT NULL,\n");
    strcat (sql, "ordinal INTEGER NOT NULL,\n");
    strcat (sql, "column_name TEXT NOT NULL,\n");
    strcat (sql, "null_values INTEGER NOT NULL,\n");
    strcat (sql, "integer_values INTEGER NOT NULL,\n");
    strcat (sql, "double_values INTEGER NOT NULL,\n");
    strcat (sql, "text_values INTEGER NOT NULL,\n");
    strcat (sql, "blob_values INTEGER NOT NULL,\n");
    strcat (sql, "max_size INTEGER,\n");
    strcat (sql, "CONSTRAINT pk_vrtgcfld_infos PRIMARY KEY ");
    strcat (sql, "(virt_name, virt_geometry, ordinal, column_name),\n");
    strcat (sql, "CONSTRAINT fk_vrtgcfld_infos FOREIGN KEY ");
    strcat (sql, "(virt_name, virt_geometry) REFERENCES ");
    strcat (sql, "virts_geometry_columns (virt_name, virt_geometry) ");
    strcat (sql, "ON DELETE CASCADE)");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    strcpy (sql, "INSERT OR IGNORE INTO virts_geometry_columns_statistics ");
    strcat (sql, "(virt_name, virt_geometry) ");
    strcat (sql, "SELECT virt_name, virt_geometry ");
    strcat (sql, "FROM virts_geometry_columns");

    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO SELECT error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static int
cvt_extra_stuff (sqlite3 * handle, int version)
{
/* converting extra-sfuff Tables and Views */
    int ret;

    if (version != 3)
	drop_layer_statistics (handle);

    if (version == 4)
      {
	  /* creating any other Version=4 specific Table or View */
	  feed_times (handle);
	  feed_auths (handle);
	  feed_views_auths (handle);
	  feed_virts_auths (handle);
	  feed_statistics (handle);
	  feed_views_statistics (handle);
	  feed_virts_statistics (handle);
      }
    else
      {
	  /* dropping any other Version=4 specific Table or View */
	  if (version == 2)
	    {
		drop_geometry_columns_auth (handle);
		drop_views_geometry_columns (handle);
		drop_virts_geometry_columns (handle);
	    }
	  drop_geometry_columns_field_infos (handle);
	  drop_geometry_columns_statistics (handle);
	  drop_geometry_columns_time (handle);
	  drop_vector_layers_auth (handle);
	  drop_vector_layers_field_infos (handle);
	  drop_vector_layers_statistics (handle);
	  drop_views_geometry_columns_auth (handle);
	  drop_views_geometry_columns_field_infos (handle);
	  drop_views_geometry_columns_statistics (handle);
	  drop_virts_geometry_columns_auth (handle);
	  drop_virts_geometry_columns_field_infos (handle);
	  drop_virts_geometry_columns_statistics (handle);
      }
    return 1;
}

static int
create_views_2 (sqlite3 * handle)
{
/* creating MetaData Views Version=2 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];

/* creating the GEOM_COLS_REF_SYS view */
    strcpy (sql, "CREATE VIEW geom_cols_ref_sys AS\n");
    strcat (sql, "SELECT f_table_name, f_geometry_column, type,\n");
    strcat (sql, "coord_dimension, spatial_ref_sys.srid AS srid,\n");
    strcat (sql, "auth_name, auth_srid, ref_sys_name, proj4text\n");
    strcat (sql, "FROM geometry_columns, spatial_ref_sys\n");
    strcat (sql, "WHERE geometry_columns.srid = spatial_ref_sys.srid");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW GEOM_COLS_REF_SYS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_views_3 (sqlite3 * handle)
{
/* creating MetaData Views Version=3 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];

/* creating the GEOM_COLS_REF_SYS view */
    strcpy (sql, "CREATE VIEW geom_cols_ref_sys AS\n");
    strcat (sql, "SELECT f_table_name, f_geometry_column, type,\n");
    strcat (sql, "coord_dimension, spatial_ref_sys.srid AS srid,\n");
    strcat (sql, "auth_name, auth_srid, ref_sys_name, proj4text, srs_wkt\n");
    strcat (sql, "FROM geometry_columns, spatial_ref_sys\n");
    strcat (sql, "WHERE geometry_columns.srid = spatial_ref_sys.srid");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW GEOM_COLS_REF_SYS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_views_4 (sqlite3 * handle)
{
/* creating MetaData Views Version=4 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];

/* creating the GEOM_COLS_REF_SYS view */
    strcpy (sql, "CREATE VIEW geom_cols_ref_sys AS\n");
    strcat (sql, "SELECT f_table_name, f_geometry_column, geometry_type,\n");
    strcat (sql, "coord_dimension, spatial_ref_sys.srid AS srid,\n");
    strcat (sql, "auth_name, auth_srid, ref_sys_name, proj4text, srtext\n");
    strcat (sql, "FROM geometry_columns, spatial_ref_sys\n");
    strcat (sql, "WHERE geometry_columns.srid = spatial_ref_sys.srid");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW GEOM_COLS_REF_SYS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* creating the VECTOR LAYERS view */
    strcpy (sql, "CREATE VIEW vector_layers AS\n");
    strcat (sql, "SELECT 'SpatialTable' AS layer_type, ");
    strcat (sql, "f_table_name AS table_name, ");
    strcat (sql, "f_geometry_column AS geometry_column, ");
    strcat (sql, "geometry_type AS geometry_type, ");
    strcat (sql, "coord_dimension AS coord_dimension, ");
    strcat (sql, "srid AS srid, ");
    strcat (sql, "spatial_index_enabled AS spatial_index_enabled\n");
    strcat (sql, "FROM geometry_columns\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'SpatialView' AS layer_type, ");
    strcat (sql, "a.view_name AS table_name, ");
    strcat (sql, "a.view_geometry AS geometry_column, ");
    strcat (sql, "b.geometry_type AS geometry_type, ");
    strcat (sql, "b.coord_dimension AS coord_dimension, ");
    strcat (sql, "b.srid AS srid, ");
    strcat (sql, "b.spatial_index_enabled AS spatial_index_enabled\n");
    strcat (sql, "FROM views_geometry_columns AS a\n");
    strcat (sql, "LEFT JOIN geometry_columns AS b ON (");
    strcat (sql, "Upper(a.f_table_name) = Upper(b.f_table_name) AND ");
    strcat (sql, "Upper(a.f_geometry_column) = Upper(b.f_geometry_column))\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'VirtualShape' AS layer_type, ");
    strcat (sql, "virt_name AS table_name, ");
    strcat (sql, "virt_geometry AS geometry_column, ");
    strcat (sql, "geometry_type AS geometry_type, ");
    strcat (sql, "coord_dimension AS coord_dimension, ");
    strcat (sql, "srid AS srid, ");
    strcat (sql, "0 AS spatial_index_enabled\n");
    strcat (sql, "FROM virts_geometry_columns");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW VECTOR_LAYERS error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* creating the VECTOR_LAYERS_AUTH view */
    strcpy (sql, "CREATE VIEW vector_layers_auth AS\n");
    strcat (sql, "SELECT 'SpatialTable' AS layer_type, ");
    strcat (sql, "f_table_name AS table_name, ");
    strcat (sql, "f_geometry_column AS geometry_column, ");
    strcat (sql, "read_only AS read_only, ");
    strcat (sql, "hidden AS hidden\n");
    strcat (sql, "FROM geometry_columns_auth\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'SpatialView' AS layer_type, ");
    strcat (sql, "a.view_name AS table_name, ");
    strcat (sql, "a.view_geometry AS geometry_column, ");
    strcat (sql, "b.read_only AS read_only, ");
    strcat (sql, "a.hidden AS hidden\n");
    strcat (sql, "FROM views_geometry_columns_auth AS a\n");
    strcat (sql, "JOIN views_geometry_columns AS b ON (");
    strcat (sql, "Upper(a.view_name) = Upper(b.view_name) AND ");
    strcat (sql, "Upper(a.view_geometry) = Upper(b.view_geometry))\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'VirtualShape' AS layer_type, ");
    strcat (sql, "virt_name AS table_name, ");
    strcat (sql, "virt_geometry AS geometry_column, ");
    strcat (sql, "1 AS read_only, ");
    strcat (sql, "hidden AS hidden\n");
    strcat (sql, "FROM virts_geometry_columns_auth");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW VECTOR_LAYERS_AUTH error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* creating the VECTOR_LAYERS_STATISTICS view */
    strcpy (sql, "CREATE VIEW vector_layers_statistics AS\n");
    strcat (sql, "SELECT 'SpatialTable' AS layer_type, ");
    strcat (sql, "f_table_name AS table_name, ");
    strcat (sql, "f_geometry_column AS geometry_column, ");
    strcat (sql, "last_verified AS last_verified, ");
    strcat (sql, "row_count AS row_count, ");
    strcat (sql, "extent_min_x AS extent_min_x, ");
    strcat (sql, "extent_min_y AS extent_min_y, ");
    strcat (sql, "extent_max_x AS extent_max_x, ");
    strcat (sql, "extent_max_y AS extent_max_y\n");
    strcat (sql, "FROM geometry_columns_statistics\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'SpatialView' AS layer_type, ");
    strcat (sql, "view_name AS table_name, ");
    strcat (sql, "view_geometry AS geometry_column, ");
    strcat (sql, "last_verified AS last_verified, ");
    strcat (sql, "row_count AS row_count, ");
    strcat (sql, "extent_min_x AS extent_min_x, ");
    strcat (sql, "extent_min_y AS extent_min_y, ");
    strcat (sql, "extent_max_x AS extent_max_x, ");
    strcat (sql, "extent_max_y AS extent_max_y\n");
    strcat (sql, "FROM views_geometry_columns_statistics\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'VirtualShape' AS layer_type, ");
    strcat (sql, "virt_name AS table_name, ");
    strcat (sql, "virt_geometry AS geometry_column, ");
    strcat (sql, "last_verified AS last_verified, ");
    strcat (sql, "row_count AS row_count, ");
    strcat (sql, "extent_min_x AS extent_min_x, ");
    strcat (sql, "extent_min_y AS extent_min_y, ");
    strcat (sql, "extent_max_x AS extent_max_x, ");
    strcat (sql, "extent_max_y AS extent_max_y\n");
    strcat (sql, "FROM virts_geometry_columns_statistics");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW VECTOR_LAYERS_STATISTICS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

/* creating the VECTOR_LAYERS_FIELD_INFOS view */
    strcpy (sql, "CREATE VIEW vector_layers_field_infos AS\n");
    strcat (sql, "SELECT 'SpatialTable' AS layer_type, ");
    strcat (sql, "f_table_name AS table_name, ");
    strcat (sql, "f_geometry_column AS geometry_column, ");
    strcat (sql, "ordinal AS ordinal, ");
    strcat (sql, "column_name AS column_name, ");
    strcat (sql, "null_values AS null_values, ");
    strcat (sql, "integer_values AS integer_values, ");
    strcat (sql, "double_values AS double_values, ");
    strcat (sql, "text_values AS text_values, ");
    strcat (sql, "blob_values AS blob_values, ");
    strcat (sql, "max_size AS max_size\n");
    strcat (sql, "FROM geometry_columns_field_infos\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'SpatialView' AS layer_type, ");
    strcat (sql, "view_name AS table_name, ");
    strcat (sql, "view_geometry AS geometry_column, ");
    strcat (sql, "ordinal AS ordinal, ");
    strcat (sql, "column_name AS column_name, ");
    strcat (sql, "null_values AS null_values, ");
    strcat (sql, "integer_values AS integer_values, ");
    strcat (sql, "double_values AS double_values, ");
    strcat (sql, "text_values AS text_values, ");
    strcat (sql, "blob_values AS blob_values, ");
    strcat (sql, "max_size AS max_size\n");
    strcat (sql, "FROM views_geometry_columns_field_infos\n");
    strcat (sql, "UNION\n");
    strcat (sql, "SELECT 'VirtualShape' AS layer_type, ");
    strcat (sql, "virt_name AS table_name, ");
    strcat (sql, "virt_geometry AS geometry_column, ");
    strcat (sql, "ordinal AS ordinal, ");
    strcat (sql, "column_name AS column_name, ");
    strcat (sql, "null_values AS null_values, ");
    strcat (sql, "integer_values AS integer_values, ");
    strcat (sql, "double_values AS double_values, ");
    strcat (sql, "text_values AS text_values, ");
    strcat (sql, "blob_values AS blob_values, ");
    strcat (sql, "max_size AS max_size\n");
    strcat (sql, "FROM virts_geometry_columns_field_infos");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE VIEW VECTOR_LAYERS_FIELD_INFOS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
cvt_views (sqlite3 * handle, int version)
{
/* adjusting the MetaData views */
    int ret;
    char *sql_err = NULL;

/* dropping anyway GEOM_COLS_REF_SYS ... just in case ... */
    ret =
	sqlite3_exec (handle, "DROP VIEW IF EXISTS geom_cols_ref_sys", NULL,
		      NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP VIEW error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

/* dropping anyway VECTOR_LAYERS ... just in case ... */
    ret =
	sqlite3_exec (handle, "DROP VIEW IF EXISTS vector_layers", NULL, NULL,
		      &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP VIEW error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

    ret = 1;
    if (version == 2)
	ret = create_views_2 (handle);
    if (version == 3)
	ret = create_views_3 (handle);
    if (version == 4)
	ret = create_views_4 (handle);
    if (!ret)
	return 0;
    return 1;
}

static int
register_virtual (sqlite3 * sqlite, const char *table, int version)
{
/* attempting to register a VirtualGeometry */
    char sql[8192];
    char sql2[8192];
    char xtable[4096];
    char gtype[64];
    int xtype = -1;
    int srid;
    char **results;
    int ret;
    int rows;
    int columns;
    int i;
    char *errMsg = NULL;
    int xdims = -1;

/* determining Geometry Type and dims */
    strcpy (xtable, table);
    double_quoted_sql (xtable);
    sprintf (sql,
	     "SELECT DISTINCT ST_GeometryType(Geometry), ST_Srid(Geometry) FROM %s",
	     xtable);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "register_virtual() error: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    for (i = 1; i <= rows; i++)
      {
	  strcpy (gtype, results[(i * columns)]);
	  srid = atoi (results[(i * columns) + 1]);
      }
    sqlite3_free_table (results);

/* normalized Geometry type */
    if (strcmp (gtype, "POINT") == 0)
	xtype = 1;
    if (strcmp (gtype, "POINT Z") == 0)
	xtype = 1001;
    if (strcmp (gtype, "POINT M") == 0)
	xtype = 2001;
    if (strcmp (gtype, "POINT ZM") == 0)
	xtype = 3001;
    if (strcmp (gtype, "LINESTRING") == 0)
	xtype = 2;
    if (strcmp (gtype, "LINESTRING Z") == 0)
	xtype = 1002;
    if (strcmp (gtype, "LINESTRING M") == 0)
	xtype = 2002;
    if (strcmp (gtype, "LINESTRING ZM") == 0)
	xtype = 3002;
    if (strcmp (gtype, "POLYGON") == 0)
	xtype = 3;
    if (strcmp (gtype, "POLYGON Z") == 0)
	xtype = 1003;
    if (strcmp (gtype, "POLYGON M") == 0)
	xtype = 2003;
    if (strcmp (gtype, "POLYGON ZM") == 0)
	xtype = 3003;
    if (strcmp (gtype, "MULTIPOINT") == 0)
	xtype = 4;
    if (strcmp (gtype, "MULTIPOINT Z") == 0)
	xtype = 1004;
    if (strcmp (gtype, "MULTIPOINT M") == 0)
	xtype = 2004;
    if (strcmp (gtype, "MULTIPOINT ZM") == 0)
	xtype = 3004;
    if (strcmp (gtype, "MULTILINESTRING") == 0)
	xtype = 5;
    if (strcmp (gtype, "MULTILINESTRING Z") == 0)
	xtype = 1005;
    if (strcmp (gtype, "MULTILINESTRING M") == 0)
	xtype = 2005;
    if (strcmp (gtype, "MULTILINESTRING ZM") == 0)
	xtype = 3005;
    if (strcmp (gtype, "MULTIPOLYGON") == 0)
	xtype = 6;
    if (strcmp (gtype, "MULTIPOLYGON Z") == 0)
	xtype = 1006;
    if (strcmp (gtype, "MULTIPOLYGON M") == 0)
	xtype = 2006;
    if (strcmp (gtype, "MULTIPOLYGON ZM") == 0)
	xtype = 3006;

/* updating metadata tables */
    xdims = -1;
    switch (xtype)
      {
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
	  xdims = 2;
	  break;
      case 1001:
      case 1002:
      case 1003:
      case 1004:
      case 1005:
      case 1006:
      case 2001:
      case 2002:
      case 2003:
      case 2004:
      case 2005:
      case 2006:
	  xdims = 3;
	  break;
      case 3001:
      case 3002:
      case 3003:
      case 3004:
      case 3005:
      case 3006:
	  xdims = 4;
	  break;
      };
    strcpy (xtable, table);
    clean_sql_string (xtable);
    if (version == 4)
      {
	  /* has the "geometry_type" column */
	  strcpy (sql, "INSERT INTO virts_geometry_columns ");
	  strcat (sql,
		  "(virt_name, virt_geometry, geometry_type, coord_dimension, srid) ");
	  sprintf (sql2, "VALUES ('%s', 'Geometry', %d, %d, %d)", xtable, xtype,
		   xdims, srid);
	  strcat (sql, sql2);
      }
    else
      {
	  /* has the "type" column */
	  const char *xgtype = "UNKNOWN";
	  switch (xtype)
	    {
	    case 1:
	    case 1001:
	    case 2001:
	    case 3001:
		xgtype = "POINT";
		break;
	    case 2:
	    case 1002:
	    case 2002:
	    case 3002:
		xgtype = "LINESTRING";
		break;
	    case 3:
	    case 1003:
	    case 2003:
	    case 3003:
		xgtype = "POLYGON";
		break;
	    case 4:
	    case 1004:
	    case 2004:
	    case 3004:
		xgtype = "MULTIPOINT";
		break;
	    case 5:
	    case 1005:
	    case 2005:
	    case 3005:
		xgtype = "MULTILINESTRING";
		break;
	    case 6:
	    case 1006:
	    case 2006:
	    case 3006:
		xgtype = "MULTIPOLYGON";
		break;
	    };
	  strcpy (sql, "INSERT INTO virts_geometry_columns ");
	  strcat (sql, "(virt_name, virt_geometry, type, srid) ");
	  sprintf (sql2, "VALUES ('%s', 'Geometry', '%s', %d)", xtable, xgtype,
		   srid);
	  strcat (sql, sql2);
      }
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "register_virtual() error: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    return 1;
}

static int
auto_register_virtual_shapes (sqlite3 * handle, int version)
{
/* attempting to register all VirtualShape tables */
    int ret;
    char sql[1024];
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;

/* retrieving all VirtualShape tables */
    strcpy (sql, "SELECT tbl_name FROM sqlite_master ");
    strcat (sql, "WHERE type = 'table' AND sql LIKE '%VirtualShape%' ");
    strcat (sql, "AND sql LIKE 'CREATE VIRTUAL TABLE%'");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 0];
		if (!register_virtual (handle, name, version))
		  {
		      sqlite3_free_table (results);
		      return 0;
		  }
	    }
      }
    sqlite3_free_table (results);
    return 1;
  unknown:
    return 0;
}

static int
register_virtual_shapes (sqlite3 * handle, int version)
{
/* attempting to register any already registered VirtualShape table */
    int ret;
    char sql[1024];
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;

/* retrieving any already registered VirtualShape table */
    strcpy (sql, "SELECT virt_name FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 0];
		if (!register_virtual (handle, name, version))
		  {
		      sqlite3_free_table (results);
		      return 0;
		  }
	    }
      }
    sqlite3_free_table (results);
    return 1;
  unknown:
    return 0;
}

static int
create_virts_geometry_columns_3 (sqlite3 * handle)
{
/* creating VIRTS_GEOMETRY_COLUMNS Version=3 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "virts_geometry_columns (\n");
    strcat (sql, "virt_name TEXT NOT NULL,\n");
    strcat (sql, "virt_geometry TEXT NOT NULL,\n");
    strcat (sql, "type VARCHAR(30) NOT NULL,\n");
    strcat (sql, "srid INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_geom_cols_virts PRIMARY KEY ");
    strcat (sql, "(virt_name, virt_geometry),\n");
    strcat (sql, "CONSTRAINT fk_vgc_srid FOREIGN KEY ");
    strcat (sql, "(srid) REFERENCES spatial_ref_sys (srid))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE GEOMETRY_COLUMNS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    strcpy (sql, "CREATE INDEX IF NOT EXISTS ");
    strcat (sql, "idx_virtssrid ON virts_geometry_columns\n");
    strcat (sql, "(srid)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_VIRTSSRID error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_virts_geometry_columns_4 (sqlite3 * handle)
{
/* creating VIRTS_GEOMETRY_COLUMNS Version=4 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "virts_geometry_columns (\n");
    strcat (sql, "virt_name TEXT NOT NULL,\n");
    strcat (sql, "virt_geometry TEXT NOT NULL,\n");
    strcat (sql, "geometry_type INTEGER NOT NULL,\n");
    strcat (sql, "coord_dimension INTEGER NOT NULL,\n");
    strcat (sql, "srid INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_geom_cols_virts PRIMARY KEY ");
    strcat (sql, "(virt_name, virt_geometry),\n");
    strcat (sql, "CONSTRAINT fk_vgc_srid FOREIGN KEY ");
    strcat (sql, "(srid) REFERENCES spatial_ref_sys (srid),\n");
    strcat (sql, "CONSTRAINT ck_vgc_type CHECK (geometry_type IN ");
    strcat (sql, "(1,2,3,4,5,6,1001,1002,1003,1004,1005,1006,");
    strcat (sql, "2001,2002,2003,2004,2005,2006,3001,3002,");
    strcat (sql, "3003,3004,3005,3006)),\n");
    strcat (sql, "CONSTRAINT ck_vgc_dims CHECK (coord_dimension IN ");
    strcat (sql, "(2,3,4)))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE GEOMETRY_COLUMNS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    strcpy (sql, "CREATE INDEX IF NOT EXISTS ");
    strcat (sql, "idx_virtssrid ON virts_geometry_columns\n");
    strcat (sql, "(srid)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_VIRTSSRID error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
cvt_virts_geometry_columns (sqlite3 * handle, int in_version, int version)
{
/* converting VIRTS_GEOMETRY_COLUMNS */
    int ret;

    if (in_version != 2)
      {
	  /* preparing the input table */
	  if (!prepare_input (handle, "virts_geometry_columns"))
	      return 0;
      }
    drop_virts_geometry_columns (handle);

/* creating the output table */
    ret = 0;
    if (version == 2)
	ret = 1;
    if (version == 3)
	ret = create_virts_geometry_columns_3 (handle);
    if (version == 4)
	ret = create_virts_geometry_columns_4 (handle);
    if (!ret)
	return 0;

/* copying any row */
    ret = 0;
    if (in_version == 2)
	ret = auto_register_virtual_shapes (handle, version);
    if (in_version == 3)
      {
	  if (version == 2)
	      ret = 1;
	  if (version == 4)
	      ret = register_virtual_shapes (handle, version);
      }
    if (in_version == 4)
      {
	  if (version == 2)
	      ret = 1;
	  if (version == 3)
	      ret = register_virtual_shapes (handle, version);
      }
    if (!ret)
	return 0;

    if (in_version != 2)
      {
	  /* dropping the temporary input table */
	  if (!drop_input_table (handle))
	      return 0;
      }

    return 1;
}

static int
copy_geometry_columns_2_3 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=2 to Version=3 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column, type, srid, spatial_index_enabled ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, type, ");
    strcat (sql,
	    "coord_dimension, srid, spatial_index_enabled) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 2);
		sqlite3_bind_text (stmt_out, 3, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt_out, 4, "XY", 2, SQLITE_STATIC);
		if (sqlite3_column_type (stmt_in, 3) == SQLITE_NULL)
		    int_value = -1;
		else
		    int_value = sqlite3_column_int (stmt_in, 3);
		sqlite3_bind_int (stmt_out, 5, int_value);
		int_value = sqlite3_column_int (stmt_in, 4);
		sqlite3_bind_int (stmt_out, 6, int_value);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_geometry_columns_2_4 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=2 to Version=4 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column, type, srid, spatial_index_enabled ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, geometry_type, ");
    strcat (sql,
	    "coord_dimension, srid, spatial_index_enabled) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 2);
		int_value = 0;
		if (strcasecmp (txt_value, "POINT") == 0)
		    int_value = 1;
		if (strcasecmp (txt_value, "LINESTRING") == 0)
		    int_value = 2;
		if (strcasecmp (txt_value, "POLYGON") == 0)
		    int_value = 3;
		if (strcasecmp (txt_value, "MULTIPOINT") == 0)
		    int_value = 4;
		if (strcasecmp (txt_value, "MULTILINESTRING") == 0)
		    int_value = 5;
		if (strcasecmp (txt_value, "MULTIPOLYGON") == 0)
		    int_value = 6;
		if (strcasecmp (txt_value, "GEOMETRYCOLLECTION") == 0)
		    int_value = 7;
		sqlite3_bind_int (stmt_out, 3, int_value);
		sqlite3_bind_int (stmt_out, 4, 2);
		if (sqlite3_column_type (stmt_in, 3) == SQLITE_NULL)
		    int_value = -1;
		else
		    int_value = sqlite3_column_int (stmt_in, 3);
		sqlite3_bind_int (stmt_out, 5, int_value);
		int_value = sqlite3_column_int (stmt_in, 4);
		sqlite3_bind_int (stmt_out, 6, int_value);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_geometry_columns_3_2 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=3 to Version=2 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column, type, srid, spatial_index_enabled ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, type, ");
    strcat (sql,
	    "coord_dimension, srid, spatial_index_enabled) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 2);
		sqlite3_bind_text (stmt_out, 3, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		sqlite3_bind_int (stmt_out, 4, 2);
		int_value = sqlite3_column_int (stmt_in, 3);
		sqlite3_bind_int (stmt_out, 5, int_value);
		int_value = sqlite3_column_int (stmt_in, 4);
		sqlite3_bind_int (stmt_out, 6, int_value);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_geometry_columns_3_4 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=3 to Version=4 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column, type, coord_dimension, ");
    strcat (sql, "srid, spatial_index_enabled ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, geometry_type, ");
    strcat (sql,
	    "coord_dimension, srid, spatial_index_enabled) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 2);
		int_value = 0;
		if (strcasecmp (txt_value, "POINT") == 0)
		    int_value = 1;
		if (strcasecmp (txt_value, "LINESTRING") == 0)
		    int_value = 2;
		if (strcasecmp (txt_value, "POLYGON") == 0)
		    int_value = 3;
		if (strcasecmp (txt_value, "MULTIPOINT") == 0)
		    int_value = 4;
		if (strcasecmp (txt_value, "MULTILINESTRING") == 0)
		    int_value = 5;
		if (strcasecmp (txt_value, "MULTIPOLYGON") == 0)
		    int_value = 6;
		if (strcasecmp (txt_value, "GEOMETRYCOLLECTION") == 0)
		    int_value = 7;
		sqlite3_bind_int (stmt_out, 3, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 3);
		int_value = 2;
		if (strcmp (txt_value, "3") == 0)
		    int_value = 3;
		if (strcmp (txt_value, "XYZ") == 0)
		    int_value = 3;
		if (strcmp (txt_value, "XYM") == 0)
		    int_value = 3;
		if (strcmp (txt_value, "XYZM") == 0)
		    int_value = 4;
		sqlite3_bind_int (stmt_out, 4, int_value);
		int_value = sqlite3_column_int (stmt_in, 4);
		sqlite3_bind_int (stmt_out, 5, int_value);
		int_value = sqlite3_column_int (stmt_in, 5);
		sqlite3_bind_int (stmt_out, 6, int_value);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_geometry_columns_4_2 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=4 to Version=2 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column, geometry_type, srid, spatial_index_enabled ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, type, ");
    strcat (sql,
	    "coord_dimension, srid, spatial_index_enabled) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		switch (int_value)
		  {
		  case 0:
		      txt_value = "GEOMETRY";
		      break;
		  case 1:
		      txt_value = "POINT";
		      break;
		  case 2:
		      txt_value = "LINESTRING";
		      break;
		  case 3:
		      txt_value = "POLYGON";
		      break;
		  case 4:
		      txt_value = "MULTIPOINT";
		      break;
		  case 5:
		      txt_value = "MULTILINESTRING";
		      break;
		  case 6:
		      txt_value = "MULTIPOLYGON";
		      break;
		  case 7:
		      txt_value = "GEOMETRYCOLLECTION";
		      break;
		  };
		sqlite3_bind_text (stmt_out, 3, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		sqlite3_bind_int (stmt_out, 4, 2);
		int_value = sqlite3_column_int (stmt_in, 3);
		if (int_value <= 0)
		    int_value = -1;
		sqlite3_bind_int (stmt_out, 5, int_value);
		int_value = sqlite3_column_int (stmt_in, 4);
		sqlite3_bind_int (stmt_out, 6, int_value);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_geometry_columns_4_3 (sqlite3 * handle)
{
/* copying GEOMETRY_COLUMNS Version=4 to Version=3 */
    char sql[8192];
    int ret;
    int int_value;
    const char *dims;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column, geometry_type, srid, spatial_index_enabled ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, type, ");
    strcat (sql,
	    "coord_dimension, srid, spatial_index_enabled) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 0);
		sqlite3_bind_text (stmt_out, 1, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		switch (int_value)
		  {
		  case 0:
		      txt_value = "GEOMETRY";
		      dims = "XY";
		      break;
		  case 1:
		      txt_value = "POINT";
		      dims = "XY";
		      break;
		  case 2:
		      txt_value = "LINESTRING";
		      dims = "XY";
		      break;
		  case 3:
		      txt_value = "POLYGON";
		      dims = "XY";
		      break;
		  case 4:
		      txt_value = "MULTIPOINT";
		      dims = "XY";
		      break;
		  case 5:
		      txt_value = "MULTILINESTRING";
		      dims = "XY";
		      break;
		  case 6:
		      txt_value = "MULTIPOLYGON";
		      dims = "XY";
		      break;
		  case 7:
		      txt_value = "GEOMETRYCOLLECTION";
		      dims = "XY";
		      break;
		  case 1000:
		      txt_value = "GEOMETRY";
		      dims = "XYZ";
		      break;
		  case 1001:
		      txt_value = "POINT";
		      dims = "XYZ";
		      break;
		  case 1002:
		      txt_value = "LINESTRING";
		      dims = "XYZ";
		      break;
		  case 1003:
		      txt_value = "POLYGON";
		      dims = "XYZ";
		      break;
		  case 1004:
		      txt_value = "MULTIPOINT";
		      dims = "XYZ";
		      break;
		  case 1005:
		      txt_value = "MULTILINESTRING";
		      dims = "XYZ";
		      break;
		  case 1006:
		      txt_value = "MULTIPOLYGON";
		      dims = "XYZ";
		      break;
		  case 1007:
		      txt_value = "GEOMETRYCOLLECTION";
		      dims = "XYZ";
		      break;
		  case 2000:
		      txt_value = "GEOMETRY";
		      dims = "XYM";
		      break;
		  case 2001:
		      txt_value = "POINT";
		      dims = "XYM";
		      break;
		  case 2002:
		      txt_value = "LINESTRING";
		      dims = "XYM";
		      break;
		  case 2003:
		      txt_value = "POLYGON";
		      dims = "XYM";
		      break;
		  case 2004:
		      txt_value = "MULTIPOINT";
		      dims = "XYM";
		      break;
		  case 2005:
		      txt_value = "MULTILINESTRING";
		      dims = "XYM";
		      break;
		  case 2006:
		      txt_value = "MULTIPOLYGON";
		      dims = "XYM";
		      break;
		  case 2007:
		      txt_value = "GEOMETRYCOLLECTION";
		      dims = "XYM";
		      break;
		  case 3000:
		      txt_value = "GEOMETRY";
		      dims = "XYZM";
		      break;
		  case 3001:
		      txt_value = "POINT";
		      dims = "XYZM";
		      break;
		  case 3002:
		      txt_value = "LINESTRING";
		      dims = "XYZM";
		      break;
		  case 3003:
		      txt_value = "POLYGON";
		      dims = "XYZM";
		      break;
		  case 3004:
		      txt_value = "MULTIPOINT";
		      dims = "XYZM";
		      break;
		  case 3005:
		      txt_value = "MULTILINESTRING";
		      dims = "XYZM";
		      break;
		  case 3006:
		      txt_value = "MULTIPOLYGON";
		      dims = "XYZM";
		      break;
		  case 3007:
		      txt_value = "GEOMETRYCOLLECTION";
		      dims = "XYZM";
		      break;
		  };
		sqlite3_bind_text (stmt_out, 3, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt_out, 4, dims, strlen (dims),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 3);
		if (int_value <= 0)
		    int_value = -1;
		sqlite3_bind_int (stmt_out, 5, int_value);
		int_value = sqlite3_column_int (stmt_in, 4);
		sqlite3_bind_int (stmt_out, 6, int_value);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
create_geometry_columns_2 (sqlite3 * handle)
{
/* creating GEOMETRY_COLUMNS Version=2 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE geometry_columns (\n");
    strcat (sql, "f_table_name VARCHAR(256) NOT NULL,\n");
    strcat (sql, "f_geometry_column VARCHAR(256) NOT NULL,\n");
    strcat (sql, "type VARCHAR(30) NOT NULL,\n");
    strcat (sql, "coord_dimension INTEGER NOT NULL,\n");
    strcat (sql, "srid INTEGER,\n");
    strcat (sql, "spatial_index_enabled INTEGER NOT NULL)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE GEOMETRY_COLUMNS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_geometry_columns_3 (sqlite3 * handle)
{
/* creating GEOMETRY_COLUMNS Version=3 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE geometry_columns (\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql, "type TEXT NOT NULL,\n");
    strcat (sql, "coord_dimension TEXT NOT NULL,\n");
    strcat (sql, "srid INTEGER NOT NULL,\n");
    strcat (sql, "spatial_index_enabled INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_geom_cols PRIMARY KEY ");
    strcat (sql, "(f_table_name, f_geometry_column),\n");
    strcat (sql, "CONSTRAINT fk_gc_srs FOREIGN KEY ");
    strcat (sql, "(srid) REFERENCES spatial_ref_sys (srid))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE GEOMETRY_COLUMNS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    strcpy (sql, "CREATE INDEX idx_srid_geocols ON geometry_columns\n");
    strcat (sql, "(srid) ");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_SRID_GEOCOLS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_geometry_columns_4 (sqlite3 * handle)
{
/* creating GEOMETRY_COLUMNS Version=4 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE geometry_columns (\n");
    strcat (sql, "f_table_name TEXT NOT NULL,\n");
    strcat (sql, "f_geometry_column TEXT NOT NULL,\n");
    strcat (sql, "geometry_type INTEGER NOT NULL,\n");
    strcat (sql, "coord_dimension INTEGER NOT NULL,\n");
    strcat (sql, "srid INTEGER NOT NULL,\n");
    strcat (sql, "spatial_index_enabled INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_geom_cols PRIMARY KEY ");
    strcat (sql, "(f_table_name, f_geometry_column),\n");
    strcat (sql, "CONSTRAINT fk_gc_srs FOREIGN KEY ");
    strcat (sql, "(srid) REFERENCES spatial_ref_sys (srid),\n");
    strcat (sql, "CONSTRAINT ck_gc_type CHECK (geometry_type IN ");
    strcat (sql, "(0,1,2,3,4,5,6,7,1000,1001,1002,1003,1004,1005,1006,");
    strcat (sql, "1007,2000,2001,2002,2003,2004,2005,2006,2007,3000,3001,");
    strcat (sql, "3002,3003,3004,3005,3006,3007)),\n");
    strcat (sql, "CONSTRAINT ck_gc_dims CHECK (coord_dimension IN ");
    strcat (sql, "(2,3,4)),\n");
    strcat (sql, "CONSTRAINT ck_gc_rtree CHECK ");
    strcat (sql, "(spatial_index_enabled IN (0,1,2)))");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE GEOMETRY_COLUMNS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    strcpy (sql, "CREATE INDEX idx_srid_geocols ON geometry_columns\n");
    strcat (sql, "(srid) ");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_SRID_GEOCOLS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
cvt_geometry_columns (sqlite3 * handle, int in_version, int version)
{
/* converting GEOMETRY_COLUMNS */
    int ret;

/* preparing the input table */
    if (!prepare_input (handle, "geometry_columns"))
	return 0;

/* creating the output table */
    ret = 0;
    if (version == 2)
	ret = create_geometry_columns_2 (handle);
    if (version == 3)
	ret = create_geometry_columns_3 (handle);
    if (version == 4)
	ret = create_geometry_columns_4 (handle);
    if (!ret)
	return 0;

/* copying any row */
    ret = 0;
    if (in_version == 2)
      {
	  if (version == 3)
	      ret = copy_geometry_columns_2_3 (handle);
	  if (version == 4)
	      ret = copy_geometry_columns_2_4 (handle);
      }
    if (in_version == 3)
      {
	  if (version == 2)
	      ret = copy_geometry_columns_3_2 (handle);
	  if (version == 4)
	      ret = copy_geometry_columns_3_4 (handle);
      }
    if (in_version == 4)
      {
	  if (version == 2)
	      ret = copy_geometry_columns_4_2 (handle);
	  if (version == 3)
	      ret = copy_geometry_columns_4_3 (handle);
      }
    if (!ret)
	return 0;

/* dropping the temporary input table */
    if (!drop_input_table (handle))
	return 0;

    return 1;
}

static int
copy_spatial_ref_sys_2_3 (sqlite3 * handle)
{
/* copying SPATIAL_REF_SYS Version=2 to Version=3 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql, "SELECT srid, auth_name, auth_srid, ref_sys_name, proj4text ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql, "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ");
    strcat (sql, "ref_sys_name, proj4text) VALUES (?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		int_value = sqlite3_column_int (stmt_in, 0);
		sqlite3_bind_int (stmt_out, 1, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		sqlite3_bind_int (stmt_out, 3, int_value);
		if (sqlite3_column_type (stmt_in, 3) == SQLITE_NULL)
		    sqlite3_bind_null (stmt_out, 4);
		else
		  {
		      txt_value =
			  (const char *) sqlite3_column_text (stmt_in, 3);
		      sqlite3_bind_text (stmt_out, 4, txt_value,
					 strlen (txt_value), SQLITE_STATIC);
		  }
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_spatial_ref_sys_2_4 (sqlite3 * handle)
{
/* copying SPATIAL_REF_SYS Version=2 to Version=4 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql, "SELECT srid, auth_name, auth_srid, ref_sys_name, proj4text ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql, "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ");
    strcat (sql, "ref_sys_name, proj4text) VALUES (?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		int_value = sqlite3_column_int (stmt_in, 0);
		sqlite3_bind_int (stmt_out, 1, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		sqlite3_bind_int (stmt_out, 3, int_value);
		if (sqlite3_column_type (stmt_in, 3) == SQLITE_NULL)
		    sqlite3_bind_text (stmt_out, 4, "Unknown", 7,
				       SQLITE_STATIC);
		else
		  {
		      txt_value =
			  (const char *) sqlite3_column_text (stmt_in, 3);
		      sqlite3_bind_text (stmt_out, 4, txt_value,
					 strlen (txt_value), SQLITE_STATIC);
		  }
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_spatial_ref_sys_3_2 (sqlite3 * handle)
{
/* copying SPATIAL_REF_SYS Version=3 to Version=2 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql, "SELECT srid, auth_name, auth_srid, ref_sys_name, proj4text ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql, "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ");
    strcat (sql, "ref_sys_name, proj4text) VALUES (?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		int_value = sqlite3_column_int (stmt_in, 0);
		sqlite3_bind_int (stmt_out, 1, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		sqlite3_bind_int (stmt_out, 3, int_value);
		if (sqlite3_column_type (stmt_in, 3) == SQLITE_NULL)
		    sqlite3_bind_null (stmt_out, 4);
		else
		  {
		      txt_value =
			  (const char *) sqlite3_column_text (stmt_in, 3);
		      sqlite3_bind_text (stmt_out, 4, txt_value,
					 strlen (txt_value), SQLITE_STATIC);
		  }
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_spatial_ref_sys_3_4 (sqlite3 * handle)
{
/* copying SPATIAL_REF_SYS Version=3 to Version=4 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT srid, auth_name, auth_srid, ref_sys_name, proj4text, srs_wkt ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql, "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ");
    strcat (sql, "ref_sys_name, proj4text, srtext) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		int_value = sqlite3_column_int (stmt_in, 0);
		sqlite3_bind_int (stmt_out, 1, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		sqlite3_bind_int (stmt_out, 3, int_value);
		if (sqlite3_column_type (stmt_in, 3) == SQLITE_NULL)
		    sqlite3_bind_text (stmt_out, 4, "Unknown", 7,
				       SQLITE_STATIC);
		else
		  {
		      txt_value =
			  (const char *) sqlite3_column_text (stmt_in, 3);
		      sqlite3_bind_text (stmt_out, 4, txt_value,
					 strlen (txt_value), SQLITE_STATIC);
		  }
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		if (sqlite3_column_type (stmt_in, 5) == SQLITE_NULL)
		    sqlite3_bind_text (stmt_out, 6, "Undefined", 9,
				       SQLITE_STATIC);
		else
		  {
		      txt_value =
			  (const char *) sqlite3_column_text (stmt_in, 5);
		      sqlite3_bind_text (stmt_out, 6, txt_value,
					 strlen (txt_value), SQLITE_STATIC);
		  }
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_spatial_ref_sys_4_2 (sqlite3 * handle)
{
/* copying SPATIAL_REF_SYS Version=4 to Version=2 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql, "SELECT srid, auth_name, auth_srid, ref_sys_name, proj4text ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql, "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ");
    strcat (sql, "ref_sys_name, proj4text) VALUES (?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		int_value = sqlite3_column_int (stmt_in, 0);
		sqlite3_bind_int (stmt_out, 1, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		sqlite3_bind_int (stmt_out, 3, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 3);
		if (strcasecmp (txt_value, "Unknown") == 0)
		    sqlite3_bind_null (stmt_out, 4);
		else
		    sqlite3_bind_text (stmt_out, 4, txt_value,
				       strlen (txt_value), SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
copy_spatial_ref_sys_4_3 (sqlite3 * handle)
{
/* copying SPATIAL_REF_SYS Version=4 to Version=3 */
    char sql[8192];
    int ret;
    int int_value;
    const char *txt_value;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;

/* preparing the input statement */
    strcpy (sql,
	    "SELECT srid, auth_name, auth_srid, ref_sys_name, proj4text, srtext ");
    strcat (sql, "FROM \"cvt-input tmp-cvt\"");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_in, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

/* preparing the output statement */
    strcpy (sql, "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, ");
    strcat (sql, "ref_sys_name, proj4text, srs_wkt) VALUES (?, ?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt_out, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  goto error;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* copying one row */
		sqlite3_reset (stmt_out);
		sqlite3_clear_bindings (stmt_out);
		int_value = sqlite3_column_int (stmt_in, 0);
		sqlite3_bind_int (stmt_out, 1, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 1);
		sqlite3_bind_text (stmt_out, 2, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		int_value = sqlite3_column_int (stmt_in, 2);
		sqlite3_bind_int (stmt_out, 3, int_value);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 3);
		if (strcasecmp (txt_value, "Unknown") == 0)
		    sqlite3_bind_null (stmt_out, 4);
		else
		    sqlite3_bind_text (stmt_out, 4, txt_value,
				       strlen (txt_value), SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 4);
		sqlite3_bind_text (stmt_out, 5, txt_value, strlen (txt_value),
				   SQLITE_STATIC);
		txt_value = (const char *) sqlite3_column_text (stmt_in, 5);
		if (strcasecmp (txt_value, "Undefined") == 0)
		    sqlite3_bind_null (stmt_out, 6);
		else

		    sqlite3_bind_text (stmt_out, 6, txt_value,
				       strlen (txt_value), SQLITE_STATIC);
		ret = sqlite3_step (stmt_out);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    continue;
		fprintf (stderr, "(OUT) sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		goto error;
	    }
	  else
	    {
		/* some unexpected read error */
		printf ("(IN) sqlite3_step() error: %s\n",
			sqlite3_errmsg (handle));
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    return 1;

  error:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
    return 0;
}

static int
create_spatial_ref_sys_2 (sqlite3 * handle)
{
/* creating SPATIAL_REF_SYS Version=2 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE spatial_ref_sys (\n");
    strcat (sql, "srid INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "auth_name VARCHAR(256) NOT NULL,\n");
    strcat (sql, "auth_srid INTEGER NOT NULL,\n");
    strcat (sql, "ref_sys_name VARCHAR(256),\n");
    strcat (sql, "proj4text VARCHAR(2048) NOT NULL)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE SPATIAL_REF_SYS error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_spatial_ref_sys_3 (sqlite3 * handle)
{
/* creating SPATIAL_REF_SYS Version=3 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE spatial_ref_sys (\n");
    strcat (sql, "srid INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "auth_name TEXT NOT NULL,\n");
    strcat (sql, "auth_srid INTEGER NOT NULL,\n");
    strcat (sql, "ref_sys_name TEXT,\n");
    strcat (sql, "proj4text TEXT NOT NULL,\n");
    strcat (sql, "srs_wkt TEXT)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE SPATIAL_REF_SYS error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    strcpy (sql, "CREATE UNIQUE INDEX idx_spatial_ref_sys \n");
    strcat (sql, "ON spatial_ref_sys (auth_srid, auth_name)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_SPATIAL_REF_SYS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
create_spatial_ref_sys_4 (sqlite3 * handle)
{
/* creating SPATIAL_REF_SYS Version=4 */
    int ret;
    char *sql_err = NULL;
    char sql[8192];
    strcpy (sql, "CREATE TABLE spatial_ref_sys (\n");
    strcat (sql, "srid INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "auth_name TEXT NOT NULL,\n");
    strcat (sql, "auth_srid INTEGER NOT NULL,\n");
    strcat (sql, "ref_sys_name TEXT NOT NULL DEFAULT 'Unknown',\n");
    strcat (sql, "proj4text TEXT NOT NULL,\n");
    strcat (sql, "srtext TEXT NOT NULL DEFAULT 'Undefined')");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE SPATIAL_REF_SYS error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
    strcpy (sql, "CREATE UNIQUE INDEX idx_spatial_ref_sys \n");
    strcat (sql, "ON spatial_ref_sys (auth_srid, auth_name)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX IDX_SPATIAL_REF_SYS error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 1;
}

static int
cvt_spatial_ref_sys (sqlite3 * handle, int in_version, int version)
{
/* converting SPATIAL_REF_SYS */
    int ret;

/* preparing the input table */
    if (!prepare_input (handle, "spatial_ref_sys"))
	return 0;

/* creating the output table */
    ret = 0;
    if (version == 2)
	ret = create_spatial_ref_sys_2 (handle);
    if (version == 3)
	ret = create_spatial_ref_sys_3 (handle);
    if (version == 4)
	ret = create_spatial_ref_sys_4 (handle);
    if (!ret)
	return 0;

/* copying any row */
    ret = 0;
    if (in_version == 2)
      {
	  if (version == 3)
	      ret = copy_spatial_ref_sys_2_3 (handle);
	  if (version == 4)
	      ret = copy_spatial_ref_sys_2_4 (handle);
      }
    if (in_version == 3)
      {
	  if (version == 2)
	      ret = copy_spatial_ref_sys_3_2 (handle);
	  if (version == 4)
	      ret = copy_spatial_ref_sys_3_4 (handle);
      }
    if (in_version == 4)
      {
	  if (version == 2)
	      ret = copy_spatial_ref_sys_4_2 (handle);
	  if (version == 3)
	      ret = copy_spatial_ref_sys_4_3 (handle);
      }
    if (!ret)
	return 0;

/* dropping the temporary input table */
    if (!drop_input_table (handle))
	return 0;

    return 1;
}

static int
check_3d_v3 (sqlite3 * handle, int *has_3d)
{
/* checking for 3D geometries - version 4 */
    int ret;
    char sql[1024];
    const char *value;
    int i;
    char **results;
    int rows;
    int columns;

    *has_3d = 0;

/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "SELECT coord_dimension FROM geometry_columns");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (strcasecmp (value, "3") == 0)
		    *has_3d = 1;
		if (strcasecmp (value, "XYZ") == 0)
		    *has_3d = 1;
		if (strcasecmp (value, "XYM") == 0)
		    *has_3d = 1;
		if (strcasecmp (value, "XYZM") == 0)
		    *has_3d = 1;
	    }
      }
    sqlite3_free_table (results);
    return 1;
  unknown:
    return 0;
}

static int
check_3d_v4 (sqlite3 * handle, int *has_3d)
{
/* checking for 3D geometries - version 4 */
    int ret;
    char sql[1024];
    int value;
    int i;
    char **results;
    int rows;
    int columns;

    *has_3d = 0;

/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "SELECT geometry_type FROM geometry_columns");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = atoi (results[(i * columns) + 0]);
		switch (value)
		  {
		  case 1000:
		  case 1001:
		  case 1002:
		  case 1003:
		  case 1004:
		  case 1005:
		  case 1006:
		  case 1007:
		  case 2000:
		  case 2001:
		  case 2002:
		  case 2003:
		  case 2004:
		  case 2005:
		  case 2006:
		  case 2007:
		  case 3000:
		  case 3001:
		  case 3002:
		  case 3003:
		  case 3004:
		  case 3005:
		  case 3006:
		  case 3007:
		      *has_3d = 1;
		      break;
		  };
	    }
      }
    sqlite3_free_table (results);
    return 1;
  unknown:
    return 0;
}

static int
check_version (sqlite3 * handle, int *version, int *has_3d)
{
/* checking the current Version */
    int ret;
    char sql[1024];
    int spatialite_rs = 0;
    int spatialite_gc = 0;
    int rs_srid = 0;
    int auth_name = 0;
    int auth_srid = 0;
    int ref_sys_name = 0;
    int proj4text = 0;
    int srs_wkt = 0;
    int srtext = 0;
    int f_table_name = 0;
    int f_geometry_column = 0;
    int coord_dimension = 0;
    int gc_srid = 0;
    int type = 0;
    int geometry_type = 0;
    int spatial_index_enabled = 0;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;

    *version = 0;
    *has_3d = 0;

/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "PRAGMA table_info(geometry_columns)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "f_table_name") == 0)
		    f_table_name = 1;
		if (strcasecmp (name, "f_geometry_column") == 0)
		    f_geometry_column = 1;
		if (strcasecmp (name, "coord_dimension") == 0)
		    coord_dimension = 1;
		if (strcasecmp (name, "srid") == 0)
		    gc_srid = 1;
		if (strcasecmp (name, "type") == 0)
		    type = 1;
		if (strcasecmp (name, "geometry_type") == 0)
		    geometry_type = 1;
		if (strcasecmp (name, "spatial_index_enabled") == 0)
		    spatial_index_enabled = 1;
	    }
      }
    sqlite3_free_table (results);
    if (f_table_name && f_geometry_column && type && coord_dimension
	&& gc_srid && spatial_index_enabled)
	spatialite_gc = 1;
    if (f_table_name && f_geometry_column && geometry_type && coord_dimension
	&& gc_srid && spatial_index_enabled)
	spatialite_gc = 3;

/* checking the SPATIAL_REF_SYS table */
    strcpy (sql, "PRAGMA table_info(spatial_ref_sys)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "srid") == 0)
		    rs_srid = 1;
		if (strcasecmp (name, "auth_name") == 0)
		    auth_name = 1;
		if (strcasecmp (name, "auth_srid") == 0)
		    auth_srid = 1;
		if (strcasecmp (name, "ref_sys_name") == 0)
		    ref_sys_name = 1;
		if (strcasecmp (name, "proj4text") == 0)
		    proj4text = 1;
		if (strcasecmp (name, "srs_wkt") == 0)
		    srs_wkt = 1;
		if (strcasecmp (name, "srtext") == 0)
		    srtext = 1;
	    }
      }
    sqlite3_free_table (results);
    if (rs_srid && auth_name && auth_srid && ref_sys_name && proj4text)
	spatialite_rs = 1;
/* verifying the MetaData format */
    if (spatialite_gc == 1 && spatialite_rs == 1)
      {
	  if (srs_wkt == 1)
	    {
		if (!check_3d_v3 (handle, has_3d))
		    return 0;
		*version = 3;
		return 1;
	    }
	  else
	    {
		*version = 2;
		return 1;
	    }
      }
    if (spatialite_gc == 3 && spatialite_rs == 1 && srtext == 1)
      {
	  if (!check_3d_v4 (handle, has_3d))
	      return 0;
	  *version = 4;
	  return 1;
      }
  unknown:
    return 0;
}

static void
open_db (const char *path, sqlite3 ** handle)
{
/* opening the DB */
    sqlite3 *db_handle;
    int ret;

    *handle = NULL;
    spatialite_init (0);
    printf ("SQLite version: %s\n", sqlite3_libversion ());
    printf ("SpatiaLite version: %s\n\n", spatialite_version ());

    ret = sqlite3_open_v2 (path, &db_handle, SQLITE_OPEN_READWRITE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", path,
		   sqlite3_errmsg (db_handle));
	  sqlite3_close (db_handle);
	  return;
      }
    *handle = db_handle;
    return;
}

static void
do_help ()
{
/* printing the argument list */
    fprintf (stderr, "\n\nusage: spatialite_convert ARGLIST\n");
    fprintf (stderr,
	     "==============================================================\n");
    fprintf (stderr,
	     "-h or --help                    print this help message\n");
    fprintf (stderr,
	     "-d or --db-path  pathname       the SpatiaLite DB path\n\n");
    fprintf (stderr,
	     "-v or --version    num          target Version (2, 3, 4)\n");
}

int
main (int argc, char *argv[])
{
/* the MAIN function simply perform arguments checking */
    sqlite3 *handle;
    int ret;
    char *sql_err = NULL;
    int i;
    int next_arg = ARG_NONE;
    const char *db_path = NULL;
    int version = -1;
    int error = 0;
    int in_version = 0;
    int has_3d = 0;

    for (i = 1; i < argc; i++)
      {
	  /* parsing the invocation arguments */
	  if (next_arg != ARG_NONE)
	    {
		switch (next_arg)
		  {
		  case ARG_DB_PATH:
		      db_path = argv[i];
		      break;
		  case ARG_VERSION:
		      version = atoi (argv[i]);
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
	  if (strcmp (argv[i], "-d") == 0)
	    {
		next_arg = ARG_DB_PATH;
		continue;
	    }
	  if (strcasecmp (argv[i], "--db-path") == 0)
	    {
		next_arg = ARG_DB_PATH;
		continue;
	    }
	  if (strcasecmp (argv[i], "--version") == 0
	      || strcmp (argv[i], "-v") == 0)
	    {
		next_arg = ARG_VERSION;
		continue;
	    }
	  fprintf (stderr, "unknown argument: %s\n", argv[i]);
	  error = 1;
      }
    if (error)
      {
	  do_help ();
	  return -1;
      }

/* checking the arguments */
    if (!db_path)
      {
	  fprintf (stderr, "did you forget setting the --db-path argument ?\n");
	  error = 1;
      }
    if (version == 2 || version == 3 || version == 4)
	;
    else
      {
	  fprintf (stderr, "wrong --version argument (%d): should be 2, 3, 4\n",
		   version);
	  error = 1;
      }

    if (error)
      {
	  do_help ();
	  return -1;
      }

/* opening the DB */
    open_db (db_path, &handle);
    if (!handle)
	return -1;

/* determing the actual Version */
    if (!check_version (handle, &in_version, &has_3d))
      {
	  fprintf (stderr, "DB '%s'\n", db_path);
	  fprintf (stderr,
		   "doesn't seems to contain valid Spatial Metadata ...\n");
	  fprintf (stderr, "sorry, cowardly quitting\n\n");
	  goto stop;
      }
    if (in_version == version)
      {
	  fprintf (stderr, "DB '%s'\n", db_path);
	  fprintf (stderr, "already contains Version=%d Spatial Metadata ...\n",
		   version);
	  fprintf (stderr, "sorry, cowardly quitting\n\n");
	  goto stop;
      }
    if (version == 2 && has_3d)
      {
	  fprintf (stderr, "DB '%s'\n", db_path);
	  fprintf (stderr, "seems to contain 3D geometries\n");
	  fprintf (stderr,
		   "converting to Version=2 isn't possible, because 3D isn't supported\n");
	  fprintf (stderr, "sorry, cowardly quitting\n\n");
	  goto stop;
      }

/* ok, going to convert */
    printf ("DB '%s'\n", db_path);
    printf ("converting from Version=%d to Version=%d\n", in_version, version);

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  goto stop;
      }

    if (!cvt_spatial_ref_sys (handle, in_version, version))
	goto stop;
    fprintf (stderr, "\t* converted: spatial_ref_sys\n");
    if (!cvt_geometry_columns (handle, in_version, version))
	goto stop;
    fprintf (stderr, "\t* converted: geometry_columns\n");
    if (!cvt_virts_geometry_columns (handle, in_version, version))
	goto stop;
    fprintf (stderr, "\t* converted: virts_geometry_columns\n");
    if (!cvt_views_geometry_columns (handle, in_version, version))
	goto stop;
    fprintf (stderr, "\t* converted: views_geometry_columns\n");
    if (!cvt_spatial_index (handle, version))
	goto stop;
    fprintf (stderr, "\t* converted: SpatialIndex\n");
    if (!cvt_triggers (handle, version))
	goto stop;
    fprintf (stderr, "\t* converted: triggers\n");
    if (!cvt_extra_stuff (handle, version))
	goto stop;
    fprintf (stderr, "\t* converted: Extra-Stuff\n");
    if (!cvt_views (handle, version))
	goto stop;
    fprintf (stderr, "\t* converted: MetaData views\n");
    if (!update_history (handle, in_version, version))
	goto stop;

/* committing the pending SQL Transaction */
    ret = sqlite3_exec (handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  goto stop;
      }
    printf ("\tDB-file succesfully converted !!!\n\n");

/* closing the DB */
  stop:
    sqlite3_close (handle);
    return 0;
}
