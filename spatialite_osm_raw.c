/* 
/ spatialite_osm_raw
/
/ a tool loading "raw" OSM-XML maps into a SpatiaLite DB
/
/ version 1.0, 2010 September 13
/
/ Author: Sandro Furieri a.furieri@lqt.it
/
/ Copyright (C) 2010  Alessandro Furieri
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
#include <float.h>

#include <expat.h>

#ifdef SPATIALITE_AMALGAMATION
#include <spatialite/sqlite3.h>
#else
#include <sqlite3.h>
#endif

#include <spatialite/gaiageo.h>
#include <spatialite.h>

#define ARG_NONE		0
#define ARG_OSM_PATH	1
#define ARG_DB_PATH		2

#define MAX_TAG		16

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	_stricmp
#endif /* not WIN32 */

#if defined(_WIN32)
#define atol_64		_atoi64
#else
#define atol_64		atoll
#endif

#define BUFFSIZE	8192

#define CURRENT_TAG_UNKNOWN	0
#define CURRENT_TAG_IS_NODE	1
#define CURRENT_TAG_IS_WAY	2
#define CURRENT_TAG_IS_RELATION	3

struct aux_params
{
/* an auxiliary struct used for XML parsing */
    sqlite3 *db_handle;
    sqlite3_stmt *ins_nodes_stmt;
    sqlite3_stmt *ins_node_tags_stmt;
    sqlite3_stmt *ins_ways_stmt;
    sqlite3_stmt *ins_way_tags_stmt;
    sqlite3_stmt *ins_way_node_refs_stmt;
    sqlite3_stmt *ins_relations_stmt;
    sqlite3_stmt *ins_relation_tags_stmt;
    sqlite3_stmt *ins_relation_node_refs_stmt;
    sqlite3_stmt *ins_relation_way_refs_stmt;
    sqlite3_stmt *ins_relation_relation_refs_stmt;
    int wr_nodes;
    int wr_node_tags;
    int wr_ways;
    int wr_way_tags;
    int wr_way_node_refs;
    int wr_relations;
    int wr_rel_tags;
    int wr_rel_node_refs;
    int wr_rel_way_refs;
    int wr_rel_rel_refs;
    int current_tag;
};

struct tag
{
    char *k;
    char *v;
    struct tag *next;
};

struct node
{
    sqlite3_int64 id;
    sqlite3_int64 version;
    char *timestamp;
    sqlite3_int64 uid;
    char *user;
    sqlite3_int64 changeset;
    double lat;
    double lon;
    struct tag *first;
    struct tag *last;
} glob_node;

struct node_ref
{
    sqlite3_int64 node_id;
    char *role;
    struct node_ref *next;
};

struct way
{
    sqlite3_int64 id;
    sqlite3_int64 version;
    char *timestamp;
    sqlite3_int64 uid;
    char *user;
    sqlite3_int64 changeset;
    struct tag *first;
    struct tag *last;
    struct node_ref *first_node;
    struct node_ref *last_node;
} glob_way;

struct way_ref
{
    sqlite3_int64 way_id;
    char *role;
    struct way_ref *next;
};

struct rel_ref
{
    sqlite3_int64 rel_id;
    char *role;
    struct rel_ref *next;
};

struct relation
{
    sqlite3_int64 id;
    sqlite3_int64 version;
    char *timestamp;
    sqlite3_int64 uid;
    char *user;
    sqlite3_int64 changeset;
    struct tag *first;
    struct tag *last;
    struct node_ref *first_node;
    struct node_ref *last_node;
    struct way_ref *first_way;
    struct way_ref *last_way;
    struct rel_ref *first_rel;
    struct rel_ref *last_rel;
} glob_relation;

static void
insert_node (struct aux_params *params)
{
    int ret;
    unsigned char *blob;
    int blob_size;
    int sub = 0;
    struct tag *p_tag;
    gaiaGeomCollPtr geom = gaiaAllocGeomColl ();
    geom->Srid = 4326;
    gaiaAddPointToGeomColl (geom, glob_node.lon, glob_node.lat);
    sqlite3_reset (params->ins_nodes_stmt);
    sqlite3_clear_bindings (params->ins_nodes_stmt);
    sqlite3_bind_int64 (params->ins_nodes_stmt, 1, glob_node.id);
    sqlite3_bind_int64 (params->ins_nodes_stmt, 2, glob_node.version);
    if (glob_node.timestamp == NULL)
	sqlite3_bind_null (params->ins_nodes_stmt, 3);
    else
	sqlite3_bind_text (params->ins_nodes_stmt, 3, glob_node.timestamp,
			   strlen (glob_node.timestamp), SQLITE_STATIC);
    sqlite3_bind_int64 (params->ins_nodes_stmt, 4, glob_node.uid);
    if (glob_node.user == NULL)
	sqlite3_bind_null (params->ins_nodes_stmt, 5);
    else
	sqlite3_bind_text (params->ins_nodes_stmt, 5, glob_node.user,
			   strlen (glob_node.user), SQLITE_STATIC);
    sqlite3_bind_int64 (params->ins_nodes_stmt, 6, glob_node.changeset);
    gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
    gaiaFreeGeomColl (geom);
    sqlite3_bind_blob (params->ins_nodes_stmt, 7, blob, blob_size, free);
    ret = sqlite3_step (params->ins_nodes_stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  fprintf (stderr, "sqlite3_step() error: INSERT INTO osm_nodes\n");
	  return;
      }
    params->wr_nodes += 1;

    p_tag = glob_node.first;
    while (p_tag)
      {
	  sqlite3_reset (params->ins_node_tags_stmt);
	  sqlite3_clear_bindings (params->ins_node_tags_stmt);
	  sqlite3_bind_int64 (params->ins_node_tags_stmt, 1, glob_node.id);
	  sqlite3_bind_int (params->ins_node_tags_stmt, 2, sub);
	  sub++;
	  if (p_tag->k == NULL)
	      sqlite3_bind_null (params->ins_node_tags_stmt, 3);
	  else
	      sqlite3_bind_text (params->ins_node_tags_stmt, 3, p_tag->k,
				 strlen (p_tag->k), SQLITE_STATIC);
	  if (p_tag->k == NULL)
	      sqlite3_bind_null (params->ins_node_tags_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_node_tags_stmt, 4, p_tag->v,
				 strlen (p_tag->v), SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_node_tags_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_node_tags\n");
		return;
	    }
	  params->wr_node_tags += 1;
	  p_tag = p_tag->next;
      }
}

static void
insert_way (struct aux_params *params)
{
    int ret;
    int sub = 0;
    struct tag *p_tag;
    struct node_ref *nr;
    sqlite3_reset (params->ins_ways_stmt);
    sqlite3_clear_bindings (params->ins_ways_stmt);
    sqlite3_bind_int64 (params->ins_ways_stmt, 1, glob_way.id);
    sqlite3_bind_int64 (params->ins_ways_stmt, 2, glob_way.version);
    if (glob_way.timestamp == NULL)
	sqlite3_bind_null (params->ins_ways_stmt, 3);
    else
	sqlite3_bind_text (params->ins_ways_stmt, 3, glob_way.timestamp,
			   strlen (glob_way.timestamp), SQLITE_STATIC);
    sqlite3_bind_int64 (params->ins_ways_stmt, 4, glob_way.uid);
    if (glob_way.user == NULL)
	sqlite3_bind_null (params->ins_ways_stmt, 5);
    else
	sqlite3_bind_text (params->ins_ways_stmt, 5, glob_way.user,
			   strlen (glob_way.user), SQLITE_STATIC);
    sqlite3_bind_int64 (params->ins_ways_stmt, 6, glob_way.changeset);
    ret = sqlite3_step (params->ins_ways_stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  fprintf (stderr, "sqlite3_step() error: INSERT INTO osm_ways\n");
	  return;
      }
    params->wr_ways += 1;

    p_tag = glob_way.first;
    while (p_tag)
      {
	  sqlite3_reset (params->ins_way_tags_stmt);
	  sqlite3_clear_bindings (params->ins_way_tags_stmt);
	  sqlite3_bind_int64 (params->ins_way_tags_stmt, 1, glob_way.id);
	  sqlite3_bind_int (params->ins_way_tags_stmt, 2, sub);
	  sub++;
	  if (p_tag->k == NULL)
	      sqlite3_bind_null (params->ins_way_tags_stmt, 3);
	  else
	      sqlite3_bind_text (params->ins_way_tags_stmt, 3, p_tag->k,
				 strlen (p_tag->k), SQLITE_STATIC);
	  if (p_tag->v == NULL)
	      sqlite3_bind_null (params->ins_way_tags_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_way_tags_stmt, 4, p_tag->v,
				 strlen (p_tag->v), SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_way_tags_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_way_tags\n");
		return;
	    }
	  params->wr_way_tags += 1;
	  p_tag = p_tag->next;
      }

    sub = 0;
    nr = glob_way.first_node;
    while (nr)
      {
	  sqlite3_reset (params->ins_way_node_refs_stmt);
	  sqlite3_clear_bindings (params->ins_way_node_refs_stmt);
	  sqlite3_bind_int64 (params->ins_way_node_refs_stmt, 1, glob_way.id);
	  sqlite3_bind_int (params->ins_way_node_refs_stmt, 2, sub);
	  sub++;
	  sqlite3_bind_int64 (params->ins_way_node_refs_stmt, 3, nr->node_id);
	  ret = sqlite3_step (params->ins_way_node_refs_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_way_node_refs\n");
		return;
	    }
	  params->wr_way_node_refs += 1;
	  nr = nr->next;
      }
}

static void
insert_relation (struct aux_params *params)
{
    int ret;
    int sub = 0;
    struct tag *p_tag;
    struct node_ref *nr;
    struct way_ref *wr;
    struct rel_ref *rr;
    sqlite3_reset (params->ins_relations_stmt);
    sqlite3_clear_bindings (params->ins_relations_stmt);
    sqlite3_bind_int64 (params->ins_relations_stmt, 1, glob_relation.id);
    sqlite3_bind_int64 (params->ins_relations_stmt, 2, glob_relation.version);
    if (glob_relation.timestamp == NULL)
	sqlite3_bind_null (params->ins_relations_stmt, 3);
    else
	sqlite3_bind_text (params->ins_relations_stmt, 3,
			   glob_relation.timestamp,
			   strlen (glob_relation.timestamp), SQLITE_STATIC);
    sqlite3_bind_int64 (params->ins_relations_stmt, 4, glob_relation.uid);
    if (glob_relation.user == NULL)
	sqlite3_bind_null (params->ins_relations_stmt, 5);
    else
	sqlite3_bind_text (params->ins_relations_stmt, 5, glob_relation.user,
			   strlen (glob_relation.user), SQLITE_STATIC);
    sqlite3_bind_int64 (params->ins_relations_stmt, 6, glob_relation.changeset);
    ret = sqlite3_step (params->ins_relations_stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  fprintf (stderr, "sqlite3_step() error: INSERT INTO osm_relations\n");
	  return;
      }
    params->wr_relations += 1;

    p_tag = glob_relation.first;
    while (p_tag)
      {
	  sqlite3_reset (params->ins_relation_tags_stmt);
	  sqlite3_clear_bindings (params->ins_relation_tags_stmt);
	  sqlite3_bind_int64 (params->ins_relation_tags_stmt, 1,
			      glob_relation.id);
	  sqlite3_bind_int (params->ins_relation_tags_stmt, 2, sub);
	  sub++;
	  if (p_tag->k == NULL)
	      sqlite3_bind_null (params->ins_relation_tags_stmt, 3);
	  else
	      sqlite3_bind_text (params->ins_relation_tags_stmt, 3, p_tag->k,
				 strlen (p_tag->k), SQLITE_STATIC);
	  if (p_tag->v == NULL)
	      sqlite3_bind_null (params->ins_relation_tags_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_relation_tags_stmt, 4, p_tag->v,
				 strlen (p_tag->v), SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_relation_tags_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_relation_tags\n");
		return;
	    }
	  params->wr_rel_tags += 1;
	  p_tag = p_tag->next;
      }

    sub = 0;
    nr = glob_relation.first_node;
    while (nr)
      {
	  sqlite3_reset (params->ins_relation_node_refs_stmt);
	  sqlite3_clear_bindings (params->ins_relation_node_refs_stmt);
	  sqlite3_bind_int64 (params->ins_relation_node_refs_stmt, 1,
			      glob_relation.id);
	  sqlite3_bind_int (params->ins_relation_node_refs_stmt, 2, sub);
	  sub++;
	  sqlite3_bind_int64 (params->ins_relation_node_refs_stmt, 3,
			      nr->node_id);
	  if (nr->role == NULL)
	      sqlite3_bind_null (params->ins_relation_node_refs_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_relation_node_refs_stmt, 4,
				 nr->role, strlen (nr->role), SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_relation_node_refs_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_relation_node_refs\n");
		return;
	    }
	  params->wr_rel_node_refs += 1;
	  nr = nr->next;
      }

    sub = 0;
    wr = glob_relation.first_way;
    while (wr)
      {
	  sqlite3_reset (params->ins_relation_way_refs_stmt);
	  sqlite3_clear_bindings (params->ins_relation_way_refs_stmt);
	  sqlite3_bind_int64 (params->ins_relation_way_refs_stmt, 1,
			      glob_relation.id);
	  sqlite3_bind_int (params->ins_relation_way_refs_stmt, 2, sub);
	  sub++;
	  sqlite3_bind_int64 (params->ins_relation_way_refs_stmt, 3,
			      wr->way_id);
	  if (wr->role == NULL)
	      sqlite3_bind_null (params->ins_relation_way_refs_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_relation_way_refs_stmt, 4,
				 wr->role, strlen (wr->role), SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_relation_way_refs_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_relation_way_refs\n");
		return;
	    }
	  params->wr_rel_way_refs += 1;
	  wr = wr->next;
      }

    sub = 0;
    rr = glob_relation.first_rel;
    while (rr)
      {
	  sqlite3_reset (params->ins_relation_relation_refs_stmt);
	  sqlite3_clear_bindings (params->ins_relation_relation_refs_stmt);
	  sqlite3_bind_int64 (params->ins_relation_relation_refs_stmt, 1,
			      glob_relation.id);
	  sqlite3_bind_int (params->ins_relation_relation_refs_stmt, 2, sub);
	  sub++;
	  sqlite3_bind_int64 (params->ins_relation_relation_refs_stmt, 3,
			      rr->rel_id);
	  if (rr->role == NULL)
	      sqlite3_bind_null (params->ins_relation_relation_refs_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_relation_relation_refs_stmt, 4,
				 rr->role, strlen (rr->role), SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_relation_relation_refs_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		fprintf (stderr,
			 "sqlite3_step() error: INSERT INTO osm_relation_relation_refs\n");
		return;
	    }
	  params->wr_rel_rel_refs += 1;
	  rr = rr->next;
      }
}

static void
clean_node ()
{
/* cleaning the current node */
    struct tag *pt;
    struct tag *ptn;
    if (glob_node.timestamp)
	free (glob_node.timestamp);
    if (glob_node.user)
	free (glob_node.user);
    pt = glob_node.first;
    while (pt)
      {
	  ptn = pt->next;
	  if (pt->k)
	      free (pt->k);
	  if (pt->v)
	      free (pt->v);
	  free (pt);
	  pt = ptn;
      }
    glob_node.timestamp = NULL;
    glob_node.user = NULL;
    glob_node.first = NULL;
    glob_node.last = NULL;
}

static void
clean_way ()
{
/* cleaning the current way */
    struct tag *pt;
    struct tag *ptn;
    struct node_ref *nr;
    struct node_ref *nrn;
    if (glob_way.timestamp)
	free (glob_way.timestamp);
    if (glob_way.user)
	free (glob_way.user);
    pt = glob_way.first;
    while (pt)
      {
	  ptn = pt->next;
	  if (pt->k)
	      free (pt->k);
	  if (pt->v)
	      free (pt->v);
	  free (pt);
	  pt = ptn;
      }
    nr = glob_way.first_node;
    while (nr)
      {
	  nrn = nr->next;
	  free (nr);
	  nr = nrn;
      }
    glob_way.timestamp = NULL;
    glob_way.user = NULL;
    glob_way.first = NULL;
    glob_way.last = NULL;
    glob_way.first_node = NULL;
    glob_way.last_node = NULL;
}

static void
clean_relation ()
{
/* cleaning the current relation */
    struct tag *pt;
    struct tag *ptn;
    struct node_ref *nr;
    struct node_ref *nrn;
    struct way_ref *wr;
    struct way_ref *wrn;
    struct rel_ref *rr;
    struct rel_ref *rrn;
    if (glob_relation.timestamp)
	free (glob_relation.timestamp);
    if (glob_relation.user)
	free (glob_relation.user);
    pt = glob_relation.first;
    while (pt)
      {
	  ptn = pt->next;
	  if (pt->k)
	      free (pt->k);
	  if (pt->v)
	      free (pt->v);
	  free (pt);
	  pt = ptn;
      }
    nr = glob_relation.first_node;
    while (nr)
      {
	  nrn = nr->next;
	  if (nr->role)
	      free (nr->role);
	  free (nr);
	  nr = nrn;
      }
    wr = glob_relation.first_way;
    while (wr)
      {
	  wrn = wr->next;
	  if (wr->role)
	      free (wr->role);
	  free (wr);
	  wr = wrn;
      }
    rr = glob_relation.first_rel;
    while (rr)
      {
	  rrn = rr->next;
	  if (rr->role)
	      free (rr->role);
	  free (rr);
	  rr = rrn;
      }
    glob_relation.timestamp = NULL;
    glob_relation.user = NULL;
    glob_relation.first = NULL;
    glob_relation.last = NULL;
    glob_relation.first_node = NULL;
    glob_relation.last_node = NULL;
    glob_relation.first_way = NULL;
    glob_relation.last_way = NULL;
    glob_relation.first_rel = NULL;
    glob_relation.last_rel = NULL;
}

static void
start_node (struct aux_params *params, const char **attr)
{
    int i;
    int len;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      glob_node.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "version") == 0)
	      glob_node.version = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "timestamp") == 0)
	    {
		if (glob_node.timestamp)
		    free (glob_node.timestamp);
		len = strlen (attr[i + 1]);
		glob_node.timestamp = malloc (len + 1);
		strcpy (glob_node.timestamp, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "uid") == 0)
	      glob_node.uid = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "user") == 0)
	    {
		if (glob_node.user)
		    free (glob_node.user);
		len = strlen (attr[i + 1]);
		glob_node.user = malloc (len + 1);
		strcpy (glob_node.user, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "changeset") == 0)
	      glob_node.changeset = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "lat") == 0)
	      glob_node.lat = atof (attr[i + 1]);
	  if (strcmp (attr[i], "lon") == 0)
	      glob_node.lon = atof (attr[i + 1]);
      }
    params->current_tag = CURRENT_TAG_IS_NODE;
}

static void
end_node (struct aux_params *params)
{
    insert_node (params);
    clean_node ();
    params->current_tag = CURRENT_TAG_UNKNOWN;
}

static void
start_way (struct aux_params *params, const char **attr)
{
    int i;
    int len;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      glob_way.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "version") == 0)
	      glob_way.version = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "timestamp") == 0)
	    {
		if (glob_way.timestamp)
		    free (glob_way.timestamp);
		len = strlen (attr[i + 1]);
		glob_way.timestamp = malloc (len + 1);
		strcpy (glob_way.timestamp, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "uid") == 0)
	      glob_way.uid = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "user") == 0)
	    {
		if (glob_way.user)
		    free (glob_way.user);
		len = strlen (attr[i + 1]);
		glob_way.user = malloc (len + 1);
		strcpy (glob_way.user, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "changeset") == 0)
	      glob_way.changeset = atol_64 (attr[i + 1]);
      }
    params->current_tag = CURRENT_TAG_IS_WAY;
}

static void
end_way (struct aux_params *params)
{
    insert_way (params);
    clean_way ();
    params->current_tag = CURRENT_TAG_UNKNOWN;
}

static void
start_relation (struct aux_params *params, const char **attr)
{
    int i;
    int len;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      glob_relation.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "version") == 0)
	      glob_relation.version = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "timestamp") == 0)
	    {
		if (glob_relation.timestamp)
		    free (glob_relation.timestamp);
		len = strlen (attr[i + 1]);
		glob_relation.timestamp = malloc (len + 1);
		strcpy (glob_relation.timestamp, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "uid") == 0)
	      glob_relation.uid = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "user") == 0)
	    {
		if (glob_relation.user)
		    free (glob_relation.user);
		len = strlen (attr[i + 1]);
		glob_relation.user = malloc (len + 1);
		strcpy (glob_relation.user, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "changeset") == 0)
	      glob_relation.changeset = atol_64 (attr[i + 1]);
      }
    params->current_tag = CURRENT_TAG_IS_RELATION;
}

static void
end_relation (struct aux_params *params)
{
    insert_relation (params);
    clean_relation ();
    params->current_tag = CURRENT_TAG_UNKNOWN;
}

static void
start_xtag (struct aux_params *params, const char **attr)
{
    int i;
    int len;
    const char *k = NULL;
    const char *v = NULL;
    struct tag *p_tag;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "k") == 0)
	      k = attr[i + 1];
	  if (strcmp (attr[i], "v") == 0)
	      v = attr[i + 1];
      }

/* appending a new tag to the current item */
    if (params->current_tag == CURRENT_TAG_IS_NODE)
      {
	  p_tag = malloc (sizeof (struct tag));
	  if (glob_node.first == NULL)
	      glob_node.first = p_tag;
	  if (glob_node.last != NULL)
	      glob_node.last->next = p_tag;
	  glob_node.last = p_tag;
	  p_tag->next = NULL;
	  len = strlen (k);
	  p_tag->k = malloc (len + 1);
	  strcpy (p_tag->k, k);
	  len = strlen (v);
	  p_tag->v = malloc (len + 1);
	  strcpy (p_tag->v, v);
      }
    if (params->current_tag == CURRENT_TAG_IS_WAY)
      {
	  p_tag = malloc (sizeof (struct tag));
	  if (glob_way.first == NULL)
	      glob_way.first = p_tag;
	  if (glob_way.last != NULL)
	      glob_way.last->next = p_tag;
	  glob_way.last = p_tag;
	  p_tag->next = NULL;
	  len = strlen (k);
	  p_tag->k = malloc (len + 1);
	  strcpy (p_tag->k, k);
	  len = strlen (v);
	  p_tag->v = malloc (len + 1);
	  strcpy (p_tag->v, v);
      }
    if (params->current_tag == CURRENT_TAG_IS_RELATION)
      {
	  p_tag = malloc (sizeof (struct tag));
	  if (glob_relation.first == NULL)
	      glob_relation.first = p_tag;
	  if (glob_relation.last != NULL)
	      glob_relation.last->next = p_tag;
	  glob_relation.last = p_tag;
	  p_tag->next = NULL;
	  len = strlen (k);
	  p_tag->k = malloc (len + 1);
	  strcpy (p_tag->k, k);
	  len = strlen (v);
	  p_tag->v = malloc (len + 1);
	  strcpy (p_tag->v, v);
      }
}

static void
start_nd (struct aux_params *params, const char **attr)
{
    int i;
    sqlite3_int64 node_id;
    struct node_ref *nr;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "ref") == 0)
	      node_id = atol_64 (attr[i + 1]);
      }
/* appending a new node-ref-id to the current item */
    if (params->current_tag == CURRENT_TAG_IS_WAY)
      {
	  nr = malloc (sizeof (struct node_ref));
	  if (glob_way.first_node == NULL)
	      glob_way.first_node = nr;
	  if (glob_way.last_node != NULL)
	      glob_way.last_node->next = nr;
	  glob_way.last_node = nr;
	  nr->next = NULL;
	  nr->node_id = node_id;
      }
}

static void
start_member (struct aux_params *params, const char **attr)
{
    int i;
    sqlite3_int64 id;
    const char *role = NULL;
    int len;
    int is_way = 0;
    int is_node = 0;
    int is_rel = 0;
    struct node_ref *nr;
    struct way_ref *wr;
    struct rel_ref *rr;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "type") == 0)
	    {
		if (strcmp (attr[i + 1], "way") == 0)
		    is_way = 1;
		if (strcmp (attr[i + 1], "node") == 0)
		    is_node = 1;
		if (strcmp (attr[i + 1], "relation") == 0)
		    is_rel = 1;
	    }
	  if (strcmp (attr[i], "ref") == 0)
	      id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "role") == 0)
	      role = attr[i + 1];
      }
    if (is_node)
      {
/* appending a new node-ref-id to the current item */
	  if (params->current_tag == CURRENT_TAG_IS_RELATION)
	    {
		nr = malloc (sizeof (struct node_ref));
		if (glob_relation.first_node == NULL)
		    glob_relation.first_node = nr;
		if (glob_relation.last_node != NULL)
		    glob_relation.last_node->next = nr;
		glob_relation.last_node = nr;
		nr->next = NULL;
		nr->node_id = id;
		nr->role = NULL;
		if (role)
		  {
		      len = strlen (role);
		      nr->role = malloc (len + 1);
		      strcpy (nr->role, role);
		  }
	    }
      }
    if (is_way)
      {
/* appending a new way-ref-id to the current item */
	  if (params->current_tag == CURRENT_TAG_IS_RELATION)
	    {
		wr = malloc (sizeof (struct way_ref));
		if (glob_relation.first_way == NULL)
		    glob_relation.first_way = wr;
		if (glob_relation.last_way != NULL)
		    glob_relation.last_way->next = wr;
		glob_relation.last_way = wr;
		wr->next = NULL;
		wr->way_id = id;
		wr->role = NULL;
		if (role)
		  {
		      len = strlen (role);
		      wr->role = malloc (len + 1);
		      strcpy (wr->role, role);
		  }
	    }
      }
    if (is_rel)
      {
/* appending a new relation-ref-id to the current item */
	  if (params->current_tag == CURRENT_TAG_IS_RELATION)
	    {
		rr = malloc (sizeof (struct rel_ref));
		if (glob_relation.first_rel == NULL)
		    glob_relation.first_rel = rr;
		if (glob_relation.last_rel != NULL)
		    glob_relation.last_rel->next = rr;
		glob_relation.last_rel = rr;
		rr->next = NULL;
		rr->rel_id = id;
		rr->role = NULL;
		if (role)
		  {
		      len = strlen (role);
		      rr->role = malloc (len + 1);
		      strcpy (rr->role, role);
		  }
	    }
      }
}

static void
start_tag (void *data, const char *el, const char **attr)
{
    struct aux_params *params = (struct aux_params *) data;
    if (strcmp (el, "node") == 0)
	start_node (params, attr);
    if (strcmp (el, "way") == 0)
	start_way (params, attr);
    if (strcmp (el, "relation") == 0)
	start_relation (params, attr);
    if (strcmp (el, "tag") == 0)
	start_xtag (params, attr);
    if (strcmp (el, "nd") == 0)
	start_nd (params, attr);
    if (strcmp (el, "member") == 0)
	start_member (params, attr);
}

static void
end_tag (void *data, const char *el)
{
    struct aux_params *params = (struct aux_params *) data;
    if (strcmp (el, "node") == 0)
	end_node (params);
    if (strcmp (el, "way") == 0)
	end_way (params);
    if (strcmp (el, "relation") == 0)
	end_relation (params);
}

static void
finalize_sql_stmts (struct aux_params *params)
{
    int ret;
    char *sql_err = NULL;

    if (params->ins_nodes_stmt != NULL)
	sqlite3_finalize (params->ins_nodes_stmt);
    if (params->ins_node_tags_stmt != NULL)
	sqlite3_finalize (params->ins_node_tags_stmt);
    if (params->ins_ways_stmt != NULL)
	sqlite3_finalize (params->ins_ways_stmt);
    if (params->ins_way_tags_stmt != NULL)
	sqlite3_finalize (params->ins_way_tags_stmt);
    if (params->ins_way_node_refs_stmt != NULL)
	sqlite3_finalize (params->ins_way_node_refs_stmt);
    if (params->ins_relations_stmt != NULL)
	sqlite3_finalize (params->ins_relations_stmt);
    if (params->ins_relation_tags_stmt != NULL)
	sqlite3_finalize (params->ins_relation_tags_stmt);
    if (params->ins_relation_node_refs_stmt != NULL)
	sqlite3_finalize (params->ins_relation_node_refs_stmt);
    if (params->ins_relation_way_refs_stmt != NULL)
	sqlite3_finalize (params->ins_relation_way_refs_stmt);
    if (params->ins_relation_relation_refs_stmt != NULL)
	sqlite3_finalize (params->ins_relation_relation_refs_stmt);

/* committing the still pending SQL Transaction */
    ret = sqlite3_exec (params->db_handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
}

static void
create_sql_stmts (struct aux_params *params)
{
    sqlite3_stmt *ins_nodes_stmt;
    sqlite3_stmt *ins_node_tags_stmt;
    sqlite3_stmt *ins_ways_stmt;
    sqlite3_stmt *ins_way_tags_stmt;
    sqlite3_stmt *ins_way_node_refs_stmt;
    sqlite3_stmt *ins_relations_stmt;
    sqlite3_stmt *ins_relation_tags_stmt;
    sqlite3_stmt *ins_relation_node_refs_stmt;
    sqlite3_stmt *ins_relation_way_refs_stmt;
    sqlite3_stmt *ins_relation_relation_refs_stmt;
    char sql[1024];
    int ret;
    char *sql_err = NULL;

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (params->db_handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO osm_nodes (node_id, version, timestamp, uid, user, changeset, Geometry) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_nodes_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO osm_node_tags (node_id, sub, k, v) ");
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_node_tags_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO osm_ways (way_id, version, timestamp, uid, user, changeset) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_ways_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO osm_way_tags (way_id, sub, k, v) ");
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_way_tags_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO osm_way_node_refs (way_id, sub, node_id) ");
    strcat (sql, "VALUES (?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_way_node_refs_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO osm_relations (rel_id, version, timestamp, uid, user, changeset) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_relations_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO osm_relation_tags (rel_id, sub, k, v) ");
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_relation_tags_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO osm_relation_node_refs (rel_id, sub, node_id, role) ");
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_relation_node_refs_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO osm_relation_way_refs (rel_id, sub, way_id, role) ");
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_relation_way_refs_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO osm_relation_relation_refs (rel_id, sub, relation_id, role) ");
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_relation_relation_refs_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }

    params->ins_nodes_stmt = ins_nodes_stmt;
    params->ins_node_tags_stmt = ins_node_tags_stmt;
    params->ins_ways_stmt = ins_ways_stmt;
    params->ins_way_tags_stmt = ins_way_tags_stmt;
    params->ins_way_node_refs_stmt = ins_way_node_refs_stmt;
    params->ins_relations_stmt = ins_relations_stmt;
    params->ins_relation_tags_stmt = ins_relation_tags_stmt;
    params->ins_relation_node_refs_stmt = ins_relation_node_refs_stmt;
    params->ins_relation_way_refs_stmt = ins_relation_way_refs_stmt;
    params->ins_relation_relation_refs_stmt = ins_relation_relation_refs_stmt;
}

static void
spatialite_autocreate (sqlite3 * db)
{
/* attempting to perform self-initialization for a newly created DB */
    int ret;
    char sql[1024];
    char *err_msg = NULL;
    int count;
    int i;
    char **results;
    int rows;
    int columns;

/* checking if this DB is really empty */
    strcpy (sql, "SELECT Count(*) from sqlite_master");
    ret = sqlite3_get_table (db, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	      count = atoi (results[(i * columns) + 0]);
      }
    sqlite3_free_table (results);

    if (count > 0)
	return;

/* all right, it's empty: proceding to initialize */
    strcpy (sql, "SELECT InitSpatialMetadata()");
    ret = sqlite3_exec (db, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "InitSpatialMetadata() error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    spatial_ref_sys_init (db, 0);
}

static void
open_db (const char *path, sqlite3 ** handle)
{
/* opening the DB */
    sqlite3 *db_handle;
    int ret;
    char sql[1024];
    char *err_msg = NULL;
    int spatialite_rs = 0;
    int spatialite_gc = 0;
    int rs_srid = 0;
    int auth_name = 0;
    int auth_srid = 0;
    int ref_sys_name = 0;
    int proj4text = 0;
    int f_table_name = 0;
    int f_geometry_column = 0;
    int coord_dimension = 0;
    int gc_srid = 0;
    int type = 0;
    int spatial_index_enabled = 0;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;

    *handle = NULL;
    spatialite_init (0);
    printf ("SQLite version: %s\n", sqlite3_libversion ());
    printf ("SpatiaLite version: %s\n\n", spatialite_version ());

    ret =
	sqlite3_open_v2 (path, &db_handle,
			 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", path,
		   sqlite3_errmsg (db_handle));
	  sqlite3_close (db_handle);
	  return;
      }
    spatialite_autocreate (db_handle);

/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "PRAGMA table_info(geometry_columns)");
    ret = sqlite3_get_table (db_handle, sql, &results, &rows, &columns, NULL);
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
		if (strcasecmp (name, "spatial_index_enabled") == 0)
		    spatial_index_enabled = 1;
	    }
      }
    sqlite3_free_table (results);
    if (f_table_name && f_geometry_column && type && coord_dimension
	&& gc_srid && spatial_index_enabled)
	spatialite_gc = 1;

/* checking the SPATIAL_REF_SYS table */
    strcpy (sql, "PRAGMA table_info(spatial_ref_sys)");
    ret = sqlite3_get_table (db_handle, sql, &results, &rows, &columns, NULL);
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
	    }
      }
    sqlite3_free_table (results);
    if (rs_srid && auth_name && auth_srid && ref_sys_name && proj4text)
	spatialite_rs = 1;
/* verifying the MetaData format */
    if (spatialite_gc && spatialite_rs)
	;
    else
	goto unknown;

/* creating the OSM "raw" nodes */
    strcpy (sql, "CREATE TABLE osm_nodes (\n");
    strcat (sql, "node_id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "version INTEGER NOT NULL,\n");
    strcat (sql, "timestamp TEXT,\n");
    strcat (sql, "uid INTEGER NOT NULL,\n");
    strcat (sql, "user TEXT,\n");
    strcat (sql, "changeset INTEGER NOT NULL)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_nodes' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
    strcpy (sql,
	    "SELECT AddGeometryColumn('osm_nodes', 'Geometry', 4326, 'POINT', 'XY')");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_nodes' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" node tags */
    strcpy (sql, "CREATE TABLE osm_node_tags (\n");
    strcat (sql, "node_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "k TEXT,\n");
    strcat (sql, "v TEXT,\n");
    strcat (sql, "CONSTRAINT pk_osm_nodetags PRIMARY KEY (node_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_nodetags FOREIGN KEY (node_id) ");
    strcat (sql, "REFERENCES osm_nodes (node_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_node_tags' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" ways */
    strcpy (sql, "CREATE TABLE osm_ways (\n");
    strcat (sql, "way_id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "version INTEGER NOT NULL,\n");
    strcat (sql, "timestamp TEXT,\n");
    strcat (sql, "uid INTEGER NOT NULL,\n");
    strcat (sql, "user TEXT,\n");
    strcat (sql, "changeset INTEGER NOT NULL)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_ways' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" way tags */
    strcpy (sql, "CREATE TABLE osm_way_tags (\n");
    strcat (sql, "way_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "k TEXT,\n");
    strcat (sql, "v TEXT,\n");
    strcat (sql, "CONSTRAINT pk_osm_waytags PRIMARY KEY (way_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_waytags FOREIGN KEY (way_id) ");
    strcat (sql, "REFERENCES osm_ways (way_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_way_tags' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" way-node refs */
    strcpy (sql, "CREATE TABLE osm_way_node_refs (\n");
    strcat (sql, "way_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "node_id INTEGER NOT NULL,\n");
    strcat (sql, "CONSTRAINT pk_osm_waynoderefs PRIMARY KEY (way_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_waynoderefs FOREIGN KEY (way_id) ");
    strcat (sql, "REFERENCES osm_ways (way_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_way_node_refs' error: %s\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" relations */
    strcpy (sql, "CREATE TABLE osm_relations (\n");
    strcat (sql, "rel_id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "version INTEGER NOT NULL,\n");
    strcat (sql, "timestamp TEXT,\n");
    strcat (sql, "uid INTEGER NOT NULL,\n");
    strcat (sql, "user TEXT,\n");
    strcat (sql, "changeset INTEGER NOT NULL)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_relations' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" relation tags */
    strcpy (sql, "CREATE TABLE osm_relation_tags (\n");
    strcat (sql, "rel_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "k TEXT,\n");
    strcat (sql, "v TEXT,\n");
    strcat (sql, "CONSTRAINT pk_osm_reltags PRIMARY KEY (rel_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_reltags FOREIGN KEY (rel_id) ");
    strcat (sql, "REFERENCES osm_relations (rel_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_relation_tags' error: %s\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" relation-node refs */
    strcpy (sql, "CREATE TABLE osm_relation_node_refs (\n");
    strcat (sql, "rel_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "node_id INTEGER NOT NULL,\n");
    strcat (sql, "role TEXT,");
    strcat (sql, "CONSTRAINT pk_osm_relnoderefs PRIMARY KEY (rel_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_relnoderefs FOREIGN KEY (rel_id) ");
    strcat (sql, "REFERENCES osm_relations (rel_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_relation_node_refs' error: %s\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" relation-way refs */
    strcpy (sql, "CREATE TABLE osm_relation_way_refs (\n");
    strcat (sql, "rel_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "way_id INTEGER NOT NULL,\n");
    strcat (sql, "role TEXT,");
    strcat (sql, "CONSTRAINT pk_osm_relwayrefs PRIMARY KEY (rel_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_relwayrefs FOREIGN KEY (rel_id) ");
    strcat (sql, "REFERENCES osm_relations (rel_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_relation_way_refs' error: %s\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM "raw" relation-relation refs */
    strcpy (sql, "CREATE TABLE osm_relation_relation_refs (\n");
    strcat (sql, "rel_id INTEGER NOT NULL,\n");
    strcat (sql, "sub INTEGER NOT NULL,\n");
    strcat (sql, "relation_id INTEGER NOT NULL,\n");
    strcat (sql, "role TEXT,");
    strcat (sql, "CONSTRAINT pk_osm_relrelrefs PRIMARY KEY (rel_id, sub),\n");
    strcat (sql, "CONSTRAINT fk_osm_relrelrefs FOREIGN KEY (rel_id) ");
    strcat (sql, "REFERENCES osm_relations (rel_id))\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr,
		   "CREATE TABLE 'osm_relation_relation_refs' error: %s\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }

    *handle = db_handle;
    return;

  unknown:
    if (db_handle)
	sqlite3_close (db_handle);
    fprintf (stderr, "DB '%s'\n", path);
    fprintf (stderr, "doesn't seems to contain valid Spatial Metadata ...\n\n");
    fprintf (stderr, "Please, initialize Spatial Metadata\n\n");
    return;
}

static void
do_help ()
{
/* printing the argument list */
    fprintf (stderr, "\n\nusage: spatialite_osm_raw ARGLIST\n");
    fprintf (stderr,
	     "==============================================================\n");
    fprintf (stderr,
	     "-h or --help                    print this help message\n");
    fprintf (stderr, "-o or --osm-path pathname       the OSM-XML file path\n");
    fprintf (stderr,
	     "-d or --db-path  pathname       the SpatiaLite DB path\n\n");
    fprintf (stderr, "you can specify the following options as well\n");
    fprintf (stderr,
	     "-m or --in-memory               using IN-MEMORY database\n");
}

int
main (int argc, char *argv[])
{
/* the MAIN function simply perform arguments checking */
    sqlite3 *handle;
    int i;
    int next_arg = ARG_NONE;
    const char *osm_path = NULL;
    const char *db_path = NULL;
    int in_memory = 0;
    int error = 0;
    char Buff[BUFFSIZE];
    int done = 0;
    int len;
    XML_Parser parser;
    struct aux_params params;
    FILE *xml_file;

/* initializing the aux-structs */
    params.db_handle = NULL;
    params.ins_nodes_stmt = NULL;
    params.ins_node_tags_stmt = NULL;
    params.ins_ways_stmt = NULL;
    params.ins_way_tags_stmt = NULL;
    params.ins_way_node_refs_stmt = NULL;
    params.ins_relations_stmt = NULL;
    params.ins_relation_tags_stmt = NULL;
    params.ins_relation_node_refs_stmt = NULL;
    params.ins_relation_way_refs_stmt = NULL;
    params.ins_relation_relation_refs_stmt = NULL;
    params.wr_nodes = 0;
    params.wr_node_tags = 0;
    params.wr_ways = 0;
    params.wr_way_tags = 0;
    params.wr_way_node_refs = 0;
    params.wr_relations = 0;
    params.wr_rel_tags = 0;
    params.wr_rel_node_refs = 0;
    params.wr_rel_way_refs = 0;
    params.wr_rel_rel_refs = 0;
    params.current_tag = CURRENT_TAG_UNKNOWN;

    glob_node.timestamp = NULL;
    glob_node.user = NULL;
    glob_node.first = NULL;
    glob_node.last = NULL;

    glob_way.timestamp = NULL;
    glob_way.user = NULL;
    glob_way.first = NULL;
    glob_way.last = NULL;
    glob_way.first_node = NULL;
    glob_way.last_node = NULL;

    glob_relation.timestamp = NULL;
    glob_relation.user = NULL;
    glob_relation.first = NULL;
    glob_relation.last = NULL;
    glob_relation.first_node = NULL;
    glob_relation.last_node = NULL;
    glob_relation.first_way = NULL;
    glob_relation.last_way = NULL;
    glob_relation.first_rel = NULL;
    glob_relation.last_rel = NULL;

    for (i = 1; i < argc; i++)
      {
	  /* parsing the invocation arguments */
	  if (next_arg != ARG_NONE)
	    {
		switch (next_arg)
		  {
		  case ARG_OSM_PATH:
		      osm_path = argv[i];
		      break;
		  case ARG_DB_PATH:
		      db_path = argv[i];
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
	  if (strcmp (argv[i], "-o") == 0)
	    {
		next_arg = ARG_OSM_PATH;
		continue;
	    }
	  if (strcasecmp (argv[i], "--osm-path") == 0)
	    {
		next_arg = ARG_OSM_PATH;
		continue;
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
	  if (strcasecmp (argv[i], "-m") == 0)
	    {
		in_memory = 1;
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "-in-memory") == 0)
	    {
		in_memory = 1;
		next_arg = ARG_NONE;
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
    if (!osm_path)
      {
	  fprintf (stderr,
		   "did you forget setting the --osm-path argument ?\n");
	  error = 1;
      }
    if (!db_path)
      {
	  fprintf (stderr, "did you forget setting the --db-path argument ?\n");
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
    params.db_handle = handle;
    if (in_memory)
      {
	  /* loading the DB in-memory */
	  sqlite3 *mem_db_handle;
	  sqlite3_backup *backup;
	  int ret;
	  ret =
	      sqlite3_open_v2 (":memory:", &mem_db_handle,
			       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			       NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "cannot open 'MEMORY-DB': %s\n",
			 sqlite3_errmsg (mem_db_handle));
		sqlite3_close (mem_db_handle);
		return -1;
	    }
	  backup = sqlite3_backup_init (mem_db_handle, "main", handle, "main");
	  if (!backup)
	    {
		fprintf (stderr, "cannot load 'MEMORY-DB'\n");
		sqlite3_close (handle);
		sqlite3_close (mem_db_handle);
		return -1;
	    }
	  while (1)
	    {
		ret = sqlite3_backup_step (backup, 1024);
		if (ret == SQLITE_DONE)
		    break;
	    }
	  ret = sqlite3_backup_finish (backup);
	  sqlite3_close (handle);
	  handle = mem_db_handle;
	  printf ("\nusing IN-MEMORY database\n");
      }

/* creating SQL prepared statements */
    create_sql_stmts (&params);

/* XML parsing */
    xml_file = fopen (osm_path, "rb");
    if (!xml_file)
      {
	  fprintf (stderr, "cannot open %s\n", osm_path);
	  sqlite3_close (handle);
	  return -1;
      }
    parser = XML_ParserCreate (NULL);
    if (!parser)
      {
	  fprintf (stderr, "Couldn't allocate memory for parser\n");
	  sqlite3_close (handle);
	  return -1;
      }
    XML_SetUserData (parser, &params);
    XML_SetElementHandler (parser, start_tag, end_tag);
    while (!done)
      {
	  len = fread (Buff, 1, BUFFSIZE, xml_file);
	  if (ferror (xml_file))
	    {
		fprintf (stderr, "XML Read error\n");
		sqlite3_close (handle);
		return -1;
	    }
	  done = feof (xml_file);
	  if (!XML_Parse (parser, Buff, len, done))
	    {
		fprintf (stderr, "Parse error at line %d:\n%s\n",
			 (int) XML_GetCurrentLineNumber (parser),
			 XML_ErrorString (XML_GetErrorCode (parser)));
		sqlite3_close (handle);
		return -1;
	    }
      }
    XML_ParserFree (parser);
    fclose (xml_file);

/* finalizing SQL prepared statements */
    finalize_sql_stmts (&params);

/* printing out statistics */
    printf ("inserted %d nodes\n", params.wr_nodes);
    printf ("\t%d tags\n", params.wr_node_tags);
    printf ("inserted %d ways\n", params.wr_ways);
    printf ("\t%d tags\n", params.wr_way_tags);
    printf ("\t%d node-refs\n", params.wr_way_node_refs);
    printf ("inserted %d relations\n", params.wr_relations);
    printf ("\t%d tags\n", params.wr_rel_tags);
    printf ("\t%d node-refs\n", params.wr_rel_node_refs);
    printf ("\t%d way-refs\n", params.wr_rel_way_refs);
    printf ("\t%d relation-refs\n", params.wr_rel_rel_refs);

    if (in_memory)
      {
	  /* exporting the in-memory DB to filesystem */
	  sqlite3 *disk_db_handle;
	  sqlite3_backup *backup;
	  int ret;
	  printf ("\nexporting IN_MEMORY database ... wait please ...\n");
	  ret =
	      sqlite3_open_v2 (db_path, &disk_db_handle,
			       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			       NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "cannot open '%s': %s\n", db_path,
			 sqlite3_errmsg (disk_db_handle));
		sqlite3_close (disk_db_handle);
		return -1;
	    }
	  backup = sqlite3_backup_init (disk_db_handle, "main", handle, "main");
	  if (!backup)
	    {
		fprintf (stderr, "Backup failure: 'MEMORY-DB' wasn't saved\n");
		sqlite3_close (handle);
		sqlite3_close (disk_db_handle);
		return -1;
	    }
	  while (1)
	    {
		ret = sqlite3_backup_step (backup, 1024);
		if (ret == SQLITE_DONE)
		    break;
	    }
	  ret = sqlite3_backup_finish (backup);
	  sqlite3_close (handle);
	  handle = disk_db_handle;
	  printf ("\tIN_MEMORY database succesfully exported\n");
      }

/* closing the DB connection */
    sqlite3_close (handle);
    return 0;
}
