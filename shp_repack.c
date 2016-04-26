/* 
/ shp_repack
/
/ an analysis / sanitizing tool for  broken SHAPEFILES
/
/ version 1.0, 2016 April 25
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

#if defined(_WIN32) && !defined(__MINGW32__)
/* MSVC strictly requires this include [off_t] */
#include <sys/types.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <sys/types.h>
#if defined(_WIN32) && !defined(__MINGW32__)
#include <io.h>
#include <direct.h>
#else
#include <dirent.h>
#endif

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#ifdef SPATIALITE_AMALGAMATION
#include <spatialite/sqlite3.h>
#else
#include <sqlite3.h>
#endif

#include <spatialite/gaiageo.h>

#define ARG_NONE		0
#define ARG_IN_DIR		1
#define ARG_OUT_DIR		2

#define SUFFIX_DISCARD	0
#define SUFFIX_SHP		1
#define SUFFIX_SHX		2
#define SUFFIX_DBF		3

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	_stricmp
#endif /* not WIN32 */

struct shp_entry
{
/* an item of the SHP list */
    char *base_name;
    char *file_name;
    int has_shp;
    int has_shx;
    int has_dbf;
    struct shp_entry *next;
};

struct shp_list
{
/* the SHP list */
    struct shp_entry *first;
    struct shp_entry *last;
};

static struct shp_list *
alloc_shp_list (void)
{
/* allocating an empty SHP list */
    struct shp_list *list = malloc (sizeof (struct shp_list));
    list->first = NULL;
    list->last = NULL;
    return list;
}

static void
free_shp_list (struct shp_list *list)
{
/* memory cleanup: freeing an SHP list */
    struct shp_entry *pi;
    struct shp_entry *pin;
    if (list == NULL)
	return;

    pi = list->first;
    while (pi != NULL)
      {
	  pin = pi->next;
	  if (pi->base_name != NULL)
	      sqlite3_free (pi->base_name);
	  if (pi->file_name != NULL)
	      sqlite3_free (pi->file_name);
	  free (pi);
	  pi = pin;
      }
    free (list);
}

static void
do_add_shapefile (struct shp_list *list, char *base_name, char *file_name,
		  int suffix)
{
/* adding a possible SHP to the list */
    struct shp_entry *pi;
    if (list == NULL)
	return;

    pi = list->first;
    while (pi != NULL)
      {
	  /* searching if already defined */
	  if (strcmp (pi->base_name, base_name) == 0)
	    {
		switch (suffix)
		  {
		  case SUFFIX_SHP:
		      pi->has_shp = 1;
		      break;
		  case SUFFIX_SHX:
		      pi->has_shx = 1;
		      break;
		  case SUFFIX_DBF:
		      pi->has_dbf = 1;
		      break;
		  };
		sqlite3_free (base_name);
		sqlite3_free (file_name);
		return;
	    }
	  pi = pi->next;
      }

/* adding a new SHP entry */
    pi = malloc (sizeof (struct shp_entry));
    pi->base_name = base_name;
    pi->file_name = file_name;
    pi->has_shp = 0;
    pi->has_shx = 0;
    pi->has_dbf = 0;
    pi->next = NULL;

    switch (suffix)
      {
      case SUFFIX_SHP:
	  pi->has_shp = 1;
	  break;
      case SUFFIX_SHX:
	  pi->has_shx = 1;
	  break;
      case SUFFIX_DBF:
	  pi->has_dbf = 1;
	  break;
      };

    if (list->first == NULL)
	list->first = pi;
    if (list->last != NULL)
	list->last->next = pi;
    list->last = pi;
}

static int
test_valid_shp (struct shp_entry *p)
{
/* testing for a valid SHP candidate */
    if (p == NULL)
	return 0;
    if (p->has_shp && p->has_shx && p->has_dbf)
	return 1;
    return 0;
}

static gaiaShapefilePtr
allocShapefile ()
{
/* allocates and initializes the Shapefile object */
    gaiaShapefilePtr shp = malloc (sizeof (gaiaShapefile));
    shp->endian_arch = 1;
    shp->Path = NULL;
    shp->Shape = -1;
    shp->EffectiveType = GAIA_UNKNOWN;
    shp->EffectiveDims = GAIA_XY;
    shp->flShp = NULL;
    shp->flShx = NULL;
    shp->flDbf = NULL;
    shp->Dbf = NULL;
    shp->ShpBfsz = 0;
    shp->BufShp = NULL;
    shp->BufDbf = NULL;
    shp->DbfHdsz = 0;
    shp->DbfReclen = 0;
    shp->DbfSize = 0;
    shp->DbfRecno = 0;
    shp->ShpSize = 0;
    shp->ShxSize = 0;
    shp->MinX = DBL_MAX;
    shp->MinY = DBL_MAX;
    shp->MaxX = -DBL_MAX;
    shp->MaxY = -DBL_MAX;
    shp->Valid = 0;
    shp->IconvObj = NULL;
    shp->LastError = NULL;
    return shp;
}

static void
freeShapefile (gaiaShapefilePtr shp)
{
/* frees all memory allocations related to the Shapefile object */
    if (shp->Path)
	free (shp->Path);
    if (shp->flShp)
	fclose (shp->flShp);
    if (shp->flShx)
	fclose (shp->flShx);
    if (shp->flDbf)
	fclose (shp->flDbf);
    if (shp->Dbf)
	gaiaFreeDbfList (shp->Dbf);
    if (shp->BufShp)
	free (shp->BufShp);
    if (shp->BufDbf)
	free (shp->BufDbf);
    if (shp->LastError)
	free (shp->LastError);
    free (shp);
}

static void
openShpRead (gaiaShapefilePtr shp, const char *path)
{
/* trying to open the shapefile and initial checkings */
    FILE *fl_shx = NULL;
    FILE *fl_shp = NULL;
    FILE *fl_dbf = NULL;
    char xpath[1024];
    int rd;
    unsigned char buf_shx[256];
    unsigned char *buf_shp = NULL;
    int buf_size = 1024;
    int shape;
    unsigned char bf[1024];
    int dbf_size;
    int dbf_reclen = 0;
    int off_dbf;
    int ind;
    char field_name[2048];
    char *sys_err;
    char errMsg[1024];
    int len;
    int endian_arch = gaiaEndianArch ();
    gaiaDbfListPtr dbf_list = NULL;
    if (shp->flShp != NULL || shp->flShx != NULL || shp->flDbf != NULL)
      {
	  sprintf (errMsg,
		   "attempting to reopen an already opened Shapefile\n");
	  goto unsupported_conversion;
      }
    sprintf (xpath, "%s.shx", path);
    fl_shx = fopen (xpath, "rb");
    if (!fl_shx)
      {
	  sys_err = strerror (errno);
	  sprintf (errMsg, "unable to open '%s' for reading: %s", xpath,
		   sys_err);
	  goto no_file;
      }
    sprintf (xpath, "%s.shp", path);
    fl_shp = fopen (xpath, "rb");
    if (!fl_shp)
      {
	  sys_err = strerror (errno);
	  sprintf (errMsg, "unable to open '%s' for reading: %s", xpath,
		   sys_err);
	  goto no_file;
      }
    sprintf (xpath, "%s.dbf", path);
    fl_dbf = fopen (xpath, "rb");
    if (!fl_dbf)
      {
	  sys_err = strerror (errno);
	  sprintf (errMsg, "unable to open '%s' for reading: %s", xpath,
		   sys_err);
	  goto no_file;
      }
/* reading SHX file header */
    rd = fread (buf_shx, sizeof (unsigned char), 100, fl_shx);
    if (rd != 100)
	goto error;
    if (gaiaImport32 (buf_shx + 0, GAIA_BIG_ENDIAN, endian_arch) != 9994)	/* checks the SHX magic number */
	goto error;
/* reading SHP file header */
    buf_shp = malloc (sizeof (unsigned char) * buf_size);
    rd = fread (buf_shp, sizeof (unsigned char), 100, fl_shp);
    if (rd != 100)
	goto error;
    if (gaiaImport32 (buf_shp + 0, GAIA_BIG_ENDIAN, endian_arch) != 9994)	/* checks the SHP magic number */
	goto error;
    shape = gaiaImport32 (buf_shp + 32, GAIA_LITTLE_ENDIAN, endian_arch);
    if (shape == GAIA_SHP_POINT || shape == GAIA_SHP_POINTZ
	|| shape == GAIA_SHP_POINTM || shape == GAIA_SHP_POLYLINE
	|| shape == GAIA_SHP_POLYLINEZ || shape == GAIA_SHP_POLYLINEM
	|| shape == GAIA_SHP_POLYGON || shape == GAIA_SHP_POLYGONZ
	|| shape == GAIA_SHP_POLYGONM || shape == GAIA_SHP_MULTIPOINT
	|| shape == GAIA_SHP_MULTIPOINTZ || shape == GAIA_SHP_MULTIPOINTM)
	;
    else
	goto unsupported;
/* reading DBF file header */
    rd = fread (bf, sizeof (unsigned char), 32, fl_dbf);
    if (rd != 32)
	goto error;
    switch (*bf)
      {
	  /* checks the DBF magic number */
      case 0x03:
      case 0x83:
	  break;
      case 0x02:
      case 0xF8:
	  sprintf (errMsg, "'%s'\ninvalid magic number %02x [FoxBASE format]",
		   path, *bf);
	  goto dbf_bad_magic;
      case 0xF5:
	  sprintf (errMsg,
		   "'%s'\ninvalid magic number %02x [FoxPro 2.x (or earlier) format]",
		   path, *bf);
	  goto dbf_bad_magic;
      case 0x30:
      case 0x31:
      case 0x32:
	  sprintf (errMsg,
		   "'%s'\ninvalid magic number %02x [Visual FoxPro format]",
		   path, *bf);
	  goto dbf_bad_magic;
      case 0x43:
      case 0x63:
      case 0xBB:
      case 0xCB:
	  sprintf (errMsg, "'%s'\ninvalid magic number %02x [dBASE IV format]",
		   path, *bf);
	  goto dbf_bad_magic;
      default:
	  sprintf (errMsg, "'%s'\ninvalid magic number %02x [unknown format]",
		   path, *bf);
	  goto dbf_bad_magic;
      };
    dbf_size = gaiaImport16 (bf + 8, GAIA_LITTLE_ENDIAN, endian_arch);
    dbf_reclen = gaiaImport16 (bf + 10, GAIA_LITTLE_ENDIAN, endian_arch);
    dbf_size--;
    off_dbf = 0;
    dbf_list = gaiaAllocDbfList ();
    for (ind = 32; ind < dbf_size; ind += 32)
      {
	  /* fetches DBF fields definitions */
	  rd = fread (bf, sizeof (unsigned char), 32, fl_dbf);
	  if (rd != 32)
	      goto error;
	  if (*(bf + 11) == 'M')
	    {
		/* skipping any MEMO field */
		memcpy (field_name, bf, 11);
		field_name[11] = '\0';
		off_dbf += *(bf + 16);
		fprintf (stderr,
			 "WARNING: column \"%s\" is of the MEMO type and will be ignored\n",
			 field_name);
		continue;
	    }
	  memcpy (field_name, bf, 11);
	  field_name[11] = '\0';
	  gaiaAddDbfField (dbf_list, field_name, *(bf + 11), off_dbf,
			   *(bf + 16), *(bf + 17));
	  off_dbf += *(bf + 16);
      }
    if (!gaiaIsValidDbfList (dbf_list))
      {
	  /* invalid DBF */
	  goto illegal_dbf;
      }
    len = strlen (path);
    shp->Path = malloc (len + 1);
    strcpy (shp->Path, path);
    shp->ReadOnly = 1;
    shp->Shape = shape;
    switch (shape)
      {
	  /* setting up a prudential geometry type */
      case GAIA_SHP_POINT:
      case GAIA_SHP_POINTZ:
      case GAIA_SHP_POINTM:
	  shp->EffectiveType = GAIA_POINT;
	  break;
      case GAIA_SHP_POLYLINE:
      case GAIA_SHP_POLYLINEZ:
      case GAIA_SHP_POLYLINEM:
	  shp->EffectiveType = GAIA_MULTILINESTRING;
	  break;
      case GAIA_SHP_POLYGON:
      case GAIA_SHP_POLYGONZ:
      case GAIA_SHP_POLYGONM:
	  shp->EffectiveType = GAIA_MULTIPOLYGON;
	  break;
      case GAIA_SHP_MULTIPOINT:
      case GAIA_SHP_MULTIPOINTZ:
      case GAIA_SHP_MULTIPOINTM:
	  shp->EffectiveType = GAIA_MULTIPOINT;
	  break;
      }
    switch (shape)
      {
	  /* setting up a prudential dimension model */
      case GAIA_SHP_POINTZ:
      case GAIA_SHP_POLYLINEZ:
      case GAIA_SHP_POLYGONZ:
      case GAIA_SHP_MULTIPOINTZ:
	  shp->EffectiveDims = GAIA_XY_Z_M;
	  break;
      case GAIA_SHP_POINTM:
      case GAIA_SHP_POLYLINEM:
      case GAIA_SHP_POLYGONM:
      case GAIA_SHP_MULTIPOINTM:
	  shp->EffectiveDims = GAIA_XY_M;
	  break;
      default:
	  shp->EffectiveDims = GAIA_XY;
	  break;
      }
    shp->flShp = fl_shp;
    shp->flShx = fl_shx;
    shp->flDbf = fl_dbf;
    shp->Dbf = dbf_list;
/* saving the SHP buffer */
    shp->BufShp = buf_shp;
    shp->ShpBfsz = buf_size;
/* allocating DBF buffer */
    shp->BufDbf = malloc (sizeof (unsigned char) * dbf_reclen);
    shp->DbfHdsz = dbf_size + 1;
    shp->DbfReclen = dbf_reclen;
    shp->Valid = 1;
    shp->endian_arch = endian_arch;
    return;
  unsupported_conversion:
/* illegal charset */
    if (shp->LastError)
	free (shp->LastError);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    return;
  no_file:
/* one of shapefile's files can't be accessed */
    if (shp->LastError)
	free (shp->LastError);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    if (fl_shx)
	fclose (fl_shx);
    if (fl_shp)
	fclose (fl_shp);
    if (fl_dbf)
	fclose (fl_dbf);
    return;
  dbf_bad_magic:
/* the DBF has an invalid magin number */
    if (shp->LastError)
	free (shp->LastError);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    gaiaFreeDbfList (dbf_list);
    if (buf_shp)
	free (buf_shp);
    fclose (fl_shx);
    fclose (fl_shp);
    fclose (fl_dbf);
    return;
  error:
/* the shapefile is invalid or corrupted */
    if (shp->LastError)
	free (shp->LastError);
    sprintf (errMsg, "'%s' is corrupted / has invalid format", path);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    gaiaFreeDbfList (dbf_list);
    if (buf_shp)
	free (buf_shp);
    fclose (fl_shx);
    fclose (fl_shp);
    fclose (fl_dbf);
    return;
  unsupported:
/* the shapefile has an unrecognized shape type */
    if (shp->LastError)
	free (shp->LastError);
    sprintf (errMsg, "'%s' shape=%d is not supported", path, shape);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    gaiaFreeDbfList (dbf_list);
    if (buf_shp)
	free (buf_shp);
    fclose (fl_shx);
    fclose (fl_shp);
    if (fl_dbf)
	fclose (fl_dbf);
    return;
  illegal_dbf:
/* the DBF-file contains unsupported data types */
    if (shp->LastError)
	free (shp->LastError);
    sprintf (errMsg, "'%s.dbf' contains unsupported data types", path);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    gaiaFreeDbfList (dbf_list);
    if (buf_shp)
	free (buf_shp);
    fclose (fl_shx);
    fclose (fl_shp);
    if (fl_dbf)
	fclose (fl_dbf);
    return;
}

static int
readShpEntity (gaiaShapefilePtr shp, int current_row, int *shplen)
{
/* trying to read an entity from shapefile */
    unsigned char buf[512];
    int len;
    int rd;
    int skpos;
    int offset;
    int off_shp;
    int sz;
    char errMsg[1024];

/* positioning and reading the SHX file */
    offset = 100 + (current_row * 8);	/* 100 bytes for the header + current row displacement; each SHX row = 8 bytes */
    skpos = fseek (shp->flShx, offset, SEEK_SET);
    if (skpos != 0)
	goto eof;
    rd = fread (buf, sizeof (unsigned char), 8, shp->flShx);
    if (rd != 8)
	goto eof;
    off_shp = gaiaImport32 (buf, GAIA_BIG_ENDIAN, shp->endian_arch);
/* positioning and reading the DBF file */
    offset = shp->DbfHdsz + (current_row * shp->DbfReclen);
    skpos = fseek (shp->flDbf, offset, SEEK_SET);
    if (skpos != 0)
	goto error;
    rd = fread (shp->BufDbf, sizeof (unsigned char), shp->DbfReclen,
		shp->flDbf);
    if (rd != shp->DbfReclen)
	goto error;
    if (*(shp->BufDbf) == '*')
	goto dbf_deleted;
/* positioning and reading corresponding SHP entity - geometry */
    offset = off_shp * 2;
    skpos = fseek (shp->flShp, offset, SEEK_SET);
    if (skpos != 0)
	goto error;
    rd = fread (buf, sizeof (unsigned char), 8, shp->flShp);
    if (rd != 8)
	goto error;
    sz = gaiaImport32 (buf + 4, GAIA_BIG_ENDIAN, shp->endian_arch);
    if ((sz * 2) > shp->ShpBfsz)
      {
	  /* current buffer is too small; we need to allocate a bigger buffer */
	  free (shp->BufShp);
	  shp->ShpBfsz = sz * 2;
	  shp->BufShp = malloc (sizeof (unsigned char) * shp->ShpBfsz);
      }
    /* reading the raw Geometry */
    rd = fread (shp->BufShp, sizeof (unsigned char), sz * 2, shp->flShp);
    if (rd != sz * 2)
	goto error;
    *shplen = rd;
    return 1;

  eof:
    if (shp->LastError)
	free (shp->LastError);
    shp->LastError = NULL;
    return 0;
  error:
    if (shp->LastError)
	free (shp->LastError);
    sprintf (errMsg, "'%s' is corrupted / has invalid format", shp->Path);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    return 0;
  dbf_deleted:
    if (shp->LastError)
	free (shp->LastError);
    shp->LastError = NULL;
    return -1;
}

static int
do_read_shp (const char *shp_path, int *deleted)
{
/* reading some Shapefile and testing for validity */
    int current_row;
    gaiaShapefilePtr shp = NULL;
    int ret;

    *deleted = 0;
    shp = allocShapefile ();
    openShpRead (shp, shp_path);
    if (!(shp->Valid))
      {
	  char extra[512];
	  *extra = '\0';
	  if (shp->LastError)
	      sprintf (extra, "\n\tcause: %s\n", shp->LastError);
	  fprintf (stderr,
		   "\terror: cannot open shapefile '%s'%s", shp_path, extra);
	  freeShapefile (shp);
	  return 0;
      }

    current_row = 0;
    while (1)
      {
	  /* reading rows from shapefile */
	  int shplen;
	  ret = readShpEntity (shp, current_row, &shplen);
	  if (ret < 0)
	    {
		/* found a DBF deleted record */
		fprintf (stderr, "\t\trow #%d: logical deletion found !!!\n",
			 current_row);
		current_row++;
		*deleted += 1;
		continue;
	    }
	  if (!ret)
	    {
		if (!(shp->LastError))	/* normal SHP EOF */
		    break;
		fprintf (stderr, "\tERROR: %s\n", shp->LastError);
		goto stop;
	    }
	  current_row++;
      }
    freeShapefile (shp);
    return 1;

  stop:
    freeShapefile (shp);
    fprintf (stderr, "\tMalformed shapefile: quitting\n");
    return 0;
}

static void
openShpWrite (gaiaShapefilePtr shp, const char *path, int shape,
	      gaiaDbfListPtr dbf_list)
{
/* trying to create the shapefile */
    FILE *fl_shx = NULL;
    FILE *fl_shp = NULL;
    FILE *fl_dbf = NULL;
    char xpath[1024];
    unsigned char *buf_shp = NULL;
    int buf_size = 1024;
    unsigned char *dbf_buf = NULL;
    gaiaDbfFieldPtr fld;
    char *sys_err;
    char errMsg[1024];
    short dbf_reclen = 0;
    int shp_size = 0;
    int shx_size = 0;
    unsigned short dbf_size = 0;
    int len;
    int endian_arch = gaiaEndianArch ();
    char buf[2048];
    if (shp->flShp != NULL || shp->flShx != NULL || shp->flDbf != NULL)
      {
	  sprintf (errMsg,
		   "attempting to reopen an already opened Shapefile\n");
	  goto unsupported_conversion;
      }
    buf_shp = malloc (buf_size);
/* trying to open shapefile files */
    sprintf (xpath, "%s.shx", path);
    fl_shx = fopen (xpath, "wb");
    if (!fl_shx)
      {
	  sys_err = strerror (errno);
	  sprintf (errMsg, "unable to open '%s' for writing: %s", xpath,
		   sys_err);
	  goto no_file;
      }
    sprintf (xpath, "%s.shp", path);
    fl_shp = fopen (xpath, "wb");
    if (!fl_shp)
      {
	  sys_err = strerror (errno);
	  sprintf (errMsg, "unable to open '%s' for writing: %s", xpath,
		   sys_err);
	  goto no_file;
      }
    sprintf (xpath, "%s.dbf", path);
    fl_dbf = fopen (xpath, "wb");
    if (!fl_dbf)
      {
	  sys_err = strerror (errno);
	  sprintf (errMsg, "unable to open '%s' for writing: %s", xpath,
		   sys_err);
	  goto no_file;
      }
/* allocating DBF buffer */
    dbf_reclen = 1;		/* an extra byte is needed because in DBF rows first byte is a marker for deletion */
    fld = dbf_list->First;
    while (fld)
      {
	  /* computing the DBF record length */
	  dbf_reclen += fld->Length;
	  fld = fld->Next;
      }
    dbf_buf = malloc (dbf_reclen);
/* writing an empty SHP file header */
    memset (buf_shp, 0, 100);
    fwrite (buf_shp, 1, 100, fl_shp);
    shp_size = 50;		/* note: shapefile [SHP and SHX] counts sizes in WORDS of 16 bits, not in bytes of 8 bits !!!! */
/* writing an empty SHX file header */
    memset (buf_shp, 0, 100);
    fwrite (buf_shp, 1, 100, fl_shx);
    shx_size = 50;
/* writing the DBF file header */
    memset (buf_shp, '\0', 32);
    fwrite (buf_shp, 1, 32, fl_dbf);
    dbf_size = 32;		/* note: DBF counts sizes in bytes */
    fld = dbf_list->First;
    while (fld)
      {
	  /* exporting DBF Fields specifications */
	  memset (buf_shp, 0, 32);
	  strcpy (buf, fld->Name);
	  memcpy (buf_shp, buf, strlen (buf));
	  *(buf_shp + 11) = fld->Type;
	  *(buf_shp + 16) = fld->Length;
	  *(buf_shp + 17) = fld->Decimals;
	  fwrite (buf_shp, 1, 32, fl_dbf);
	  dbf_size += 32;
	  fld = fld->Next;
      }
    fwrite ("\r", 1, 1, fl_dbf);	/* this one is a special DBF delimiter that closes file header */
    dbf_size++;
/* setting up the SHP struct */
    len = strlen (path);
    shp->Path = malloc (len + 1);
    strcpy (shp->Path, path);
    shp->ReadOnly = 0;
    shp->Shape = shape;
    shp->flShp = fl_shp;
    shp->flShx = fl_shx;
    shp->flDbf = fl_dbf;
    shp->Dbf = dbf_list;
    shp->BufShp = buf_shp;
    shp->ShpBfsz = buf_size;
    shp->BufDbf = dbf_buf;
    shp->DbfHdsz = dbf_size + 1;
    shp->DbfReclen = dbf_reclen;
    shp->DbfSize = dbf_size;
    shp->DbfRecno = 0;
    shp->ShpSize = shp_size;
    shp->ShxSize = shx_size;
    shp->MinX = DBL_MAX;
    shp->MinY = DBL_MAX;
    shp->MaxX = -DBL_MAX;
    shp->MaxY = -DBL_MAX;
    shp->Valid = 1;
    shp->endian_arch = endian_arch;
    return;
  unsupported_conversion:
/* illegal charset */
    if (shp->LastError)
	free (shp->LastError);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    return;
  no_file:
/* one of shapefile's files can't be created/opened */
    if (shp->LastError)
	free (shp->LastError);
    len = strlen (errMsg);
    shp->LastError = malloc (len + 1);
    strcpy (shp->LastError, errMsg);
    if (buf_shp)
	free (buf_shp);
    if (fl_shx)
	fclose (fl_shx);
    if (fl_shp)
	fclose (fl_shp);
    if (fl_dbf)
	fclose (fl_dbf);
    return;
}

static int
writeShpEntity (gaiaShapefilePtr shp, const unsigned char *bufshp, int shplen,
		const unsigned char *bufdbf, int dbflen)
{
/* trying to write an entity into shapefile */
    unsigned char buf[64];
    int endian_arch = shp->endian_arch;
    int shape;
    double minx;
    double maxx;
    double miny;
    double maxy;

/* inserting entity in SHX file */
    gaiaExport32 (buf, shp->ShpSize, GAIA_BIG_ENDIAN, endian_arch);	/* exports current SHP file position */
    gaiaExport32 (buf + 4, shplen / 2, GAIA_BIG_ENDIAN, endian_arch);	/* exports entitiy size [in 16 bits words !!!] */
    fwrite (buf, 1, 8, shp->flShx);
    (shp->ShxSize) += 4;	/* updating current SHX file position [in 16 bits words !!!] */

/* inserting entity in SHP file */
    gaiaExport32 (buf, shp->DbfRecno + 1, GAIA_BIG_ENDIAN, endian_arch);	/* exports entity ID */
    gaiaExport32 (buf + 4, shplen / 2, GAIA_BIG_ENDIAN, endian_arch);	/* exports entity size [in 16 bits words !!!] */
    fwrite (buf, 1, 8, shp->flShp);
    (shp->ShpSize) += 4;
    fwrite (bufshp, 1, shplen, shp->flShp);
    (shp->ShpSize) += shplen / 2;	/* updating current SHP file position [in 16 bits words !!!] */

/* inserting entity in DBF file */
    fwrite (bufdbf, 1, dbflen, shp->flDbf);
    (shp->DbfRecno)++;

/* updating the full extent BBOX */
    shape = gaiaImport32 (bufshp + 0, GAIA_LITTLE_ENDIAN, endian_arch);
    if (shape == GAIA_SHP_POINT || shape == GAIA_SHP_POINTZ
	|| shape == GAIA_SHP_POINTM)
      {
	  minx = gaiaImport64 (bufshp + 4, GAIA_LITTLE_ENDIAN, endian_arch);
	  maxx = minx;
	  miny = gaiaImport64 (bufshp + 12, GAIA_LITTLE_ENDIAN, endian_arch);
	  maxy = miny;
	  if (minx < shp->MinX)
	      shp->MinX = minx;
	  if (maxx > shp->MaxX)
	      shp->MaxX = maxx;
	  if (miny < shp->MinY)
	      shp->MinY = miny;
	  if (maxy > shp->MaxY)
	      shp->MaxY = maxy;
      }
    if (shape == GAIA_SHP_POLYLINE || shape == GAIA_SHP_POLYLINEZ
	|| shape == GAIA_SHP_POLYLINEM || shape == GAIA_SHP_POLYGON
	|| shape == GAIA_SHP_POLYGONZ || shape == GAIA_SHP_POLYGONM
	|| shape == GAIA_SHP_MULTIPOINT || shape == GAIA_SHP_MULTIPOINTZ
	|| shape == GAIA_SHP_MULTIPOINTM)
      {
	  minx = gaiaImport64 (bufshp + 4, GAIA_LITTLE_ENDIAN, endian_arch);
	  miny = gaiaImport64 (bufshp + 12, GAIA_LITTLE_ENDIAN, endian_arch);
	  maxx = gaiaImport64 (bufshp + 20, GAIA_LITTLE_ENDIAN, endian_arch);
	  maxy = gaiaImport64 (bufshp + 28, GAIA_LITTLE_ENDIAN, endian_arch);
	  if (minx < shp->MinX)
	      shp->MinX = minx;
	  if (maxx > shp->MaxX)
	      shp->MaxX = maxx;
	  if (miny < shp->MinY)
	      shp->MinY = miny;
	  if (maxy > shp->MaxY)
	      shp->MaxY = maxy;
      }
    return 1;
}

static int
do_repair_shapefile (const char *shp_path, const char *out_path)
{
/* repairing some Shapefile */
    int current_row;
    gaiaShapefilePtr shp_in = NULL;
    gaiaShapefilePtr shp_out = NULL;
    int ret;
    gaiaDbfListPtr dbf_list = NULL;
    gaiaDbfFieldPtr in_fld;

/* opening the INPUT SHP */
    shp_in = allocShapefile ();
    openShpRead (shp_in, shp_path);
    if (!(shp_in->Valid))
      {
	  char extra[512];
	  *extra = '\0';
	  if (shp_in->LastError)
	      sprintf (extra, "\n\tcause: %s\n", shp_in->LastError);
	  fprintf (stderr,
		   "\terror: cannot open shapefile '%s'%s", shp_path, extra);
	  freeShapefile (shp_in);
	  return 0;
      }

/* preparing the DBF fields list - OUTPUT */
    dbf_list = gaiaAllocDbfList ();
    in_fld = shp_in->Dbf->First;
    while (in_fld)
      {
	  /* adding a DBF field - OUTPUT */
	  gaiaAddDbfField (dbf_list, in_fld->Name, in_fld->Type, in_fld->Offset,
			   in_fld->Length, in_fld->Decimals);
	  in_fld = in_fld->Next;
      }

/* creating the OUTPUT SHP */
    shp_out = allocShapefile ();
    openShpWrite (shp_out, out_path, shp_in->Shape, dbf_list);
    if (!(shp_out->Valid))
      {
	  char extra[512];
	  *extra = '\0';
	  if (shp_out->LastError)
	      sprintf (extra, "\n\tcause: %s\n", shp_out->LastError);
	  fprintf (stderr,
		   "\terror: cannot open shapefile '%s'%s", out_path, extra);
	  freeShapefile (shp_in);
	  freeShapefile (shp_out);
	  gaiaFreeDbfList (dbf_list);
	  return 0;
      }

    current_row = 0;
    while (1)
      {
	  /* reading rows from shapefile */
	  int shplen;
	  ret = readShpEntity (shp_in, current_row, &shplen);
	  if (ret < 0)
	    {
		/* found a DBF deleted record */
		current_row++;
		continue;
	    }
	  if (!ret)
	    {
		if (!(shp_in->LastError))	/* normal SHP EOF */
		    break;
		fprintf (stderr, "\tERROR: %s\n", shp_in->LastError);
		goto stop;
	    }
	  current_row++;
	  if (!writeShpEntity
	      (shp_out, shp_in->BufShp, shplen, shp_in->BufDbf,
	       shp_in->DbfReclen))
	      goto stop;
      }
    gaiaFlushShpHeaders (shp_out);
    freeShapefile (shp_in);
    freeShapefile (shp_out);
    return 1;

  stop:
    freeShapefile (shp_in);
    freeShapefile (shp_out);
    fprintf (stderr, "\tMalformed shapefile: quitting\n");
    return 0;
}

static int
do_test_shapefile (const char *shp_path, int *invalid)
{
/* testing a Shapefile for validity */
    int deleted;
    fprintf (stderr, "\nVerifying %s.shp\n", shp_path);
    *invalid = 0;
    if (!do_read_shp (shp_path, &deleted))
	return 0;
    if (deleted)
      {
	  fprintf (stderr, "\tfound %d invalidities: cleaning required.\n",
		   deleted);
	  *invalid = 1;
      }
    else
	fprintf (stderr, "\tfound to be already valid.\n");
    return 1;
}

static int
check_extension (const char *file_name)
{
/* checks the file extension */
    const char *mark = NULL;
    int len = strlen (file_name);
    const char *p = file_name + len - 1;

    while (p >= file_name)
      {
	  if (*p == '.')
	      mark = p;
	  p--;
      }
    if (mark == NULL)
	return SUFFIX_DISCARD;
    if (strcasecmp (mark, ".shp") == 0)
	return SUFFIX_SHP;
    if (strcasecmp (mark, ".shx") == 0)
	return SUFFIX_SHX;
    if (strcasecmp (mark, ".dbf") == 0)
	return SUFFIX_DBF;
    return SUFFIX_DISCARD;
}

static int
do_scan_dir (const char *in_dir, const char *out_dir, int *n_shp, int *r_shp,
	     int *x_shp)
{
/* scanning a directory and searching for Shapefiles to be checked */
    struct shp_entry *p_shp;
    struct shp_list *list = alloc_shp_list ();

#if defined(_WIN32) && !defined(__MINGW32__)
/* Visual Studio .NET */
    struct _finddata_t c_file;
    intptr_t hFile;
    char *path;
    char *name;
    int len;
    int ret;
    if (_chdir (in_dir) < 0)
	goto error;
    if ((hFile = _findfirst ("*.shp", &c_file)) == -1L)
	;
    else
      {
	  while (1)
	    {
		if ((c_file.attrib & _A_RDONLY) == _A_RDONLY
		    || (c_file.attrib & _A_NORMAL) == _A_NORMAL)
		  {
		      name = sqlite3_mprintf ("%s", c_file.name);
		      len = strlen (name);
		      name[len - 4] = '\0';
		      path = sqlite3_mprintf ("%s/%s", in_dir, name);
		      do_add_shapefile (list, path, name, SUFFIX_SHP);
		  }
		if (_findnext (hFile, &c_file) != 0)
		    break;
	    }
	  _findclose (hFile);
	  if ((hFile = _findfirst ("*.shx", &c_file)) == -1L)
	      ;
	  else
	    {
		while (1)
		  {
		      if ((c_file.attrib & _A_RDONLY) == _A_RDONLY
			  || (c_file.attrib & _A_NORMAL) == _A_NORMAL)
			{
			    name = sqlite3_mprintf ("%s", c_file.name);
			    len = strlen (name);
			    name[len - 4] = '\0';
			    path = sqlite3_mprintf ("%s/%s", in_dir, name);
			    do_add_shapefile (list, path, name, SUFFIX_SHX);
			}
		      if (_findnext (hFile, &c_file) != 0)
			  break;
		  }
		_findclose (hFile);
		if ((hFile = _findfirst ("*.dbf", &c_file)) == -1L)
		    ;
		else
		  {
		      while (1)
			{
			    if ((c_file.attrib & _A_RDONLY) == _A_RDONLY
				|| (c_file.attrib & _A_NORMAL) == _A_NORMAL)
			      {
				  name = sqlite3_mprintf ("%s", c_file.name);
				  len = strlen (name);
				  name[len - 4] = '\0';
				  path =
				      sqlite3_mprintf ("%s/%s", in_dir, name);
				  do_add_shapefile (list, path, name,
						    SUFFIX_DBF);
			      }
			    if (_findnext (hFile, &c_file) != 0)
				break;
			}
		      _findclose (hFile);
		  }
	    }
      }
#else
/* not Visual Studio .NET */
    char *path;
    char *name;
    struct dirent *entry;
    int len;
    int suffix;
    DIR *dir = opendir (in_dir);
    if (!dir)
	goto error;
    while (1)
      {
	  /* extracting the SHP candidates */
	  entry = readdir (dir);
	  if (!entry)
	      break;
	  suffix = check_extension (entry->d_name);
	  if (suffix == SUFFIX_DISCARD)
	      continue;
	  path = sqlite3_mprintf ("%s/%s", in_dir, entry->d_name);
	  len = strlen (path);
	  path[len - 4] = '\0';
	  name = sqlite3_mprintf ("%s", entry->d_name);
	  len = strlen (name);
	  name[len - 4] = '\0';
	  do_add_shapefile (list, path, name, suffix);
      }
    closedir (dir);
#endif

    p_shp = list->first;
    while (p_shp != NULL)
      {
	  if (test_valid_shp (p_shp))
	    {
		int invalid;
		if (!do_test_shapefile (p_shp->base_name, &invalid))
		    goto error;
		*n_shp += 1;
		if (invalid)
		    *x_shp += 1;
		if (invalid && out_dir != NULL)
		  {
		      /* attempting to repair */
		      int ret;
		      char *out_path = sqlite3_mprintf ("%s/%s.shp", out_dir,
							p_shp->file_name);
		      fprintf (stderr, "\tAttempting to repair: %s\n",
			       out_path);
		      ret = do_repair_shapefile (p_shp->base_name, out_path);
		      sqlite3_free (out_path);
		      if (!ret)
			  goto error;
		      *r_shp += 1;
		      fprintf (stderr, "\tOK, successfully repaired.\n");
		  }
	    }
	  p_shp = p_shp->next;
      }

    free_shp_list (list);
    return 1;

  error:
    free_shp_list (list);
    fprintf (stderr, "Unable to access \"%s\"\n", in_dir);
    return 0;
}

static void
do_help ()
{
/* printing the argument list */
    fprintf (stderr, "\n\nusage: shp_repack ARGLIST\n");
    fprintf (stderr,
	     "==============================================================\n");
    fprintf (stderr,
	     "-h or --help                      print this help message\n");
    fprintf (stderr,
	     "-idir or --in-dir   dir-path      directory expected to contain\n"
	     "                                  all SHP to be checked\n");
    fprintf (stderr,
	     "-odir or --out-dir  dir-path      <optional> directory where to\n"
	     "                                  store all repaired SHPs\n\n");
}

int
main (int argc, char *argv[])
{
/* the MAIN function simply perform arguments checking */
    int i;
    int error = 0;
    int next_arg = ARG_NONE;
    char *in_dir = NULL;
    char *out_dir = NULL;
    int n_shp = 0;
    int r_shp = 0;
    int x_shp = 0;
    for (i = 1; i < argc; i++)
      {
	  /* parsing the invocation arguments */
	  if (next_arg != ARG_NONE)
	    {
		switch (next_arg)
		  {
		  case ARG_IN_DIR:
		      in_dir = argv[i];
		      break;
		  case ARG_OUT_DIR:
		      out_dir = argv[i];
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
	  if (strcasecmp (argv[i], "-idir") == 0
	      || strcasecmp (argv[i], "--in-dir") == 0)
	    {
		next_arg = ARG_IN_DIR;
		continue;
	    }
	  if (strcasecmp (argv[i], "-odir") == 0
	      || strcasecmp (argv[i], "--out-dir") == 0)
	    {
		next_arg = ARG_OUT_DIR;
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
    if (!in_dir)
      {
	  fprintf (stderr, "did you forget setting the --in-dir argument ?\n");
	  error = 1;
      }
    if (error)
      {
	  do_help ();
	  return -1;
      }

    if (out_dir != NULL)
      {
#ifdef _WIN32
	  if (_mkdir (out_dir) != 0)
#else
	  if (mkdir (out_dir, 0777) != 0)
#endif
	    {
		fprintf (stderr,
			 "ERROR: unable to create the output directory\n%s\n%s\n\n",
			 out_dir, strerror (errno));
		return -1;
	    }
      }

    fprintf (stderr, "\nInput dir: %s\n", in_dir);
    if (out_dir != NULL)
	fprintf (stderr, "Output dir: %s\n", out_dir);
    else
	fprintf (stderr, "Only a diagnostic report will be reported\n");

    if (!do_scan_dir (in_dir, out_dir, &n_shp, &r_shp, &x_shp))
      {
	  fprintf (stderr,
		   "\n... quitting ... some unexpected error occurred\n");
	  return -1;
      }

    fprintf (stderr, "\n===========================================\n");
    fprintf (stderr, "%d Shapefil%s ha%s been inspected.\n", n_shp,
	     (n_shp > 1) ? "es" : "e", (n_shp > 1) ? "ve" : "s");
    fprintf (stderr, "%d malformed Shapefil%s ha%s been identified.\n", x_shp,
	     (x_shp > 1) ? "es" : "e", (x_shp > 1) ? "ve" : "s");
    fprintf (stderr, "%d Shapefil%s ha%s been repaired.\n\n", r_shp,
	     (r_shp > 1) ? "es" : "e", (r_shp > 1) ? "ve" : "s");

    return 0;
}
