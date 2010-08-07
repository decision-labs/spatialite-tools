
AM_CFLAGS = -D_LARGE_FILE=1 -D_FILE_OFFSET_BITS=64 \
	-D_LARGEFILE_SOURCE=1

bin_PROGRAMS =spatialite \
	spatialite_tool \
	spatialite_network \
	shp_doctor \
	exif_loader \
	spatialite_osm \
	spatialite_osm_map \
	spatialite_gml

INCLUDES = @CFLAGS@

spatialite_SOURCES = shell.c shapefiles.c
spatialite_tool_SOURCES = spatialite_tool.c shapefiles.c
spatialite_network_SOURCES = spatialite_network.c
shp_doctor_SOURCES = shp_doctor.c
exif_loader_SOURCES = exif_loader.c
spatialite_osm_SOURCES = spatialite_osm.c
spatialite_osm_map_SOURCES = spatialite_osm_map.c
spatialite_gml_SOURCES = spatialite_gml.c

if MINGW
spatialite_osm_map_LDADD = -lspatialite \
	-liconv -lexpat -lm -lstdc++
spatialite_gml_LDADD = -lspatialite \
	-liconv -lexpat -lm -lstdc++
LDADD = -lspatialite -liconv -lm -lstdc++
spatialite_LDADD = -lspatialite -liconv -lm -lstdc++
else
if MACOSX
spatialite_osm_map_LDADD = -lspatialite \
	-lm -liconv -lexpat -lstdc++ -lpthread -ldl
spatialite_gml_LDADD = -lspatialite \
	-lm -liconv -lexpat -lstdc++ -lpthread -ldl
spatialite_LDADD = -lspatialite \
	-lm -liconv -lstdc++ -lpthread -ldl \
	@READLINE_LIBS@
LDADD = -lspatialite -lm -liconv -lstdc++ -lpthread -ldl
else
spatialite_osm_map_LDADD = -lspatialite \
	-lexpat -lm -lstdc++ -lpthread -ldl 
spatialite_gml_LDADD = -lspatialite \
	-lexpat -lm -lstdc++ -lpthread -ldl 
spatialite_LDADD = -lspatialite \
	-lm -lstdc++ -lpthread -ldl \
	@READLINE_LIBS@
LDADD = -lspatialite -lm -lstdc++ -lpthread -ldl
endif
endif

EXTRA_DIST = makefile.vc nmake.opt \
	Makefile-static-MinGW \
	Makefile-static-Linux \
	Makefile-static-MacOsX

AUTOMAKE_OPTIONS = dist-zip

