/* ./config-msvc.h.in - manually maintained */

/* Must be =64 in order to enable huge-file support. */
#undef _FILE_OFFSET_BITS

/* Must be defined in order to enable huge-file support. */
#undef _LARGEFILE_SOURCE

/* Must be defined in order to enable huge-file support. */
#undef _LARGE_FILE

/* includes gaiaconfig.h */
#include <spatialite/gaiaconfig-msvc.h>
