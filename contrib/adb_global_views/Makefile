# contrib/adb_global_views/Makefile

EXTENSION = adb_global_views
DATA = adb_global_views--1.0.sql
PGFILEDESC = "adb_global_views - Global Views of AntDB Cluster"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/adb_global_views
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif