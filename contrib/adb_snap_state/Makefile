# contrib/adb_snap_state/Makefile
MODULES = adb_snap_state
EXTENSION =adb_snap_state
DATA = adb_snap_state--1.0.sql
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/adb_snap_state
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
