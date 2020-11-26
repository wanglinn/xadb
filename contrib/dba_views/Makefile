MODULE_big = dba_views

EXTENSION = dba_views
DATA = dba_views--1.0.sql
PGFILEDESC = "dba_views -  Views of Oracle Compatible Cluster"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dba_views
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
