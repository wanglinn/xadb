# contrib/adb_stat_statements/Makefile

MODULE_big = adb_stat_statements
OBJS = adb_stat_statements.o $(WIN32RES)

EXTENSION = adb_stat_statements
DATA = adb_stat_statements--1.0.sql
PGFILEDESC = "adb_stat_statements - execution plan statistics of SQL statements"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/adb_stat_statements
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif