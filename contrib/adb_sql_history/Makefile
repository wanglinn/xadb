# contrib/adb_sql_history/Makefile

MODULE_big = adb_sql_history
OBJS = adb_sql_history.o $(WIN32RES)

EXTENSION = adb_sql_history
DATA = adb_sql_history--1.0.sql
PGFILEDESC = "adb_sql_history - record SQL statements of process"

LDFLAGS_SL += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/adb_sql_history
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

