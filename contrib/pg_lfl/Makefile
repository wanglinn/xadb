# pg_lfl/Makefile

MODULE_big = pg_lfl
OBJS = pg_lfl.o $(WIN32RES)

EXTENSION = pg_lfl
PGFILEDESC = "pg_lfl - execution login failed lock"

LDFLAGS_SL += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_lfl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
