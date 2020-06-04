#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_rewind
#
# Portions Copyright (c) 2013-2019, PostgreSQL Global Development Group
#
# src/bin/pg_rewind/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_rewind - synchronize a data directory with another one forked from"
PGAPPICON = win32

subdir = src/bin/pg_rewind
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -DFRONTEND $(CPPFLAGS)
LDFLAGS_INTERNAL += $(libpq_pgport)

BASE_OBJS	= pg_rewind.o parsexlog.o xlogreader.o datapagemap.o timeline.o \
	fetch.o file_ops.o copy_fetch.o libpq_fetch.o filemap.o

PG_OBJS = $(BASE_OBJS:%.o=%_pg.o) $(WIN32RES)
ADB_OBJS = $(BASE_OBJS:%.o=%_adb.o) $(WIN32RES)

EXTRA_CLEAN = xlogreader.c

ifeq ($(enable_cluster),yes)
all: pg_rewind adb_rewind
else
all: pg_rewind
endif

pg_rewind: $(PG_OBJS) | submake-libpq submake-libpgport
ifeq ($(enable_cluster),yes)
	$(CC) $(filter-out -DADB, $(CFLAGS)) $^ $(patsubst -lpgcommon, -lpgcommon_pg, $(LDFLAGS)) $(LDFLAGS_EX) $(patsubst -lpgcommon, -lpgcommon_pg, $(LIBS)) -o $@$(X)
else
	$(CC) $(filter-out -DADB, $(CFLAGS)) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
endif

adb_rewind: $(ADB_OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

ifeq ($(autodepend), yes)

DEPDIR = .deps

$(PG_OBJS): %_pg.o : %.c
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CC) $(patsubst -DADB,,$(CFLAGS)) $(CPPFLAGS) -c -o $@ $< -MMD -MP -MF $(DEPDIR)/$(*F).Po

$(ADB_OBJS): %_adb.o : %.c
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<  -MMD -MP -MF $(DEPDIR)/$(*F).Po

else #autodepend

$(PG_OBJS): %_pg.o : %.c
	$(CC) $(patsubst -DADB,,$(CFLAGS)) $(CPPFLAGS) -c -o $@ $<

$(ADB_OBJS): %_adb.o : %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

endif #autodepend

install: all installdirs
	$(INSTALL_PROGRAM) pg_rewind$(X) '$(DESTDIR)$(bindir)/pg_rewind$(X)'
ifeq ($(enable_cluster),yes)
	$(INSTALL_PROGRAM) adb_rewind$(X) '$(DESTDIR)$(bindir)/adb_rewind$(X)'
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_rewind$(X)'
ifeq ($(enable_cluster),yes)
	rm -f '$(DESTDIR)$(bindir)/adb_rewind$(X)'
endif

clean distclean maintainer-clean:
	rm -f pg_rewind$(X) $(PG_OBJS) xlogreader.c
ifeq ($(enable_cluster),yes)
	rm -f adb_rewind$(X) $(ADB_OBJS)
endif
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
