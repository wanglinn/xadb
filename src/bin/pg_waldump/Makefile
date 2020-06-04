# src/bin/pg_waldump/Makefile

PGFILEDESC = "pg_waldump - decode and display WAL"
PGAPPICON=win32

subdir = src/bin/pg_waldump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

BASE_OBJS = pg_waldump.o compat.o xlogreader.o rmgrdesc.o $(RMGRDESCOBJS)
PG_OBJS  = $(BASE_OBJS:%.o=%_pg.o) $(WIN32RES)
ADB_OBJS = $(BASE_OBJS:%.o=%_adb.o) $(WIN32RES)

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc.c)))
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))

ifeq ($(enable_cluster),yes)
all: pg_waldump adb_waldump
else
all: pg_waldump
endif

pg_waldump: $(PG_OBJS) | submake-libpgport
ifeq ($(enable_cluster),yes)
	$(CC) $(filter-out, -DADB, $(CFLAGS)) $^ $(patsubst -lpgcommon, -lpgcommon_pg, $(LDFLAGS)) $(LDFLAGS_EX) $(patsubst -lpgcommon, -lpgcommon_pg, $(LIBS)) -o $@$(X)
else
	$(CC) $(filter-out, -DADB, $(CFLAGS)) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
endif

adb_waldump: $(ADB_OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

ifeq ($(autodepend), yes)

DEPDIR = .deps

$(PG_OBJS): %_pg.o : %.c
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CC) $(filter-out -DADB, $(CFLAGS)) $(CPPFLAGS) -c -o $@ $< -MMD -MP -MF $(DEPDIR)/$(*F).Po

$(ADB_OBJS): %_adb.o : %.c
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<  -MMD -MP -MF $(DEPDIR)/$(*F).Po

else #autodepend

$(PG_OBJS): %_pg.o : %.c
	$(CC) $(filter-out -DADB, $(CFLAGS)) $(CPPFLAGS) -c -o $@ $<

$(ADB_OBJS): %_adb.o : %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

endif #autodepend

install: all installdirs
	$(INSTALL_PROGRAM) pg_waldump$(X) '$(DESTDIR)$(bindir)/pg_waldump$(X)'
ifeq ($(enable_cluster),yes)
	$(INSTALL_PROGRAM) adb_waldump$(X) '$(DESTDIR)$(bindir)/adb_waldump$(X)'
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_waldump$(X)'
ifeq ($(enable_cluster),yes)
	rm -f '$(DESTDIR)$(bindir)/adb_waldump$(X)'
endif

clean distclean maintainer-clean:
	rm -f pg_waldump$(X) $(OBJS) $(RMGRDESCSOURCES) xlogreader.c adb_waldump$(X) $(ADB_OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
