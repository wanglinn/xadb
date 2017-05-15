# src/bin/pg_xlogdump/Makefile

PGFILEDESC = "pg_xlogdump - decode and display WAL"
PGAPPICON=win32

subdir = src/bin/pg_xlogdump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

BASE_OBJS = pg_xlogdump.o compat.o xlogreader.o rmgrdesc.o $(RMGRDESCOBJS)
PG_OBJS  = $(BASE_OBJS:%.o=%_pg.o) $(WIN32RES)
ADB_OBJS = $(BASE_OBJS:%.o=%_adb.o) $(WIN32RES) 

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc.c)))
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))


all: pg_xlogdump adb_xlogdump

pg_xlogdump: $(PG_OBJS) | submake-libpgport
	$(CC) $(filter-out, -DADB, $(CFLAGS)) $^ $(patsubst -lpgcommon, -lpgcommon_pg, $(LDFLAGS)) $(LDFLAGS_EX) $(patsubst -lpgcommon, -lpgcommon_pg, $(LIBS)) -o $@$(X)

adb_xlogdump: $(ADB_OBJS) | submake-libpgport
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
	$(INSTALL_PROGRAM) pg_xlogdump$(X) '$(DESTDIR)$(bindir)/pg_xlogdump$(X)'
	$(INSTALL_PROGRAM) adb_xlogdump$(X) '$(DESTDIR)$(bindir)/adb_xlogdump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_xlogdump$(X)'
	rm -f '$(DESTDIR)$(bindir)/adb_xlogdump$(X)'

clean distclean maintainer-clean:
	rm -f pg_xlogdump$(X) $(PG_OBJS) $(RMGRDESCSOURCES) xlogreader.c
	rm -f adb_xlogdump$(X) $(ADB_OBJS)
