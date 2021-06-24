#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_ctl
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_ctl/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_ctl - starts/stops/restarts the PostgreSQL server"
PGAPPICON=win32

subdir = src/bin/pg_ctl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# On Windows, we need to link with libpq, just for use of pqexpbuffer;
# but let's not pull that in on platforms where we don't need it.
ifeq ($(PORTNAME), win32)
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += $(libpq_pgport)
SUBMAKE_LIBPQ := submake-libpq
endif

OBJS = \
	$(WIN32RES) \
	pg_ctl.o
OBJS_MGR = $(WIN32RES) \
	mgr_ctl.o

ifeq ($(enable_cluster),yes)
all: pg_ctl mgr_ctl
else
all: pg_ctl
endif

pg_ctl: $(OBJS) | submake-libpgport $(SUBMAKE_LIBPQ)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

ifeq ($(enable_cluster),yes)
mgr_ctl: $(OBJS_MGR) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS_MGR) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

mgr_ctl.o: pg_ctl.c
	$(CC) $(patsubst -DPGXC,, $(patsubst -DADB,, $(CFLAGS))) $(CPPFLAGS) -DMGR_CTL -c -o $@ $<
endif

install: all installdirs
	$(INSTALL_PROGRAM) pg_ctl$(X) '$(DESTDIR)$(bindir)/pg_ctl$(X)'
ifeq ($(enable_cluster),yes)
	$(INSTALL_PROGRAM) mgr_ctl$(X) '$(DESTDIR)$(bindir)/mgr_ctl$(X)'
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_ctl$(X)'
ifeq ($(enable_cluster),yes)
	rm -f '$(DESTDIR)$(bindir)/mgr_ctl$(X)'
endif

clean distclean maintainer-clean:
	rm -f pg_ctl$(X) $(OBJS)
ifeq ($(enable_cluster),yes)
	rm -f mgr_ctl$(X) $(OBJS_MGR)
endif
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
