#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_ctl
#
# Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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

OBJS=	pg_ctl.o $(WIN32RES)
OBJS_MGR = mgr_ctl.o $(WIN32RES)
OBJS_ATM = agtm_ctl.o $(WIN32RES)

ifeq ($(enable_cluster),yes)
all: pg_ctl mgr_ctl agtm_ctl
else
all: pg_ctl
endif

pg_ctl: $(OBJS) | submake-libpgport $(SUBMAKE_LIBPQ)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

ifeq ($(enable_cluster),yes)
mgr_ctl: $(OBJS_MGR) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS_MGR) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

agtm_ctl: $(OBJS_ATM) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS_ATM) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

mgr_ctl.o: pg_ctl.c
	$(CC) $(patsubst -DPGXC,, $(patsubst -DADB,, $(CFLAGS))) $(CPPFLAGS) -DMGR_CTL -c -o $@ $<

agtm_ctl.o: pg_ctl.c
	$(CC) $(patsubst -DPGXC,, $(patsubst -DADB,, $(CFLAGS))) $(CPPFLAGS) -DAGTM_CTL -c -o $@ $<
endif

install: all installdirs
	$(INSTALL_PROGRAM) pg_ctl$(X) '$(DESTDIR)$(bindir)/pg_ctl$(X)'
ifeq ($(enable_cluster),yes)
	$(INSTALL_PROGRAM) mgr_ctl$(X) '$(DESTDIR)$(bindir)/mgr_ctl$(X)'
	$(INSTALL_PROGRAM) agtm_ctl$(X) '$(DESTDIR)$(bindir)/agtm_ctl$(X)'
endif

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_ctl$(X)'
ifeq ($(enable_cluster),yes)
	rm -f '$(DESTDIR)$(bindir)/mgr_ctl$(X)' '$(DESTDIR)$(bindir)/agtm_ctl$(X)'
endif

clean distclean maintainer-clean:
	rm -f pg_ctl$(X) $(OBJS)
ifeq ($(enable_cluster),yes)
	rm -f mgr_ctl$(X) $(OBJS_MGR) agtm_ctl$(X) $(OBJS_ATM)
endif
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
