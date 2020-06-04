#-------------------------------------------------------------------------
#
# Makefile for the postgres adbmgrd
#
# Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/adbmgrd/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "ADB manager daemon"
# This is a daemon process, which is why it is not labeled as an executable
#PGAPPICON=win32

subdir = src/adbmgrd
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = access bootstrap catalog parser commands executor foreign lib libpq \
	main nodes optimizer partitioning port postmaster \
	regex replication rewrite manager \
	statistics storage tcop tsearch utils $(top_builddir)/src/timezone \
	jit

include $(srcdir)/common.mk

# As of 1/2010:
# The probes.o file is necessary for dtrace support on Solaris, and on recent
# versions of systemtap.  (Older systemtap releases just produce an empty
# file, but that's okay.)  However, OS X's dtrace doesn't use it and doesn't
# even recognize the -G option.  So, build probes.o except on Darwin.
# This might need adjustment as other platforms add dtrace support.
ifneq ($(PORTNAME), darwin)
ifeq ($(enable_dtrace), yes)
LOCALOBJS += utils/probes.o
endif
endif

OBJS = $(SUBDIROBJS) $(LOCALOBJS) $(top_builddir)/src/port/libpgport_srv.a \
	$(top_builddir)/src/common/libpgcommon_mgr.a
# We put libpgport and libpgcommon into OBJS, so remove it from LIBS; also add
# libldap and ICU
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS)) $(LDAP_LIBS_BE) $(ICU_LIBS)

# The admbgrd doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))
LIBS := $(LIBS) $(libpq) -lssh2

ifeq ($(with_systemd),yes)
LIBS += -lsystemd
endif

##########################################################################

all: submake-libpgport submake-catalog-headers submake-utils-headers generated-headers adbmgrd $(MGR_IMP)

ifneq ($(PORTNAME), cygwin)
ifneq ($(PORTNAME), win32)
ifneq ($(PORTNAME), aix)

adbmgrd: $(OBJS) | submake-libpq
	$(CC) $(CFLAGS) $(call expand_subsys,$^) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(LIBS) -o $@

endif
endif
endif

ifeq ($(PORTNAME), cygwin)

adbmgrd: $(OBJS) | submake-libpq
	$(CC) $(CFLAGS) $(call expand_subsys,$^) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) -Wl,--stack,$(WIN32_STACK_RLIMIT) -Wl,--export-all-symbols -Wl,--out-implib=libadbmgrd.a $(LIBS) -o $@

# libpostgres.a is actually built in the preceding rule, but we need this to
# ensure it's newer than postgres; see notes in src/backend/parser/Makefile
libadbmgrd.a: adbmgrd
	touch $@

endif # cygwin

ifeq ($(PORTNAME), win32)
LIBS += -lsecur32

adbmgrd: $(OBJS) $(WIN32RES) | submake-libpq
	$(CC) $(CFLAGS) $(call expand_subsys,$(OBJS)) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack=$(WIN32_STACK_RLIMIT) -Wl,--export-all-symbols -Wl,--out-implib=libadbmgrd.a $(WIN32RES) $(LIBS) -o $@$(X)

# libpostgres.a is actually built in the preceding rule, but we need this to
# ensure it's newer than postgres; see notes in src/backend/parser/Makefile
libadbmgrd.a: adbmgrd
	touch $@

endif # win32

ifeq ($(PORTNAME), aix)

adbmgrd: $(AGTM_IMP) | submake-libpq
	$(CC) $(CFLAGS) $(call expand_subsys,$(OBJS)) $(LDFLAGS) $(LDFLAGS_EX) -Wl,-bE:$(top_builddir)/src/adbmgrd/$(AGTM_IMP) $(LIBS) -Wl,-brtllib -o $@

$(AGTM_IMP): $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(call expand_subsys,$^)
ifeq ($(host_os), aix3.2.5)
	$(MKLDEXPORT) SUBSYS.o $(bindir)/adbmgrd > $@
else
ifneq (,$(findstring aix4.1, $(host_os)))
	$(MKLDEXPORT) SUBSYS.o $(bindir)/adbmgrd > $@
else
	$(MKLDEXPORT) SUBSYS.o . > $@
endif
endif
	@rm -f SUBSYS.o

endif # aix

$(top_builddir)/src/port/libpgport_srv.a: | submake-libpgport


# The postgres.o target is needed by the rule in Makefile.global that
# creates the exports file when MAKE_EXPORTS = true.
adbmgrd.o: $(OBJS)
	$(CC) $(LDREL) $(call expand_subsys,$^) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@


# The following targets are specified in make commands that appear in
# the make files in our subdirectories. Note that it's important we
# match the dependencies shown in the subdirectory makefiles!

parser/gram.h: $(top_srcdir)/src/backend/parser/gram.y
	$(MAKE) -C parser gram.h

storage/lmgr/lwlocknames.h: $(backend_src_dir)/storage/lmgr/generate-lwlocknames.pl storage/lmgr/lwlocknames.txt
	$(MAKE) -C storage/lmgr lwlocknames.h

# run this unconditionally to avoid needing to know its dependencies here:
submake-catalog-headers:
	$(MAKE) -C catalog distprep generated-header-symlinks

# run this unconditionally to avoid needing to know its dependencies here:
submake-utils-headers:
	$(MAKE) -C utils distprep generated-header-symlinks

.PHONY: submake-catalog-headers submake-utils-headers

# Make symlinks for these headers in the include directory. That way
# we can cut down on the -I options. Also, a symlink is automatically
# up to date when we update the base file.
#
# The point of the prereqdir incantation in some of the rules below is to
# force the symlink to use an absolute path rather than a relative path.
# For headers which are generated by make distprep, the actual header within
# src/adbmgrd will be in the source tree, while the symlink in src/include
# will be in the build tree, so a simple ../.. reference won't work.
# For headers generated during regular builds, we prefer a relative symlink.

.PHONY: generated-headers

generated-headers: $(top_builddir)/src/adbmgrd/include/parser/gram.h $(mgr_inc_dir)/storage/lwlocknames.h submake-catalog-headers submake-utils-headers

$(top_builddir)/src/adbmgrd/include/parser/gram.h: parser/gram.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(mgr_inc_dir)/storage/lwlocknames.h: storage/lmgr/lwlocknames.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .


utils/probes.o: utils/probes.d $(SUBDIROBJS)
	$(DTRACE) $(DTRACEFLAGS) -C -G -s $(call expand_subsys,$^) -o $@


##########################################################################

# Be sure that these files get removed by the maintainer-clean target
distprep:
	$(MAKE) -C parser	gram.c gram.h scan.c
	$(MAKE) -C bootstrap	bootparse.c bootscanner.c
	$(MAKE) -C catalog	distprep
	$(MAKE) -C replication	repl_gram.c repl_scanner.c syncrep_gram.c syncrep_scanner.c
	$(MAKE) -C storage/lmgr	lwlocknames.h lwlocknames.c
	$(MAKE) -C utils	distprep
	$(MAKE) -C utils/misc	guc-file.c
	$(MAKE) -C utils/sort	qsort_tuple.c


##########################################################################

install: all installdirs install-bin
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(INSTALL_DATA) libadbmgrd.a '$(DESTDIR)$(libdir)/libadbmgrd.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(INSTALL_DATA) libadbmgrd.a '$(DESTDIR)$(libdir)/libadbmgrd.a'
endif
endif
	$(MAKE) -C catalog install-data
	$(MAKE) -C tsearch install-data
	$(MAKE) -C utils install-data
	$(INSTALL_DATA) $(srcdir)/utils/misc/postgresql.conf.sample '$(DESTDIR)$(datadir)/adbmgrd.conf.sample'
	$(INSTALL_DATA) $(srcdir)/manager/adbmgr_init.sql '$(DESTDIR)$(datadir)/adbmgr_init.sql'

install-bin: adbmgrd $(POSTGRES_IMP) installdirs
	$(INSTALL_PROGRAM) adbmgrd$(X) '$(DESTDIR)$(bindir)/adbmgrd$(X)'
ifeq ($(MAKE_EXPORTS), true)
	$(INSTALL_DATA) $(AGTM_IMP) '$(DESTDIR)$(pkglibdir)/$(AGTM_IMP)'
endif

.PHONY: install-bin

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(MAKE_EXPORTS), true)
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)'
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(MKLDEXPORT_DIR)'
endif


##########################################################################

uninstall:
	rm -f '$(DESTDIR)$(bindir)/adbmgrd$(X)'
ifeq ($(MAKE_EXPORTS), true)
	rm -f '$(DESTDIR)$(pkglibdir)/$(AGTM_IMP)'
endif
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libadbmgrd.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libadbmgrd.a'
endif
endif
	$(MAKE) -C catalog uninstall-data
	$(MAKE) -C tsearch uninstall-data
	$(MAKE) -C utils uninstall-data
	rm -f '$(DESTDIR)$(datadir)/adbmgrd.conf.sample'


##########################################################################

clean:
	rm -f $(LOCALOBJS) adbmgrd$(X) $(AGTM_IMP)
ifeq ($(PORTNAME), cygwin)
	rm -f adbmgrd.dll libadbmgrd.a
endif
ifeq ($(PORTNAME), win32)
	rm -f adbmgrd.dll libadbmgrd.a $(WIN32RES)
endif

distclean: clean
	rm -f port/tas.s port/dynloader.c port/pg_sema.c port/pg_shmem.c

maintainer-clean: distclean
	$(MAKE) -C catalog $@
	$(MAKE) -C utils $@
	rm -f bootstrap/bootparse.c \
	      bootstrap/bootscanner.c \
	      parser/gram.c \
	      parser/gram.h \
	      parser/scan.c \
	      replication/repl_gram.c \
	      replication/repl_scanner.c \
	      replication/syncrep_gram.c \
	      replication/syncrep_scanner.c \
	      storage/lmgr/lwlocknames.c \
	      storage/lmgr/lwlocknames.h \
	      utils/misc/guc-file.c \
	      utils/sort/qsort_tuple.c


##########################################################################
#
# Support for code development.
#
# Use target "quick" to build "adbmgrd" when you know all the subsystems
# are up to date.  It saves the time of doing all the submakes.
.PHONY: quick
quick: $(OBJS)
	$(CC) $(CFLAGS) $(call expand_subsys,$^) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(LIBS) -o adbmgrd
