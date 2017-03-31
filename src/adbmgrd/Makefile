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
	main nodes optimizer port postmaster regex replication rewrite manager \
	storage tcop tsearch utils $(top_builddir)/src/timezone \
	$(top_builddir)/src/interfaces/libpq

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
# libldap
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS)) $(LDAP_LIBS_BE)

# The admbgrd doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))
LIBS := $(LIBS) $(libpq) -lssh2

ifeq ($(with_systemd),yes)
LIBS += -lsystemd
endif

##########################################################################

all: submake-libpgport submake-schemapg adbmgrd $(MGR_IMP)

ifneq ($(PORTNAME), cygwin)
ifneq ($(PORTNAME), win32)
ifneq ($(PORTNAME), aix)

adbmgrd: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o $@

endif
endif
endif

ifeq ($(PORTNAME), cygwin)

adbmgrd: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) -Wl,--stack,$(WIN32_STACK_RLIMIT) -Wl,--export-all-symbols -Wl,--out-implib=libadbmgrd.a $(call expand_subsys,$^) $(LIBS) -o $@

# There is no correct way to write a rule that generates two files.
# Rules with two targets don't have that meaning, they are merely
# shorthand for two otherwise separate rules.  To be safe for parallel
# make, we must chain the dependencies like this.  The semicolon is
# important, otherwise make will choose some built-in rule.

libadbmgrd.a: adbmgrd ;

endif # cygwin

ifeq ($(PORTNAME), win32)
LIBS += -lsecur32

adbmgrd: $(OBJS) $(WIN32RES)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) -Wl,--stack=$(WIN32_STACK_RLIMIT) -Wl,--export-all-symbols -Wl,--out-implib=libadbmgrd.a $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS) -o $@$(X)

libadbmgrd.a: adbmgrd ;

endif # win32

ifeq ($(PORTNAME), aix)

adbmgrd: $(AGTM_IMP)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$(OBJS)) -Wl,-bE:$(top_builddir)/src/adbmgrd/$(AGTM_IMP) $(LIBS) -Wl,-brtllib -o $@

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

# Update the commonly used headers before building the subdirectories
$(SUBDIRS:%=%-recursive): | generated-headers

submake-errcodes:
	$(MAKE) -C $(backend_obj_dir) submake-errcodes

.PHONY: submake-errcodes

$(top_builddir)/src/port/libpgport_mgr.a: | submake-libpgport


# The postgres.o target is needed by the rule in Makefile.global that
# creates the exports file when MAKE_EXPORTS = true.
adbmgrd.o: $(OBJS)
	$(CC) $(LDREL) $(LDFLAGS) $(LDFLAGS_EX) $(call expand_subsys,$^) $(LIBS) -o $@


# The following targets are specified in make commands that appear in
# the make files in our subdirectories. Note that it's important we
# match the dependencies shown in the subdirectory makefiles!

parser/gram.h: parser/gram.y
	$(MAKE) -C parser gram.h

storage/lmgr/lwlocknames.h: $(backend_src_dir)/storage/lmgr/generate-lwlocknames.pl storage/lmgr/lwlocknames.txt
	$(MAKE) -C storage/lmgr lwlocknames.h

utils/fmgroids.h: $(backend_src_dir)/utils/Gen_fmgrtab.pl $(backend_src_dir)/catalog/Catalog.pm $(top_srcdir)/src/include/catalog/pg_proc.h
	$(MAKE) -C utils fmgroids.h

utils/probes.h: utils/probes.d
	$(MAKE) -C utils probes.h

# run this unconditionally to avoid needing to know its dependencies here:
catalog/schemapg.h: | submake-schemapg

submake-schemapg:
	$(MAKE) -C catalog schemapg.h

.PHONY: submake-schemapg

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

generated-headers: $(top_builddir)/src/adbmgrd/include/parser/gram.h $(mgr_inc_dir)/catalog/schemapg.h $(mgr_inc_dir)/storage/lwlocknames.h $(top_builddir)/src/include/utils/errcodes.h $(mgr_inc_dir)/utils/fmgroids.h $(mgr_inc_dir)/utils/probes.h

$(top_builddir)/src/adbmgrd/include/parser/gram.h: parser/gram.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(mgr_inc_dir)/catalog/schemapg.h: catalog/schemapg.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(mgr_inc_dir)/storage/lwlocknames.h: storage/lmgr/lwlocknames.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/errcodes.h: | submake-errcodes

$(mgr_inc_dir)/utils/fmgroids.h: utils/fmgroids.h
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	  cd '$(dir $@)' && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(mgr_inc_dir)/utils/probes.h: utils/probes.h
	cd '$(dir $@)' && rm -f $(notdir $@) && \
	    $(LN_S) "../../utils/probes.h" .


utils/probes.o: utils/probes.d $(SUBDIROBJS)
	$(DTRACE) $(DTRACEFLAGS) -C -G -s $(call expand_subsys,$^) -o $@


##########################################################################

# Be sure that these files get removed by the maintainer-clean target
distprep:
	$(MAKE) -C parser	gram.c gram.h scan.c
	$(MAKE) -C bootstrap	bootparse.c bootscanner.c
	$(MAKE) -C catalog	schemapg.h adbmgrd.bki adbmgrd.description adbmgrd.shdescription
	$(MAKE) -C replication	repl_gram.c repl_scanner.c syncrep_gram.c syncrep_scanner.c
	$(MAKE) -C storage/lmgr	lwlocknames.h
	$(MAKE) -C utils	fmgrtab.c fmgroids.h
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
	$(INSTALL_DATA) $(srcdir)/utils/misc/adbmgrd.conf.sample '$(DESTDIR)$(datadir)/adbmgrd.conf.sample'

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
	rm -f '$(DESTDIR)$(datadir)/adbmgrd.conf.sample'


##########################################################################

clean:
	rm -f $(LOCALOBJS) adbmgrd$(X) $(AGTM_IMP) \
		$(mgr_inc_dir)/parser/gram.h \
		$(mgr_inc_dir)/catalog/schemapg.h \
		$(mgr_inc_dir)/storage/lwlocknames.h \
		$(mgr_inc_dir)/utils/fmgroids.h \
		$(mgr_inc_dir)/utils/probes.h
ifeq ($(PORTNAME), cygwin)
	rm -f adbmgrd.dll libadbmgrd.a
endif
ifeq ($(PORTNAME), win32)
	rm -f adbmgrd.dll libadbmgrd.a $(WIN32RES)
endif

distclean: clean
	rm -f port/tas.s port/dynloader.c port/pg_sema.c port/pg_shmem.c

maintainer-clean: distclean
	rm -f bootstrap/bootparse.c \
	      bootstrap/bootscanner.c \
	      parser/gram.c \
	      parser/gram.h \
	      parser/scan.c \
	      catalog/schemapg.h \
	      catalog/adbmgrd.bki \
	      catalog/adbmgrd.description \
	      catalog/adbmgrd.shdescription \
	      replication/repl_gram.c \
	      replication/repl_scanner.c \
	      replication/syncrep_gram.c \
	      replication/syncrep_scanner.c \
	      storage/lmgr/lwlocknames.c \
	      storage/lmgr/lwlocknames.h \
	      utils/fmgroids.h \
	      utils/fmgrtab.c \
	      utils/errcodes.h \
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
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o adbmgrd
