#
# Makefile for utils
#
# src/adbmgrd/utils/Makefile
#

subdir = src/adbmgrd/utils
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS        = fmgrtab.o
SUBDIRS     = adt cache error fmgr hash init mb misc mmgr resowner sort time

# location of Catalog.pm
catalogdir  = $(top_srcdir)/src/backend/catalog

include $(top_srcdir)/src/adbmgrd/common.mk

all: distprep generated-header-symlinks

distprep: fmgr-stamp $(top_builddir)/src/include/utils/errcodes.h

.PHONY: generated-header-symlinks

generated-header-symlinks: $(top_builddir)/src/include/utils/header-stamp \
	$(mgr_inc_dir)/utils/header-stamp \
	$(top_builddir)/src/include/utils/probes.h

$(SUBDIRS:%=%-recursive): fmgr-stamp $(top_builddir)/src/include/utils/errcodes.h

FMGR_DATA := $(addprefix $(top_srcdir)/src/include/catalog/,\
	pg_language.dat pg_proc.dat \
	)

# fmgr-stamp records the last time we ran Gen_fmgrtab.pl.  We don't rely on
# the timestamps of the individual output files, because the Perl script
# won't update them if they didn't change (to avoid unnecessary recompiles).
fmgr-stamp: Gen_fmgrtab.pl $(catalogdir)/Catalog.pm $(FMGR_DATA) $(top_srcdir)/src/include/access/transam.h
	$(PERL) -I $(catalogdir) $< --include-path=$(top_srcdir)/src/include/ "--defines=-DADBMGRD" $(top_srcdir)/src/include/catalog/pg_proc.dat
	touch $@

$(top_builddir)/src/include/utils/errcodes.h: $(top_builddir)/src/include/utils/header-stamp

$(top_builddir)/src/include/utils/header-stamp:
	$(MAKE) -C $(backend_obj_dir)/utils $@

$(mgr_inc_dir)/utils/header-stamp: fmgr-stamp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	cd '$(dir $@)' && for file in fmgroids.h fmgrprotos.h; do \
	  rm -f $$file && $(LN_S) "$$prereqdir/$$file" . ; \
	done
	touch $@

$(top_builddir)/src/include/utils/probes.h:
	$(MAKE) -C $(backend_obj_dir)/utils $@

.PHONY: install-data
install-data:
	$(MAKE) -C $(backend_obj_dir)/utils $@

maintainer-clean: clean
	rm -f fmgroids.h fmgrprotos.h fmgrtab.c fmgr-stamp errcodes.h
