#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/lmgr
#
# IDENTIFICATION
#    src/backend/storage/lmgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/lmgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	condition_variable.o \
	deadlock.o \
	lmgr.o \
	lock.o \
	lwlock.o \
	lwlocknames.o \
	predicate.o \
	proc.o \
	s_lock.o \
	spin.o

include $(top_srcdir)/src/backend/common.mk

ifdef TAS
TASPATH = $(top_builddir)/src/backend/port/tas.o
endif

s_lock_test: s_lock.c $(top_builddir)/src/port/libpgport.a
	$(CC) $(CPPFLAGS) $(CFLAGS) -DS_LOCK_TEST=1 $(srcdir)/s_lock.c \
		$(TASPATH) -L $(top_builddir)/src/port -lpgport -o s_lock_test

# see notes in src/backend/parser/Makefile
lwlocknames.c: lwlocknames.h
	touch $@

lwlocknames.h: $(top_srcdir)/src/backend/storage/lmgr/lwlocknames.txt generate-lwlocknames.pl
ifeq ($(enable_cluster),yes)
	$(PERL) $(srcdir)/generate-lwlocknames.pl $<
else
	$(PERL) $(abs_top_srcdir)/src/backend/parser/drop-adb.pl $< tmpnames.txt
	$(PERL) $(srcdir)/generate-lwlocknames.pl tmpnames.txt
endif

check: s_lock_test
	./s_lock_test

clean distclean:
	rm -f s_lock_test

maintainer-clean: clean
	rm -f lwlocknames.h lwlocknames.c
ifneq ($(enable_cluster),yes)
	rm -f tmpnames.txt
endif
