#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/replication
#
# IDENTIFICATION
#    src/backend/replication/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/replication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = \
	backup_manifest.o \
	basebackup.o \
	repl_gram.o \
	slot.o \
	slotfuncs.o \
	syncrep.o \
	syncrep_gram.o \
	walreceiver.o \
	walreceiverfuncs.o \
	walsender.o

SUBDIRS = logical

include $(top_srcdir)/src/adbmgrd/common.mk

repl_gram.c repl_scanner.c syncrep_gram.c syncrep_scanner.c: % : $(backend_obj_dir)/$(cur_dir)/%
	rm -f $@
	$(MAKE) -C $(dir $<) $@
	$(LN_S) $< .

# repl_scanner is compiled as part of repl_gram
repl_gram.o: repl_scanner.c

# syncrep_scanner is complied as part of syncrep_gram
syncrep_gram.o: syncrep_scanner.c
syncrep_scanner.c: FLEXFLAGS = -CF -p
syncrep_scanner.c: FLEX_NO_BACKUP=yes

# repl_gram.c, repl_scanner.c, syncrep_gram.c and syncrep_scanner.c
# are in the distribution tarball, so they are not cleaned here.
