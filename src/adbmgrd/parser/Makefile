#-------------------------------------------------------------------------
#
# Makefile for parser
#
# src/adbmgrd/parser/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/parser
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS= analyze.o gram.o scan.o parser.o \
      parse_agg.o parse_clause.o parse_coerce.o parse_collate.o parse_cte.o \
      parse_enr.o parse_expr.o parse_func.o parse_node.o parse_oper.o \
      parse_param.o parse_relation.o parse_target.o parse_type.o \
      parse_utilcmd.o scansup.o mgr_gram.o mgr_kwlist.o

include $(top_srcdir)/src/adbmgrd/common.mk

# where to find gen_keywordlist.pl and subsidiary files
TOOLSDIR = $(top_srcdir)/src/tools
GEN_KEYWORDLIST = $(PERL) -I $(TOOLSDIR) $(TOOLSDIR)/gen_keywordlist.pl
GEN_KEYWORDLIST_DEPS = $(TOOLSDIR)/gen_keywordlist.pl $(TOOLSDIR)/PerfectHash.pm

mgr_kwlist.o: mgr_kwlist_d.h

mgr_kwlist_d.h: $(top_srcdir)/src/include/parser/mgr_kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(CC) $(CFLAGS) -E $< -o mgr_kwlist.h
	$(GEN_KEYWORDLIST) --varname=ManagerKeywords --extern mgr_kwlist.h
	rm -f mgr_kwlist.h

# scan is compiled as part of gram
gram.o mgr_gram.o: scan.c

# Latest flex causes warnings in this file.
ifeq ($(GCC),yes)
gram.o mgr_gram.o: CFLAGS += -Wno-error
scan.o: CFLAGS += -Wno-error
endif

# There is no correct way to write a rule that generates two files.
# Rules with two targets don't have that meaning, they are merely
# shorthand for two otherwise separate rules.  To be safe for parallel
# make, we must chain the dependencies like this.  The semicolon is
# important, otherwise make will choose the built-in rule for
# gram.y=>gram.c.

gram.h: gram.c ;
mgr_gram.h: mgr_gram.c ;

gram.c mgr_gram.c: BISONFLAGS += -d
gram.c:$(backend_src_dir)/$(cur_dir)/gram.y
	$(PERL) $(backend_src_dir)/$(cur_dir)/drop-adb.pl $< gram.y
	@echo $(PERL) $(backend_src_dir)/$(cur_dir)/check_keywords.pl gram.y $(top_srcdir)/src/agtm/include/parser/kwlist.h
ifdef BISON
	$(BISON) $(BISONFLAGS) -o $@ gram.y
else
	@$(missing) bison $< $@
endif
	$(PERL) $(backend_src_dir)/$(cur_dir)/modify-line.pl gram.c tmp_gram.c gram.y "$<"
	$(PERL) $(backend_src_dir)/$(cur_dir)/modify-line.pl gram.h tmp_gram.h gram.y "$<"
	rm gram.h gram.c
	mv tmp_gram.h gram.h
	mv tmp_gram.c gram.c
#mgr_gram.c: BISON_CHECK_CMD = $(PERL) $(backend_src_dir)/check_keywords.pl $< $(mgr_inc_dir)/parser/kwlist.h


scan.c: FLEXFLAGS = -CF -p -p
scan.c: FLEX_NO_BACKUP=yes


# Force these dependencies to be known even without dependency info built:
gram.o keywords.o parser.o: gram.h
mgr_gram.o mgr_kwlist.o: mgr_gram.h

clean:
	rm -f gram.c gram.h scan.c mgr_gram.c mgr_gram.h lex.backup

distclean maintainer-clean:
	rm -f lex.backup mgr_kwlist_d.h

keywords.c:$(top_srcdir)/src/common/keywords.c
	$(LN_S) $<
