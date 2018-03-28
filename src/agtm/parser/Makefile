#-------------------------------------------------------------------------
#
# Makefile for parser
#
# src/agtm/parser/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/parser
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS= analyze.o gram.o scan.o parser.o \
      parse_agg.o parse_clause.o parse_coerce.o parse_collate.o parse_cte.o \
      parse_enr.o parse_expr.o parse_func.o parse_node.o parse_oper.o \
      parse_param.o parse_relation.o parse_target.o parse_type.o \
      parse_utilcmd.o scansup.o

include $(top_srcdir)/src/agtm/common.mk


# Latest flex causes warnings in this file.
ifeq ($(GCC),yes)
scan.o: CFLAGS += -Wno-error
endif


# There is no correct way to write a rule that generates two files.
# Rules with two targets don't have that meaning, they are merely
# shorthand for two otherwise separate rules.  To be safe for parallel
# make, we must chain the dependencies like this.  The semicolon is
# important, otherwise make will choose the built-in rule for
# gram.y=>gram.c.

#agtm_gram.y:$(backend_src_dir)/$(cur_dir)/gram.y
#	$(PERL) $(backend_src_dir)/$(cur_dir)/drop-adb.pl $< $@

gram.h: gram.c ;

gram.c: BISONFLAGS += -d
gram.c:$(backend_src_dir)/$(cur_dir)/gram.y
	$(PERL) $(backend_src_dir)/$(cur_dir)/drop-adb.pl $< gram.y
	@echo $(PERL) $(backend_src_dir)/$(cur_dir)/check_keywords.pl gram.y $(top_srcdir)/src/agtm/include/parser/kwlist.h
ifdef BISON
	$(BISON) $(BISONFLAGS) -o $@ gram.y
else
	@$(missing) bison gram.y $@
endif
	$(PERL) $(backend_src_dir)/$(cur_dir)/modify-line.pl gram.c agtm_gram.c gram.y "$<"
	$(PERL) $(backend_src_dir)/$(cur_dir)/modify-line.pl gram.h agtm_gram.h gram.y "$<"
	rm gram.h gram.c
	mv agtm_gram.h gram.h
	mv agtm_gram.c gram.c

scan.c: FLEXFLAGS = -CF -p -p
scan.c: FLEX_NO_BACKUP=yes


# Force these dependencies to be known even without dependency info built:
gram.o scan.o parser.o: gram.h


# gram.c, gram.h, and scan.c are in the distribution tarball, so they
# are not cleaned here.
clean distclean maintainer-clean:
	rm -f lex.backup
