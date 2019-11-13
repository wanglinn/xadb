#-------------------------------------------------------------------------
#
# Makefile for parser
#
# src/backend/parser/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/parser
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS= analyze.o gram.o scan.o parser.o \
      parse_agg.o parse_clause.o parse_coerce.o parse_collate.o parse_cte.o \
      parse_enr.o parse_expr.o parse_func.o parse_node.o parse_oper.o \
      parse_param.o parse_relation.o parse_target.o parse_type.o \
      parse_utilcmd.o scansup.o
ifeq ($(enable_grammar_oracle),yes)
	OBJS += ora_gram.o
endif
ifeq ($(enable_grammar_db2),yes)
	OBJS += db2_gram.o
endif

include $(top_srcdir)/src/backend/common.mk


# There is no correct way to write a rule that generates two files.
# Rules with two targets don't have that meaning, they are merely
# shorthand for two otherwise separate rules.  If we have an action
# that in fact generates two or more files, we must choose one of them
# as primary and show it as the action's output, then make all of the
# other output files dependent on the primary, like this.  Furthermore,
# the "touch" action is essential, because it ensures that gram.h is
# marked as newer than (or at least no older than) gram.c.  Without that,
# make is likely to try to rebuild gram.h in subsequent runs, which causes
# failures in VPATH builds from tarballs.

gram.h: gram.c
	touch $@
ora_gram.h: ora_gram.c
	touch $@
db2_gram.h: db2_gram.c
	touch $@

gram.c: BISONFLAGS += -d
gram.c: BISON_CHECK_CMD = $(PERL) $(srcdir)/check_keywords.pl $< $(top_srcdir)/src/include/parser/kwlist.h
ifneq ($(enable_cluster),yes)
gram.c: $(abs_top_srcdir)/$(subdir)/gram.y drop-adb.pl modify-line.pl
	$(PERL) $(abs_top_srcdir)/$(subdir)/drop-adb.pl $< gram.y
ifdef BISON
	$(BISON) $(BISONFLAGS) -o tmp_$@ gram.y
else
	@$(missing) bison gram.y $@
endif
	$(PERL) $(abs_top_srcdir)/$(subdir)/modify-line.pl tmp_gram.c gram.c gram.y "$<"
	$(PERL) $(abs_top_srcdir)/$(subdir)/modify-line.pl tmp_gram.h gram.h gram.y "$<"
endif

ora_gram.c: BISONFLAGS += -d
ifneq ($(enable_cluster),yes)
ora_gram.c: $(abs_top_srcdir)/$(subdir)/ora_gram.y drop-adb.pl modify-line.pl
	$(PERL) $(abs_top_srcdir)/$(subdir)/drop-adb.pl $< ora_gram.y
ifdef BISON
	$(BISON) $(BISONFLAGS) -o tmp_$@ ora_gram.y
else
	@$(missing) bison ora_gram.y $@
endif
	$(PERL) $(abs_top_srcdir)/$(subdir)/modify-line.pl tmp_ora_gram.c ora_gram.c ora_gram.y "$<"
	$(PERL) $(abs_top_srcdir)/$(subdir)/modify-line.pl tmp_ora_gram.h ora_gram.h ora_gram.y "$<"
endif

db2_gram.c: BISONFLAGS += -d
ifneq ($(enable_cluster),yes)
db2_gram.c: $(abs_top_srcdir)/$(subdir)/db2_gram.y drop-adb.pl modify-line.pl
	$(PERL) $(abs_top_srcdir)/$(subdir)/drop-adb.pl $< db2_gram.y
ifdef BISON
	$(BISON) $(BISONFLAGS) -o tmp_$@ db2_gram.y
else
	@$(missing) bison db2_gram.y $@
endif
	$(PERL) $(abs_top_srcdir)/$(subdir)/modify-line.pl tmp_db2_gram.c db2_gram.c db2_gram.y "$<"
	$(PERL) $(abs_top_srcdir)/$(subdir)/modify-line.pl tmp_db2_gram.h db2_gram.h db2_gram.y "$<"
endif

scan.c: FLEXFLAGS = -CF -p -p
scan.c: FLEX_NO_BACKUP=yes
scan.c: FLEX_FIX_WARNING=yes


# Force these dependencies to be known even without dependency info built:
gram.o scan.o parser.o: gram.h
ifeq ($(enable_grammar_oracle),yes)
ora_gram.o: ora_gram.h gram.h
endif
ifeq ($(enable_grammar_db2),yes)
db2_gram.o: db2_gram.h gram.h
endif

# gram.c, gram.h, and scan.c are in the distribution tarball, so they
# are not cleaned here.
clean distclean maintainer-clean:
	rm -f lex.backup tmp_gram.h tmp_gram.c \
	tmp_ora_gram.h tmp_ora_gram.c \
	tmp_db2_gram.h tmp_db2_gram.c
