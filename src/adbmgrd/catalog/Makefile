#-------------------------------------------------------------------------
#
# Makefile for mgr/catalog
#
# src/adbmgrd/catalog/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/catalog
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = catalog.o dependency.o heap.o index.o indexing.o namespace.o aclchk.o \
       objectaccess.o objectaddress.o pg_aggregate.o pg_collation.o \
       pg_constraint.o pg_conversion.o \
       pg_depend.o pg_enum.o pg_inherits.o pg_largeobject.o pg_namespace.o \
       pg_operator.o pg_proc.o pg_range.o pg_db_role_setting.o pg_shdepend.o \
       pg_type.o storage.o toasting.o

BKIFILES = mgr.bki mgr.description mgr.shdescription

include $(top_srcdir)/src/adbmgrd/common.mk

all: $(BKIFILES) schemapg.h

# Note: there are some undocumented dependencies on the ordering in which
# the catalog header files are assembled into postgres.bki.  In particular,
# indexing.h had better be last, and toasting.h just before it.

POSTGRES_BKI_SRCS = \
	pg_proc.h pg_type.h pg_attribute.h pg_class.h \
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.h \
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.h pg_amproc.h \
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h \
	pg_statistic.h pg_rewrite.h pg_trigger.h pg_event_trigger.h pg_description.h \
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h \
	pg_database.h pg_db_role_setting.h pg_tablespace.h pg_pltemplate.h \
	pg_authid.h pg_auth_members.h pg_shdepend.h pg_shdescription.h \
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h \
	pg_ts_parser.h pg_ts_template.h pg_extension.h \
	pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h \
	pg_foreign_table.h pg_policy.h pg_replication_origin.h \
	pg_default_acl.h pg_init_privs.h pg_seclabel.h pg_shseclabel.h \
	pg_collation.h pg_range.h pg_transform.h \
	toasting.h indexing.h 

$(POSTGRES_BKI_SRCS): % : $(top_srcdir)/src/include/catalog/%
	rm -f $@.c && $(LN_S) $< $@.c
	$(CPP) -I$(top_srcdir)/src/include -DBUILD_BKI -DCATALOG_VARLEN -DADBMGRD -o $@ $@.c
	rm -f $@.c

# location of Catalog.pm
catalogdir = $(backend_src_dir)/catalog

# locations of headers that genbki.pl needs to read
pg_includes = -I$(top_srcdir)/src/include/catalog -I$(top_builddir)/src/include/catalog

# see explanation in ../parser/Makefile
mgr.description: mgr.bki ;

mgr.shdescription: mgr.bki ;

schemapg.h: mgr.bki ;

# Technically, this should depend on Makefile.global, but then
# postgres.bki would need to be rebuilt after every configure run,
# even in distribution tarballs.  So this is cheating a bit, but it
# will achieve the goal of updating the version number when it
# changes.
mgr.bki: genbki.pl Catalog.pm $(POSTGRES_BKI_SRCS) $(top_srcdir)/configure $(top_srcdir)/src/include/catalog/duplicate_oids
	cd $(top_srcdir)/src/include/catalog && $(PERL) ./duplicate_oids
	$(PERL) -I $(catalogdir) $< $(pg_includes) --set-version=$(MAJORVERSION) $(POSTGRES_BKI_SRCS)
	mv postgres.bki mgr.bki
	mv postgres.description mgr.description
	mv postgres.shdescription mgr.shdescription

.PHONY: install-data
install-data: $(BKIFILES) installdirs
	$(INSTALL_DATA) $(call vpathsearch,mgr.bki) '$(DESTDIR)$(datadir)/mgr.bki'
	$(INSTALL_DATA) $(call vpathsearch,mgr.description) '$(DESTDIR)$(datadir)/mgr.description'
	$(INSTALL_DATA) $(call vpathsearch,mgr.shdescription) '$(DESTDIR)$(datadir)/mgr.shdescription'
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/mgr_views.sql'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, $(BKIFILES) mgr_views.sql)

# postgres.bki, postgres.description, postgres.shdescription, and schemapg.h
# are in the distribution tarball, so they are not cleaned here.
clean:
	rm -f $(POSTGRES_BKI_SRCS)

maintainer-clean: clean
	rm -f $(BKIFILES)
