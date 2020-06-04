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
       objectaccess.o objectaddress.o partition.o pg_aggregate.o pg_collation.o \
       pg_constraint.o pg_conversion.o \
       pg_depend.o pg_enum.o pg_inherits.o pg_largeobject.o pg_namespace.o \
       pg_operator.o pg_proc.o pg_publication.o pg_range.o \
	   pg_db_role_setting.o pg_shdepend.o pg_subscription.o pg_type.o \
	   storage.o toasting.o

BKIFILES = adbmgrd.bki adbmgrd.description adbmgrd.shdescription

include $(top_srcdir)/src/adbmgrd/common.mk

# Note: the order of this list determines the order in which the catalog
# header files are assembled into postgres.bki.  BKI_BOOTSTRAP catalogs
# must appear first, and there are reputedly other, undocumented ordering
# dependencies.
CATALOG_HEADERS := \
	pg_proc.h pg_type.h pg_attribute.h pg_class.h \
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.h \
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.h pg_amproc.h \
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h \
	pg_statistic_ext.h \
	pg_statistic.h pg_rewrite.h pg_trigger.h pg_event_trigger.h pg_description.h \
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h \
	pg_database.h pg_db_role_setting.h pg_tablespace.h pg_pltemplate.h \
	pg_authid.h pg_auth_members.h pg_shdepend.h pg_shdescription.h \
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h \
	pg_ts_parser.h pg_ts_template.h pg_extension.h \
	pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h \
	pg_foreign_table.h pg_policy.h pg_replication_origin.h \
	pg_default_acl.h pg_init_privs.h pg_seclabel.h pg_shseclabel.h \
	pg_collation.h pg_partitioned_table.h pg_range.h pg_transform.h \
	pg_sequence.h pg_publication.h pg_publication_rel.h pg_subscription.h \
	pg_subscription_rel.h

MGR_CATALOG_HEADERS := \
	mgr_host.h mgr_parm.h mgr_updateparm.h mgr_node.h mgr_hba.h\
	monitor_host.h monitor_cpu.h monitor_mem.h monitor_net.h monitor_disk.h \
	monitor_varparm.h monitor_host_threshold.h monitor_databasetps.h monitor_databaseitem.h \
	monitor_slowlog.h monitor_alarm.h monitor_resolve.h monitor_user.h monitor_job.h monitor_jobitem.h

CATALOG_HEADERS += $(MGR_CATALOG_HEADERS)

MYCPPFLAGS = -DBUILD_BKI -DCATALOG_VARLEN -DADBMGRD -DNADB

GENERATED_HEADERS := $(CATALOG_HEADERS:%.h=%_d.h) schemapg.h
MGR_GENERATED_HEADERS := $(MGR_CATALOG_HEADERS:%.h=%_d.h)

# In the list of headers used to assemble postgres.bki, indexing.h needs
# be last, and toasting.h just before it.  This ensures we don't try to
# create indexes or toast tables before their catalogs exist.
toasting.h indexing.h: % : $(top_srcdir)/src/include/catalog/%
	rm -f $@.c && $(LN_S) $< $@.c
	$(CPP) -I$(top_srcdir)/src/include $(MYCPPFLAGS) -o $@ $@.c
	rm -f $@.c

POSTGRES_BKI_SRCS := $(addprefix $(top_srcdir)/src/include/catalog/,\
	$(CATALOG_HEADERS)) toasting.h indexing.h

# The .dat files we need can just be listed alphabetically.
POSTGRES_BKI_DATA = $(addprefix $(top_srcdir)/src/include/catalog/,\
	pg_aggregate.dat pg_am.dat pg_amop.dat pg_amproc.dat pg_authid.dat \
	pg_cast.dat pg_class.dat pg_collation.dat \
	pg_database.dat pg_language.dat \
	pg_namespace.dat pg_opclass.dat pg_operator.dat pg_opfamily.dat \
	pg_pltemplate.dat pg_proc.dat pg_range.dat pg_tablespace.dat \
	pg_ts_config.dat pg_ts_config_map.dat pg_ts_dict.dat pg_ts_parser.dat \
	pg_ts_template.dat pg_type.dat \
	)

# location of Catalog.pm
catalogdir = $(backend_src_dir)/catalog

all: distprep generated-header-symlinks

distprep: bki-stamp

.PHONY: generated-header-symlinks

generated-header-symlinks: $(top_builddir)/src/include/catalog/mgr-header-stamp

# bki-stamp records the last time we ran genbki.pl.  We don't rely on
# the timestamps of the individual output files, because the Perl script
# won't update them if they didn't change (to avoid unnecessary recompiles).
# Technically, this should depend on Makefile.global which supplies
# $(MAJORVERSION); but then genbki.pl would need to be re-run after every
# configure run, even in distribution tarballs.  So depending on configure.in
# instead is cheating a bit, but it will achieve the goal of updating the
# version number when it changes.
bki-stamp: genbki.pl Catalog.pm $(POSTGRES_BKI_SRCS) $(POSTGRES_BKI_DATA) $(top_srcdir)/configure.in
	$(PERL) $< --include-path=$(top_srcdir)/src/include/ \
		--set-version=$(MAJORVERSION) "--defines=$(MYCPPFLAGS)" $(POSTGRES_BKI_SRCS)
	mv postgres.bki adbmgrd.bki
	mv postgres.description adbmgrd.description
	mv postgres.shdescription adbmgrd.shdescription
	touch $@

$(top_builddir)/src/include/catalog/header-stamp:
	$(MAKE) -C "$(backend_obj_dir)/catalog" $@

$(top_builddir)/src/include/catalog/mgr-header-stamp: bki-stamp $(top_builddir)/src/include/catalog/header-stamp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	cd '$(dir $@)' && for file in $(MGR_GENERATED_HEADERS); do \
	  rm -f $$file && $(LN_S) "$$prereqdir/$$file" . ; \
	done
	touch $@

.PHONY: install-data
install-data: $(BKIFILES) installdirs
	$(INSTALL_DATA) $(call vpathsearch,adbmgrd.bki) '$(DESTDIR)$(datadir)/adbmgrd.bki'
	$(INSTALL_DATA) $(call vpathsearch,adbmgrd.description) '$(DESTDIR)$(datadir)/adbmgrd.description'
	$(INSTALL_DATA) $(call vpathsearch,adbmgrd.shdescription) '$(DESTDIR)$(datadir)/adbmgrd.shdescription'
	$(INSTALL_DATA) $(backend_src_dir)/catalog/system_views.sql '$(DESTDIR)$(datadir)/adbmgrd_views.sql'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, $(BKIFILES) mgr_views.sql)

# postgres.bki, postgres.description, postgres.shdescription, and schemapg.h
# are in the distribution tarball, so they are not cleaned here.
clean:
	rm -f toasting.h indexing.h

maintainer-clean: clean
	rm -f bki-stamp $(BKIFILES) $(GENERATED_HEADERS)
