#-------------------------------------------------------------------------
#
# Makefile for backend/oraschema
#
# src/backend/oraschema/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/oraschema
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = convert.o nvarchar2.o orastr.o oradate.o others.o random.o varchar2.o oracoerce.o oraraw.o
ORA_SCHEMA = oracle_schema.sql
ORA_SQL_FILES = oracle_type.sql \
				oracle_proc.sql \
				oracle_random.sql \
				oracle_operator.sql \
				oracle_view.sql

include $(top_srcdir)/src/backend/common.mk

all: $(ORA_SCHEMA)

$(ORA_SCHEMA):$(ORA_SQL_FILES)
	echo "GRANT USAGE ON SCHEMA oracle TO PUBLIC;" > $@
	cat $^ >> $@

.PHONY: install-data
install-data: $(ORA_SCHEMA) installdirs
	$(INSTALL_DATA) $(call vpathsearch,$(ORA_SCHEMA)) '$(DESTDIR)$(datadir)/$(ORA_SCHEMA)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, $(ORA_SCHEMA) $(ORA_VIEW))

clean:
	-rm -f $(ORA_SCHEMA)
