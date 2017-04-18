# src/bin/pg_extendpage/Makefile

PGFILEDESC = "pg_extendpage - Extend file with one or more zero pages."
PGAPPICON=win32

subdir = src/bin/pg_extendpage
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = pg_extendpage.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

all: pg_extendpage

pg_extendpage: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_extendpage$(X) '$(DESTDIR)$(bindir)/pg_extendpage$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_extendpage$(X)'

clean distclean maintainer-clean:
	rm -f pg_extendpage$(X) $(OBJS)
