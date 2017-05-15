# src/bin/adb_clogdump/Makefile

PGFILEDESC = "adb_clogdump - decode and display transaction status of any transaction id"
PGAPPICON=win32

subdir = src/bin/adb_clogdump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = adb_clogdump.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

all: adb_clogdump

adb_clogdump: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) adb_clogdump$(X) '$(DESTDIR)$(bindir)/adb_clogdump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/adb_clogdump$(X)'

clean distclean maintainer-clean:
	rm -f adb_clogdump$(X) $(OBJS)
