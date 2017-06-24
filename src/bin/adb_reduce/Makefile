# src/bin/adb_reduce/Makefile

PGFILEDESC = "adb_reduce - receive and dispatch data among ADB cluster nodes."
PGAPPICON=win32

subdir = src/bin/adb_reduce
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LINKS = assert.c aset.c mcxt.c stringinfo.c wait_event.c

OBJS = 	rdc_main.o rdc_msg.o rdc_comm.o rdc_tupstore.o rdc_msg.o \
		rdc_globals.o rdc_elog.o rdc_format.o rdc_exit.o rdc_list.o \
		assert.o aset.o mcxt.o stringinfo.o wait_event.o

override CPPFLAGS := -DRDC_FRONTEND $(CPPFLAGS)
override CFLAGS := -I$(top_srcdir)/$(subdir) $(CFLAGS)

all: adb_reduce

assert.c: % : $(top_srcdir)/src/backend/utils/error/%
	rm -f $@ && $(LN_S) $< .

aset.c mcxt.c: % : $(top_srcdir)/src/backend/utils/mmgr/%
	rm -f $@ && $(LN_S) $< .

stringinfo.c: % : $(top_srcdir)/src/backend/lib/%
	rm -f $@ && $(LN_S) $< .

wait_event.c: % : $(top_srcdir)/src/backend/reduce/%
	rm -f $@ && $(LN_S) $< .

adb_reduce: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) adb_reduce$(X) '$(DESTDIR)$(bindir)/adb_reduce$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/adb_reduce$(X)'

clean distclean maintainer-clean:
	rm -f adb_reduce$(X) $(OBJS) $(LINKS)
