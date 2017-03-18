#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/agtm/replication/logical
#
# IDENTIFICATION
#    src/agtm/replication/logical/Makefile
#
#-------------------------------------------------------------------------

subdir = src/agtm/replication/logical
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) $(CPPFLAGS)

OBJS = decode.o logical.o logicalfuncs.o message.o origin.o reorderbuffer.o \
	snapbuild.o

include $(top_srcdir)/src/agtm/common.mk
