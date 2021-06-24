#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/sort
#
# IDENTIFICATION
#    src/adbmgrd/utils/sort/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/utils/sort
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

OBJS = \
	logtape.o \
	sharedtuplestore.o \
	sortsupport.o \
	tuplesort.o \
	tuplestore.o \
	hashstore.o \
	batchstore.o

include $(top_srcdir)/src/adbmgrd/common.mk
