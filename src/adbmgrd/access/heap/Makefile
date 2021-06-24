#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/heap
#
# IDENTIFICATION
#    src/adbmgrd/access/heap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/access/heap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	heapam.o \
	heapam_handler.o \
	heapam_visibility.o \
	heaptoast.o \
	hio.o \
	pruneheap.o \
	rewriteheap.o \
	vacuumlazy.o \
	visibilitymap.o

include $(top_srcdir)/src/adbmgrd/common.mk
