#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/heap
#
# IDENTIFICATION
#    src/backend/access/heap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/heap
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
	syncscan.o \
	vacuumlazy.o \
	visibilitymap.o

ifeq ($(enable_cluster),yes)
  vacuumlazy.o: CFLAGS += -I$(libpq_srcdir)
  vacuumlazy.bc: BITCODE_CFLAGS += -I$(libpq_srcdir)
endif

include $(top_srcdir)/src/backend/common.mk
