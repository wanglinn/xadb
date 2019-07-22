#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for lib (miscellaneous stuff)
#
# IDENTIFICATION
#    src/backend/lib/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/lib
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = binaryheap.o bipartite_match.o bloomfilter.o dshash.o hyperloglog.o \
       ilist.o knapsack.o pairingheap.o rbtree.o stringinfo.o
ifeq ($(enable_cluster),yes)
  OBJS += oidbuffer.o
endif

include $(top_srcdir)/src/backend/common.mk
