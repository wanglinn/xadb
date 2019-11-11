#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for optimizer/util
#
# IDENTIFICATION
#    src/backend/optimizer/util/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/optimizer/util
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = clauses.o joininfo.o orclauses.o \
       paramassign.o pathnode.o placeholder.o \
       plancat.o predtest.o relnode.o restrictinfo.o tlist.o var.o

ifeq ($(enable_cluster), yes)
  OBJS += pgxcship.o reduceinfo.o remotetest.o
remotetest.o: CFLAGS += -I$(libpq_srcdir)
remotetest.bc: BITCODE_CFLAGS += -I$(libpq_srcdir)
endif

include $(top_srcdir)/src/backend/common.mk
