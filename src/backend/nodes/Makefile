#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/nodes
#
# IDENTIFICATION
#    src/backend/nodes/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/nodes
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = nodeFuncs.o nodes.o list.o bitmapset.o tidbitmap.o \
       copyfuncs.o equalfuncs.o extensible.o makefuncs.o \
       outfuncs.o readfuncs.o print.o read.o params.o value.o \
       outobject.o enum_funcs.o
ifeq ($(enable_cluster),yes)
  OBJS += saveload.o
endif
ifneq ($(enable_cluster)$(enable_grammar_oracle),nono)
  OBJS += nodeWalker.o nodeMutator.o
endif

include $(top_srcdir)/src/backend/common.mk
