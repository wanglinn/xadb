#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for node management routines
#
#  Portions Copyright (c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/backend/pgxc/nodemgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = nodemgr.o groupmgr.o expansion.o

expansion.o: CFLAGS += -I$(libpq_srcdir)
expansion.bc: BITCODE_CFLAGS += -I$(libpq_srcdir)

include $(top_srcdir)/src/backend/common.mk
