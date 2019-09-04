#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pool
#
#  Portions Copyright (c) 2010-2012, Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/backend/pgxc/pool
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS =  poolmgr_adb.o poolcomm.o poolutils.o pgxcnode.o execRemote.o
poolmgr_adb.o pgxcnode.o: CFLAGS += -I$(libpq_srcdir)
poolmgr_adb.bc pgxcnode.bc: BITCODE_CFLAGS += -I$(libpq_srcdir)

include $(top_srcdir)/src/backend/common.mk
