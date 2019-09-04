#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/nodes/intercomm
#
# IDENTIFICATION
#    src/backend/nodes/intercomm/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/intercomm
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
CFLAGS += -I$(libpq_srcdir)
BITCODE_CFLAGS += -I$(libpq_srcdir)

OBJS = inter-node.o inter-comm.o inter-xact.o inter-query.o inter-copy.o

include $(top_srcdir)/src/backend/common.mk
