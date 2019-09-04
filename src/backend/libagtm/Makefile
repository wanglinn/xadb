#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for client
#
# IDENTIFICATION
#    src/backend/libagtm/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/libagtm
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = agtm.o agtm_client.o agtm_2pc.o agtm_utils.o

CFLAGS += -I$(abs_top_srcdir)/src/interfaces
BITCODE_CFLAGS += -I$(top_srcdir)/src/interfaces

include $(top_srcdir)/src/backend/common.mk 
