#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for JIT code that's provider independent.
#
# Note that the LLVM JIT provider is recursed into by src/Makefile,
# not from here.
#
# IDENTIFICATION
#    src/adbmgrd/jit/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/jit
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS += -DDLSUFFIX=\"$(DLSUFFIX)\"

OBJS = jit.o

include $(top_srcdir)/src/adbmgrd/common.mk
