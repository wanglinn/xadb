#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for libpq subsystem (mgr half of libpq interface)
#
# IDENTIFICATION
#    src/adbmgrd/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/adbmgrd/libpq
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# be-fsstubs is here for historical reasons, probably belongs elsewhere

OBJS = be-fsstubs.o be-secure.o be-secure-common.o auth.o crypt.o hba.o ifaddr.o pqcomm.o \
       pqformat.o pqmq.o pqsignal.o auth-scram.o

ifeq ($(with_openssl),yes)
OBJS += be-secure-openssl.o
endif

include $(top_srcdir)/src/adbmgrd/common.mk
