#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/dynamicreduce
#
# IDENTIFICATION
#    src/backend/utils/dynamicreduce/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/dynamicreduce
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = dynamicreduce.o dr_node.o dr_connect.o dr_utils.o dr_shm.o dr_fetch.o \
	   plan_public.o plan_normal.o plan_merge.o plan_sfs.o plan_parallel.o \
	   plan_sts.o

include $(top_srcdir)/src/backend/common.mk
