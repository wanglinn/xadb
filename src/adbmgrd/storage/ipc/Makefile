#
# Makefile for storage/ipc
#
# src/adbmgrd/storage/ipc/Makefile
#

subdir = src/adbmgrd/storage/ipc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = barrier.o dsm_impl.o dsm.o ipc.o ipci.o latch.o pmsignal.o procarray.o \
	procsignal.o shmem.o shmqueue.o shm_mq.o shm_toc.o signalfuncs.o \
	sinval.o sinvaladt.o standby.o

include $(top_srcdir)/src/adbmgrd/common.mk
