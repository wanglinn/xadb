#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for optimizer/plan
#
# IDENTIFICATION
#    src/backend/optimizer/plan/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/optimizer/plan
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = analyzejoins.o createplan.o initsplan.o planagg.o planmain.o planner.o \
	setrefs.o subselect.o

ifeq ($(enable_cluste), yes)
  OBJS += pgxcplan.o
endif

include $(top_srcdir)/src/backend/common.mk
