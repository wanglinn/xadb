#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/commands
#
# IDENTIFICATION
#    src/backend/commands/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/commands
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	aggregatecmds.o \
	alter.o \
	amcmds.o \
	analyze.o \
	async.o \
	cluster.o \
	collationcmds.o \
	comment.o \
	constraint.o \
	conversioncmds.o \
	copy.o \
	createas.o \
	dbcommands.o \
	define.o \
	discard.o \
	dropcmds.o \
	event_trigger.o \
	explain.o \
	extension.o \
	foreigncmds.o \
	functioncmds.o \
	indexcmds.o \
	lockcmds.o \
	matview.o \
	opclasscmds.o \
	operatorcmds.o \
	policy.o \
	portalcmds.o \
	prepare.o \
	proclang.o \
	publicationcmds.o \
	schemacmds.o \
	seclabel.o \
	sequence.o \
	statscmds.o \
	subscriptioncmds.o \
	tablecmds.o \
	tablespace.o \
	trigger.o \
	tsearchcmds.o \
	typecmds.o \
	user.o \
	vacuum.o \
	variable.o \
	view.o

ifeq ($(enable_cluster),yes)
	OBJS += auxtablecmds.o
	CFLAGS += -I$(top_srcdir)/src/interfaces
	BITCODE_CFLAGS += -I$(top_srcdir)/src/interfaces
endif

ifeq ($(enable_grammar_oracle),yes)
	OBJS += ora_cast.o ora_convert.o
endif

include $(top_srcdir)/src/backend/common.mk
