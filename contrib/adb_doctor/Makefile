# contrib/adb_doctor/Makefile

MODULE_big = adb_doctor
OBJS = adb_doctor_conf.o adb_doctor_utils.o adb_doctor_data.o adb_doctor.o adb_doctor_launcher.o adb_doctor_log.o adb_doctor_node_monitor.o adb_doctor_host_monitor.o adb_doctor_switcher.o adb_doctor_repairer.o $(WIN32RES)
PGFILEDESC = "adb_doctor - cure the adb cluster automatically"

EXTENSION = adb_doctor
DATA = adb_doctor--1.0.sql

REGRESS = adb_doctor

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/adb_doctor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
CFLAGS := $(patsubst -DADB,-DADBMGRD, $(CFLAGS))
override with_llvm = no
include $(top_srcdir)/contrib/contrib-global.mk
endif
