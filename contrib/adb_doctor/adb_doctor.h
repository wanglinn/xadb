/*--------------------------------------------------------------------------
 *
 * adb_doctor.h
 *		Definitions for adb doctor
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * IDENTIFICATION
 *		contrib/adb_doctor/adb_doctor.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_H
#define ADB_DOCTOR_H

#include "postgres.h"
#include "pgstat.h"

#include "storage/procarray.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "adb_doctor_conf.h"
#include "adb_doctor_data.h"

#define ADB_DOCTOR_LAUNCHER_MAGIC 0x79fb2448

#define ADB_DOCTOR_BGW_TYPE_WORKER "antdb doctor worker"
#define ADB_DOCTOR_BGW_TYPE_LAUNCHER "antdb doctor launcher"
#define ADB_DOCTOR_BGW_LIBRARY_NAME "adb_doctor"
#define ADB_DOCTOR_FUNCTION_NAME_LAUNCHER "adbDoctorLauncherMain"
#define ADB_DOCTOR_FUNCTION_NAME_NODE_MONITOR "adbDoctorNodeMonitorMain"
#define ADB_DOCTOR_FUNCTION_NAME_HOST_MONITOR "adbDoctorHostMonitorMain"
#define ADB_DOCTOR_FUNCTION_NAME_SWITCHER "adbDoctorSwitcherMain"
#define ADB_DOCTOR_FUNCTION_NAME_REPAIRER "adbDoctorRepairerMain"

#define ADB_DOCTORS_LAUNCH_OK "OK"
#define ADB_DOCTORS_LAUNCH_FAILURE "FAILURE"

extern void adbDoctorLauncherMain(Datum main_arg) pg_attribute_noreturn();
extern void adbDoctorStopLauncher(bool waitForStopped);
extern void adbDoctorStopBgworkers(bool waitForStopped);
extern void adbDoctorSignalLauncher(void);

extern void adbDoctorNodeMonitorMain(Datum main_arg) pg_attribute_noreturn();
extern void adbDoctorHostMonitorMain(Datum main_arg) pg_attribute_noreturn();
extern void adbDoctorSwitcherMain(Datum main_arg) pg_attribute_noreturn();
extern void adbDoctorRepairerMain(Datum main_arg) pg_attribute_noreturn();

extern void cleanupAdbDoctorBgworker(dsm_segment *seg, Datum arg);
extern void notifyAdbDoctorRegistrant(void);

#endif