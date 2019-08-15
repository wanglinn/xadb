/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_SQL_H
#define ADB_DOCTOR_SQL_H

#include "utils/memutils.h"
#include "adb_doctor_conf.h"
#include "adb_doctor_data.h"

#ifndef ADBMGR_DBNAME
#define ADBMGR_DBNAME "postgres"
#endif

/* SPI functions */
extern int SPI_selectAdbDoctorConfInt(char *key);
extern char *SPI_selectAdbDoctConfByKey(char *key);
extern AdbDoctorConf *SPI_selectAllAdbDoctorConf(MemoryContext ctx);
extern int SPI_selectEditableAdbDoctorConf(MemoryContext ctx, AdbDoctorConfRow **rowDataP);
extern void SPI_updateAdbDoctorConf(char *key, char *value);
extern AdbDoctorList *SPI_selectMgrNodeForMonitor(MemoryContext ctx);
extern AdbDoctorSwitcherData *SPI_selectMgrNodeForSwitcher(MemoryContext ctx);
extern AdbMgrNodeWrapper *SPI_selectMgrNodeByOid(Oid oid, MemoryContext spiContext);
extern int SPI_updateMgrNodeCureStatus(Oid oid, char *oldValue, char *newValue);
extern AdbDoctorHostData *SPI_selectMgrHostForMonitor(MemoryContext ctx);
extern AdbMgrHostWrapper *SPI_selectMgrHostByOid(Oid oid, MemoryContext spiContext);
extern AdbDoctorList *SPI_selectMgrNode(MemoryContext ctx, char *sql);
extern AdbDoctorList *SPI_selectMgrHost(MemoryContext ctx, char *sql);
extern int SPI_countSlaveMgrNode(Oid masterOid, char nodetype);

#endif