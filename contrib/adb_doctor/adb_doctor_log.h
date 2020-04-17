/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_LOG_H
#define ADB_DOCTOR_LOG_H

#include "storage/lwlock.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "adb_doctor_utils.h"
#include "datatype/timestamp.h"

typedef struct AdbDoctorLogRow
{
	int64 id;
	TimestampTz begintime;
	TimestampTz endtime;
	char *faultnode;
	char *assistnode;
	char *strategy;
	char *status;
	char *errormsg;
} AdbDoctorLogRow;

#define ADBDOCTORLOG_STRATEGY_SWITCH "switch"
#define ADBDOCTORLOG_STRATEGY_ABORT_SWITCH "abort_switch"
#define ADBDOCTORLOG_STRATEGY_STARTUP "startup"
#define ADBDOCTORLOG_STRATEGY_RESTART "restart"
#define ADBDOCTORLOG_STRATEGY_CLEAN_REBUILD "clean_rebuild"
#define ADBDOCTORLOG_STRATEGY_ISOLATE_REPAIR "isolate_repair"
#define ADBDOCTORLOG_STRATEGY_REWIND "rewind"
#define ADBDOCTORLOG_STRATEGY_FOLLOW "follow"

#define ADBDOCTORLOG_STATUS_PROCESSING "processing"
#define ADBDOCTORLOG_STATUS_SUCCESS "success"
#define ADBDOCTORLOG_STATUS_FAILURE "failure"

extern char *ereport_message;
extern emit_log_hook_type pre_emit_log_hook;
#define BEGIN_CATCH_ERR_MSG()              \
	do                                     \
	{                                      \
		if (ereport_message)               \
		{                                  \
			pfree(ereport_message);        \
			ereport_message = NULL;        \
		}                                  \
		pre_emit_log_hook = emit_log_hook; \
		emit_log_hook = copy_err_msg;      \
	} while (0)

#define END_CATCH_ERR_MSG()                \
	do                                     \
	{                                      \
		emit_log_hook = pre_emit_log_hook; \
	} while (0)

static inline char *toStringAdbDoctorLogRow(AdbDoctorLogRow *logRow)
{
	StringInfoData s;
	initStringInfo(&s);
	appendStringInfo(&s,
					 INT64_FORMAT "," INT64_FORMAT "," INT64_FORMAT ",%s,%s,%s,%s,%s",
					 logRow->id,
					 logRow->begintime,
					 logRow->endtime,
					 logRow->faultnode,
					 logRow->assistnode,
					 logRow->strategy,
					 logRow->status,
					 logRow->errormsg);
	return s.data;
}

extern void copy_err_msg(ErrorData *edata);
extern AdbDoctorLogRow *beginAdbDoctorLog(char *nodename, char *strategy);
extern void endAdbDoctorLog(AdbDoctorLogRow *logRow, bool success);
extern void insertAdbDoctorLogReturningInNewTransaction(AdbDoctorLogRow *logRow);
extern void insertAdbDoctorLogReturning(AdbDoctorLogRow *logRow);
extern void updateAdbDoctorLogByKeyInNewTransaction(AdbDoctorLogRow *logRow);
extern void updateAdbDoctorLogByKey(AdbDoctorLogRow *logRow);

#endif