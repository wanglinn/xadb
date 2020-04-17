/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "catalog/indexing.h"
#include "utils/resowner.h"
#include "adb_doctor_log.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "adb_doctor_utils.h"
#include "utils/memutils.h"

char *ereport_message = NULL;
emit_log_hook_type pre_emit_log_hook = NULL;

void copy_err_msg(ErrorData *edata)
{
	int len;
	if (edata->elevel < ERROR ||
		edata->message == NULL)
		return;
	if (ereport_message)
	{
		pfree(ereport_message);
		ereport_message = NULL;
	}
	len = strlen(edata->message) + 1;
	ereport_message = MemoryContextAllocExtended(TopMemoryContext, len, MCXT_ALLOC_NO_OOM);
	if (ereport_message)
		memmove(ereport_message, edata->message, len);
}

extern AdbDoctorLogRow *beginAdbDoctorLog(char *nodename, char *strategy)
{
	AdbDoctorLogRow *logRow;

	logRow = palloc0(sizeof(AdbDoctorLogRow));
	logRow->begintime = GetCurrentTimestamp();
	logRow->faultnode = nodename;
	logRow->strategy = strategy;
	logRow->status = ADBDOCTORLOG_STATUS_PROCESSING;
	insertAdbDoctorLogReturningInNewTransaction(logRow);
	return logRow;
}

extern void endAdbDoctorLog(AdbDoctorLogRow *logRow, bool success)
{
	logRow->endtime = GetCurrentTimestamp();
	if (success)
		logRow->status = ADBDOCTORLOG_STATUS_SUCCESS;
	else
		logRow->status = ADBDOCTORLOG_STATUS_FAILURE;
	updateAdbDoctorLogByKeyInNewTransaction(logRow);
	pfree(logRow);
}

void insertAdbDoctorLogReturningInNewTransaction(AdbDoctorLogRow *logRow)
{
	int ret;
	SPI_CONNECT_TRANSACTIONAL_START(ret, false);
	if (ret == SPI_OK_CONNECT)
	{
		insertAdbDoctorLogReturning(logRow);
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
}

void insertAdbDoctorLogReturning(AdbDoctorLogRow *logRow)
{
	StringInfoData sql;
	StringInfoData columnNames;
	StringInfoData columnValues;
	char *idStr = NULL;
	int ret;
	uint64 rows;
	HeapTuple tuple;
	TupleDesc tupdesc;
	SPITupleTable *tupTable;

	initStringInfo(&columnNames);
	initStringInfo(&columnValues);
	if (logRow->begintime)
	{
		appendStringInfo(&columnNames, "begintime,");
		appendStringInfo(&columnValues, "'%s',", DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(logRow->begintime))));
	}
	if (logRow->endtime)
	{
		appendStringInfo(&columnNames, "endtime,");
		appendStringInfo(&columnValues, "'%s',", DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(logRow->endtime))));
	}
	if (logRow->faultnode)
	{
		appendStringInfo(&columnNames, "faultnode,");
		appendStringInfo(&columnValues, "'%s',", logRow->faultnode);
	}
	if (logRow->assistnode)
	{
		appendStringInfo(&columnNames, "assistnode,");
		appendStringInfo(&columnValues, "'%s',", logRow->assistnode);
	}
	if (logRow->strategy)
	{
		appendStringInfo(&columnNames, "strategy,");
		appendStringInfo(&columnValues, "'%s',", logRow->strategy);
	}
	if (logRow->status)
	{
		appendStringInfo(&columnNames, "status,");
		appendStringInfo(&columnValues, "'%s',", logRow->status);
	}
	if (logRow->errormsg)
	{
		appendStringInfo(&columnNames, "errormsg,");
		appendStringInfo(&columnValues, "'%s',", logRow->errormsg);
	}
	// delete last comma
	if (columnNames.data[columnNames.len - 1] == ',')
	{
		columnNames.data[columnNames.len - 1] = '\0';
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("You must specify at least one column")));
	}
	if (columnValues.data[columnValues.len - 1] == ',')
	{
		columnValues.data[columnValues.len - 1] = '\0';
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("You must specify at least one column")));
	}
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "insert into adb_doctor.adb_doctor_log( %s ) "
					 "VALUES ( %s ) RETURNING id; ",
					 columnNames.data,
					 columnValues.data);

	ret = SPI_execute(sql.data, false, 0);

	pfree(columnNames.data);
	pfree(columnValues.data);
	pfree(sql.data);

	if (ret != SPI_OK_INSERT_RETURNING)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d",
						ret)));

	rows = SPI_processed;
	tupTable = SPI_tuptable;
	tupdesc = tupTable->tupdesc;
	if (rows == 1 && tupTable != NULL)
	{
		tuple = tupTable->vals[0];
		idStr = SPI_getvalue(tuple, tupdesc, 1);
		if (idStr)
		{
			logRow->id = atoll(idStr);
			return;
		}
	}
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("SPI_execute failed: invalid rows: " INT64_FORMAT " or returnning id: %s",
					rows, idStr)));
}

void updateAdbDoctorLogByKeyInNewTransaction(AdbDoctorLogRow *logRow)
{
	int ret;
	SPI_CONNECT_TRANSACTIONAL_START(ret, false);
	if (ret == SPI_OK_CONNECT)
	{
		updateAdbDoctorLogByKey(logRow);
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
}

void updateAdbDoctorLogByKey(AdbDoctorLogRow *logRow)
{
	StringInfoData sql;
	StringInfoData columns;
	int ret;
	uint64 rows;

	initStringInfo(&columns);
	if (logRow->endtime)
	{
		appendStringInfo(&columns, "endtime = '%s',", DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(logRow->endtime))));
	}
	if (logRow->assistnode)
	{
		appendStringInfo(&columns, "assistnode = '%s',", logRow->assistnode);
	}
	if (logRow->strategy)
	{
		appendStringInfo(&columns, "strategy = '%s',", logRow->strategy);
	}
	if (logRow->status)
	{
		appendStringInfo(&columns, "status = '%s',", logRow->status);
	}
	if (logRow->errormsg)
	{
		appendStringInfo(&columns, "errormsg = '%s',", logRow->errormsg);
	}
	// delete last comma
	if (columns.data[columns.len - 1] == ',')
	{
		columns.data[columns.len - 1] = '\0';
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("You must specify at least one column")));
	}
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "update adb_doctor.adb_doctor_log set %s "
					 "where id = " INT64_FORMAT,
					 columns.data,
					 logRow->id);

	ret = SPI_execute(sql.data, false, 0);

	pfree(columns.data);
	pfree(sql.data);

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d",
						ret)));

	rows = SPI_processed;
	if (rows != 1)
	{
		appendStringInfo(&sql,
						 "updated rows is not 1, row data: %s",
						 toStringAdbDoctorLogRow(logRow));
	}
}
