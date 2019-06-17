#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "pgxc/pgxc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define FAILED 0
#define SUCESS 1
#define COMMAND_SIZE 256
#define BEGIN_COMMAND_SIZE 1024

static StringInfo ErrorBuffer = NULL;

//#define ProcessResult(a) (AssertMacro(0),false)
//#define ResetCancelConn() Assert(0)

static char* agtm_generate_begin_command(void);
static bool  AcceptAgtmResult(const PGresult *result);
static bool  AgtmProcessResult(PGresult **results);
static bool  agtm_execute_query(const char *query);
static bool  CheckAgtmConnection(void);
static bool  ConnectionAgtmUp(void);

static const char *
GetAgtmQueryError(void)
{
	if (ErrorBuffer && ErrorBuffer->len > 0)
		return ErrorBuffer->data;

	return "missing error message.";
}

/* ConnectionAgtmUp
 *
 * Returns whether our backend connection is still there.
 */
static bool
ConnectionAgtmUp(void)
{
	return PQstatus(getAgtmConnection()) != CONNECTION_BAD;
}

static bool
CheckAgtmConnection(void)
{
	bool		OK;

	OK = ConnectionAgtmUp();
	if (!OK)
	{
		ereport(INFO,
			(errmsg("AGTMconn status CONNECTION_BAD, try to reconnect")));

		/* try reconnect to agtm */
		agtm_Reset();
		OK = ConnectionAgtmUp();
		if (!OK)
			ereport(INFO, (errmsg("reconnect agtm error")));
	}
	return OK;
}

static bool
AcceptAgtmResult(const PGresult *result)
{
	bool		OK;

	if (!result)
		OK = false;
	else
		switch (PQresultStatus(result))
		{
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
			case PGRES_EMPTY_QUERY:
			case PGRES_COPY_IN:
			case PGRES_COPY_OUT:
				/* Fine, do nothing */
				OK = true;
				break;

			case PGRES_BAD_RESPONSE:
			case PGRES_NONFATAL_ERROR:
			case PGRES_FATAL_ERROR:
				OK = false;
				break;

			default:
				OK = false;
				ereport(WARNING,
					(errmsg("unexpected PQresultStatus: %d",
						PQresultStatus(result))));
				break;
		}

	if (!OK)
	{
		ereport(WARNING,
			(errmsg("[From AGTM] %s", PQresultErrorMessage(result))));
		CheckAgtmConnection();
	}
	return OK;
}

static bool
AgtmProcessResult(PGresult **results)
{
	bool		success = true;

	for(;;)
	{
		ExecStatusType result_status;
		PGresult   *next_result;

		if (!AcceptAgtmResult(*results))
		{
			/*
			 * Failure at this point is always a server-side failure or a
			 * failure to submit the command string.  Either way, we're
			 * finished with this command string.
			 */
			success = false;
			break;
		}

		result_status = PQresultStatus(*results);
		switch (result_status)
		{
			case PGRES_EMPTY_QUERY:
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
				break;

			case PGRES_COPY_OUT:
			case PGRES_COPY_IN:
				break;
			default:
				ereport(WARNING,(errmsg("unexpected PQresultStatus: %d",
					result_status)));
				break;
		}

		next_result = PQgetResult(getAgtmConnection());
		if (!next_result)
			break;
		PQclear(*results);
		*results = next_result;
	}
	return success;
}

static char*
agtm_generate_begin_command(void)
{
	static char begin_cmd[BEGIN_COMMAND_SIZE];
	const char *read_only;
	const char *isolation_level;
	TransactionId xid;

	/*
	 * First get the READ ONLY status because the next call to GetConfigOption
	 * will overwrite the return buffer
	 */
	if (strcmp(GetConfigOption("transaction_read_only", false, false), "on") == 0)
		read_only = "READ ONLY";
	else
		read_only = "READ WRITE";

	/* Now get the isolation_level for the transaction */
	isolation_level = GetConfigOption("transaction_isolation", false, false);
	if (strcmp(isolation_level, "default") == 0)
		isolation_level = GetConfigOption("default_transaction_isolation", false, false);

	/* Get local new xid, also is minimum xid from AGTM absolutely */
	xid = ReadNewTransactionId();

	/* Finally build a START TRANSACTION command */
	sprintf(begin_cmd,
		"START TRANSACTION ISOLATION LEVEL %s %s LEAST XID IS %u",
		isolation_level, read_only, xid);

	return begin_cmd;
}

static bool
agtm_execute_query(const char *query)
{
	PGresult 	*results = NULL;
	PGconn		*agtm_conn = NULL;
	bool		OK = false;

	if (ErrorBuffer == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		ErrorBuffer = makeStringInfo();
		(void) MemoryContextSwitchTo(old_context);
	} else
	{
		resetStringInfo(ErrorBuffer);
	}

	agtm_conn = getAgtmConnection();
	if (agtm_conn == NULL)
	{
		appendStringInfoString(ErrorBuffer,
			"Fail to get valid agtm connection");
		return false;
	}

	if ((results = PQexec(agtm_conn,query)) == NULL)
	{
		appendStringInfo(ErrorBuffer, "Fail to do query \"%s\": %s",
			query, PQerrorMessage(agtm_conn));
		return false;
	}

	OK = AgtmProcessResult(&results);
	PQclear(results);
	if (!OK)
	{
		appendStringInfo(ErrorBuffer, "Fail to get correct result for query \"%s\"",
			query);
		return false;
	}

	return true;
}

void agtm_BeginTransaction(void)
{
	char *agtm_begin_cmd;

	if (!IsUnderAGTM() ||
		IsGTMNode() ||
		IsConnFromGTM())
		return ;

	if (!GetForceXidFromAGTM() && !IsCnMaster())
		return;

	if (TopXactBeginAGTM())
		return ;

	agtm_begin_cmd = agtm_generate_begin_command();

	if (!agtm_execute_query(agtm_begin_cmd))
		ereport(ERROR,
				(errmsg("%s", GetAgtmQueryError())));

	SetTopXactBeginAGTM(true);
}

void agtm_PrepareTransaction(const char *prepared_gid)
{
	StringInfoData prepare_cmd;

	if (!IsUnderAGTM())
		return ;

	if (prepared_gid == NULL || prepared_gid[0] == 0x00)
		return ;

	if (!IsCnMaster() || IsGTMNode())
		return ;

	if (!TopXactBeginAGTM())
		return ;

	initStringInfo(&prepare_cmd);
	appendStringInfo(&prepare_cmd, "PREPARE TRANSACTION '%s'", prepared_gid);

	if (!agtm_execute_query(prepare_cmd.data))
	{
		pfree(prepare_cmd.data);
		agtm_Close();
		SetTopXactBeginAGTM(false);
		ereport(ERROR,
				(errmsg("%s", GetAgtmQueryError())));
	}

	pfree(prepare_cmd.data);
	SetTopXactBeginAGTM(false);
}

void agtm_CommitTransaction(const char *prepared_gid, bool missing_ok)
{
	StringInfoData commit_cmd;

	if (!IsUnderAGTM() || IsGTMNode())
		return ;

	if (!GetForceXidFromAGTM() && !IsCnMaster())
		return ;

	/*
	 * Return directly if prepared_gid is null and current transaction
	 * does not begin at AGTM.
	 */
	if (!TopXactBeginAGTM() && !prepared_gid)
		return ;

	initStringInfo(&commit_cmd);
	if(prepared_gid != NULL)
	{
		/*
		 * If prepared_gid is not null, assert that top transaction
		 * does not begin at AGTM.
		 */
		Assert(!TopXactBeginAGTM());
		appendStringInfo(&commit_cmd,
						 "COMMIT PREPARED%s '%s'",
						 missing_ok ? " IF EXISTS" : "",
						 prepared_gid);
	} else
	{
		appendStringInfoString(&commit_cmd, "COMMIT TRANSACTION");
	}

	if (!agtm_execute_query(commit_cmd.data))
	{
		pfree(commit_cmd.data);
		if (TopXactBeginAGTM())
		{
			agtm_Close();
			SetTopXactBeginAGTM(false);
		}
		ereport(ERROR,
				(errmsg("%s", GetAgtmQueryError())));
	}
	pfree(commit_cmd.data);
	SetTopXactBeginAGTM(false);
}

void agtm_AbortTransaction(const char *prepared_gid, bool missing_ok, bool no_error)
{
	StringInfoData abort_cmd;

	if (!IsUnderAGTM() || IsGTMNode())
		return;

	if (!GetForceXidFromAGTM() && !IsCnMaster())
		return ;

	/*
	 * return directly if prepared_gid is null and current xact
	 * does not begin at AGTM.
	 */
	if (!TopXactBeginAGTM() && !prepared_gid)
		return ;

	initStringInfo(&abort_cmd);
	if(prepared_gid != NULL)
	{
		/*
		 * If prepared_gid is not null, assert that top transaction
		 * does not begin at AGTM.
		 */
		Assert(!TopXactBeginAGTM());
		appendStringInfo(&abort_cmd,
						 "ROLLBACK PREPARED%s '%s'",
						 missing_ok ? " IF EXISTS" : "",
						 prepared_gid);
	} else
	{
		appendStringInfoString(&abort_cmd, "ROLLBACK TRANSACTION");
	}

	if (!agtm_execute_query(abort_cmd.data))
	{
		pfree(abort_cmd.data);
		if (TopXactBeginAGTM())
		{
			agtm_Close();
			SetTopXactBeginAGTM(false);
		}

		if (no_error)
			return;

		ereport(ERROR,
				(errmsg("%s", GetAgtmQueryError())));
	}

	pfree(abort_cmd.data);
	SetTopXactBeginAGTM(false);
}
