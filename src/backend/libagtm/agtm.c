#include "postgres.h"

#include "access/htup_details.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_utils.h"
#include "agtm/agtm_transaction.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "catalog/indexing.h"
#include "funcapi.h"
#include "nodes/parsenodes.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"

typedef struct AGTM_Conn
{
	struct pg_conn 		*pg_Conn;
	struct pg_result	*pg_res;
} AGTM_Conn;

/* Configuration variables */
extern char				*AGtmHost;
extern int 				AGtmPort;

static char				*save_AGtmHost = NULL;
static int				save_AGtmPort = 0;

static PGresult* agtmcoord_get_result(AGTM_MessageType msg_type, const char* dbname);

static void agtmcoord_send_message(AGTM_MessageType msg, const char* dbname, const char *fmt, ...)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
static StringInfo agtm_use_result_data(const struct pg_result *res, StringInfo buf);
static StringInfo agtm_use_result_type(const struct pg_result *res, StringInfo buf, AGTM_ResultType type);
static void agtm_check_result(StringInfo buf, AGTM_ResultType type);
static void agtm_use_result_end(struct pg_result *res, StringInfo buf);
static AGTM_Conn	*conn_gtmcoord = NULL;

/* GUC check hook for agtm_host */
bool
check_agtm_host(char **newval, void **extra, GucSource source)
{
	if (!IsCnMaster())
		return true;

	if (save_AGtmHost == NULL)
		return true;

	if (!newval || *newval == NULL || (*newval)[0] == '\0')
		return false;

	if (pg_strcasecmp(save_AGtmHost, *newval) == 0)
		return true;

	return true;
}

/* GUC check hook for agtm_port */
bool
check_agtm_port(int *newval, void **extra, GucSource source)
{
	if (!IsCnMaster())
		return true;

	if (save_AGtmPort == 0)
		return true;

	if (!newval)
		return false;

	if (*newval == save_AGtmPort)
		return true;

	return true;
}



static StringInfo agtm_use_result_data(const PGresult *res, StringInfo buf)
{
	AssertArg(res && buf);

	if(PQftype(res, 0) != BYTEAOID
		|| (buf->data = PQgetvalue(res, 0, 0)) == NULL)
	{
		ereport(ERROR, (errmsg("Invalid AGTM message")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	buf->cursor = 0;
	buf->len = PQgetlength(res, 0, 0);
	buf->maxlen = buf->len;
	return buf;
}

static StringInfo agtm_use_result_type(const PGresult *res, StringInfo buf, AGTM_ResultType type)
{
	PG_TRY();
	{
		agtm_use_result_data(res, buf);
		agtm_check_result(buf, type);
	} PG_CATCH();
	{
		PQclear((PGresult *) res);
		PG_RE_THROW();
	} PG_END_TRY();

	return buf;
}

static void agtm_check_result(StringInfo buf, AGTM_ResultType type)
{
	int res = pq_getmsgint(buf, 4);
	if(res != type)
	{
		ereport(ERROR, (errmsg("need AGTM message %s, but result %s"
			, gtm_util_result_name(type), gtm_util_result_name((AGTM_ResultType)res))));
	}
}

static void agtm_use_result_end(PGresult *res, StringInfo buf)
{
	if (buf->cursor != buf->len)
	{
		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format from AGTM")));
	}
	PQclear(res);
}

static void agtmcoord_close(void)
{
	if (conn_gtmcoord)
	{
		if(conn_gtmcoord->pg_res)
		{
			PQclear(conn_gtmcoord->pg_res);
			conn_gtmcoord->pg_res = NULL;
		}

		if (conn_gtmcoord->pg_Conn)
		{
			PQfinish(conn_gtmcoord->pg_Conn);
			conn_gtmcoord->pg_Conn = NULL;
		}

		pfree(conn_gtmcoord);
	}
	conn_gtmcoord = NULL;
}

static void
connect_gtmcoord_rel(const char* host, const int32 port, const char* user, const char* dbname)
{
	char			port_buf[10];
	PGconn volatile	*pg_conn;

	/* libpq connection keywords */
	const char *keywords[] = {
								"host", "port", "user",
								"dbname", "client_encoding",
								NULL							/* must be last */
							 };

	const char *values[]   = {
								host, port_buf, user,
								dbname, GetDatabaseEncodingName(),
								NULL							/* must be last */
							 };

	snprintf(port_buf, sizeof(port_buf), "%d", port);
	agtmcoord_close();

	pg_conn = PQconnectdbParams(keywords, values, true);
	if(pg_conn == NULL)
		ereport(ERROR,
			(errmsg("Fail to connect to AGTM(return NULL pointer)."),
			 errhint("AGTM info(host=%s port=%d dbname=%s user=%s)",
		 		host, port, dbname, user)));

	PG_TRY();
	{
		if (PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to AGTM %s",
					PQerrorMessage((PGconn*)pg_conn)),
				 errhint("AGTM info(host=%s port=%d dbname=%s user=%s)",
					host, port, dbname, user)));
		}

		/*
		 * Make sure agtm_conn is null pointer.
		 */
		Assert(conn_gtmcoord == NULL);

		if(conn_gtmcoord == NULL)
		{
			MemoryContext oldctx = NULL;

			oldctx = MemoryContextSwitchTo(TopMemoryContext);
			conn_gtmcoord = (AGTM_Conn *)palloc0(sizeof(AGTM_Conn));
			conn_gtmcoord->pg_Conn = (PGconn*)pg_conn;
			(void)MemoryContextSwitchTo(oldctx);
		}
	}PG_CATCH();
	{
		PQfinish((PGconn*)pg_conn);
		conn_gtmcoord = NULL;
		PG_RE_THROW();
	}PG_END_TRY();

	ereport(LOG,
		(errmsg("Connect to AGTM(host=%s port=%d dbname=%s user=%s) successfully.",
		host, port, dbname, user)));
}

static void get_gtmcorrdmaster_rel(const char* dbname)
{
	char* 			user_name;
	user_name = GetUserNameFromId(GetUserId(), false);
	Assert(user_name);

	connect_gtmcoord_rel(AGtmHost, AGtmPort, user_name, dbname);
	pfree(user_name);
}

static PGconn* get_gtmcoord_connect(const char* dbname)
{
	ConnStatusType status;
	if (conn_gtmcoord == NULL)
	{
		get_gtmcorrdmaster_rel(dbname);

		return conn_gtmcoord ? conn_gtmcoord->pg_Conn : NULL;
	}

	status = PQstatus(conn_gtmcoord->pg_Conn);
	if (status == CONNECTION_OK)
		return conn_gtmcoord->pg_Conn;

	
	agtmcoord_close();
	get_gtmcorrdmaster_rel(dbname);

	return conn_gtmcoord->pg_Conn;
}

AGTM_Sequence
get_seqnextval_from_gtmcorrd(const char *seqname, const char * database,	const char * schema,
				   int64 min, int64 max, int64 cache, int64 inc, bool cycle, int64 *cached)
{
	PGresult		*res;
	AGTM_Sequence	result;
	StringInfoData	buf;
	int				seqNameSize;
	int 			databaseSize;
	int				schemaSize;

	Assert(seqname != NULL && database != NULL && schema != NULL);
	if(seqname[0] == '\0' || database[0] == '\0' || schema[0] == '\0')
		ereport(ERROR,
				(errmsg("get_seqnextval_from_gtmcorrd parameter seqname is null")));
	if(!IsUnderAGTM())
		ereport(ERROR,
				(errmsg("get_seqnextval_from_gtmcorrd function must under AGTM")));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);
	agtmcoord_send_message(AGTM_MSG_SEQUENCE_GET_NEXT, database, 
					  "%d%d %p%d %d%d %p%d %d%d %p%d" "%p%d %p%d %p%d %p%d %c",
					  seqNameSize, 4,
					  seqname, seqNameSize,
					  databaseSize, 4,
					  database, databaseSize,
					  schemaSize, 4,
					  schema, schemaSize,
					  &min, (int)sizeof(min),
					  &max, (int)sizeof(max),
					  &cache, (int)sizeof(cache),
					  &inc, (int)sizeof(inc),
					  cycle);

	res = agtmcoord_get_result(AGTM_MSG_SEQUENCE_GET_NEXT, database);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SEQUENCE_GET_NEXT_RESULT);
	
	pq_copymsgbytes(&buf, (char*)&result, sizeof(result));
	pq_copymsgbytes(&buf, (char*)cached, sizeof(*cached));

	agtm_use_result_end(res, &buf);

	return result;
}

AGTM_Sequence
set_seqnextval_from_gtmcorrd(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextval, bool iscalled)
{
	PGresult		*res;
	StringInfoData	buf;
	AGTM_Sequence	seq;

	int				seqNameSize;
	int 			databaseSize;
	int				schemaSize;

	Assert(seqname != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("set_seqnextval_from_gtmcorrd function must under AGTM")));

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = %s, parameter seqname is null",
			"AGTM_MSG_SEQUENCE_SET_VAL")));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtmcoord_send_message(AGTM_MSG_SEQUENCE_SET_VAL, database, 
					"%d%d %p%d %d%d %p%d %d%d %p%d" INT64_FORMAT "%c",
					seqNameSize, 4,
					seqname, seqNameSize,
					databaseSize, 4,
					database, databaseSize,
					schemaSize, 4,
					schema, schemaSize,
					nextval, iscalled);

	res = agtmcoord_get_result(AGTM_MSG_SEQUENCE_SET_VAL, database);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SEQUENCE_SET_VAL_RESULT);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(res, &buf);

	return seq;
}

void disconnect_gtmcoord(int code, Datum arg)
{
	agtmcoord_close();
}

static void agtmcoord_send_message(AGTM_MessageType msg, const char* dbname, const char *fmt, ...)
{
	va_list args;
	PGconn *conn;
	void *p;
	int len;
	char c;
	AssertArg(fmt);

	conn = get_gtmcoord_connect(dbname);

	/* start message */
	if(PQsendQueryStart(conn) == false
		|| pqPutMsgStart('A', true, conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR, (errmsg("Start message for agtm failed:%s", PQerrorMessage(conn))));
	}

	va_start(args, fmt);
	/* put AGTM message type */
	if(pqPutInt(msg, 4, conn) < 0)
		goto put_error_;

	while(*fmt)
	{
		if(isspace(fmt[0]))
		{
			/* skip space */
			++fmt;
			continue;
		}else if(fmt[0] != '%')
		{
			goto format_error_;
		}
		++fmt;

		c = *fmt;
		++fmt;

		if(c == 's')
		{
			/* %s for string */
			p = va_arg(args, char *);
			len = strlen(p);
			++len; /* include '\0' */
			if(pqPutnchar(p, len, conn) < 0)
				goto put_error_;
		}else if(c == 'c')
		{
			/* %c for char */
			c = (char)va_arg(args, int);
			if(pqPutc(c, conn) < 0)
				goto put_error_;
		}else if(c == 'p')
		{
			/* %p for binary */
			p = va_arg(args, void *);
			/* and need other "%d" for value binary length */
			if(fmt[0] != '%' || fmt[1] != 'd')
				goto format_error_;
			fmt += 2;
			len = va_arg(args, int);
			if(pqPutnchar(p, len, conn) < 0)
				goto put_error_;
		}else if(c == 'd')
		{
			/* %d for int */
			int val = va_arg(args, int);
			/* and need other "%d" for value binary length */
			if(fmt[0] != '%' || fmt[1] != 'd')
				goto format_error_;
			fmt += 2;
			len = va_arg(args, int);
			if(pqPutInt(val, len, conn) < 0)
				goto put_error_;
		}else if(c == 'l')
		{
			if(fmt[0] == 'd')
			{
				long val = va_arg(args, long);
				fmt += 1;
				if(pqPutnchar((char*)&val, sizeof(val), conn) < 0)
					goto put_error_;
			}
			else if(fmt[0] == 'l' && fmt[1] == 'd')
			{
				long long val = va_arg(args, long long);
				fmt += 2;
				if(pqPutnchar((char*)&val, sizeof(val), conn) < 0)
					goto put_error_;
			}
			else
			{
				goto put_error_;
			}
		}
		else
		{
			goto format_error_;
		}
	}
	va_end(args);

	if(pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR, (errmsg("End message for agtm failed:%s", PQerrorMessage(conn))));
	}

	conn->asyncStatus = PGASYNC_BUSY;
	return;

format_error_:
	va_end(args);
	pqHandleSendFailure(conn);
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
		, errmsg("format message error for agtmcoord")));
	return;

put_error_:
	va_end(args);
	pqHandleSendFailure(conn);
	ereport(ERROR, (errmsg("put message to AGTM error:%s", PQerrorMessage(conn))));
	return;
}

static void
agtmcoord_PrepareResult(PGconn *conn)
{
	PGresult *result = NULL;

	if (!conn)
		return ;

	result  = PQmakeEmptyPGresult(conn, PGRES_TUPLES_OK);
	result->numAttributes = 1;
	result->attDescs = (PGresAttDesc *) PQresultAlloc(result, 1 * sizeof(PGresAttDesc));
	if (!result->attDescs)
	{
		PQclear(result);
		ereport(ERROR,
				(errmsg("Fail to prepare agtm result: out of memory")));
	}
	MemSet(result->attDescs, 0, 1 * sizeof(PGresAttDesc));
	result->binary = 1;
	result->attDescs[0].name = pqResultStrdup(result, "result");
	result->attDescs[0].tableid = 0;
	result->attDescs[0].columnid = 0;
	result->attDescs[0].format = 1;
	result->attDescs[0].typid = BYTEAOID;
	result->attDescs[0].typlen = -1;
	result->attDescs[0].atttypmod = -1;

	conn->result = result;
}

/*
 * call pqFlush, pqWait, pqReadData and return agtm_GetResult
 */
static PGresult* agtmcoord_get_result(AGTM_MessageType msg_type, const char* dbname)
{
	PGconn *conn;
	PGresult *result;
	ExecStatusType state;
	int res;

	conn = get_gtmcoord_connect(dbname);
	while((res=pqFlush(conn)) > 0)
		; /* nothing todo */
	if(res < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("flush message to AGTM error:%s, message type:%s",
			PQerrorMessage(conn), gtm_util_message_name(msg_type))));
	}

	agtmcoord_PrepareResult(conn);

	result = NULL;
	if(pqWait(true, false, conn) != 0
		|| pqReadData(conn) < 0
		|| (result = PQexecFinish(conn)) == NULL)
	{
		PQclear(result);
		ereport(ERROR,
			(errmsg("read message from AGTM error:%s, message type:%s",
			PQerrorMessage(conn), gtm_util_message_name(msg_type))));
	}

	state = PQresultStatus(result);
	if(state == PGRES_FATAL_ERROR)
	{
		PQclear(result);
		ereport(ERROR,
				(errmsg("got error message from AGTM %s", PQresultErrorMessage(result))));
	}else if(state != PGRES_TUPLES_OK && state != PGRES_COMMAND_OK)
	{
		PQclear(result);
		ereport(ERROR,
				(errmsg("AGTM result a \"%s\" message", PQresStatus(state))));
	}

	return result;
}
