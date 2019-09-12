#include "postgres.h"

#include "access/htup_details.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_utils.h"
#include "agtm/agtm_client.h"
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

static AGTM_Sequence agtm_DealSequence(const char *seqname, const char * database,
								const char * schema, AGTM_MessageType type, AGTM_ResultType rtype);
static PGresult* agtm_get_result(AGTM_MessageType msg_type);
static PGresult* agtmcoord_get_result(AGTM_MessageType msg_type, const char* dbname);
static void agtm_send_message(AGTM_MessageType msg, const char *fmt, ...)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

static void agtmcoord_send_message(AGTM_MessageType msg, const char* dbname, const char *fmt, ...)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

static AGTM_Conn	*conn_gtmcoord = NULL;

TransactionId
agtm_GetGlobalTransactionId(bool isSubXact)
{
	PGresult 		*res;
	StringInfoData	buf;
	GlobalTransactionId gxid;

	if(!IsUnderAGTM())
		return InvalidGlobalTransactionId;

	agtm_send_message(AGTM_MSG_GET_GXID, "%c", isSubXact);
	res = agtm_get_result(AGTM_MSG_GET_GXID);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_GXID_RESULT);
	pq_copymsgbytes(&buf, (char*)&gxid, sizeof(TransactionId));

	ereport(DEBUG1,
		(errmsg("get global xid: %d from agtm", gxid)));

	agtm_use_result_end(res, &buf);

	return gxid;
}

void
agtm_CreateSequence(const char * seqName, const char * database,
						const char * schema , List * seqOptions)
{
	PGresult 		*res;

	int				nameSize;
	int				databaseSize;
	int				schemaSize;
	StringInfoData	buf;
	StringInfoData	strOption;

	Assert(seqName != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		return;

	initStringInfo(&strOption);
	parse_seqOption_to_string(seqOptions, &strOption);

	nameSize = strlen(seqName);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_INIT,
					  "%d%d %p%d %d%d %p%d %d%d %p%d %p%d",
					  nameSize, 4,
					  seqName, nameSize,
					  databaseSize, 4,
					  database, databaseSize,
					  schemaSize, 4,
					  schema, schemaSize,
					  strOption.data, strOption.len);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_INIT);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_INIT_RESULT);

	agtm_use_result_end(res, &buf);
	pfree(strOption.data);

	ereport(DEBUG1,
		(errmsg("create sequence on agtm :%s", seqName)));
}

void
agtm_AlterSequence(const char * seqName, const char * database,
						const char * schema ,  List * seqOptions)
{
	PGresult 		*res;

	int				nameSize;
	int				databaseSize;
	int				schemaSize;
	StringInfoData	buf;
	StringInfoData	strOption;

	Assert(seqName != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		return;

	initStringInfo(&strOption);
	parse_seqOption_to_string(seqOptions, &strOption);

	nameSize = strlen(seqName);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_ALTER,
					  "%d%d %p%d %d%d %p%d %d%d %p%d %p%d",
					  nameSize, 4,
					  seqName, nameSize,
					  databaseSize, 4,
					  database, databaseSize,
					  schemaSize, 4, schema,
					  schemaSize, strOption.data,
					  strOption.len);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_ALTER);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_ALTER_RESULT);

	agtm_use_result_end(res, &buf);
	pfree(strOption.data);

	ereport(DEBUG1,
		(errmsg("alter sequence on agtm :%s", seqName)));
}

void
agtm_DropSequence(const char * seqName, const char * database, const char * schema)
{
	int				seqNameSize;
	int				dbNameSize;
	int				schemaNameSize;
	StringInfoData	buf;

	PGresult 		*res;

	Assert(seqName != NULL && database != NULL && schema != NULL);

	if(!IsUnderAGTM())
		return;

	seqNameSize = strlen(seqName);
	dbNameSize = strlen(database);
	schemaNameSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_DROP,
					 "%d%d %p%d %d%d %p%d %d%d %p%d",
					 seqNameSize, 4,
					 seqName, seqNameSize,
					 dbNameSize, 4,
					 database, dbNameSize,
					 schemaNameSize, 4,
					 schema, schemaNameSize);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_DROP);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_DROP_RESULT);

	agtm_use_result_end(res, &buf);

	ereport(DEBUG1,
		(errmsg("drop sequence on agtm :%s", seqName)));
}

void
agtms_DropSequenceByDataBase(const char * database)
{
	int				dbNameSize;
	StringInfoData	buf;

	PGresult 		*res;

	Assert(database != NULL);

	if(!IsUnderAGTM())
		return;

	dbNameSize = strlen(database);
	agtm_send_message(AGTM_MSG_SEQUENCE_DROP_BYDB, "%d%d %p%d", dbNameSize, 4, database, dbNameSize);
	res = agtm_get_result(AGTM_MSG_SEQUENCE_DROP_BYDB);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_DROP_BYDB_RESULT);

	agtm_use_result_end(res, &buf);

	ereport(DEBUG1,
		(errmsg("drop sequence on agtm by database :%s", database)));
}

void agtm_RenameSequence(const char * seqName, const char * database,
							const char * schema, const char* newName, SequenceRenameType type)
{
	int	seqNameSize;
	int	dbNameSize;
	int	schemaNameSize;
	int	newNameSize;

	PGresult 		*res;
	StringInfoData	buf;
	Assert(seqName != NULL && database != NULL && schema != NULL && newName != NULL);
	if(!IsUnderAGTM())
		return;

	seqNameSize = strlen(seqName);
	dbNameSize = strlen(database);
	schemaNameSize = strlen(schema);
	newNameSize = strlen(newName);

	agtm_send_message(AGTM_MSG_SEQUENCE_RENAME,
					  "%d%d %p%d %d%d %p%d %d%d %p%d %d%d %p%d %d%d",
					  seqNameSize, 4,
					  seqName, seqNameSize,
					  dbNameSize, 4,
					  database, dbNameSize,
					  schemaNameSize, 4,
					  schema, schemaNameSize,
					  newNameSize, 4,
					  newName, newNameSize,
					  type, 4);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_RENAME);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_RENAME_RESULT);

	agtm_use_result_end(res, &buf);

	ereport(DEBUG1,
		(errmsg("rename sequence %s rename to %s", seqName, newName)));
}

extern void
agtm_RenameSeuqneceByDataBase(const char * oldDatabase,
									const char * newDatabase)
{
	int 			oldNameSize;
	int 			newNameSize;
	StringInfoData	buf;

	PGresult		*res;

	Assert(oldDatabase != NULL && newDatabase != NULL);

	if(!IsUnderAGTM())
		return;

	oldNameSize = strlen(oldDatabase);
	newNameSize = strlen(newDatabase);
	agtm_send_message(AGTM_MSG_SEQUENCE_RENAME_BYDB,
					"%d%d %p%d %d%d %p%d",
					oldNameSize, 4,
					oldDatabase, oldNameSize,
					newNameSize, 4,
					newDatabase, newNameSize);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_RENAME_BYDB);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_RENAME_BYDB_RESULT);

	agtm_use_result_end(res, &buf);

	ereport(DEBUG1,
		(errmsg("alter sequence on agtm by database rename old name :%s, new name :%s ",
				oldDatabase , newDatabase)));
}

Timestamp
agtm_GetTimestamptz(void)
{
	PGresult		*res;
	StringInfoData	buf;
	Timestamp		timestamp;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetTimestamptz function must under AGTM")));

	agtm_send_message(AGTM_MSG_GET_TIMESTAMP, " ");
	res = agtm_get_result(AGTM_MSG_GET_TIMESTAMP);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_TIMESTAMP_RESULT);
	pq_copymsgbytes(&buf, (char*)&timestamp, sizeof(timestamp));

	ereport(DEBUG1,
		(errmsg("get timestamp: %ld from agtm", timestamp)));

	agtm_use_result_end(res, &buf);

	return timestamp;
}

Snapshot
agtm_GetGlobalSnapShot(Snapshot snapshot)
{
	PGresult 	*res;
	const char *str;
	StringInfoData	buf;
	uint32 xcnt;
	TimestampTz	globalXactStartTimestamp;

	AssertArg(snapshot && snapshot->xip && snapshot->subxip);

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_GetGlobalSnapShot function must under AGTM")));

	agtm_send_message(AGTM_MSG_SNAPSHOT_GET, " ");
	res = agtm_get_result(AGTM_MSG_SNAPSHOT_GET);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SNAPSHOT_GET_RESULT);

	pq_copymsgbytes(&buf, (char*)&(globalXactStartTimestamp), sizeof(globalXactStartTimestamp));
	SetCurrentTransactionStartTimestamp(globalXactStartTimestamp);
	pq_copymsgbytes(&buf, (char*)&(RecentGlobalXmin), sizeof(RecentGlobalXmin));
	pq_copymsgbytes(&buf, (char*)&(snapshot->xmin), sizeof(snapshot->xmin));
	pq_copymsgbytes(&buf, (char*)&(snapshot->xmax), sizeof(snapshot->xmax));
	xcnt = pq_getmsgint(&buf, sizeof(snapshot->xcnt));
	EnlargeSnapshotXip(snapshot, xcnt);
	snapshot->xcnt = xcnt;
	pq_copymsgbytes(&buf, (char*)(snapshot->xip)
		, sizeof(snapshot->xip[0]) * (snapshot->xcnt));
	snapshot->subxcnt = pq_getmsgint(&buf, sizeof(snapshot->subxcnt));
	str = pq_getmsgbytes(&buf, snapshot->subxcnt * sizeof(snapshot->subxip[0]));
	snapshot->suboverflowed = pq_getmsgbyte(&buf);
	if(snapshot->subxcnt > GetMaxSnapshotSubxidCount())
	{
		snapshot->subxcnt = GetMaxSnapshotSubxidCount();
		snapshot->suboverflowed = true;
	}
	memcpy(snapshot->subxip, str, sizeof(snapshot->subxip[0]) * snapshot->subxcnt);
	snapshot->takenDuringRecovery = pq_getmsgbyte(&buf);
	pq_copymsgbytes(&buf, (char*)&(snapshot->curcid), sizeof(snapshot->curcid));
	pq_copymsgbytes(&buf, (char*)&(snapshot->active_count), sizeof(snapshot->active_count));
	pq_copymsgbytes(&buf, (char*)&(snapshot->regd_count), sizeof(snapshot->regd_count));

	agtm_use_result_end(res, &buf);

	if (GetCurrentCommandId(false) > snapshot->curcid)
		snapshot->curcid = GetCurrentCommandId(false);
	return snapshot;
}

XidStatus
agtm_TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	PGresult		*res;
	StringInfoData	buf;
	XidStatus		xid_status;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_TransactionIdGetStatus function must under AGTM")));

	agtm_send_message(AGTM_MSG_GET_XACT_STATUS, "%d%d", (int)xid, (int)sizeof(xid));
	res = agtm_get_result(AGTM_MSG_GET_XACT_STATUS);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_GET_XACT_STATUS_RESULT);
	pq_copymsgbytes(&buf, (char*)&xid_status, sizeof(xid_status));
	pq_copymsgbytes(&buf, (char*)lsn, sizeof(XLogRecPtr));

	ereport(DEBUG1,
		(errmsg("get xid %u status %d", xid, xid_status)));

	agtm_use_result_end(res, &buf);

	return xid_status;
}

static void
get_cluster_nextXids(TransactionId **xidarray,	/* output */
					 TransactionId *max_cxid,	/* output */
					 Oid **oidarray,			/* output */
					 Oid *max_node,				/* output */
					 int *arraylen)				/* output */
{
	TransactionId	xid;
	TransactionId	max_xid = FirstNormalTransactionId;
	Datum			value;
	uint32			numcoords = 0;
	uint32			numdnodes = 0;
	uint32			i;
	uint32			num, idx = 0;
	const char	   *query = "select current_xid()";
	Oid			   *oids = NULL;
	Oid				node = InvalidOid;

	/* Only master-coordinator can do this */
	if (!IsCnMaster())
		return ;

	/* Get cluster nodes' oids */
	adb_get_all_node_oid_array(&oids, &numcoords, &numdnodes, false);

	num = numcoords + numdnodes;
	if (arraylen)
		*arraylen = num;
	if (xidarray)
		*xidarray = (TransactionId *) palloc0(num * sizeof(TransactionId));

	/* Get all ndoes' nextXid */
	for (i = 0; i < num; i++)
	{
		value = pgxc_execute_on_nodes(1, &oids[i], query);
		xid = DatumGetTransactionId(value);
		if (TransactionIdFollows(xid, max_xid))
		{
			max_xid = xid;
			node = oids[i];
		}
		if (xidarray)
			(*xidarray)[idx] = xid;
		idx++;
	}

	if (oidarray)
		*oidarray = oids;
	else
		pfree(oids);

	if (max_node)
		*max_node = node;
	if (max_cxid)
		*max_cxid = max_xid;
}

static Oid
agtm_SyncNextXid(TransactionId *src_xid,		/* output */
				 TransactionId *agtm_xid,		/* output */
				 bool src_from_local)			/* input, decide where "*src_xid" is from */
{
	TransactionId	sxid = InvalidTransactionId;
	TransactionId	axid = InvalidTransactionId;
	Oid				node = InvalidOid;
	PGresult	   *volatile res = NULL;
	StringInfoData	buf;

	if (src_from_local)
		sxid = ReadNewTransactionId();
	else
		get_cluster_nextXids(NULL, &sxid, NULL, &node, NULL);
	agtm_send_message(AGTM_MSG_SYNC_XID, "%d%d", (int)sxid, (int)sizeof(sxid));
	res = agtm_get_result(AGTM_MSG_SYNC_XID);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SYNC_XID_RESULT);
	axid = (TransactionId) pq_getmsgint(&buf, 4);

	ereport(DEBUG1,
		(errmsg("Sync source xid %u with AGTM xid %u OK", sxid, axid)));

	agtm_use_result_end(res, &buf);

	if (src_xid)
		*src_xid = sxid;
	if (agtm_xid)
		*agtm_xid = axid;

	return node;
}

Oid
agtm_SyncLocalNextXid(TransactionId *cluster_xid, TransactionId *agtm_xid)
{
	return agtm_SyncNextXid(cluster_xid, agtm_xid, true);
}

Oid
agtm_SyncClusterNextXid(TransactionId *cluster_xid, TransactionId *agtm_xid)
{
	return agtm_SyncNextXid(cluster_xid, agtm_xid, false);
}

Datum
sync_cluster_xid(PG_FUNCTION_ARGS)
{
	TransactionId	cxid,	/* cluster max xid */
					axid;	/* agtm xid */
	TupleDesc		tupdesc;
	Datum			values[3];
	bool			isnull[3];
	NameData		nodename;
	Oid				nodeoid;

	if (!IsCnMaster())
		PG_RETURN_NULL();

	nodeoid = agtm_SyncClusterNextXid(&cxid, &axid);

	tupdesc = CreateTemplateTupleDesc(3, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "node",
					   NAMEOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "local",
					   XIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "agtm",
					   XIDOID, -1, 0);

	BlessTupleDesc(tupdesc);

	memset(isnull, 0, sizeof(isnull));

	namestrcpy(&nodename, get_pgxc_nodename(nodeoid));
	values[0] = NameGetDatum(&nodename);
	values[1] = TransactionIdGetDatum(cxid);
	values[2] = TransactionIdGetDatum(axid);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

Datum
sync_local_xid(PG_FUNCTION_ARGS)
{
	TransactionId	lxid,	/* local nextXid */
					axid;	/* agtm xid */
	TupleDesc		tupdesc;
	Datum			values[2];
	bool			isnull[2];

	(void) agtm_SyncLocalNextXid(&lxid, &axid);

	tupdesc = CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "local",
					   XIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "agtm",
					   XIDOID, -1, 0);

	BlessTupleDesc(tupdesc);

	memset(isnull, 0, sizeof(isnull));

	values[0] = TransactionIdGetDatum(lxid);
	values[1] = TransactionIdGetDatum(axid);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

Datum
show_cluster_xid(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	typedef struct ClusterNextXids
	{
		Oid				*nodes;
		TransactionId	*xids;
		int				 length;
		int				 curidx;
	} ClusterNextXids;
	ClusterNextXids *status = NULL;

	if (!IsCnMaster())
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "node",
						   NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "nextXid",
						   XIDOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		status = (ClusterNextXids *) palloc(sizeof(ClusterNextXids));
		funcctx->user_fctx = (void *) status;

		get_cluster_nextXids(&(status->xids), NULL, &(status->nodes), NULL, &(status->length));
		status->curidx = 0;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status = (ClusterNextXids *) funcctx->user_fctx;

	while (status->nodes != NULL && status->xids != NULL && status->curidx < status->length)
	{
		TransactionId 	xid = status->xids[status->curidx];
		Oid				node = status->nodes[status->curidx];
		NameData 		nodename;
		Datum			values[2];
		bool			nulls[2];
 		HeapTuple		tuple;
		Datum			result;

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		namestrcpy(&nodename, get_pgxc_nodename(node));
		values[0] = NameGetDatum(&nodename);
		values[1] = TransactionIdGetDatum(xid);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		status->curidx++;
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

static AGTM_Sequence
agtm_DealSequence(const char *seqname, const char * database,
								const char * schema, AGTM_MessageType type, AGTM_ResultType rtype)
{
	PGresult		*res;
	StringInfoData	buf;
	int				seqNameSize;
	int 			databaseSize;
	int				schemaSize;
	AGTM_Sequence	seq;

	if(!IsUnderAGTM())
		ereport(ERROR,
			(errmsg("agtm_DealSequence function must under AGTM")));

	if(seqname[0] == '\0' || database[0] == '\0' || schema[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = (%s), parameter seqname is null", gtm_util_message_name(type))));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);
	agtm_send_message(type,
					"%d%d %p%d %d%d %p%d %d%d %p%d",
					seqNameSize, 4,
					seqname, seqNameSize,
					databaseSize, 4,
					database, databaseSize,
					schemaSize, 4,
					schema, schemaSize);

	res = agtm_get_result(type);
	Assert(res);
	agtm_use_result_type(res, &buf, rtype);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(res, &buf);

	return seq;
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
	Relation		nodeRelation;
	ScanKeyData 	skey[1];
	HeapScanDesc 	scan;
	HeapTuple		nodeTuple;
	Form_pgxc_node	node_gtm;
	char* 			user_name;

	user_name = GetUserNameFromId(GetUserId(), false);
	Assert(user_name);

	/* Prepare to scan pg_index for entries having indrelid = this rel. */
	nodeRelation = heap_open(PgxcNodeRelationId, AccessShareLock);
	ScanKeyInit(&skey[0],
				Anum_pgxc_node_nodeis_gtm,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));

	scan = heap_beginscan_catalog(nodeRelation, 1, skey);
	nodeTuple = heap_getnext(scan, ForwardScanDirection);

	Assert(nodeTuple);
	
	node_gtm = (Form_pgxc_node)GETSTRUCT(nodeTuple);
	Assert(node_gtm);
	Assert(node_gtm->nodeis_gtm == true);

	connect_gtmcoord_rel(NameStr(node_gtm->node_host), node_gtm->node_port, user_name, dbname);
	
	heap_endscan(scan);
	heap_close(nodeRelation, AccessShareLock);

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
				(errmsg("agtm_GetSeqNextVal parameter seqname is null")));
	if(!IsUnderAGTM())
		ereport(ERROR,
				(errmsg("agtm_GetSeqNextVal function must under AGTM")));

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
			const char * schema, AGTM_Sequence nextval)
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
			(errmsg("agtm_SetSeqValCalled function must under AGTM")));

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
					nextval, true);

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

AGTM_Sequence
agtm_GetSeqNextVal(const char *seqname, const char * database,	const char * schema,
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
				(errmsg("agtm_GetSeqNextVal parameter seqname is null")));
	if(!IsUnderAGTM())
		ereport(ERROR,
				(errmsg("agtm_GetSeqNextVal function must under AGTM")));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);
	agtm_send_message(AGTM_MSG_SEQUENCE_GET_NEXT,
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

	res = agtm_get_result(AGTM_MSG_SEQUENCE_GET_NEXT);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SEQUENCE_GET_NEXT_RESULT);
	
	pq_copymsgbytes(&buf, (char*)&result, sizeof(result));
	pq_copymsgbytes(&buf, (char*)cached, sizeof(*cached));

	agtm_use_result_end(res, &buf);

	return result;
}

AGTM_Sequence
agtm_GetSeqCurrVal(const char *seqname, const char * database,	const char * schema)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);

	return agtm_DealSequence(seqname, database, schema, AGTM_MSG_SEQUENCE_GET_CUR
			, AGTM_MSG_SEQUENCE_GET_CUR_RESULT);
}

AGTM_Sequence
agtm_GetSeqLastVal(const char *seqname, const char * database,	const char * schema)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);

	return agtm_DealSequence(seqname, database, schema, AGTM_MSG_SEQUENCE_GET_LAST
			, AGTM_SEQUENCE_GET_LAST_RESULT);
}

AGTM_Sequence
agtm_SetSeqVal(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextval)
{
	Assert(seqname != NULL && database != NULL && schema != NULL);

	return (agtm_SetSeqValCalled(seqname, database, schema, nextval, true));
}

AGTM_Sequence
agtm_SetSeqValCalled(const char *seqname, const char * database,
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
			(errmsg("agtm_SetSeqValCalled function must under AGTM")));

	if(seqname == NULL || seqname[0] == '\0')
		ereport(ERROR,
			(errmsg("message type = %s, parameter seqname is null",
			"AGTM_MSG_SEQUENCE_SET_VAL")));

	seqNameSize = strlen(seqname);
	databaseSize = strlen(database);
	schemaSize = strlen(schema);

	agtm_send_message(AGTM_MSG_SEQUENCE_SET_VAL,
					"%d%d %p%d %d%d %p%d %d%d %p%d" INT64_FORMAT "%c",
					seqNameSize, 4,
					seqname, seqNameSize,
					databaseSize, 4,
					database, databaseSize,
					schemaSize, 4,
					schema, schemaSize,
					nextval, iscalled);

	res = agtm_get_result(AGTM_MSG_SEQUENCE_SET_VAL);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_SEQUENCE_SET_VAL_RESULT);
	pq_copymsgbytes(&buf, (char*)&seq, sizeof(seq));

	agtm_use_result_end(res, &buf);

	return seq;
}
/*
extern void
agtm_ResetSequenceCaches(void)
{
	PGresult		*res;
	StringInfoData	buf;

	if(!IsUnderAGTM())
	ereport(ERROR,
		(errmsg("agtm_ResetSequenceCaches function must under AGTM")));

	agtm_send_message(AGTM_MSG_SEQUENCE_RESET_CACHE, "");
	res = agtm_get_result(AGTM_MSG_SEQUENCE_RESET_CACHE);
	Assert(res);
	agtm_use_result_type(res, &buf, AGTM_MSG_SEQUENCE_RESET_CACHE_RESULT);
	agtm_use_result_end(res, &buf);
}*/

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
		, errmsg("format message error for agtm_send_message")));
	return;

put_error_:
	va_end(args);
	pqHandleSendFailure(conn);
	ereport(ERROR, (errmsg("put message to AGTM error:%s", PQerrorMessage(conn))));
	return;
}
/*
 * call pqPutMsgStart ... pqPutMsgEnd
 * only support:
 *   %d%d: first is value, second is length
 *   %p%d: first is binary point, second is binary length
 *   %s: string, include '\0'
 *   %c: one char
 *   space: skip it
 */
static void agtm_send_message(AGTM_MessageType msg, const char *fmt, ...)
{
	va_list args;
	PGconn *conn;
	void *p;
	int len;
	char c;
	AssertArg(fmt);

	/* get connection */
	conn = getAgtmConnection();

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
		, errmsg("format message error for agtm_send_message")));
	return;

put_error_:
	va_end(args);
	pqHandleSendFailure(conn);
	ereport(ERROR, (errmsg("put message to AGTM error:%s", PQerrorMessage(conn))));
	return;
}

static void
agtm_PrepareResult(PGconn *conn)
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

	agtm_PrepareResult(conn);

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

/*
 * call pqFlush, pqWait, pqReadData and return agtm_GetResult
 */
static PGresult* agtm_get_result(AGTM_MessageType msg_type)
{
	PGconn *conn;
	PGresult *result;
	ExecStatusType state;
	int res;

	conn = getAgtmConnection();

	while((res=pqFlush(conn)) > 0)
		; /* nothing todo */
	if(res < 0)
	{
		pqHandleSendFailure(conn);
		ereport(ERROR,
			(errmsg("flush message to AGTM error:%s, message type:%s",
			PQerrorMessage(conn), gtm_util_message_name(msg_type))));
	}

	agtm_PrepareResult(conn);

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

void
parse_seqOption_to_string(List * seqOptions, StringInfo strOption)
{
	ListCell   *option;
	int listSize = list_length(seqOptions);
	int listSizeOffset = strOption->cursor;

	/* must binary, at this function last maybe change it */
	appendBinaryStringInfo(strOption, (const char *) &listSize, sizeof(listSize));

	if(listSize == 0)
		return;

	foreach(option, seqOptions)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "owned_by") == 0)
		{
			/* ignore "OWNED BY" */
			listSize--;
			continue;
		}

		if(defel->defnamespace)
		{
			int defnamespaceSize = strlen(defel->defnamespace);
			appendBinaryStringInfo(strOption, (const char *)&defnamespaceSize, sizeof(defnamespaceSize));
			appendStringInfo(strOption, "%s", defel->defnamespace);
		}
		else
		{
			int defnamespaceSize = 0;
			appendBinaryStringInfo(strOption, (const char *)&defnamespaceSize, sizeof(defnamespaceSize));
		}

		if (defel->defname)
		{
			int defnameSize = strlen(defel->defname);
			appendBinaryStringInfo(strOption, (const char *)&defnameSize, sizeof(defnameSize));
			appendStringInfo(strOption, "%s", defel->defname);
		}
		else
		{
			int defnameSize = 0;
			appendBinaryStringInfo(strOption, (const char *)&defnameSize, sizeof(defnameSize));
		}

		if (defel->arg)
		{
			AgtmNodeTag type = T_AgtmInvalid;

			switch(nodeTag(defel->arg))
			{
				case T_Integer:
				{
					long ival = intVal(defel->arg);
					type = T_AgtmInteger;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char *)&ival, sizeof(ival));
					break;
				}
				case T_Float:
				{
					char *str = strVal(defel->arg);
					int strSize = strlen(str);
					type = T_AgtmFloat;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char *)&strSize, sizeof(strSize));
					appendStringInfo(strOption, "%s", str);
					break;
				}
				case T_String:
				{
					char *str = strVal(defel->arg);
					int strSize = strlen(str);
					type = T_AgtmString;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char *)&strSize, sizeof(strSize));
					appendStringInfo(strOption, "%s", str);
					break;
				}
				case T_BitString:
				{
					ereport(ERROR,
						(errmsg("T_BitString is not support")));
					break;
				}
				case T_Null:
				{
					type = T_AgtmNull;
					appendBinaryStringInfo(strOption, (const char *)&type, sizeof(type));
					break;
				}
				case T_TypeName:
				{
					char *strNode = nodeToString(defel->arg);
					int len = strlen(strNode);
					type = T_AgtmStringNode;
					appendBinaryStringInfo(strOption, (char*)&type, sizeof(type));
					appendBinaryStringInfo(strOption, (const char*)&len, sizeof(len));
					appendStringInfoString(strOption, strNode);
					break;
				}
				default:
				{
					ereport(ERROR,
						(errmsg("sequence DefElem type error : %d", nodeTag(defel->arg))));
					break;
				}
			}
		}
		else
		{
			int argSize = 0;
			appendBinaryStringInfo(strOption, (const char *)&argSize, sizeof(argSize));
		}

		appendBinaryStringInfo(strOption, (const char *)&defel->defaction, sizeof(defel->defaction));
	}

	if (listSize != list_length(seqOptions))
	{
		/* has ignored option(s) */
		memcpy(strOption->data + listSizeOffset,
			   &listSize,
			   sizeof(listSize));
	}
}
