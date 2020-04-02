
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_helper.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "mgr/mgr_msg_type.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_node.h"
#include "utils/rel.h"
#include "utils/array.h"
#include "utils/tqual.h"
#include "executor/spi.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "../../interfaces/libpq/libpq-int.h"
#include "utils/fmgroids.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"

#define MAXLINE (8192-1)
#define MAXPATH (512-1)
#define RETRY 3
#define SLEEP_MICRO 100*1000     /* 100 millisec */
#define SQL_PG_IS_IN_RECOVERY "select pg_is_in_recovery()"
#define SQL_PG_QUERY_STARTTIME "select pg_postmaster_start_time()"

char *mgr_zone;

static struct enum_sync_state sync_state_tab[] =
{
	{SYNC_STATE_SYNC, "sync"},
	{SYNC_STATE_ASYNC, "async"},
	{SYNC_STATE_POTENTIAL, "potential"},
	{-1, NULL}
};

static struct enum_recovery_status enum_recovery_status_tab[] =
{
	{RECOVERY_IN, "true"},
	{RECOVERY_NOT_IN, "false"},
	{RECOVERY_UNKNOWN, "unknown"},
	{-1, NULL}
};

#define format_lsn(x) (uint32) (x >> 32), (uint32) x

static TupleDesc common_command_tuple_desc = NULL;
static TupleDesc common_list_acl_tuple_desc = NULL;
static TupleDesc showparam_command_tuple_desc = NULL;
static TupleDesc ha_replication_tuple_desc = NULL;
static TupleDesc common_command_tuple_desc_four_col = NULL;
static void mgr_cmd_run_backend(const char nodetype, const char cmdtype, const List* nodenamelist
, const char *shutdown_mode, PG_FUNCTION_ARGS);
static TupleDesc get_common_command_tuple_desc_four_col(void);
static XLogRecPtr mgr_get_last_wal_receive_location(const Oid hostOid, char *sqlString, const int32 nodePort
	, const char *database, const bool bgtmtype);
static XLogRecPtr parse_lsn(const char *str);
static TupleDesc get_list_nodesize_tuple_desc(void);
bool mgr_recv_msg_for_nodesize(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
HeapTuple build_list_nodesize_tuple(const Name nodename, char nodetype, int32 nodeport, const char *nodepath, int64 nodesize);
TupleDesc common_list_nodesize = NULL;


TupleDesc get_common_command_tuple_desc(void)
{
	if(common_command_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(3, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "name",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "success",
							   BOOLOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "message",
							   TEXTOID, -1, 0);
			common_command_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_command_tuple_desc);
	return common_command_tuple_desc;
}

HeapTuple build_common_command_tuple(const Name name, bool success, const char *message)
{
	Datum datums[3];
	bool nulls[3];
	TupleDesc desc;
	AssertArg(name && message);
	desc = get_common_command_tuple_desc();

	AssertArg(desc && desc->natts == 3
		&& TupleDescAttr(desc, 0)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 1)->atttypid == BOOLOID
		&& TupleDescAttr(desc, 2)->atttypid == TEXTOID);

	datums[0] = NameGetDatum(name);
	datums[1] = BoolGetDatum(success);
	datums[2] = CStringGetTextDatum(message);
	nulls[0] = nulls[1] = nulls[2] = false;
	return heap_form_tuple(desc, datums, nulls);
}


TupleDesc get_list_nodesize_tuple_desc(void)
{
	if(common_list_nodesize == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(5, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "nodename",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "type",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "port",
							   INT4OID, -1, 0);							   
			TupleDescInitEntry(desc, (AttrNumber) 4, "nodepath",
							   TEXTOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 5, "nodesize",
							   INT8OID, -1, 0);
			common_list_nodesize = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_list_nodesize);
	return common_list_nodesize;
}

HeapTuple build_list_nodesize_tuple(const Name nodename, 
									char nodetype,
									int32 nodeport,	
									const char *nodepath, 
									int64 nodesize)
{
	Datum datums[5];
	bool nulls[5];
	TupleDesc desc;
	NameData nodeTypeStr;
	AssertArg(nodename && nodepath);
	desc = get_list_nodesize_tuple_desc();

	get_node_type_str(nodetype, &nodeTypeStr);
	AssertArg(desc && desc->natts == 5
		&& TupleDescAttr(desc, 0)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 1)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 2)->atttypid == INT4OID
		&& TupleDescAttr(desc, 3)->atttypid == TEXTOID
		&& TupleDescAttr(desc, 4)->atttypid == INT8OID);

	datums[0] = NameGetDatum(nodename);
	datums[1] = NameGetDatum(&nodeTypeStr);
	datums[2] = Int32GetDatum(nodeport);
	datums[3] = CStringGetTextDatum(nodepath);
	datums[4] = Int64GetDatum(nodesize);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = false;
	return heap_form_tuple(desc, datums, nulls);
}

HeapTuple build_list_acl_command_tuple(const Name name, const char *message)
{
	Datum datums[2];
	bool nulls[2];
	TupleDesc desc;
	AssertArg(name && message);
	desc = get_list_acl_command_tuple_desc();

	AssertArg(desc && desc->natts == 2
		&& TupleDescAttr(desc, 0)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 1)->atttypid == TEXTOID);

	datums[0] = NameGetDatum(name);
	datums[1] = CStringGetTextDatum(message);
	nulls[0] = nulls[1] = false;
	return heap_form_tuple(desc, datums, nulls);
}

TupleDesc get_list_acl_command_tuple_desc(void)
{
	if(common_list_acl_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(2, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "name",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "message",
							   TEXTOID, -1, 0);
			common_list_acl_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_list_acl_tuple_desc);
	return common_list_acl_tuple_desc;
}
/*hba replication tuple desc*/
HeapTuple build_ha_replication_tuple(const Name type, const Name nodename, const Name app
	, const Name client_addr, const Name state, const Name sent_location, const Name replay_location
	, const Name sync_state, const Name master_location, const Name sent_delay, const Name replay_delay)
{
	Datum datums[11];
	bool nulls[11];
	TupleDesc desc;
	int i = 0;
	AssertArg(type && nodename && app && client_addr && state && sent_location && replay_location && sync_state
								&& master_location && sent_delay && replay_delay);
	desc = get_ha_replication_tuple_desc();

	AssertArg(desc && desc->natts == 11);
	while(i<11)
	{
		AssertArg(TupleDescAttr(desc, i)->atttypid == NAMEOID);
		nulls[i] = false;
		i++;
	}

	datums[0] = NameGetDatum(type);
	datums[1] = NameGetDatum(nodename);
	datums[2] = NameGetDatum(app);
	datums[3] = NameGetDatum(client_addr);
	datums[4] = NameGetDatum(state);
	datums[5] = NameGetDatum(sent_location);
	datums[6] = NameGetDatum(replay_location);
	datums[7] = NameGetDatum(sync_state);
	datums[8] = NameGetDatum(master_location);
	datums[9] = NameGetDatum(sent_delay);
	datums[10] = NameGetDatum(replay_delay);
	return heap_form_tuple(desc, datums, nulls);
}

TupleDesc get_ha_replication_tuple_desc(void)
{
	if(ha_replication_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(11, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "type", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "nodename", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "application_name", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 4, "client_addr", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 5, "state", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 6, "sent_lsn", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 7, "replay_lsn", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 8, "sync_state", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 9, "master_lsn", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 10, "sent_delay", NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 11, "replay_delay", NAMEOID, -1, 0);
			ha_replication_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(ha_replication_tuple_desc);
	return ha_replication_tuple_desc;
}

/*get the the address of host in table host*/
char *get_hostaddress_from_hostoid(Oid hostOid)
{
	Relation rel;
	HeapTuple tuple;
	Datum host_addr;
	char *hostaddress;
	bool isNull = false;

	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		 heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	host_addr = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(rel), &isNull);
	if(isNull)
	{
		ReleaseSysCache(tuple);
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	}
	hostaddress = pstrdup(TextDatumGetCString(host_addr));
	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return hostaddress;
}

/*get the the hostname in table host*/
char *get_hostname_from_hostoid(Oid hostOid)
{
	Relation rel;
	HeapTuple tuple;
	Datum host_name;
	char *hostname;
	bool isNull = false;

	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	host_name = heap_getattr(tuple, Anum_mgr_host_hostname, RelationGetDescr(rel), &isNull);
	if(isNull)
	{
		ReleaseSysCache(tuple);
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostname is null")));
	}
	hostname = pstrdup(NameStr(*DatumGetName(host_name)));
	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return hostname;
}

/*get the the user in table host*/
char *get_hostuser_from_hostoid(Oid hostOid)
{
	Relation rel;
	HeapTuple tuple;
	Datum host_user;
	char *hostuser;
	bool isNull = false;

	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	host_user = heap_getattr(tuple, Anum_mgr_host_hostuser, RelationGetDescr(rel), &isNull);
	if(isNull)
	{
		ReleaseSysCache(tuple);
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostuser is null")));
	}
	hostuser = pstrdup(NameStr(*DatumGetName(host_user)));
	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return hostuser;
}

/*
* get msg from agent
*/
bool mgr_recv_msg_original_result(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst, AGENT_RESULT_MsgTYPE resultType)
{
	char			msg_type;
	StringInfoData recvbuf;
	bool initdone = false;
	initStringInfo(&recvbuf);
	for(;;)
	{
		resetStringInfo(&recvbuf);
		msg_type = ma_get_message(ma, &recvbuf);
		if(msg_type == AGT_MSG_IDLE)
		{
			/* message end */
			break;
		}else if(msg_type == '\0')
		{
			/* has an error */
			break;
		}else if(msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_get_err_info(&recvbuf, AGT_MSG_RESULT));
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
			break;
		}else if(msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
		}
		else if(msg_type == AGT_MSG_RESULT)
		{
			getAgentCmdRst->ret = true;
			switch(resultType)
			{
				case AGENT_RESULT_LOG:
					appendStringInfoString(&(getAgentCmdRst->description), recvbuf.data);
					ereport(NOTICE, (errmsg("receive msg: %s", recvbuf.data)));
					ereport(LOG, (errmsg("receive msg: %s", recvbuf.data)));
					break;
				case AGENT_RESULT_DEBUG:
					appendStringInfoString(&(getAgentCmdRst->description), run_success);
					ereport(DEBUG1, (errmsg("receive msg: %s", recvbuf.data)));
					break;
				case AGENT_RESULT_MESSAGE:
					appendStringInfoString(&(getAgentCmdRst->description), recvbuf.data);
					break;
			}
			initdone = true;
			break;
		}
	}
	pfree(recvbuf.data);
	return initdone;
}

bool mgr_recv_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst)
{
	return mgr_recv_msg_original_result(ma, getAgentCmdRst, AGENT_RESULT_DEBUG);
}

//get node size from agent
bool mgr_recv_msg_for_nodesize(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst)
{
	return mgr_recv_msg_original_result(ma, getAgentCmdRst, AGENT_RESULT_MESSAGE);
}

/*
* get host info from agent for [ADB monitor]
*/
bool mgr_recv_msg_for_monitor(ManagerAgent *ma, bool *ret, StringInfo agentRstStr)
{
	char msg_type;
	bool initdone = false;

	for (;;)
	{
		msg_type = ma_get_message(ma, agentRstStr);
		if (msg_type == AGT_MSG_IDLE)
		{
			/* message end */
			break;
		}else if (msg_type == '\0')
		{
			/* has an error */
			break;
		}else if (msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			*ret = false;
			appendStringInfoString(agentRstStr, ma_get_err_info(agentRstStr, AGT_MSG_RESULT));
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(agentRstStr, AGT_MSG_RESULT))));
			break;
		}else if (msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(agentRstStr, AGT_MSG_RESULT))));
		}
		else if (msg_type == AGT_MSG_RESULT)
		{
			*ret = true;
			ereport(DEBUG1, (errmsg("receive msg: %s", agentRstStr->data)));
			initdone = true;
			break;
		}
	}
	return initdone;
}

bool is_valid_ip(char *ip)
{
	FILE *pPipe;
	char psBuffer[1024];
	char ping_str[1024];
	struct hostent *hptr;

	if ((hptr = gethostbyname(ip)) == NULL)
	{
		return false;
	}

	snprintf(ping_str, sizeof(ping_str), "ping -c 1 %s", ip);

	if((pPipe = popen(ping_str, "r" )) == NULL )
	{
		return false;
	}
	while(fgets(psBuffer, 1024, pPipe))
	{
		if (strstr(psBuffer, "0 received") != NULL ||
			strstr(psBuffer, "Unreachable") != NULL)
		{
			pclose(pPipe);
			return false;
		}
	}
	pclose(pPipe);
	return true;
}

/* ping someone node for monitor */
int pingNode_user(char *host_addr, char *node_port, char *node_user)
{
	int ping_status;
	bool execok = false;
	ManagerAgent *ma;
	StringInfoData sendstrmsg;
	StringInfoData buf;
	char pid_file_path[MAXPATH] = {0};
	Datum nodepath;
	int32 agent_port;
	Relation rel;
	HeapScanDesc rel_scan;
	ScanKeyData key[2];
	HeapTuple tuple;
	Oid host_tuple_oid;
	Form_mgr_host mgr_host;
	GetAgentCmdRst getAgentCmdRst;
	bool isnull;
	Assert(host_addr && node_port && node_user);

	/*get the host port base on the port of node and host*/
	ScanKeyInit(&key[0]
				,Anum_mgr_host_hostaddr
				,BTEqualStrategyNumber
				,F_TEXTEQ
				,CStringGetTextDatum(host_addr));
	rel = heap_open(HostRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel, 1, key);
	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	host_tuple_oid = HeapTupleGetOid(tuple);
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errmsg("host\"%s\" does not exist in the host table", host_addr)));
	}
	mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	agent_port = mgr_host->hostagentport;
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);

	/*get postmaster.pid file path */
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeport
				,BTEqualStrategyNumber
				,F_INT4EQ
				,Int32GetDatum(atol(node_port)));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodehost
				,BTEqualStrategyNumber
				,F_OIDEQ
				,ObjectIdGetDatum(host_tuple_oid));
	rel = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel, 2, key);
	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errmsg("port \"%s\" does not exist in the node table", node_port)));
	}
	nodepath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isnull);
	snprintf(pid_file_path, MAXPATH, "%s/postmaster.pid", TextDatumGetCString(nodepath));
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);

	/*send the node message to agent*/
	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&sendstrmsg, "%s", host_addr);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", node_port);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", node_user);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", pid_file_path);

	ma = ma_connect(host_addr, agent_port);;
	if (!ma_isconnected(ma))
	{
		/*report error message*/
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(LOG, (errmsg("could not connect socket for agent \"%s\".",
						host_addr)));
		pfree(sendstrmsg.data);
		pfree(getAgentCmdRst.description.data);
		return AGENT_DOWN;
	}
	getAgentCmdRst.ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PING_NODE);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return -1;
	}
	/*check the receive msg*/
	mgr_recv_msg_for_monitor(ma, &execok, &getAgentCmdRst.description);
	ma_close(ma);
	if (!execok)
	{
		ereport(WARNING, (errmsg("monitor (host=%s port=%s) fail \"%s\"",
			host_addr, node_port, getAgentCmdRst.description.data)));
	}
	if (getAgentCmdRst.description.len == 1)
		ping_status = getAgentCmdRst.description.data[0];
	else
		ereport(ERROR, (errmsg("receive msg from agent \"%s\" error.", host_addr)));
	pfree(getAgentCmdRst.description.data);
	switch(ping_status)
	{
		case PQPING_OK:
		case PQPING_REJECT:
		case PQPING_NO_ATTEMPT:
		case PQPING_NO_RESPONSE:
			return ping_status;
		default:
			return PQPING_NO_RESPONSE;
	}
	pfree(buf.data);
}

/*check the host in use or not*/
bool mgr_check_host_in_use(Oid hostoid, bool check_inited)
{
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Form_mgr_node mgr_node;
	Relation rel;
	ScanKeyData key[1];
	bool is_using = false;
	rel = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	if (!check_inited)
		rel_scan = heap_beginscan_catalog(rel, 0, NULL);
	else
		rel_scan = heap_beginscan_catalog(rel, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		/* check this tuple incluster or not, if it has incluster, cannot be dropped/alter. */
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(mgr_node->nodehost == hostoid)
		{
			break;
		}
	}
	if (HeapTupleIsValid(tuple))
		is_using =  true;
	else
		is_using =  false;
	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
	return is_using;
}

/*
* get database namelist
*/
List *monitor_get_dbname_list(char *user, char *address, int port)
{
	StringInfoData constr;
	PGconn* conn;
	PGresult *res;
	char *oneCoordValueStr;
	List *nodenamelist =NIL;
	int iN = 0;
	char *sqlstr = "select datname from pg_database  where datname != \'template0\' and datname != \'template1\' order by 1;";

	initStringInfo(&constr);
	appendStringInfo(&constr, "postgresql://%s@%s:%d/postgres", user, address, port);
	conn = PQconnectdb(constr.data);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		ereport(LOG,
			(errmsg("Connection to database failed: %s\n", PQerrorMessage(conn))));
		PQfinish(conn);
		pfree(constr.data);
		return NULL;
	}
	res = PQexec(conn, sqlstr);
	if(PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(LOG,
			(errmsg("Select failed: %s\n" , PQresultErrorMessage(res))));
		PQclear(res);
		PQfinish(conn);
		pfree(constr.data);
		return NULL;
	}
	/*check column number*/
	Assert(1 == PQnfields(res));
	for (iN=0; iN < PQntuples(res); iN++)
	{
		oneCoordValueStr = PQgetvalue(res, iN, 0 );
		nodenamelist = lappend(nodenamelist, pstrdup(oneCoordValueStr));
	}
	PQclear(res);
	PQfinish(conn);
	pfree(constr.data);

	return nodenamelist;
}

/*
* get user, hostaddress from coordinator
*/
void monitor_get_one_node_user_address_port(Relation rel_node, int *agentport, char **user, char **address, int *coordport, char nodetype)
{
	HeapScanDesc rel_scan;
	ScanKeyData key[3];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(rel_node, 3, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		*coordport = mgr_node->nodeport;
		*address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		*user = get_hostuser_from_hostoid(mgr_node->nodehost);
		tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
		if(!(HeapTupleIsValid(tup)))
		{
			ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errcode(ERRCODE_INTERNAL_ERROR)));
		}
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		*agentport = mgr_host->hostagentport;
		ReleaseSysCache(tup);
		break;
	}
	heap_endscan(rel_scan);
}

/*
* get len values to iarray, the values get from the given sqlstr's result
*/
bool monitor_get_sqlvalues_one_node(int agentport, char *sqlstr, char *user, char *address
										, int port, char * dbname, int64 iarray[], int len)
{
	int iloop = 0;
	char *pstr;
	char strtmp[64];
	StringInfoData resultstrdata;

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, sqlstr, user, address, port, dbname, &resultstrdata);
	if (resultstrdata.len == 0)
	{
		return false;
	}
	pstr = resultstrdata.data;
	while(pstr != NULL && iloop < len)
	{
		memset(strtmp, 0 , 64);
		strcpy(strtmp, pstr);
		iarray[iloop] = atoll(strtmp);
		pstr = pstr + strlen(strtmp) + 1;
		iloop++;
	}
	pfree(resultstrdata.data);
	return true;
}

/*
* to make the columns name for the result of command: show nodename parameter
*/
TupleDesc get_showparam_command_tuple_desc(void)
{
	if(showparam_command_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(3, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "type",
							   NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "success",
							   BOOLOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "message",
							   TEXTOID, -1, 0);
			common_command_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_command_tuple_desc);
	return common_command_tuple_desc;
}

List * DecodeTextArrayToValueList(Datum textarray)
{
	ArrayType  *array = NULL;
	Datum *elemdatums;
	int num_elems;
	List *value_list = NIL;
	int i;

	Assert( PointerIsValid(DatumGetPointer(textarray)));

	array = DatumGetArrayTypeP(textarray);
	Assert(ARR_ELEMTYPE(array) == TEXTOID);

	deconstruct_array(array, TEXTOID, -1, false, 'i', &elemdatums, NULL, &num_elems);
	for (i = 0; i < num_elems; ++i)
	{
		value_list = lappend(value_list, makeString(TextDatumGetCString(elemdatums[i])));
	}

	return value_list;
}

void check_nodename_isvalid(char *nodename)
{
	HeapTuple tuple;
	ScanKeyData key[2];
	Relation rel;
	HeapScanDesc rel_scan;

	rel = heap_open(NodeRelationId, AccessShareLock);

	ScanKeyInit(&key[0]
			,Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(nodename));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(rel, 2, key);

	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	if (tuple == NULL)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("node name \"%s\" does not exist", nodename)));

	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);
}

bool mgr_has_function_privilege_name(char *funcname, char *priv_type)
{
	Datum aclresult;
	aclresult = DirectFunctionCall2(has_function_privilege_name,
							CStringGetTextDatum(funcname),
							CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

bool mgr_has_table_privilege_name(char *tablename, char *priv_type)
{
	Datum aclresult;
	aclresult = DirectFunctionCall2(has_table_privilege_name,
							CStringGetTextDatum(tablename),
							CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

/*
* get msg from agent
*/
void mgr_recv_sql_stringvalues_msg(ManagerAgent	*ma, StringInfo resultstrdata)
{
	char msg_type;
	StringInfoData recvbuf;
	initStringInfo(&recvbuf);
	for(;;)
	{
		msg_type = ma_get_message(ma, &recvbuf);
		if(msg_type == AGT_MSG_IDLE)
		{
			/* message end */
			break;
		}else if(msg_type == '\0')
		{
			/* has an error */
			break;
		}else if(msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			if (NULL != ma_get_err_info(&recvbuf, AGT_MSG_RESULT))
				ereport(WARNING, (errmsg("%s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
			break;
		}else if(msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(NOTICE, (errmsg("%s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
		}
		else if(msg_type == AGT_MSG_RESULT)
		{
			appendBinaryStringInfo(resultstrdata, recvbuf.data, recvbuf.len);
			ereport(DEBUG1, (errmsg("%s", recvbuf.data)));
			break;
		}
	}
	pfree(recvbuf.data);
}

/*
* get active coordinator node name
*/
bool mgr_get_active_node(Name nodename, char nodetype, Oid lowPriorityOid)
{
	ScanKeyData key[4];
	Form_mgr_node mgr_node;
	Relation relNode;
	HeapScanDesc relScan;
	HeapTuple tuple;
	int res = -1;
	int iloop = 0;
	char *hostAddr;
	char *userName;
	char portBuf[10];
	bool bresult = false;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));	
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 4, key);
	for (iloop = 0; iloop < 2; iloop++)
	{
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if ((iloop == 0) && (lowPriorityOid == mgr_node->nodehost))
				continue;
			else
			{
				if ((iloop == 1) && (lowPriorityOid != mgr_node->nodehost))
					continue;
			}
			/* check node status */
			hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			userName = get_hostuser_from_hostoid(mgr_node->nodehost);
			memset(portBuf, 0, 10);
			sprintf(portBuf, "%d", mgr_node->nodeport);
			res = pingNode_user(hostAddr, portBuf, userName);
			pfree(hostAddr);
			pfree(userName);
			if (res == 0)
			{
				bresult = true;
				namestrcpy(nodename, NameStr(mgr_node->nodename));
				iloop = 2;
				break;
			}
		}
	}

	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	return bresult;
}

/*
* given the input parameter n_days as interval time, the data in the table before n_days will be droped
*
*/
void monitor_delete_data(MonitorDeleteData *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_alter())
	{
		DirectFunctionCall1(monitor_delete_data_interval_days, Int32GetDatum(node->days));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}

}

/*
* given the input parameter n_days as interval time, the data in the table before
* n_days will be droped
*
*/

Datum monitor_delete_data_interval_days(PG_FUNCTION_ARGS)
{
	int interval_days = PG_GETARG_INT32(0);
	int ret;
	int iloop = 0;
	StringInfoData sqlstrdata;
	struct del_tablename
	{
		char *tbname;
		char *coltimename;
	}del_tablename[]={
		{"monitor_cpu", "mc_timestamptz"},
		{"monitor_disk", "md_timestamptz"},
		{"monitor_host", "mh_current_time"},
		{"monitor_mem", "mm_timestamptz"},
		{"monitor_net", "mn_timestamptz"},
		{"monitor_resolve", "mr_resolve_timetz"},
		{"monitor_alarm", "ma_alarm_timetz"},
		{"monitor_databaseitem", "monitor_databaseitem_time"},
		{"monitor_databasetps", "monitor_databasetps_time"},
		{"monitor_slowlog", "slowlogtime"},
		{NULL, NULL}
		};

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Monitor SPI_connect failed: error code %d", ret)));

	initStringInfo(&sqlstrdata);

	for(iloop=0; del_tablename[iloop].tbname != NULL; iloop++)
	{
		appendStringInfo(&sqlstrdata, "delete from %s where %s < timestamp'now()' - interval'%d day';"
			,del_tablename[iloop].tbname, del_tablename[iloop].coltimename, interval_days);
		ret = SPI_execute(sqlstrdata.data, false, 0);
		if (ret != SPI_OK_DELETE)
			ereport(ERROR, (errmsg("ADB Monitor SPI_execute \"%s\"failed: error code %d", sqlstrdata.data, ret)));
		ereport(LOG, (errmsg("ADB Monitor clean data: table \"%s\", data of \"%d\" days ago"
			,del_tablename[iloop].tbname, interval_days)));
		resetStringInfo(&sqlstrdata);
		SPI_freetuptable(SPI_tuptable);
	}
	pfree(sqlstrdata.data);
	SPI_finish();

	PG_RETURN_BOOL(true);
}

/*
* set cluster init in mgr_node table,initialized=true, incluster=true
*/
void mgr_set_init(MGRSetClusterInit *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall1(mgr_set_init_cluster, (Datum)0);
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*
* update mgr_node table, set initialized=true, incluster=true
*/

Datum mgr_set_init_cluster(PG_FUNCTION_ARGS)
{
	char *sqlstr = "update mgr_node set nodeinited=true,nodeincluster=true;";
	int ret;

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));

	ret = SPI_execute(sqlstr, false, 0);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR, (errmsg("ADB Manager SPI_execute \"%s\"failed: error code %d", sqlstr, ret)));
	ereport(LOG, (errmsg("update mgr_node table, set initialized=true, incluster=true")));
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	PG_RETURN_BOOL(true);

}


/*
* promote the gtm or datanode node
*
*/

bool mgr_promote_node(char cmdtype, Oid hostOid, char *path, StringInfo strinfo)
{
	bool res = false;
	StringInfoData infosendmsg;

	/*check the cmdtype*/
	if (AGT_CMD_GTMCOORD_SLAVE_FAILOVER != cmdtype || AGT_CMD_DN_FAILOVER != cmdtype)
	{
		appendStringInfo(strinfo, "the cmdtype is \"%d\", not for gtm promote or datanode promote", cmdtype);
		return false;
	}
	/*check the path*/
	if (!path || path[0] != '/')
	{
		appendStringInfoString(strinfo, "the path is not absolute path");
		return false;
	}

	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " promote -w -D %s", path);
	res = mgr_ma_send_cmd(cmdtype, infosendmsg.data, hostOid, strinfo);
	pfree(infosendmsg.data);

	return res;
}

/*
* wait the new master accept connect
*
*/
bool mgr_check_node_connect(char nodetype, Oid hostOid, int nodeport)
{
	char *hostaddr;
	char nodeport_buf[10];
	char *username = NULL;

	hostaddr = get_hostaddress_from_hostoid(hostOid);
	/*check recovery finish*/
	fputs(_("waiting for the new master can accept connections..."), stdout);
	fflush(stdout);
	while(1)
	{
		if (mgr_check_node_recovery_finish(nodetype, hostOid, nodeport, hostaddr))
			break;
		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}
	memset(nodeport_buf, 0, 10);
	sprintf(nodeport_buf, "%d", nodeport);
	username = get_hostuser_from_hostoid(hostOid);

	while(1)
	{
		if (pingNode_user(hostaddr, nodeport_buf, username) != 0)
		{
			fputs(_("."), stdout);
			fflush(stdout);
			pg_usleep(1 * 1000000L);
		}
		else
			break;
	}
	fputs(_(" done\n"), stdout);
	fflush(stdout);

	pfree(hostaddr);
	if (username)
		pfree(username);

	return true;
}

/*
* rewind the node
*
*/

bool mgr_rewind_node(char nodetype, char *nodename, StringInfo strinfo)
{
	char cmdtype;
	char mastertype;
	char portBuf[10];
	char adbhome[MAXPGPATH];
	char *hostAddr;
	char *user;
	char *nodetypestr;
	char *node_path;
	char *masternode;
	bool res = false;
	bool slave_is_exist = false;
	bool slave_is_running = false;
	bool master_is_exist = false;
	bool master_is_running = false;
	bool isNull = false;
	bool bGtmType = false;
	int rest;
	int iloop;
	int agentPortM;
	bool resA = true;
	bool resB = true;
	const int iMax = 90;
	AppendNodeInfo slave_nodeinfo;
	AppendNodeInfo master_nodeinfo;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	GetAgentCmdRst getAgentCmdRst;
	Relation rel_node;
	Relation rel_host;
	HeapTuple tuple;
	HeapTuple node_tuple;
	HeapTuple hostTupleM;
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	Datum datumPath;
	NameData masterNameData;
	/*check node type*/
	if (nodetype != CNDN_TYPE_GTM_COOR_SLAVE && nodetype != CNDN_TYPE_DATANODE_SLAVE)
	{
		appendStringInfo(strinfo, "the nodetype is \"%d\", not for gtmcoord rewind or datanode rewind", nodetype);
		return false;
	}

	cmdtype = AGT_CMD_NODE_REWIND;
	if (CNDN_TYPE_GTM_COOR_SLAVE == nodetype)
	{
		bGtmType = true;
	}
	else
	{
		bGtmType = false;
	}

	Assert(nodename);

	/* get the master name of this node */
	masternode = mgr_get_mastername_by_nodename_type(nodename, nodetype);
	namestrcpy(&masterNameData, masternode);
	pfree(masternode);
	nodetypestr = mgr_nodetype_str(nodetype);
	/* check exists */
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(rel_node, nodename);
	if(!(HeapTupleIsValid(tuple)))
	{
		heap_close(rel_node, AccessShareLock);
		appendStringInfo(strinfo, "%s \"%s\" does not exist", nodetypestr, nodename);
		pfree(nodetypestr);
		return false;
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	memset(portBuf, 0, 10);
	snprintf(portBuf, sizeof(portBuf), "%d", mgr_node->nodeport);
	/*restart the node then stop it with fast mode*/
	initStringInfo(&(getAgentCmdRst.description));
	ereport(NOTICE, (errmsg("pg_ctl restart %s \"%s\"", nodetypestr, nodename)));
	if (bGtmType)
		mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
	else
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
	if(!getAgentCmdRst.ret)
	{
		heap_freetuple(tuple);
		heap_close(rel_node, AccessShareLock);
		ereport(WARNING, (errmsg("pg_ctl restart %s \"%s\" fail, %s", nodetypestr, nodename, getAgentCmdRst.description.data)));
		appendStringInfo(strinfo, "pg_ctl restart %s \"%s\" fail, %s", nodetypestr, nodename, getAgentCmdRst.description.data);
		pfree(nodetypestr);
		pfree(hostAddr);
		pfree(user);
		pfree(getAgentCmdRst.description.data);
		return false;
	}
	/* wait until the node running normal */
	ereport(LOG, (errmsg("wait max %d seconds to check %s \"%s\" running normal", iMax, nodetypestr, nodename)));
	ereport(NOTICE, (errmsg("wait max %d seconds to check %s \"%s\" running normal", iMax, nodetypestr, nodename)));
	pg_usleep(3000000L);
	iloop = iMax-3;
	while(iloop-- >0)
	{
		rest = pingNode_user(hostAddr, portBuf, user);
		if (PQPING_OK == rest)
			break;
		pg_usleep(1000000L);
	}
	if (iloop <= 0)
	{
		heap_freetuple(tuple);
		heap_close(rel_node, AccessShareLock);
		ereport(WARNING, (errmsg("wait max %d seconds to check %s \"%s\" running normal fail", iMax, nodetypestr, nodename)));
		pfree(getAgentCmdRst.description.data);
		pfree(nodetypestr);
		pfree(user);
		pfree(hostAddr);
		return false;
	}

	ereport(NOTICE, (errmsg("pg_ctl stop %s \"%s\" with fast mode", nodetypestr, nodename)));
	resetStringInfo(&(getAgentCmdRst.description));
	if (bGtmType)
		mgr_runmode_cndn_get_result(AGT_CMD_GTMCOORD_STOP_SLAVE, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
	else
		mgr_runmode_cndn_get_result(AGT_CMD_DN_STOP, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
	if(!getAgentCmdRst.ret)
	{
		heap_freetuple(tuple);
		heap_close(rel_node, AccessShareLock);
		ereport(WARNING, (errmsg("pg_ctl stop %s \"%s\" with fast mode fail, %s", nodetypestr, nodename, getAgentCmdRst.description.data)));
		appendStringInfo(strinfo, "pg_ctl stop %s \"%s\" with fast mode fail, %s", nodetypestr, nodename, getAgentCmdRst.description.data);
		pfree(getAgentCmdRst.description.data);
		pfree(nodetypestr);
		pfree(user);
		pfree(hostAddr);
		return false;
	}
	heap_freetuple(tuple);
	heap_close(rel_node, AccessShareLock);

	ereport(LOG, (errmsg("wait max %d seconds to check %s \"%s\" stop complete", iMax, nodetypestr, nodename)));
	ereport(NOTICE, (errmsg("wait max %d seconds to check %s \"%s\" stop complete", iMax, nodetypestr, nodename)));
	pg_usleep(2000000L);
	iloop = iMax-2;
	while(iloop-- >0)
	{
		rest = pingNode_user(hostAddr, portBuf, user);
		if (PQPING_NO_RESPONSE == rest)
			break;
		pg_usleep(1000000L);
	}
	if (iloop <= 0)
	{
		ereport(WARNING, (errmsg("wait max %d seconds to check %s \"%s\" stop complete fail", iMax, nodetypestr, nodename)));
		pfree(user);
		pfree(hostAddr);
		pfree(nodetypestr);
		pfree(getAgentCmdRst.description.data);
		appendStringInfo(strinfo, "wait max %d seconds to check %s \"%s\" stop complete fail", iMax, nodetypestr, nodename);
		return false;
	}

	pfree(user);
	pfree(hostAddr);
	pfree(nodetypestr);
	/*get the slave info, no matter it is in cluster or not*/
	mgr_get_nodeinfo_byname_type(nodename, nodetype, false, &slave_is_exist, &slave_is_running, &slave_nodeinfo);
	/*get its master info*/
	mastertype = mgr_get_master_type(nodetype);
	get_nodeinfo_byname(masterNameData.data, mastertype, &master_is_exist, &master_is_running, &master_nodeinfo);
	if (master_is_exist && (!master_is_running))
	{
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
		nodetypestr = mgr_nodetype_str(mastertype);
		appendStringInfo(strinfo, "%s \"%s\" does not running normal", nodetypestr, masterNameData.data);
		pfree(nodetypestr);
		pfree(getAgentCmdRst.description.data);
		return false;
	}

	initStringInfo(&infosendmsg);
	/*update gtm master|slave and the rewind node's master, the master's slave pg_hba.conf*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((node_tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(node_tuple);
		Assert(mgr_node);
		if (!(CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype || CNDN_TYPE_GTM_COOR_SLAVE == mgr_node->nodetype
				 || HeapTupleGetOid(node_tuple) == master_nodeinfo.tupleoid || mgr_node->nodemasternameoid ==master_nodeinfo.tupleoid))
				continue;
		nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
		ereport(NOTICE, (errmsg("update %s \"%s\" pg_hba.conf for the rewind node %s", nodetypestr, NameStr(mgr_node->nodename), nodename)));
		pfree(nodetypestr);
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		if (CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype || CNDN_TYPE_GTM_COOR_SLAVE == mgr_node->nodetype)
		{
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", slave_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", "all", slave_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		}
		else
		{
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", slave_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", "all", slave_nodeinfo.nodeaddr, 32, "trust", &infosendmsg);
			mgr_add_parameters_hbaconf(master_nodeinfo.tupleoid, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		}
		datumPath = heap_getattr(node_tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, AccessShareLock);
			pfree_AppendNodeInfo(master_nodeinfo);
			pfree_AppendNodeInfo(slave_nodeinfo);
			pfree(getAgentCmdRst.description.data);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp")
				, errmsg("column cndnpath is null")));
		}
		node_path = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							node_path,
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			pfree_AppendNodeInfo(master_nodeinfo);
			pfree_AppendNodeInfo(slave_nodeinfo);
			appendStringInfo(strinfo, "%s", getAgentCmdRst.description.data);
			pfree(getAgentCmdRst.description.data);
			pfree(infosendmsg.data);
			heap_endscan(rel_scan);
			heap_close(rel_node, AccessShareLock);
			return false;
		}
		if (!(mgr_node->nodetype == nodetype && strcmp(nodename, NameStr(mgr_node->nodename)) ==0))
			mgr_reload_conf(mgr_node->nodehost, node_path);
	}

	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);
	pfree(getAgentCmdRst.description.data);

	/* send checkpoint sql command to master */
	rel_host = heap_open(HostRelationId, AccessShareLock);
	hostTupleM = SearchSysCache1(HOSTHOSTOID, master_nodeinfo.nodehost);
	if(!(HeapTupleIsValid(hostTupleM)))
	{
		appendStringInfo(strinfo, "get the datanode master \"%s\" information in node table fail", masterNameData.data);
		ereport(WARNING, (errmsg("get the datanode master \"%s\" information in node table fail", masterNameData.data)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
		heap_close(rel_host, AccessShareLock);
		pfree(infosendmsg.data);
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
		return false;
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(hostTupleM);
	Assert(mgr_host);
	datumPath = heap_getattr(hostTupleM, Anum_mgr_host_hostadbhome, RelationGetDescr(rel_host), &isNull);
	if (isNull)
	{
		ReleaseSysCache(hostTupleM);
		heap_close(rel_host, AccessShareLock);
		pfree(infosendmsg.data);
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column adbhome is null")));
	}
	heap_close(rel_host, AccessShareLock);
	memset(adbhome, 0, MAXPGPATH);
	strncpy(adbhome, TextDatumGetCString(datumPath), MAXPGPATH-1);
	agentPortM = mgr_host->hostagentport;
	ReleaseSysCache(hostTupleM);

	ereport(LOG, (errmsg("on %s master \"%s\" execute \"checkpoint\"", bGtmType ? "gtmcoord":"datanode", NameStr(masterNameData))));
	ereport(NOTICE, (errmsg("on %s master \"%s\" execute \"checkpoint\"", bGtmType ? "gtmcoord":"datanode", NameStr(masterNameData))));
	initStringInfo(&restmsg);
	iloop = 10;
	while(iloop-- > 0)
	{
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES_COMMAND, agentPortM, "checkpoint;"
				, master_nodeinfo.nodeusername, master_nodeinfo.nodeaddr, master_nodeinfo.nodeport, DEFAULT_DB, &restmsg);
		if (restmsg.len > 0 && strcmp(restmsg.data, "CHECKPOINT") == 0)
			break;
		pg_usleep(1000000L);
	}
	/* check datanode master pg_controldata */
	resA = true;
	resB = true;
	resetStringInfo(&restmsg);
	resetStringInfo(&infosendmsg);
	/*get adbhome*/

	appendStringInfo(&infosendmsg, "%s/bin/pg_controldata '%s' | grep 'Minimum recovery ending location:' |awk '{print $5}'"
				, adbhome, master_nodeinfo.nodepath);
	appendStringInfoCharMacro(&infosendmsg, '\0');
	resA = mgr_ma_send_cmd_get_original_result(AGT_CMD_GET_BATCH_JOB, infosendmsg.data, master_nodeinfo.nodehost, &restmsg, AGENT_RESULT_LOG);
	if (resA)
	{
		if (restmsg.len == 0)
			resA = false;
		else if (strcasecmp(restmsg.data, "{\"result\":\"0/0\"}") != 0)
			resA = false;
	}

	resetStringInfo(&restmsg);
	resetStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, "%s/bin/pg_controldata '%s' |grep 'Min recovery ending loc' |awk '{print $6}'", adbhome, master_nodeinfo.nodepath);
	appendStringInfoCharMacro(&infosendmsg, '\0');
	resB = mgr_ma_send_cmd_get_original_result(AGT_CMD_GET_BATCH_JOB, infosendmsg.data, master_nodeinfo.nodehost, &restmsg, AGENT_RESULT_LOG);
	if (resB)
	{
		if (restmsg.len == 0)
			resB = false;
		else if (strcasecmp(restmsg.data, "{\"result\":\"0\"}") != 0)
			resB = false;
	}

	if (!resA || !resB)
	{
		appendStringInfo(strinfo, "on %s master \"%s\" pg_controldata get expect value fail", bGtmType?"gtm":"datanode", NameStr(masterNameData));
		ereport(WARNING, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("on the %s master \"%s\" execute \"pg_controldata %s\" to get the expect value fail"
				, bGtmType?"gtm":"datanode", NameStr(masterNameData), master_nodeinfo.nodepath)
				,errhint("execute \"checkpoint\" on %s master \"%s\", then execute  \"pg_controldata %s\" to check \"Minimum recovery \
					ending location\" is \"0/0\" and \"Min recovery ending loc's timeline\" is \"0\" before execute the rewind command again",
					bGtmType?"gtm":"datanode", NameStr(masterNameData), master_nodeinfo.nodepath)));
		pfree(restmsg.data);
		pfree(infosendmsg.data);
		pfree_AppendNodeInfo(master_nodeinfo);
		pfree_AppendNodeInfo(slave_nodeinfo);
		return false;
	}
	pg_usleep(3000000L);
	/*node rewind*/
	resetStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg,
					 " --target-pgdata %s --source-server='host=%s port=%d user=%s dbname=postgres' -T %s -S %s",
					 slave_nodeinfo.nodepath,
					 master_nodeinfo.nodeaddr,
					 master_nodeinfo.nodeport,
					 slave_nodeinfo.nodeusername,
					 nodename,
					 master_nodeinfo.nodename);

	res = mgr_ma_send_cmd_get_original_result(cmdtype, infosendmsg.data, slave_nodeinfo.nodehost, strinfo, AGENT_RESULT_LOG);
	pfree(restmsg.data);
	pfree(infosendmsg.data);
	pfree_AppendNodeInfo(master_nodeinfo);
	pfree_AppendNodeInfo(slave_nodeinfo);
	return res;
}

/*
* send adbmgr command string to agent; if fail, the error information in strinfo
*
*/
bool mgr_ma_send_cmd_get_original_result(char cmdtype, char *cmdstr, Oid hostOid, StringInfo strinfo, AGENT_RESULT_MsgTYPE resultType)
{
	char *hostaddr;
	char cmdheadstr[64];
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData buf;
	bool res = false;

	hostaddr = get_hostaddress_from_hostoid(hostOid);
	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		appendStringInfo(strinfo, "%s", ma_last_error_msg(ma));
		ma_close(ma);
		pfree(hostaddr);
		return false;
	}

	mgr_get_cmd_head_word(cmdtype, cmdheadstr);
	ereport(LOG, (errmsg("%s, %s %s", hostaddr, cmdheadstr, cmdstr)));
	pfree(hostaddr);

	/*send cmd*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,cmdstr);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		appendStringInfoString(strinfo, ma_last_error_msg(ma));
		ma_close(ma);
		return false;
	}
	/*check the receive msg*/
	initStringInfo(&(getAgentCmdRst.description));
	res = mgr_recv_msg_original_result(ma, &getAgentCmdRst, resultType);
	ma_close(ma);
	appendStringInfoString(strinfo, getAgentCmdRst.description.data);
	pfree(getAgentCmdRst.description.data);

	return res;
}

bool mgr_ma_send_cmd(char cmdtype, char *cmdstr, Oid hostOid, StringInfo strinfo)
{
	return mgr_ma_send_cmd_get_original_result(cmdtype, cmdstr, hostOid, strinfo, AGENT_RESULT_DEBUG);
}

/*
* for the comand "start all" or "stop all" or "start nodename nodetype all", send the command to agent and
* run as backend.
*/

static void mgr_cmd_run_backend(const char nodetype, const char cmdtype, const List* nodenamelist, const char *shutdown_mode, PG_FUNCTION_ARGS)
{
	Relation rel_node;
	ListCell *lc;
	char *nodestrname;
	HeapTuple aimtuple =NULL;
	GetAgentCmdRst getAgentCmdRst;


	rel_node = heap_open(NodeRelationId, AccessShareLock);
	initStringInfo(&(getAgentCmdRst.description));
	foreach(lc, nodenamelist)
	{
		nodestrname = (char *) lfirst(lc);
		aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodestrname);
		if (!HeapTupleIsValid(aimtuple))
		{
			heap_close(rel_node, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodestrname)));
		}
		/*get execute cmd result from agent*/
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, rel_node, aimtuple, shutdown_mode);
		heap_freetuple(aimtuple);
	}

	pfree(getAgentCmdRst.description.data);
	heap_close(rel_node, AccessShareLock);

}

/*
* for the comand "start all" or "stop all" or "start nodename nodetype all", send the command to agent and
* run as backend, at last, check the node's status, if fail, send the command again, then get result
*/
Datum mgr_typenode_cmd_run_backend_result(nodenames_supplier supplier,
										  nodenames_consumer consumer,
										  const char nodetype,
										  const char cmdtype,
										  const char *shutdown_mode,
										  PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ListCell  **lcp;
	ListCell  *lc;
	List *nodenamelist = NIL;
	HeapTuple tup_result;
	NameData nodenamedata;
	StringInfoData strdata;
	StringInfoData strhint;
	StringInfoData infosendmsg;
	StringInfoData alterNodeCmd;
	Relation rel_node;
	HeapTuple aimtuple = NULL;
	Form_mgr_node mgr_node;
	bool bresult = false;
	bool binit = false;
	bool bstartcmd = false;
	bool bstopcmd = false;
	int ret;
	int iloop = 90;
	char *host_addr;
	char *user;
	char *nodestrname;
	char *typestr;
	char *cmd_type;
	char port_buf[10];

	bstartcmd = (AGT_CMD_GTMCOORD_START_MASTER_BACKEND == cmdtype || AGT_CMD_GTMCOORD_START_SLAVE_BACKEND == cmdtype
								|| AGT_CMD_CN_START_BACKEND == cmdtype || AGT_CMD_DN_START_BACKEND == cmdtype);
	bstopcmd = (AGT_CMD_GTMCOORD_STOP_MASTER_BACKEND == cmdtype || AGT_CMD_GTMCOORD_STOP_SLAVE_BACKEND == cmdtype
								|| AGT_CMD_CN_STOP_BACKEND == cmdtype || AGT_CMD_DN_STOP_BACKEND == cmdtype);
	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		Assert(supplier);
		nodenamelist = supplier(fcinfo, nodetype);
		if(consumer)
			consumer(nodenamelist, nodetype);

		/* allocate memory for user context */
		lcp = (ListCell **) palloc(sizeof(ListCell *));
		*lcp = list_head(nodenamelist);
		funcctx->user_fctx = (void *) lcp;
		/*send the command to agent, running as backend*/
		mgr_cmd_run_backend(nodetype, cmdtype, nodenamelist, shutdown_mode, fcinfo);

		/*wait the max time to check the node status*/
		if (*lcp != NULL)
		{
			initStringInfo(&strhint);
			typestr = mgr_nodetype_str(nodetype);
			if (bstartcmd)
				cmd_type = "start";
			else
				cmd_type = "stop";
			appendStringInfo(&strhint, "waiting max %d seconds for %s to %s ...", iloop, typestr, cmd_type);
			ereport(LOG, (errmsg("%s", strhint.data)));
			ereport(NOTICE, (errmsg("%s\n", strhint.data)));
			fputs(_(strhint.data), stdout);
			fflush(stdout);
			pfree(strhint.data);
			pfree(typestr);

			rel_node = heap_open(NodeRelationId, AccessShareLock);
			while(iloop-- > 0)
			{
				fputs(_("."), stdout);
				fflush(stdout);
				foreach(lc, nodenamelist)
				{
					nodestrname = (char *) lfirst(lc);
					aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodestrname);
					if (!HeapTupleIsValid(aimtuple))
					{
						heap_close(rel_node, AccessShareLock);
						ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodestrname)));
					}
					mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
					Assert(mgr_node);
					binit = mgr_node->nodeinited;
					host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
					memset(port_buf, 0, sizeof(char)*10);
					sprintf(port_buf, "%d", mgr_node->nodeport);
					user = get_hostuser_from_hostoid(mgr_node->nodehost);
					ret = pingNode_user(host_addr, port_buf, user);
					if (bstartcmd)
					{
						if (PQPING_OK != ret && PQPING_REJECT != ret && AGENT_DOWN != ret && binit)
						{
							heap_freetuple(aimtuple);
							pfree(host_addr);
							pfree(user);
							pg_usleep(1 * 1000000L);
							break;
						}
						if (nodetype == CNDN_TYPE_GTM_COOR_MASTER &&
							ret == PQPING_OK &&
							mgr_node->nodeinited &&
							!mgr_node->nodeincluster)
						{
							initStringInfo(&alterNodeCmd);
							appendStringInfo(&alterNodeCmd,
											 " -h %s -p %u -d %s -U %s -a -c \"",
											 host_addr,
											 mgr_node->nodeport,
											 DEFAULT_DB,
											 user);
							appendStringInfo(&alterNodeCmd,
											 "ALTER NODE \\\"%s\\\" WITH (GTM=%d);",
											 NameStr(mgr_node->nodename),
											 true);
							appendStringInfoString(&alterNodeCmd, "\"");
							callAgentSendCmd(AGT_CMD_PSQL_CMD,
											 &alterNodeCmd,
											 host_addr,
											 get_agentPort_from_hostoid(mgr_node->nodehost));
							pfree(alterNodeCmd.data);
						}

					}
					else if (bstopcmd)
					{
						if (PQPING_NO_RESPONSE != ret && AGENT_DOWN != ret)
						{
							heap_freetuple(aimtuple);
							pfree(host_addr);
							pfree(user);
							pg_usleep(1 * 1000000L);
							break;
						}
					}
					heap_freetuple(aimtuple);
					pfree(host_addr);
					pfree(user);
				}

				if (NULL == lc)
					iloop = -1;
			}
			fputs(_("\n\n"), stdout);
			fflush(stdout);
			heap_close(rel_node, AccessShareLock);
		}
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	lcp = (ListCell **) funcctx->user_fctx;

	while (*lcp != NULL)
	{
		char	   *nodename = (char *) lfirst(*lcp);
		bresult = false;
		namestrcpy(&nodenamedata, nodename);
		*lcp = lnext(*lcp);
		rel_node = heap_open(NodeRelationId, AccessShareLock);
		aimtuple = mgr_get_tuple_node_from_name_type(rel_node, NameStr(nodenamedata));
		if (!HeapTupleIsValid(aimtuple))
		{
			heap_close(rel_node, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		binit = mgr_node->nodeinited;
		host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		memset(port_buf, 0, sizeof(char)*10);
		sprintf(port_buf, "%d", mgr_node->nodeport);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		heap_freetuple(aimtuple);
		/* check node running normal */
		ret = pingNode_user(host_addr, port_buf, user);

		pfree(host_addr);
		pfree(user);
		initStringInfo(&strdata);
		initStringInfo(&infosendmsg);
		heap_close(rel_node, AccessShareLock);

		if (bstartcmd)
		{
			if (!binit)
			{
				bresult = false;
				typestr = mgr_nodetype_str(nodetype);
				appendStringInfo(&strdata, "%s \"%s\" has not been initialized", typestr, nodename);
				pfree(typestr);
			}
			else if (PQPING_OK == ret ||  PQPING_REJECT ==  ret)
			{
				bresult = true;
				appendStringInfoString(&strdata, "success");
			}
			else
			{
				bresult = false;
				appendStringInfoString(&strdata, "failure");
			}
		}
		else if (bstopcmd)
		{
			if (PQPING_NO_RESPONSE == ret && binit)
			{
				bresult = true;
				appendStringInfoString(&strdata, "success");
			}
			else
			{
				bresult = false;
				appendStringInfoString(&strdata, "failure");
			}
		}
		else
		{
			pfree(strdata.data);
			pfree(infosendmsg.data);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
				errmsg("not support this command ID '%d'", cmdtype)));
		}

		if (bresult){
			ereportNoticeLog(errmsg("[SUCCESS] nodetype(%s) cmdtype(%s).", mgr_nodetype_str(nodetype), mgr_get_cmdname(cmdtype)));		
		}
		else{
			ereportNoticeLog(errmsg("[ERROR] nodetype(%s) cmdtype(%s).", mgr_nodetype_str(nodetype), mgr_get_cmdname(cmdtype)));
		}

		tup_result = build_common_command_tuple(&nodenamedata, bresult, strdata.data);
		pfree(strdata.data);
		pfree(infosendmsg.data);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}

	SRF_RETURN_DONE(funcctx);
}

List *nodenames_supplier_of_db(PG_FUNCTION_ARGS, char nodetype)
{
	Assert(nodetype > 0);
	return mgr_get_nodetype_namelist(nodetype);
}

List *nodenames_supplier_of_argidx_0(PG_FUNCTION_ARGS, char nodetype)
{
	return get_fcinfo_namelist("", 0, fcinfo);
}

List *nodenames_supplier_of_argidx_1(PG_FUNCTION_ARGS, char nodetype)
{
	return get_fcinfo_namelist("", 1, fcinfo);
}

List *nodenames_supplier_of_clean_node(PG_FUNCTION_ARGS, char nodetype)
{
	char *nodename;
	char *user;
	char *address;
	NameData namedata;
	List *nodenamelist = NIL;
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ListCell   *cell;
	Form_mgr_node mgr_node;
	ScanKeyData key[2];
	char port_buf[10];
	int ret;
	/*ndoe type*/
	nodetype = PG_GETARG_CHAR(0);
	nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	/*check the node not in the cluster*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	foreach(cell, nodenamelist)
	{
		nodename = (char *) lfirst(cell);
		namestrcpy(&namedata, nodename);
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));

		rel_scan = heap_beginscan_catalog(rel_node, 2, key);
		if ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) == NULL)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		if (mgr_node->nodeincluster)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" already exists in cluster, cannot be cleaned", mgr_nodetype_str(nodetype), nodename)));
		}
		/*check node stoped*/
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		sprintf(port_buf, "%d", mgr_node->nodeport);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		ret = pingNode_user(address, port_buf, user);
		pfree(address);
		pfree(user);
		if (ret == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" is running, cannot be cleaned, stop it first", mgr_nodetype_str(nodetype), nodename)));
		}
		mgr_node->allowcure = false;
		namestrcpy(&mgr_node->curestatus, CURE_STATUS_NORMAL);
		heap_inplace_update(rel_node, tuple);
		heap_endscan(rel_scan);
	}
	heap_close(rel_node, RowExclusiveLock);

	return nodenamelist;
}

void enable_doctor_consulting(List *nodenames, char nodetype)
{
	updateDoctorStatusOfMgrNodes(nodenames, nodetype, true, CURE_STATUS_NORMAL);
}

void disable_doctor_consulting(List *nodenames, char nodetype)
{
	updateDoctorStatusOfMgrNodes(nodenames, nodetype, false, CURE_STATUS_NORMAL);
}

char mgr_change_cmdtype_unbackend(char cmdtype)
{
	switch(cmdtype)
	{
		case	AGT_CMD_GTMCOORD_START_MASTER_BACKEND:
			return	AGT_CMD_GTMCOORD_START_MASTER;

		case	AGT_CMD_GTMCOORD_START_SLAVE_BACKEND:
			return	AGT_CMD_GTMCOORD_START_SLAVE;

		case	AGT_CMD_CN_START_BACKEND:
			return	AGT_CMD_CN_START;

		case	AGT_CMD_DN_START_BACKEND:
			return	AGT_CMD_DN_START;

		case	AGT_CMD_GTMCOORD_STOP_MASTER_BACKEND:
			return	AGT_CMD_GTMCOORD_STOP_MASTER;

		case 	AGT_CMD_GTMCOORD_STOP_SLAVE:
			return	AGT_CMD_GTMCOORD_STOP_SLAVE;

		case	AGT_CMD_GTMCOORD_STOP_SLAVE_BACKEND:
			return	AGT_CMD_GTMCOORD_STOP_SLAVE;

		case	AGT_CMD_CN_STOP_BACKEND:
			return AGT_CMD_CN_STOP;

		case AGT_CMD_DN_STOP_BACKEND:
			return AGT_CMD_DN_STOP;

		default:
			ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
				errmsg("not support this command ID '%d'", cmdtype)));
	}
}

HeapTuple build_common_command_tuple_four_col(const Name name, char type, bool status, const char *description)
{
    Datum datums[4];
    bool nulls[4];
    TupleDesc desc;
    NameData typestr;
    AssertArg(name && description);
    desc = get_common_command_tuple_desc_four_col();

    AssertArg(desc && desc->natts == 4
        && TupleDescAttr(desc, 0)->atttypid == NAMEOID
        && TupleDescAttr(desc, 1)->atttypid == NAMEOID
        && TupleDescAttr(desc, 2)->atttypid == BOOLOID
        && TupleDescAttr(desc, 3)->atttypid == TEXTOID);

    switch(type)
    {
        case CNDN_TYPE_GTM_COOR_MASTER:
                namestrcpy(&typestr, "gtm master");
                break;
        case CNDN_TYPE_GTM_COOR_SLAVE:
                namestrcpy(&typestr, "gtm slave");
                break;
        case CNDN_TYPE_COORDINATOR_MASTER:
                namestrcpy(&typestr, "coordinator master");
                break;
        case CNDN_TYPE_COORDINATOR_SLAVE:
                namestrcpy(&typestr, "coordinator slave");
                break;
        case CNDN_TYPE_DATANODE_MASTER:
                namestrcpy(&typestr, "datanode master");
                break;
        case CNDN_TYPE_DATANODE_SLAVE:
                namestrcpy(&typestr, "datanode slave");
                break;
        default:
                namestrcpy(&typestr, "unknown type");
                break;
    }

    datums[0] = NameGetDatum(name);
    datums[1] = NameGetDatum(&typestr);
    datums[2] = BoolGetDatum(status);
    datums[3] = CStringGetTextDatum(description);
    nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
    return heap_form_tuple(desc, datums, nulls);
}

static TupleDesc get_common_command_tuple_desc_four_col(void)
{
    if(common_command_tuple_desc_four_col == NULL)
    {
        MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
        TupleDesc volatile desc = NULL;
        PG_TRY();
        {
            desc = CreateTemplateTupleDesc(4, false);
            TupleDescInitEntry(desc, (AttrNumber) 1, "nodename",
                               NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 2, "nodetype",
                               NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 3, "status",
                               BOOLOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 4, "description",
                               TEXTOID, -1, 0);
            common_command_tuple_desc_four_col = BlessTupleDesc(desc);
        }PG_CATCH();
        {
            if(desc)
                FreeTupleDesc(desc);
            PG_RE_THROW();
        }PG_END_TRY();
        (void)MemoryContextSwitchTo(old_context);
    }
    Assert(common_command_tuple_desc_four_col);
    return common_command_tuple_desc_four_col;
}

/*
* get agent port according to given host oid
*
*/
int get_agentPort_from_hostoid(Oid hostOid)
{
	HeapTuple tuple;
	Form_mgr_host mgr_host;
	int agentPort;

	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	agentPort = mgr_host->hostagentport;
	ReleaseSysCache(tuple);

	return agentPort;
}

/*
* mgr_add_hbaconf_by_masteroid
* add one line infomation in pg_hba.conf, according whos master tuple oid is mastertupleoid
*/

void mgr_add_hbaconf_by_masteroid(Oid mastertupleoid, char *dbname, char *user, char *address)
{
	ScanKeyData key[3];
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	HeapTuple tuple;
	Datum datumPath;
	Relation relNode;
	HeapScanDesc relScan;
	bool isNull;
	Oid hostoid;
	char *nodepath;
	Form_mgr_node mgr_node;
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupleoid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 3, key);

	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
		if (isNull)
		{
			pfree(getAgentCmdRst.description.data);
			pfree(infosendmsg.data);
			heap_endscan(relScan);
			heap_close(relNode, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, dbname, user, address, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							TextDatumGetCString(datumPath),
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);
		hostoid = mgr_node->nodehost;
		nodepath = TextDatumGetCString(datumPath);
		/* reload it at last */
		mgr_reload_conf(hostoid, nodepath);

	}

	pfree(getAgentCmdRst.description.data);
	pfree(infosendmsg.data);
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);
}

/*
* get_nodepath_from_tupleoid
*  get nodepath from tuple
*
*/

char *get_nodepath_from_tupleoid(Oid tupleOid)
{
	Relation relNode;
	HeapScanDesc relScan;
	HeapTuple tuple;
	char *nodepath = NULL;
	bool isNull = false;
	Datum datumPath;
	Form_mgr_node mgr_node;

	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 0, NULL);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		if (tupleOid != HeapTupleGetOid(tuple))
			continue;
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
		if(isNull)
		{
			heap_endscan(relScan);
			heap_close(relNode, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		nodepath = pstrdup(TextDatumGetCString(datumPath));
		break;
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	if (!nodepath)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("can not get node path tuple oid %d", tupleOid)));

	return nodepath;
}

/*
* mgr_get_normal_slave_node
*  get slave node for given sync state, which is running normal
*/

int mgr_get_normal_slave_node(Relation relNode, Oid masterTupleOid, int sync_state_sync, Oid excludeOid, Name slaveNodeName)
{
	ScanKeyData key[5];
	HeapTuple tuple;
	HeapScanDesc relScan;
	int res = PQPING_NO_RESPONSE;
	Form_mgr_node mgr_node;
	NameData sync_state_name;
	XLogRecPtr	lastWalReceiveLsnMin = InvalidXLogRecPtr;
	XLogRecPtr	lastWalReceiveLsn = InvalidXLogRecPtr;
	char *address;
	char *user;
	char portBuf[10];
	char *sqlString = "SELECT pg_catalog.pg_last_wal_receive_lsn()";
	bool bgtmtype = false;

	namestrcpy(&sync_state_name, sync_state_tab[sync_state_sync].name);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(masterTupleOid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[3]
		,Anum_mgr_node_nodesync
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&sync_state_name));
	ScanKeyInit(&key[4]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	relScan = heap_beginscan_catalog(relNode, 5, key);

	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (excludeOid == HeapTupleGetOid(tuple))
			continue;
		/* test the node running status */
		memset(portBuf, 0, 10);
		sprintf(portBuf, "%d", mgr_node->nodeport);
		address= get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostname_from_hostoid(mgr_node->nodehost);
		res = pingNode_user(address, portBuf, user);

		pfree(address);
		pfree(user);

		if (res == PQPING_OK)
		{
			if (sync_state_sync != SYNC_STATE_SYNC)
			{
				lastWalReceiveLsn = mgr_get_last_wal_receive_location(mgr_node->nodehost, sqlString, mgr_node->nodeport, DEFAULT_DB, bgtmtype);;
				ereport(NOTICE, (errmsg("slave \"%s\" last receive lsn: %x/%x", NameStr(mgr_node->nodename), format_lsn(lastWalReceiveLsn))));
				if (lastWalReceiveLsnMin == InvalidXLogRecPtr)
					namestrcpy(slaveNodeName, NameStr(mgr_node->nodename));
				if (lastWalReceiveLsn > lastWalReceiveLsnMin)
				{
					lastWalReceiveLsnMin = lastWalReceiveLsn;
					namestrcpy(slaveNodeName, NameStr(mgr_node->nodename));
				}
			}
			else
			{
				namestrcpy(slaveNodeName, NameStr(mgr_node->nodename));
				break;
			}
		}

	}

	heap_endscan(relScan);

	return res;
}


/*
* mgr_get_slave_node
*  get slave node for given sync state, no matter it is running normal or not
*/

bool mgr_get_slave_node(Relation relNode, Oid masterTupleOid, int SYNC_STATE_SYNC, Oid excludeOid, Name slaveNodeName)
{
	ScanKeyData key[5];
	HeapTuple tuple;
	HeapScanDesc relScan;
	bool bget = false;
	Form_mgr_node mgr_node;
	NameData sync_state_name;

	namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(masterTupleOid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[3]
		,Anum_mgr_node_nodesync
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&sync_state_name));
	ScanKeyInit(&key[4]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 5, key);

	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (excludeOid == HeapTupleGetOid(tuple))
			continue;
		bget = true;
		namestrcpy(slaveNodeName, NameStr(mgr_node->nodename));
		break;
	}

	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	return bget;
}

/*
* mgr_get_mastername_by_nodename_type
*  get given slave node's master name
*/
char *mgr_get_mastername_by_nodename_type(char* nodename, char nodetype)
{
	ScanKeyData key[2];
	HeapTuple tuple;
	HeapTuple masterTuple;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodem;
	NameData nodenameData;
	Relation relNode;
	HeapScanDesc relScan;
	char *masterName = NULL;
	char mastertype;
	char checkmtype;

	namestrcpy(&nodenameData, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenameData));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		masterTuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		/*check the host exists*/
		if (!HeapTupleIsValid(masterTuple))
		{
			heap_endscan(relScan);
			heap_close(relNode, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("cache lookup failed for the master of \"%s\" relation %u in node table", nodename, mgr_node->nodemasternameoid)));
		}
		mgr_nodem = (Form_mgr_node)GETSTRUCT(masterTuple);
		Assert(mgr_nodem);
		mastertype = mgr_nodem->nodetype;
		masterName = pstrdup(NameStr(mgr_nodem->nodename));
		ReleaseSysCache(masterTuple);
		break;
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	if (masterName == NULL)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("the master of node \"%s\" does not exist", nodename)));
	checkmtype = mgr_get_master_type(nodetype);
	if (mastertype != checkmtype)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("the type of node \"%s\" is not %s", masterName, mgr_nodetype_str(checkmtype))));

	return masterName;
}

/*
* mgr_get_agtm_name
*  get agtm master nodename
*
*/

char *mgr_get_agtm_name(void)
{
	ScanKeyData key[2];
	Relation relNode;
	HeapScanDesc relScan;
	char *nodename = NULL;
	HeapTuple tuple;
	Form_mgr_node mgr_node;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_GTM_COOR_MASTER));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodename = pstrdup(NameStr(mgr_node->nodename));
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	if (!nodename)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("gtm master does not exist in node table")));

	return nodename;
}


/*
* check the slave node streaming replication status
*/

bool mgr_check_slave_replicate_status(const Oid masterTupleOid, const char nodetype, const char *slaveName)
{
	HeapTuple hostTuple;
	HeapTuple masterTuple;
	Form_mgr_node mgr_mnode;
	Form_mgr_host mgr_mhost;
	Relation nodeRel;
	Datum addrDatum;
	Oid masterHostOid;
	int32 masterPort;
	int32 masterAgentPort;
	bool isNull = true;
	bool res = false;
	NameData masterAddr;
	NameData masterUser;
	StringInfoData sqlstrdata;
	StringInfoData resultstrdata;

	masterTuple = SearchSysCache1(NODENODEOID, masterTupleOid);
	if(!HeapTupleIsValid(masterTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("oid \"%u\" of datanode master does not exist", masterTupleOid)));
	}
	mgr_mnode = (Form_mgr_node)GETSTRUCT(masterTuple);
	Assert(mgr_mnode);
	masterHostOid = mgr_mnode->nodehost;
	masterPort = mgr_mnode->nodeport;
	ReleaseSysCache(masterTuple);


	hostTuple = SearchSysCache1(HOSTHOSTOID, masterHostOid);
	if(!(HeapTupleIsValid(hostTuple)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", masterHostOid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_mhost = (Form_mgr_host)GETSTRUCT(hostTuple);
	Assert(mgr_mhost);
	nodeRel = heap_open(HostRelationId, AccessShareLock);
	addrDatum = heap_getattr(hostTuple, Anum_mgr_host_hostaddr, RelationGetDescr(nodeRel), &isNull);
	heap_close(nodeRel, AccessShareLock);
	if (isNull)
	{
		ReleaseSysCache(hostTuple);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nohost")
			, errmsg("column address is null")));
	}
	namestrcpy(&masterAddr, TextDatumGetCString(addrDatum));
	masterAgentPort = mgr_mhost->hostagentport;
	namestrcpy(&masterUser, NameStr(mgr_mhost->hostuser));
	ReleaseSysCache(hostTuple);

	initStringInfo(&sqlstrdata);
	initStringInfo(&resultstrdata);
	appendStringInfo(&sqlstrdata, "select count(*) from pg_stat_replication where application_name = '%s'", slaveName);
	PG_TRY();
	{
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES
								,masterAgentPort
								,sqlstrdata.data
								,NameStr(masterUser)
								,NameStr(masterAddr)
								,masterPort
								,DEFAULT_DB
								,&resultstrdata);
		if (resultstrdata.len == 0 || strcmp(resultstrdata.data, "1") != 0)
			res = false;
		else
			res = true;
	}
	PG_CATCH();
	{
		pfree(sqlstrdata.data);
		pfree(resultstrdata.data);
		PG_RE_THROW();
	}PG_END_TRY();

	pfree(sqlstrdata.data);
	pfree(resultstrdata.data);

	return res;
}

/*
* get the slave node lsn
*/

static XLogRecPtr mgr_get_last_wal_receive_location(const Oid hostOid, char *sqlString, const int32 nodePort, const char *database, const bool bgtmtype)
{
	Relation hostRel;
	HeapTuple hostTuple;
	Datum host_addr;
	Form_mgr_host mgr_host;
	XLogRecPtr	ptr = InvalidXLogRecPtr;
	StringInfoData restmsg;
	bool isNull = false;

	hostRel = heap_open(HostRelationId, AccessShareLock);
	hostTuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(hostTuple))
	{
		 heap_close(hostRel, AccessShareLock);
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
		return ptr;
	}
	mgr_host = (Form_mgr_host)GETSTRUCT(hostTuple);
	host_addr = heap_getattr(hostTuple, Anum_mgr_host_hostaddr, RelationGetDescr(hostRel), &isNull);
	if(isNull)
	{
		ReleaseSysCache(hostTuple);
		heap_close(hostRel, AccessShareLock);
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
		return ptr;
	}

	initStringInfo(&restmsg);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlString
				,NameStr(mgr_host->hostuser), TextDatumGetCString(host_addr), nodePort, DEFAULT_DB, &restmsg);
	if (restmsg.len != 0)
		ptr = parse_lsn(restmsg.data);
	pfree(restmsg.data);
	ReleaseSysCache(hostTuple);
	heap_close(hostRel, AccessShareLock);

	return ptr;
}


static XLogRecPtr parse_lsn(const char *str)
{
	XLogRecPtr	ptr = InvalidXLogRecPtr;
	uint32		high,
				low;

	if (sscanf(str, "%x/%x", &high, &low) == 2)
		ptr = (((XLogRecPtr) high) << 32) + (XLogRecPtr) low;

	return ptr;
}

/*
* get one sync slave name by given master tuple oid
*/
HeapTuple mgr_get_sync_slavenode_tuple(Oid mastertupleoid, bool bincluster, Oid includeOid, Oid excludeoid, int seqNum)
{
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	HeapTuple tupleRet = NULL;
	HeapScanDesc relScan;
	ScanKeyData key[1];
	Relation relNode;
	List *nodeList = NIL;
	ListCell *nodeCeil;
	char *nodeName = NULL;
	int i = 0;

	relNode = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupleoid));
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (HeapTupleGetOid(tuple) == excludeoid)
			continue;
		else if (HeapTupleGetOid(tuple) == includeOid)
			nodeList = lappend(nodeList, pstrdup(NameStr(mgr_node->nodename)));
		else if (strcmp(NameStr(mgr_node->nodesync), sync_state_tab[SYNC_STATE_SYNC].name) == 0
				&& mgr_node->nodeincluster == bincluster)
			nodeList = lappend(nodeList, pstrdup(NameStr(mgr_node->nodename)));
	}

	/* get the node */
	foreach(nodeCeil, nodeList)
	{
		i++;
		nodeName = (char *)lfirst(nodeCeil);
		if (i == seqNum % nodeList->length)
			break;
	}

	heap_endscan(relScan);
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (HeapTupleGetOid(tuple) == excludeoid)
			continue;
		if (nodeName != NULL && strcmp(nodeName, NameStr(mgr_node->nodename)) == 0)
		{
			tupleRet = heap_copytuple(tuple);
			break;
		}
	}
	list_free(nodeList);
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	return tupleRet;
}

/*
* get the "create node " command info for given nodename
*/
bool mgr_get_createnodeCmd_on_readonly_cn(char *nodeName, bool bincluster, StringInfo cmdstring)
{
	NameData sync_state_name;
	NameData preferredDnName;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_syncnode;
	HeapTuple tuple;
	HeapTuple syncNodeTuple;
	HeapScanDesc relScan;
	ScanKeyData key[2];
	Relation relNode;
	List *dnList = NIL;
	char *address;
	bool bgetNode = false;
	int seqNum = 0;

	Assert(nodeName);
	seqNum = -1;
	relNode = heap_open(NodeRelationId, AccessShareLock);
	namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);

	/* for coordinator */
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(bincluster));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if (strcmp(nodeName, NameStr(mgr_node->nodename)) == 0)
		{
			bgetNode = true;
			appendStringInfo(cmdstring, "ALTER NODE \\\"%s\\\" WITH (HOST='%s', PORT=%d);"
							,NameStr(mgr_node->nodename)
							,address
							,mgr_node->nodeport);
		}
		else
		{
			appendStringInfo(cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='coordinator' \
											, HOST='%s', PORT=%d);"
							,NameStr(mgr_node->nodename)
							,address
							,mgr_node->nodeport);
		}
		pfree(address);
	}
	heap_endscan(relScan);

	/* for datanode , if the datanode master has sync slave node, add the sync slave node info,
	*  else, add the datanode master info
	*/
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(bincluster));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		/* check the master has sync slave node */
		syncNodeTuple = mgr_get_sync_slavenode_tuple(HeapTupleGetOid(tuple), bincluster, InvalidOid, InvalidOid, seqNum);
		if (HeapTupleIsValid(syncNodeTuple))
		{
			/* get the sync slave ndoe info */
			mgr_syncnode = (Form_mgr_node)GETSTRUCT(syncNodeTuple);
			Assert(mgr_syncnode);
			dnList = lappend(dnList, pstrdup(NameStr(mgr_syncnode->nodename)));
			address = get_hostaddress_from_hostoid(mgr_syncnode->nodehost);
			appendStringInfo(cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='datanode' \
											, HOST='%s', PORT=%d, preferred = %s);"
							,NameStr(mgr_syncnode->nodename)
							,address
							,mgr_syncnode->nodeport
							,"false");
			pfree(address);
			heap_freetuple(syncNodeTuple);
		}
		else
		{
			/* for datanode master info */
			dnList = lappend(dnList, pstrdup(NameStr(mgr_node->nodename)));
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			appendStringInfo(cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='datanode' \
											, HOST='%s', PORT=%d, preferred = %s);"
							,NameStr(mgr_node->nodename)
							,address
							,mgr_node->nodeport
							,"false");
			pfree(address);
		}
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	/* find the preferred datanode */
	mgr_get_prefer_nodename_for_cn(nodeName, dnList, &preferredDnName);
	list_free(dnList);
	appendStringInfo(cmdstring, " ALTER NODE \\\"%s\\\" WITH (preferred = %s);"
							,NameStr(preferredDnName)
							,"true");
	return bgetNode;
}

/*
* for read only coordinator, used by given the datanode list to get the preferred datanode name
*
*/
void mgr_get_prefer_nodename_for_cn(char *cnName, List *dnNamelist, Name preferredDnName)
{
	List *cnPreferredList = NIL;
	List *cnRestList = NIL;
	List *dnList = NIL;
	List *dnPreferredList = NIL;
	List *dnRestList = NIL;
	ListCell *cnCeil, *dnCeil;
	ScanKeyData key[1];
	Form_mgr_node mgr_node;
	Relation relNode;
	HeapScanDesc relScan;
	HeapTuple tuple;
	char *nodename;
	bool bget = false;
	int nPosition = -1;
	int i = -1;

	Assert(cnName);
	Assert(dnNamelist);
	namestrcpy(preferredDnName, "");
	relNode = heap_open(NodeRelationId, AccessShareLock);
	/* check the node exist in mgr_node */
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (strcmp(cnName, NameStr(mgr_node->nodename)) == 0)
		{
			bget = true;
			break;
		}
	}
	heap_endscan(relScan);
	if (!bget)
	{
		heap_close(relNode, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("can not find the coordinator \"%s\" in mgr_node table", cnName)));
	}

	/* adjust the order, the dn slave node in the front of list */
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		foreach(dnCeil, dnNamelist)
		{
			nodename = (char *)lfirst(dnCeil);
			if (strcmp(nodename, NameStr(mgr_node->nodename)) == 0)
			{
				dnList = lappend(dnList, pstrdup(nodename));
				break;
			}
		}
	}
	heap_endscan(relScan);

	/* adjust the order, the dn slave node in the end of list */
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		foreach(dnCeil, dnNamelist)
		{
			nodename = (char *)lfirst(dnCeil);
			if (strcmp(nodename, NameStr(mgr_node->nodename)) == 0)
			{
				dnList = lappend(dnList, pstrdup(nodename));
				break;
			}
		}
	}
	heap_endscan(relScan);

	Assert(dnList->length == dnNamelist->length);

	/* get read only cn nodename list */
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		bget = false;
		foreach(dnCeil, dnList)
		{
			nodename = (char *)lfirst(dnCeil);
			if (mgr_check_list_in(dnPreferredList, nodename))
				continue;
			if (HeapTupleGetOid(tuple) == mgr_get_tupoid_from_nodename(relNode, nodename))
			{
				cnPreferredList = lappend(cnPreferredList, pstrdup(NameStr(mgr_node->nodename)));
				dnPreferredList = lappend(dnPreferredList, pstrdup(nodename));
				bget = true;
				break;
			}
		}

		if (!bget)
		{
			cnRestList = lappend(cnRestList, pstrdup(NameStr(mgr_node->nodename)));
		}
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	/* record the rest dn in dnRestList */
	foreach(dnCeil, dnList)
	{
		nodename = (char *)lfirst(dnCeil);
		if (mgr_check_list_in(dnPreferredList, nodename))
			continue;
		dnRestList = lappend(dnRestList, pstrdup(nodename));
	}
	list_free(dnList);
	dnList = NIL;

	foreach(cnCeil, cnRestList)
	{
		nodename = (char *)lfirst(cnCeil);
		cnPreferredList = lappend(cnPreferredList, pstrdup(nodename));
	}
	list_free(cnRestList);
	cnRestList = NIL;

	foreach(dnCeil, dnRestList)
	{
		nodename = (char *)lfirst(dnCeil);
		dnPreferredList = lappend(dnPreferredList, pstrdup(nodename));
	}
	list_free(dnRestList);
	dnRestList = NIL;

	/* get the position of cnName */
	nPosition  = -1;
	bget = false;
	foreach(cnCeil, cnPreferredList)
	{
		nodename = (char *)lfirst(cnCeil);
		nPosition++;
		if(strcmp(nodename, cnName) == 0)
		{
			bget = true;
			break;
		}
	}
	list_free(cnPreferredList);
	cnPreferredList = NIL;

	if (!bget)
	{
		list_free(dnPreferredList);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("can not find the coordinator \"%s\" in mgr_node table", cnName)));
	}

	i = -1;
	bget = false;
	foreach(dnCeil, dnPreferredList)
	{
		i++;
		nodename = (char *)lfirst(dnCeil);
		if (i == nPosition%dnPreferredList->length)
		{
			namestrcpy(preferredDnName, nodename);
			bget = true;
			break;
		}
	}
	list_free(dnPreferredList);
	dnPreferredList = NIL;

	if (!bget)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("can not find the preferred datanode in pgxc_node of coordinator \"%s\"", cnName)));
}


/*
* find the tuple oid of given node name in mgr_node table
*/
Oid mgr_get_tupoid_from_nodename(Relation relNode, char *nodename)
{
	Oid tupOid = InvalidOid;
	Form_mgr_node mgr_node;
	HeapScanDesc relScan;
	HeapTuple tuple;

	Assert(nodename);
	relScan = heap_beginscan_catalog(relNode, 0, NULL);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (strcmp(nodename, NameStr(mgr_node->nodename)) == 0)
		{
			tupOid = HeapTupleGetOid(tuple);
			break;
		}
	}

	heap_endscan(relScan);

	return tupOid;
}

/*
* check the given node name in the list
*/
bool mgr_check_list_in(List *list, char *checkName)
{
	ListCell *Ceil;
	char *name;

	Assert(checkName);
	foreach(Ceil, list)
	{
		name = (char *)lfirst(Ceil);
		if (strcmp(name, checkName) == 0)
			return true;
	}

	return false;
}

bool mgr_try_max_times_get_stringvalues(char cmdtype, int agentPort, char *sqlStr, char *userName
				, char *nodeAddress, int nodePort, char *dbname, StringInfo restmsg, int max, char *checkResultStr)
{
	bool bres = false;
	int k = 0;

	Assert(sqlStr);
	Assert(userName);
	Assert(nodeAddress);
	Assert(dbname);
	Assert(checkResultStr);

	while (k++ < max)
	{
		resetStringInfo(restmsg);
		monitor_get_stringvalues(cmdtype, agentPort, sqlStr
			,userName, nodeAddress, nodePort, dbname, restmsg);
		/* check result */

		if (cmdtype == AGT_CMD_GET_SQL_STRINGVALUES)
		{
			if (restmsg->len > 0 && restmsg->data[0] != '\0')
			{
				if (strcmp(restmsg->data, "t") == 0)
				{
					bres = true;
					break;
				}
			}
		}
		else
		{
			if (restmsg->len > 0 && restmsg->data[0] != '\0')
			{
				if (strcmp(restmsg->data, checkResultStr) == 0)
				{
					bres = true;
					break;
				}
			}
		}
	}

	return bres;
}

/*
* get the dn name list from given strinfo str
*
*/
bool mgr_get_dnlist(Name oldPreferredNode, char *separateStr, StringInfo restmsg, List **dnList)
{
	char *value = NULL;
	int len;
	int position = 0;

	Assert(separateStr);

	namestrcpy(oldPreferredNode, "");
	if ((restmsg->len >0 && restmsg->data[0] == '\0') || restmsg->len == 0)
	{
		return false;
	}
	else
	{
		len = restmsg->len;
		value = &(restmsg->data[0]);
		if (strcmp(value, separateStr) != 0)
			namestrcpy(oldPreferredNode, value);
		while(1)
		{
			if(position >= len)
				break;
			value = &(restmsg->data[position]);
			if (*value)
				position = position + strlen(value);
			position = position + 1;
			if (*value && strcmp(value, "*") == 0)
				break;
		}

		/* get datanode name list */
		while(1)
		{
			if(position >= len)
				break;
			value = &(restmsg->data[position]);
			if (*value)
			{
				*dnList = lappend(*dnList, pstrdup(value));
				position = position + strlen(value);
			}
			position = position + 1;
		}
	}

	return true;
}

/*
* alter the perferred node on pgxc_node table
*
*/
void mgr_set_preferred_node(char *oldPreferredNode, char * preferredDnName, char *coordname
							, char *userName, char *nodeAddress, int agentPort, int nodePort)
{
	Relation relNode;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	int port;
	char *address;
	StringInfoData sqlstrmsg;
	StringInfoData restmsg;
	bool bres = false;

	Assert(oldPreferredNode);
	Assert(preferredDnName);
	Assert(userName);
	Assert(nodeAddress);
	Assert(coordname);

	initStringInfo(&sqlstrmsg);
	initStringInfo(&restmsg);
	/* set preferred node on coordinator */
	if (strcmp(oldPreferredNode, preferredDnName) != 0)
	{
		relNode = heap_open(NodeRelationId, AccessShareLock);
		appendStringInfo(&sqlstrmsg, "set force_parallel_mode = off;");
		if (strcmp(oldPreferredNode, "") != 0)
		{
			tuple = mgr_get_tuple_node_from_name_type(relNode, oldPreferredNode);
			if (!HeapTupleIsValid(tuple))
				ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("datanode \"%s\" does not exist in mgr_node table", oldPreferredNode)));
			else
			{
				mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
				Assert(mgr_node);
				port = mgr_node->nodeport;
				address = get_hostaddress_from_hostoid(mgr_node->nodehost);
				appendStringInfo(&sqlstrmsg, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=%s) on (\"%s\");"
					,oldPreferredNode
					,oldPreferredNode
					,address
					,port
					,"false"
					,coordname);
				pfree(address);
				heap_freetuple(tuple);
			}
		}
		tuple = mgr_get_tuple_node_from_name_type(relNode, preferredDnName);
		if (!HeapTupleIsValid(tuple))
			ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("datanode \"%s\" does not exist in mgr_node table", preferredDnName)));
		else
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			port = mgr_node->nodeport;
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			appendStringInfo(&sqlstrmsg, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=%s) on (\"%s\");"
				,preferredDnName
				,preferredDnName
				,address
				,port
				,"true"
				,coordname);
			pfree(address);
			heap_freetuple(tuple);
		}
		heap_close(relNode, AccessShareLock);
		ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", coordname, sqlstrmsg.data)));
		resetStringInfo(&restmsg);
		bres = mgr_try_max_times_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES_COMMAND, agentPort
			, sqlstrmsg.data, userName, nodeAddress, nodePort, DEFAULT_DB, &restmsg, 3, "ALTER NODE");
		if (!bres)
			ereport(WARNING, (errmsg("on coordinator \"%s\" execute \"%s\" fail, you need to check it"
				, coordname, sqlstrmsg.data)));
	}

	pfree(sqlstrmsg.data);
	pfree(restmsg.data);
}

/*
* get the datanode name list in dnList, if the datanode is master type and it has sync slave node,
* use the function "alter node" to update the tuple information.
*/
List *mgr_append_coord_update_pgxcnode(StringInfo sqlstrmsg, List *dnList, Name oldPreferredNode, int nodeSeqNum, char *execNodeName)
{
	ListCell *dnCeil;
	Relation relNode;
	HeapScanDesc relScan;
	ScanKeyData key[1];
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_syncNode;
	HeapTuple tuple;
	HeapTuple syncNodeTuple;
	List *newDnList = NIL;
	NameData nameData;
	char *nodeName;
	char *address;
	int port;

	Assert(dnList);
	appendStringInfo(sqlstrmsg, "set force_parallel_mode = off;");
	relNode = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));

	foreach(dnCeil, dnList)
	{
		nodeName = (char *)lfirst(dnCeil);
		namestrcpy(&nameData, nodeName);
		/* check the node's type */
		relScan = heap_beginscan_catalog(relNode, 1, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (strcmp(nodeName, NameStr(mgr_node->nodename)) != 0)
				continue;
			if (mgr_node->nodetype == CNDN_TYPE_DATANODE_MASTER)
			{
				/* check the node has the sync slave node */
				syncNodeTuple = mgr_get_sync_slavenode_tuple(HeapTupleGetOid(tuple)
					, true, InvalidOid, InvalidOid, nodeSeqNum);
				if (HeapTupleIsValid(syncNodeTuple))
				{
					mgr_syncNode = (Form_mgr_node)GETSTRUCT(syncNodeTuple);
					Assert(mgr_syncNode);
					if(strcmp(oldPreferredNode->data, NameStr(mgr_node->nodename)) == 0)
						namestrcpy(oldPreferredNode, NameStr(mgr_syncNode->nodename));
					namestrcpy(&nameData, NameStr(mgr_syncNode->nodename));
					address = get_hostaddress_from_hostoid(mgr_syncNode->nodehost);
					port = mgr_syncNode->nodeport;
					appendStringInfo(sqlstrmsg, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=%s) on (\"%s\");"
						, nodeName, NameStr(mgr_syncNode->nodename), address
						, port
						, (strcmp(oldPreferredNode->data, NameStr(mgr_node->nodename)) == 0) 
							? "true":"false"
						, execNodeName);
					pfree(address);
					heap_freetuple(syncNodeTuple);
				}
			}
			break;
		}
		newDnList = lappend(newDnList, pstrdup(nameData.data));
		heap_endscan(relScan);
	}

	appendStringInfo(sqlstrmsg, "select pgxc_pool_reload();");
	heap_close(relNode, AccessShareLock);

	return newDnList;
}


/*
* get the the master tuple oid, if the node is gtm or datanode slave , the mgr_node->nodemasternameoid
* is its master tuple oid.
*/
Oid mgr_get_nodeMaster_tupleOid(char *nodeName)
{
	Relation relNode;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Oid masterTupleOid;

	Assert(nodeName);
	relNode = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(relNode, nodeName);
	if (!HeapTupleIsValid(tuple))
	{
		masterTupleOid = InvalidOid;
	}
	else
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_node->nodetype == CNDN_TYPE_DATANODE_MASTER)
			masterTupleOid = HeapTupleGetOid(tuple);
		else if (mgr_node->nodetype == CNDN_TYPE_DATANODE_SLAVE)
			masterTupleOid = mgr_node->nodemasternameoid;
		else
			masterTupleOid = InvalidOid;
		heap_freetuple(tuple);
	}
	heap_close(relNode, AccessShareLock);

	return masterTupleOid;
}

/*
* get the number of node in node table, which meets the conditions: nodeytpe
* , incluster, readonly
*
*/
int mgr_get_nodetype_num(const char nodeType, const bool inCluster, const bool readOnly)
{
	Relation relNode;
	HeapScanDesc relScan;
	HeapTuple tuple;
	ScanKeyData key[2];
	int num = 0;

	relNode = heap_open(NodeRelationId, AccessShareLock);
	/* traversal the slave node */
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(inCluster));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodeType));
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		num++;
	}

	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	return num;
}

/*
* when alter node port, if the node info on readonly coordinator, it will need update the node info
* on pgxc_node.
*/
bool mgr_modify_readonly_coord_pgxc_node(Relation rel_node, StringInfo infostrdata, char *nodename, int newport)
{
	StringInfoData infosendmsg;
	StringInfoData buf;
	StringInfoData restmsg;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	ScanKeyData key[2];
	char *user;
	char *address;
	bool execRes = false;
	bool bnormal= true;
	HeapScanDesc relScan;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	int agentPort;

	Assert(nodename);
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&restmsg);

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	relScan = heap_beginscan_catalog(rel_node, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		agentPort = get_agentPort_from_hostoid(mgr_node->nodehost);
		resetStringInfo(&infosendmsg);
		resetStringInfo(&restmsg);

		appendStringInfo(&infosendmsg, "select count(*) from pgxc_node where node_name = '%s'", nodename);
		/* check the node info on the pgxc_node of coordinator */
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPort, infosendmsg.data
			,user, address
			,strcmp(nodename, NameStr(mgr_node->nodename)) == 0 ? newport : mgr_node->nodeport
			,DEFAULT_DB, &restmsg);
		pfree(address);
		if (restmsg.len != 0 && strcmp(restmsg.data, "0") == 0)
		{
			/* do nothing */
			pfree(user);
		}else if (restmsg.len != 0 && strcmp(restmsg.data, "1") ==0)
		{
			/* update the pgxc_node */
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, " -h %s -p %u -d %s -U %s -a -c \""
				,"127.0.0.1"
				,strcmp(nodename, NameStr(mgr_node->nodename)) == 0 ? newport : mgr_node->nodeport
				,DEFAULT_DB
				,user);
			appendStringInfo(&infosendmsg, "%s", infostrdata->data);
			appendStringInfo(&infosendmsg, " set FORCE_PARALLEL_MODE = off; select pgxc_pool_reload();\"");
			pfree(user);
			/* connection agent */
			ma = ma_connect_hostoid(mgr_node->nodehost);
			if (!ma_isconnected(ma))
			{
				/* report error message */
				bnormal = false;
				ereport(WARNING, (errmsg("%s", ma_last_error_msg(ma))));
				ma_close(ma);
				continue;
			}
			ma_beginmessage(&buf, AGT_MSG_COMMAND);
			ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
			ma_sendstring(&buf,infosendmsg.data);
			ma_endmessage(&buf, ma);
			if (! ma_flush(ma, true))
			{
				bnormal = false;
				ereport(WARNING, (errmsg("%s", ma_last_error_msg(ma))));
				ma_close(ma);
				continue;
			}
			resetStringInfo(&getAgentCmdRst.description);
			execRes = mgr_recv_msg(ma, &getAgentCmdRst);
			ma_close(ma);
			if (!execRes)
			{
				bnormal = false;
				ereport(WARNING, (errmsg("refresh the node \"%s\" information in pgxc_node \
					of corodinator \"%s\" fail, you should check its pgxc_node, sql string is %s,\
					the error message: %s",
					nodename, NameStr(mgr_node->nodename), infosendmsg.data, getAgentCmdRst.description.data)));
			}
		}
		else
		{
			bnormal = false;
			ereport(WARNING, (errmsg("connect corodinator \"%s\" fail, you should check its pgxc_node, sql string is %s",
				NameStr(mgr_node->nodename), infosendmsg.data)));
		}
	}
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	heap_endscan(relScan);

	return bnormal;
}


void check_node_incluster(void)
{
	Relation relNode;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	HeapScanDesc scan;
	ScanKeyData key[1];
	bool gtmInCluster = false;
	char *nodetypeStr;
	Snapshot snapshot;

	/*check gtm master is init*/
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_GTM_COOR_MASTER));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(relNode,snapshot,1, key);
	while((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		gtmInCluster = mgr_node->nodeincluster;
	}

	if (!gtmInCluster)
	{
		heap_endscan(scan);
		UnregisterSnapshot(snapshot);
		heap_close(relNode, AccessShareLock);
		return;
	}

	/* check node in mgr_node table */
	heap_endscan(scan);
	scan = heap_beginscan(relNode,snapshot,0, NULL);
	while((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (!mgr_node->nodeincluster)
		{
			nodetypeStr = mgr_nodetype_str(mgr_node->nodetype);
			ereport(WARNING, (errmsg("%s %s does not in the cluster"
						, nodetypeStr, NameStr(mgr_node->nodename))));
			pfree(nodetypeStr);
		}

	}

	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(relNode, AccessShareLock);
}

/*
* mgr_check_nodename_repeate
*
* given the node name, check the node table has the repeate node name
*/
bool mgr_check_nodename_repeate(Relation rel, char *nodename)
{
	ScanKeyData key[1];
	HeapScanDesc relScan;
	HeapTuple tuple =NULL;
	NameData nameattrdata;
	bool bres = false;

	Assert(nodename);
	namestrcpy(&nameattrdata, nodename);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&nameattrdata));
	relScan = heap_beginscan_catalog(rel, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		bres = true;
		break;
	}
	heap_endscan(relScan);

	return bres;
}

/* get the nodetype of given nodename */
char mgr_get_nodetype(Name nodename)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char nodetype = CNDN_TYPE_NONE;

	Assert(nodename && nodename->data);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodename));

	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodetype = mgr_node->nodetype;
	}

	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);

	return nodetype;
}

int mgr_get_monitor_node_result(char nodetype, Oid hostOid, int nodeport
	, StringInfo strinfo, StringInfo starttime, Name recoveryStrInfo)
{
	int ret = PQPING_REJECT;
	int agentport = 0;
	bool is_valid = false;
	char nodeport_buf[10];
	char *hostAddr;
	char *user;
	StringInfoData resultstrdata;
	StringInfoData conninfo;
	PGconn *conn = NULL;

	sprintf(nodeport_buf, "%d", nodeport);

	resetStringInfo(strinfo);
	resetStringInfo(starttime);
	namestrcpy(recoveryStrInfo, enum_recovery_status_tab[RECOVERY_UNKNOWN].name);
	hostAddr = get_hostaddress_from_hostoid(hostOid);
	user = get_hostuser_from_hostoid(hostOid);
	initStringInfo(&resultstrdata);
	is_valid = is_valid_ip(hostAddr);

	if (is_valid)
	{
		ret = pingNode_user(hostAddr, nodeport_buf, user);
		switch (ret)
		{
			case PQPING_OK:
				appendStringInfoString(strinfo, "running");
				agentport = get_agentPort_from_hostoid(hostOid);
				monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, SQL_PG_IS_IN_RECOVERY
					,user, hostAddr, nodeport, DEFAULT_DB, &resultstrdata);
				if (resultstrdata.len != 0)
				{
					if (strcmp(resultstrdata.data, "f") ==0)
					{
						namestrcpy(recoveryStrInfo, enum_recovery_status_tab[RECOVERY_NOT_IN].name);
					}
					else if (strcmp(resultstrdata.data, "t") ==0)
						namestrcpy(recoveryStrInfo, enum_recovery_status_tab[RECOVERY_IN].name);
					else
					{
						/* do nothing */
					}
				}
                
				initStringInfo(&resultstrdata);
				monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, SQL_PG_QUERY_STARTTIME
					, user, hostAddr, nodeport, DEFAULT_DB, &resultstrdata);
				if (resultstrdata.len != 0)
				{
				    appendStringInfoString(starttime, resultstrdata.data);
				}
				else 
				{
				     appendStringInfoString(starttime, "unknow");
				}
				break;
			case PQPING_REJECT:
				initStringInfo(&conninfo);
				appendStringInfo(&conninfo,
								"postgresql://%s@%s:%d/%s?connect_timeout=%d",
								user,
								hostAddr,
								nodeport,
								DEFAULT_DB,
								10);
				conn = PQconnectdb(conninfo.data);
				pfree(conninfo.data);
				if(strlen(PQerrorMessage(conn)) > 0)
				{
					appendStringInfoString(strinfo, PQerrorMessage(conn));
				}
				else
				{
					if(MAKE_SQLSTATE(conn->last_sqlstate[0],
								 conn->last_sqlstate[1],
								 conn->last_sqlstate[2],
								 conn->last_sqlstate[3],
								 conn->last_sqlstate[4]) ==
					   ERRCODE_CANNOT_CONNECT_NOW)
					{
						appendStringInfoString(strinfo, "server is alive but rejecting connections");
					}
					else
					{
						appendStringInfo(strinfo, "server status unknow, error code:%s", conn->last_sqlstate);
					}
				}
				if(conn)
					PQfinish(conn);
				appendStringInfoString(starttime, "unknow");
				break;
			case PQPING_NO_RESPONSE:
				appendStringInfoString(strinfo, "not running");
				appendStringInfoString(starttime, "unknow");
				break;
			case PQPING_NO_ATTEMPT:
				appendStringInfoString(strinfo, "connection not attempted (bad params)");
				appendStringInfoString(starttime, "unknow");
				break;
			case AGENT_DOWN:
			{
				appendStringInfo(strinfo, "could not connect socket for agent \"%s\"", hostAddr);
				appendStringInfoString(starttime, "unknow");
				break;
			}
			default:
				break;
		}
	}
	else
	{
		appendStringInfoString(strinfo, "could not establish host connection");
	}

	pfree(user);
	pfree(hostAddr);
	pfree(resultstrdata.data);

	return ret;
}
char* mgr_get_cmdname(int cmdtype)
{
	int  i = 0;
	static CmdTypeName cmds[] = {
		{AGT_CMD_GTMCOORD_INIT, 				"GTMCOORD_INIT"},
		{AGT_CMD_GTMCOORD_SLAVE_INIT, 			"GTMCOORD_SLAVE_INIT"},
		{AGT_CMD_GTMCOORD_START_MASTER,			"GTMCOORD_START_MASTER"},
		{AGT_CMD_GTMCOORD_START_SLAVE,			"GTMCOORD_START_SLAVE"},	
		{AGT_CMD_GTMCOORD_STOP_MASTER,			"GTMCOORD_STOP_MASTER"},
		{AGT_CMD_GTMCOORD_STOP_SLAVE,			"GTMCOORD_STOP_SLAVE"},
		{AGT_CMD_GTMCOORD_CLEAN,				"GTMCOORD_CLEAN"},
		{AGT_CMD_GTMCOORD_SLAVE_FAILOVER,		"GTMCOORD_SLAVE_FAILOVER"},
		{AGT_CMD_AGTM_RESTART,					"AGTM_RESTART"},
		{AGT_CMD_CNDN_CNDN_INIT,				"CNDN_INIT"},
		{AGT_CMD_CNDN_SLAVE_INIT,				"CNDN_SLAVE_INIT"},
		{AGT_CMD_CN_RESTART,					"CN_RESTART"},
		{AGT_CMD_CN_START,						"CN_START"},
		{AGT_CMD_CN_STOP,						"CN_STOP"},
		{AGT_CMD_DN_START,						"DN_START"},
		{AGT_CMD_DN_RESTART,					"DN_RESTART"},
		{AGT_CMD_DN_STOP,						"DN_STOP"},
		{AGT_CMD_DN_FAILOVER,					"DN_FAILOVER"},	
		{AGT_CMD_PSQL_CMD,						"PSQL_CMD"},
		{AGT_CMD_CNDN_REFRESH_PGSQLCONF,		"CNDN_REFRESH_PGSQLCONF"},
		{AGT_CMD_CNDN_REFRESH_RECOVERCONF,		"CNDN_REFRESH_RECOVERCONF"},
		{AGT_CMD_CNDN_REFRESH_PGHBACONF,		"CNDN_REFRESH_PGHBACONF"},
		{AGT_CMD_CNDN_ADD_PGHBACONF,			"CNDN_ADD_PGHBACONF"},
		{AGT_CMD_CNDN_ALTER_PGHBACONF,			"CNDN_ALTER_PGHBACONF"},
		{AGT_CMD_CNDN_DELETE_PGHBACONF,			"CNDN_DELETE_PGHBACONF"},
		{AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD,	"CNDN_REFRESH_PGSQLCONF_RELOAD"},
		{AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE,	"CNDN_DELPARAM_PGSQLCONF_FORCE"},
		{AGT_CMD_CNDN_RENAME_RECOVERCONF,		"CNDN_RENAME_RECOVERCONF"},
		{AGT_CMD_MONITOR_GETS_HOST_INFO,		"MONITOR_GETS_HOST_INFO"},
		{AGT_CMD_PGDUMPALL,						"PGDUMPALL"},
		{AGT_CMD_RM,							"RM"},
		{AGT_CMD_NODE_RELOAD,					"NODE_RELOAD"},
		{AGT_CMD_CLEAN_NODE,					"CLEAN_NODE"},
		{AGT_CMD_STOP_AGENT,					"STOP_AGENT"},
		{AGT_CMD_SHOW_AGTM_PARAM,				"SHOW_AGTM_PARAM"},
		{AGT_CMD_SHOW_CNDN_PARAM,				"SHOW_CNDN_PARAM"},
		{AGT_CMD_GET_SQL_STRINGVALUES,			"GET_SQL_STRINGVALUES"},
		{AGT_CMD_GET_EXPLAIN_STRINGVALUES,		"GET_EXPLAIN_STRINGVALUES"},
		{AGT_CMD_GET_BATCH_JOB,					"GET_BATCH_JOB"},	
		{AGT_CMD_CHECK_DIR_EXIST,				"CHECK_DIR_EXIST"},
		{AGT_CMD_PING_NODE,						"PING_NODE"},
		{AGT_CMD_NODE_REWIND,					"NODE_REWIND"},
		{AGT_CMD_GTMCOORD_START_MASTER_BACKEND,	"GTMCOORD_START_MASTER"},
		{AGT_CMD_GTMCOORD_START_SLAVE_BACKEND,	"GTMCOORD_START_SLAVE"},
		{AGT_CMD_CN_START_BACKEND,				"CN_START"},
		{AGT_CMD_DN_START_BACKEND,				"DN_START"},
		{AGT_CMD_GTMCOORD_STOP_MASTER_BACKEND,	"GTMCOORD_STOP_MASTER"},
		{AGT_CMD_GTMCOORD_STOP_SLAVE_BACKEND,	"GTMCOORD_STOP_SLAVE"},
		{AGT_CMD_CN_STOP_BACKEND,				"CN_STOP,"},
		{AGT_CMD_DN_STOP_BACKEND,				"DN_STOP"},
		{AGT_CMD_GET_SQL_STRINGVALUES_COMMAND,	"GET_SQL_STRINGVALUES_COMMAND"},
		{AGT_CMD_AGTM_REWIND,					"AGTM_REWIND"},
		{AGT_CMD_DN_MASTER_PROMOTE,				"DN_MASTER_PROMOTE"},
		{AGT_CMD_CHECKOUT_NODE,					"CHECKOUT_NODE"},
		{AGT_CMD_LIST_NODESIZE,					"LIST_NODESIZE"},
		{AGT_CMD_LIST_NODESIZE_CHECK_SOFTLINK,	"LIST_NODESIZE_CHECK_SOFTLINK"},
		{AGT_CMD_RESET_AGENT,					"RESET_AGENT"},
	}; 	
	
	for (i=0; i<sizeof(cmds)/sizeof(CmdTypeName); i++)
	{
		if(cmdtype == cmds[i].cmdtype){
			return cmds[i].cmdname;
		}
	}
	return "";
}