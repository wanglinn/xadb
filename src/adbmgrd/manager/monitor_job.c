/*
 * monitor_job.c
 *
 * ADB Integrated Monitor Daemon
 *
 * The ADB monitor dynamic item, uses two catalog table to record the job content:
 * job table and job table. Jobitem table used to record monitor item name,
 * batch absoulte path with filename and its description. Job table used to record 
 * jobname, next_runtime, interval, status, command(SQL format) and description.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2017 ADB Development Group
 *
 * IDENTIFICATION
 *	  src/adbmgrd/manager/monitor_job.c
 */

#include "postgres.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <arpa/inet.h>

#include "access/skey.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "lib/ilist.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/adbmonitor.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "access/heapam.h"
#include "catalog/monitor_jobitem.h"
#include "catalog/monitor_job.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "parser/mgr_node.h"
#include "mgr/mgr_cmds.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "utils/formatting.h"
#include "postmaster/adbmonitor.h"
#include "mgr/mgr_msg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "common/fe_memutils.h"

/*
* GUC parameters
*/
int	adbmonitor_naptime;
bool adbmonitor_start_daemon;

typedef struct fdCtl
{
	int fd;
	int connected;
	int connectedError;
	int port;
	bool bchecked;
	bool connectStatus;
	char *address;
	NameData nodename;
} fdCtl;

typedef struct handleDnGtmArg
{
	bool bforce;
	int reconnect_attempts;
	int reconnect_interval;
	int select_timeout;
}handleDnGtmArg;

static HeapTuple montiot_job_get_item_tuple(Relation rel_job, Name jobname);
static bool mgr_element_in_array(Oid tupleOid, int array[], int count);
static int mgr_nodeType_Num_incluster(const int masterNodeOid, Relation relNode, const char nodeType);
static int mgr_get_async_connect_result(fdCtl *fdHandle, int totalFd, int selectTimeOut);
static int mgr_get_fd_noblock(fdCtl *fdHandle, int totalNum);
static void mgr_check_handle_node_func_arg(handleDnGtmArg *nodeArg);

/*
* ADD ITEM jobname(jobname, filepath, desc)
*/
void monitor_job_add(MonitorJobAdd *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall3(monitor_job_add_func,
									BoolGetDatum(node->if_not_exists),
									CStringGetDatum(node->name),
									PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum monitor_job_add_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple newtuple;
	HeapTuple checktuple;
	ListCell *lc;
	DefElem *def;
	NameData jobnamedata;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	bool if_not_exists = false;
	char *str;
	char *jobname;
	List *options;
	int32 interval;
	bool status;
	Datum datumtime;
	TimestampTz current_time = 0;

	if_not_exists = PG_GETARG_BOOL(0);
	jobname = PG_GETARG_CSTRING(1);
	options = (List *)PG_GETARG_POINTER(2);

	Assert(jobname);
	namestrcpy(&jobnamedata, jobname);
	rel = heap_open(MjobRelationId, AccessShareLock);
	/* check exists */
	checktuple = montiot_job_get_item_tuple(rel, &jobnamedata);
	if (HeapTupleIsValid(checktuple))
	{
		heap_freetuple(checktuple);
		if(if_not_exists)
		{
			ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("\"%s\" already exists, skipping", jobname)));
			PG_RETURN_BOOL(false);
		}
		heap_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("\"%s\" already exists", jobname)));
	}
	heap_close(rel, AccessShareLock);
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_monitor_job_name-1] = NameGetDatum(&jobnamedata);
	foreach(lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));

		if (strcmp(def->defname, "nexttime") == 0)
		{
			if(got[Anum_monitor_job_nexttime-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));

			str = defGetString(def);
			datumtime = DirectFunctionCall2(to_timestamp,
																			 PointerGetDatum(cstring_to_text(str)),
																			 PointerGetDatum(cstring_to_text("yyyy-mm-dd hh24:mi:ss")));
			datum[Anum_monitor_job_nexttime-1] = datumtime;
			got[Anum_monitor_job_nexttime-1] = true;
		}
		else if (strcmp(def->defname, "interval") == 0)
		{
			if(got[Anum_monitor_job_interval-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			interval = defGetInt32(def);
			if (interval <= 0)
				ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
					,errmsg("interval is out of range 1 ~ %d", INT_MAX)));
			datum[Anum_monitor_job_interval-1] = Int32GetDatum(interval);
			got[Anum_monitor_job_interval-1] = true;
		}
		else if (strcmp(def->defname, "status") == 0)
		{
			if(got[Anum_monitor_job_status-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			status = defGetBoolean(def);
			datum[Anum_monitor_job_status-1] = BoolGetDatum(status);
			got[Anum_monitor_job_status-1] = true;
		}
		else if (strcmp(def->defname, "command") == 0)
		{
			if(got[Anum_monitor_job_command-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_command-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_command-1] = true;
		}
		else if (strcmp(def->defname, "desc") == 0)
		{
			if(got[Anum_monitor_job_desc-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_desc-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_desc-1] = true;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is nexttime, interval, status, command, desc")));
		}
	}
	/* if not give, set to default */
	if (false == got[Anum_monitor_job_nexttime-1])
	{
		current_time = GetCurrentTimestamp();
		datum[Anum_monitor_job_nexttime-1] = TimestampTzGetDatum(current_time);;
	}
	if (false == got[Anum_monitor_job_interval-1])
	{
		datum[Anum_monitor_job_interval-1] = Int32GetDatum(adbmonitor_naptime);
	}
	if (false == got[Anum_monitor_job_status-1])
	{
		status = true;
		datum[Anum_monitor_job_status-1] = BoolGetDatum(true);
	}
	if (false == got[Anum_monitor_job_command-1])
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("option \"command\" must be given")));
	}
	if (false == got[Anum_monitor_job_desc-1])
	{
		datum[Anum_monitor_job_desc-1] = PointerGetDatum(cstring_to_text(""));
	}
	/* check the adbmonitor */
	if ((status == true) && (adbmonitor_start_daemon == false))
		ereport(WARNING, (errmsg("in postgresql.conf of ADBMGR adbmonitor=off and all jobs cannot be running, you should change adbmonitor=on which can be made effect by mgr_ctl reload ")));

	/* now, we can insert record */
	rel = heap_open(MjobRelationId, RowExclusiveLock);
	newtuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	simple_heap_insert(rel, newtuple);
	CatalogUpdateIndexes(rel, newtuple);
	heap_freetuple(newtuple);
	/*close relation */
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}


void monitor_job_alter(MonitorJobAlter *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall2(monitor_job_alter_func,
									CStringGetDatum(node->name),
									PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*
* alter job property, the command format : ALTER JOB jobname(INTERVAL=interval_time, ...), 
* ALTER JOB ALL (NEXTTIME=nexttime, INTERVAL=interval_time,STATUS=on|off, DESC=desc) only support thest three items
*/
Datum monitor_job_alter_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple newtuple;
	HeapTuple checktuple;
	HeapTuple tuple;
	ListCell *lc;
	DefElem *def;
	NameData jobnamedata;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	char *str;
	char *jobname;
	List *options;
	TupleDesc job_dsc;
	int32 interval;
	bool status = false;
	bool bAlterAll = false;
	Datum datumtime;
	HeapScanDesc relScan;

	jobname = PG_GETARG_CSTRING(0);
	options = (List *)PG_GETARG_POINTER(1);

	Assert(jobname);
	namestrcpy(&jobnamedata, jobname);
	if (strcmp(jobnamedata.data, MACRO_STAND_FOR_ALL_JOB) ==0)
		bAlterAll = true;
	rel = heap_open(MjobRelationId, RowExclusiveLock);
	/* check exists, MACRO_STAND_FOR_ALL_JOB stand for all jobs */
	if (!bAlterAll)
	{
		checktuple = montiot_job_get_item_tuple(rel, &jobnamedata);
		if (!HeapTupleIsValid(checktuple))
		{
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("\"%s\" does not exist", jobname)));
		}
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));


	foreach(lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));

		if (strcmp(def->defname, "nexttime") == 0)
		{
			if(got[Anum_monitor_job_nexttime-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));

			str = defGetString(def);
			datumtime = DirectFunctionCall2(to_timestamp,
						PointerGetDatum(cstring_to_text(str)),
						PointerGetDatum(cstring_to_text("yyyy-mm-dd hh24:mi:ss")));
			datum[Anum_monitor_job_nexttime-1] = datumtime;
			got[Anum_monitor_job_nexttime-1] = true;
		}
		else if (strcmp(def->defname, "interval") == 0)
		{
			if(got[Anum_monitor_job_interval-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			interval = defGetInt32(def);
			if (interval <= 0)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("interval is out of range 1 ~ %d", INT_MAX)));
			datum[Anum_monitor_job_interval-1] = Int32GetDatum(interval);
			got[Anum_monitor_job_interval-1] = true;
		}
		else if (strcmp(def->defname, "status") == 0)
		{
			if(got[Anum_monitor_job_status-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			status = defGetBoolean(def);
			datum[Anum_monitor_job_status-1] = BoolGetDatum(status);
			got[Anum_monitor_job_status-1] = true;
		}
		else if (strcmp(def->defname, "command") == 0)
		{
			if(got[Anum_monitor_job_command-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_command-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_command-1] = true;
		}
		else if (strcmp(def->defname, "desc") == 0)
		{
			if(got[Anum_monitor_job_desc-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			datum[Anum_monitor_job_desc-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_monitor_job_desc-1] = true;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" is not recognized", def->defname)
				,errhint("option is nexttime, interval, status, command, desc")));
		}
	}
	/* check the adbmonitor */
	if ((status == true) && (adbmonitor_start_daemon == false))
		ereport(WARNING, (errmsg("in postgresql.conf of ADBMGR adbmonitor=off and all jobs cannot be running, you should change adbmonitor=on which can be made effect by mgr_ctl reload ")));
	job_dsc = RelationGetDescr(rel);

	if (bAlterAll)
	{
		if (got[Anum_monitor_job_command-1] || got[Anum_monitor_job_desc-1])
		{
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errmsg("the command of \"ALTER JOB ALL\" not support modify the column \"comamnd\" and \"desc\"")));
		}
		relScan = heap_beginscan_catalog(rel, 0, NULL);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			newtuple = heap_modify_tuple(tuple, job_dsc, datum,isnull, got);
			simple_heap_update(rel, &tuple->t_self, newtuple);
			CatalogUpdateIndexes(rel, newtuple);
		}
		heap_endscan(relScan);
	}
	else
	{
		newtuple = heap_modify_tuple(checktuple, job_dsc, datum,isnull, got);
		simple_heap_update(rel, &checktuple->t_self, newtuple);
		CatalogUpdateIndexes(rel, newtuple);
		heap_freetuple(checktuple);
	}
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}


void monitor_job_drop(MonitorJobDrop *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall2(monitor_job_drop_func,
									BoolGetDatum(node->if_exists),
									PointerGetDatum(node->namelist));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum monitor_job_drop_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	NameData name;
	Datum datum[Natts_monitor_job];
	bool isnull[Natts_monitor_job];
	bool got[Natts_monitor_job];
	bool if_exists = false;
	MemoryContext context, old_context;
	List *name_list;

	if_exists = PG_GETARG_BOOL(0);
	name_list = (List *)PG_GETARG_POINTER(1);
	Assert(name_list);
	context = AllocSetContextCreate(CurrentMemoryContext
			,"DROP JOB"
			,ALLOCSET_DEFAULT_MINSIZE
			,ALLOCSET_DEFAULT_INITSIZE
			,ALLOCSET_DEFAULT_MAXSIZE);
	rel = heap_open(MjobRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, name_list)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = montiot_job_get_item_tuple(rel, &name);
		if(!HeapTupleIsValid(tuple))
		{
			if(if_exists)
			{
				ereport(NOTICE,  (errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("\"%s\" does not exist, skipping", NameStr(name))));
				continue;
			}
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("\"%s\" does not exist", NameStr(name))));
		}
		heap_freetuple(tuple);
	}

	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	foreach(lc, name_list)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = montiot_job_get_item_tuple(rel, &name);
		if(HeapTupleIsValid(tuple))
		{
			simple_heap_delete(rel, &(tuple->t_self));
			CatalogUpdateIndexes(rel, tuple);
			heap_freetuple(tuple);
		}
	}
	/* at end, close relation */
	heap_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
	PG_RETURN_BOOL(true);
}


static HeapTuple montiot_job_get_item_tuple(Relation rel_job, Name jobname)
{
	ScanKeyData key[1];
	HeapTuple tupleret = NULL;
	HeapTuple tuple = NULL;
	HeapScanDesc rel_scan;

	ScanKeyInit(&key[0]
				,Anum_monitor_job_name
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(jobname));
	rel_scan = heap_beginscan_catalog(rel_job, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		tupleret = heap_copytuple(tuple);
		break;
	}
	heap_endscan(rel_scan);

	return tupleret;
}


/*
* drop the coordinator which is not running normal
*
*/
Datum monitor_handle_coordinator(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tuple = NULL;
	ScanKeyData key[3];
	Relation relNode;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData strerr;
	NameData s_nodename;
	HeapTuple tup_result;
	PGconn* conn;
	PGresult *res;
	StringInfoData constr;
	int nodePort;
	int agentPort;
	int iloop = 0;
	int count = 0;
	int coordNum = 0;
	int dropCoordOidArray[1000];
	char *address;
	char *userName;
	char portBuf[10];
	Oid tupleOid;
	bool result = true;
	bool rest;

	namestrcpy(&s_nodename, "coordinator");
	initStringInfo(&infosendmsg);
	/* check the status of all coordinator in cluster */
	memset(dropCoordOidArray, 0, 1000);
	memset(portBuf, 0, sizeof(portBuf));
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan_catalog(relNode, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		coordNum++;
		nodePort = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		rest = port_occupancy_test(address, nodePort);
		if (!rest)
		{
			/*check it two times again*/
			pg_usleep(1000000L);
			rest = port_occupancy_test(address, nodePort);
			if (!rest)
			{
				pg_usleep(1000000L);
				rest = port_occupancy_test(address, nodePort);
			}
		}
		/*record drop coordinator*/
		if (!rest)
		{
			dropCoordOidArray[iloop] = HeapTupleGetOid(tuple);
			iloop++;
			appendStringInfo(&infosendmsg, "drop node \"%s\"; ", NameStr(mgr_node->nodename));
		}
		
		pfree(address);
	}
	count = iloop;

	if (coordNum == count)
	{
		heap_endscan(relScan);
		heap_close(relNode, RowExclusiveLock);
		pfree(infosendmsg.data);
		ereport(ERROR, (errmsg("all coordinators in cluster are not running normal!!!")));
	}

	if (0 == count)
	{
		heap_endscan(relScan);
		heap_close(relNode, RowExclusiveLock);
		pfree(infosendmsg.data);
		tup_result = build_common_command_tuple(&s_nodename, true, "all coordinators in cluster are running normal");
		return HeapTupleGetDatum(tup_result);
	}

	/*remove coordinator out of cluster*/
	initStringInfo(&constr);
	initStringInfo(&restmsg);
	initStringInfo(&strerr);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&strerr, "%s\n", infosendmsg.data);

	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		userName = get_hostuser_from_hostoid(mgr_node->nodehost);
		tupleOid = HeapTupleGetOid(tuple);
		rest = mgr_element_in_array(tupleOid, dropCoordOidArray, count);
		memset(portBuf, 0, sizeof(portBuf));
		snprintf(portBuf, sizeof(portBuf), "%d", mgr_node->nodeport);
		if (!rest)
		{
			resetStringInfo(&constr);
			appendStringInfo(&constr, "host='%s' port=%d user=%s connect_timeout=5"
				, address, mgr_node->nodeport, userName);
			rest = PQping(constr.data);
			if (rest == PQPING_OK)
			{
				PG_TRY();
				{
					/*drop the coordinator*/
					resetStringInfo(&constr);
					appendStringInfo(&constr, "postgresql://%s@%s:%d/%s", userName, address, mgr_node->nodeport, AGTM_DBNAME);
					appendStringInfoCharMacro(&constr, '\0');
					conn = PQconnectdb(constr.data);
					if (PQstatus(conn) != CONNECTION_OK)
					{
						result = false;
						ereport(WARNING, (errmsg("on ADBMGR cannot connect coordinator \"%s\"", NameStr(mgr_node->nodename))));
						appendStringInfo(&strerr, "on ADBMGR cannot connect coordinator \"%s\" \
								, you need do \"%s\" and \"set force_parallel_mode = off; select pgxc_pool_reload()\" \
								on coordinator \"%s\" by yourself!!!\n"
							, NameStr(mgr_node->nodename), infosendmsg.data, NameStr(mgr_node->nodename));
					}
					else
					{
						res = PQexec(conn, infosendmsg.data);
						if(PQresultStatus(res) == PGRES_COMMAND_OK)
						{
							ereport(LOG, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,infosendmsg.data, PQcmdStatus(res))));
							ereport(NOTICE, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,infosendmsg.data, PQcmdStatus(res))));
						}
						else
						{
							result = false;
							ereport(WARNING, (errmsg("from ADBMGR, on coordinator \"%s\", execute \"%s\" fail: %s" 
								, NameStr(mgr_node->nodename), infosendmsg.data, PQresultErrorMessage(res))));
							appendStringInfo(&strerr, "from ADBMGR, on coordinator \"%s\" execute \"%s\" fail: %s\n"
							, NameStr(mgr_node->nodename), infosendmsg.data, PQresultErrorMessage(res));
						}
						PQclear(res);

						res = PQexec(conn, "set force_parallel_mode = off; select pgxc_pool_reload()");
						if(PQresultStatus(res) == PGRES_TUPLES_OK)
						{
							ereport(LOG, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,"set force_parallel_mode = off; select pgxc_pool_reload()", "t")));
							ereport(NOTICE, (errmsg("from ADBMGR, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
								,"set force_parallel_mode = off; select pgxc_pool_reload()", "t")));
						}
						else
						{
							result = false;
							ereport(WARNING, (errmsg("from ADBMGR, on coordinator \"%s\", execute \"%s\" fail: %s"
								, NameStr(mgr_node->nodename), "set force_parallel_mode = off; select pgxc_pool_reload()" 
								, PQresultErrorMessage(res))));
							appendStringInfo(&strerr, "from ADBMGR, on coordinator \"%s\" execute \"%s\" fail: %s\n"
								, NameStr(mgr_node->nodename), "set force_parallel_mode = off; select pgxc_pool_reload()"
								, PQresultErrorMessage(res));
						}
						PQclear(res);
					}
					PQfinish(conn);
					
				}PG_CATCH();
				{
					ereport(WARNING, (errmsg("the command result : %s", strerr.data)));
					pfree(address);
					pfree(userName);
					heap_endscan(relScan);
					heap_close(relNode, RowExclusiveLock);

					pfree(constr.data);
					pfree(restmsg.data);
					pfree(strerr.data);
					pfree(infosendmsg.data);
					pfree(getAgentCmdRst.description.data);
					PG_RE_THROW();
				}PG_END_TRY();
			}
			else
			{
				agentPort = get_agentPort_from_hostoid(mgr_node->nodehost);
				rest = port_occupancy_test(address, agentPort);
				/* agent running normal */
				if (rest)
				{
					resetStringInfo(&restmsg);
					monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES_COMMAND, agentPort, infosendmsg.data
					,userName, address, mgr_node->nodeport, DEFAULT_DB, &restmsg);
					ereport(LOG, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, infosendmsg.data, restmsg.len == 0 ? "f":restmsg.data)));
					ereport(NOTICE, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, infosendmsg.data, restmsg.len == 0 ? "f":restmsg.data)));
					if (restmsg.len == 0)
					{
						result = false;
						appendStringInfo(&strerr, "from agent, on coordinator \"%s\" execute \"%s\" fail\n"
							, NameStr(mgr_node->nodename), infosendmsg.data);
					}

					resetStringInfo(&restmsg);
					monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPort
						, "set force_parallel_mode = off; select pgxc_pool_reload()"
						,userName, address, mgr_node->nodeport, DEFAULT_DB, &restmsg);
					ereport(LOG, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, "set force_parallel_mode = off; select pgxc_pool_reload()", restmsg.data)));
					ereport(NOTICE, (errmsg("from agent, on coordinator \"%s\" : %s, result: %s", NameStr(mgr_node->nodename)
						, "set force_parallel_mode = off; select pgxc_pool_reload()", restmsg.data)));
					if (restmsg.len != 0)
					{
						if (strcasecmp(restmsg.data, "t") != 0)
						{
							result = false;
							appendStringInfo(&strerr, "from agent, on coordinator \"%s\" execute \"%s\" fail: %s\n"
								, NameStr(mgr_node->nodename)
								, "set force_parallel_mode = off; select pgxc_pool_reload()", restmsg.data);
						}
					}
				}
				else
				{
					result = false;
					ereport(WARNING, (errmsg("on address \"%s\" the agent is not running normal; \
						you need execute \"%s\" and \"set force_parallel_mode = off; select pgxc_pool_reload()\" \
						on coordinator \"%s\" by yourself !!!", address, infosendmsg.data, NameStr(mgr_node->nodename))));
					appendStringInfo(&strerr,"on address \"%s\" the agent is not running normal; \
						you need execute \"%s\" and \"set force_parallel_mode = off; select pgxc_pool_reload()\" \
						on coordinator \"%s\" by yourself !!!" \
						, address, infosendmsg.data, NameStr(mgr_node->nodename));
				}
			}
		}
		else
		{
			mgr_node->nodeinited = false;
			mgr_node->nodeincluster = false;
			heap_inplace_update(relNode, tuple);
		}
		
		pfree(address);
		pfree(userName);
	}
	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);
	
	pfree(restmsg.data);
	pfree(constr.data);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	if (strerr.len > 2)
	{
		if (strerr.data[strerr.len-2] == '\n')
			strerr.data[strerr.len-2] = '\0';
		if (strerr.data[strerr.len-1] == '\n')
			strerr.data[strerr.len-1] = '\0';
	}
	ereport(LOG, (errmsg("monitor handle coordinator result : %s, description : %s"
		,result==true?"true":"false", strerr.data)));
	tup_result = build_common_command_tuple(&s_nodename, result, strerr.data);
	pfree(strerr.data);
	return HeapTupleGetDatum(tup_result);
}


static bool mgr_element_in_array(Oid tupleOid, int array[], int count)
{
	int iloop = 0;

	while(iloop < count)
	{
		if(array[iloop] == tupleOid)
			return true;
		iloop++;
	}
	return false;
}

/*
* check the substring "subjobstr" is in the "JOB" table of "COMMAND" column content
*/
bool mgr_check_job_in_updateparam(const char *subjobstr)
{
	HeapTuple tuple = NULL;
	Relation relJob;
	HeapScanDesc relScan;
	Datum datumCommand;
	ScanKeyData key[1];
	NameData jobname;
	Form_monitor_job monitor_job;
	bool isNull;
	bool res = false;
	char *command;
	char *pstr = NULL;

	/* guc parameter, control job switch */
	if (!adbmonitor_start_daemon)
		return false;

	Assert(subjobstr);

	ScanKeyInit(&key[0]
				,Anum_monitor_job_status
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	relJob = heap_open(MjobRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relJob, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		/*get the command string*/
		datumCommand = heap_getattr(tuple, Anum_monitor_job_command, RelationGetDescr(relJob), &isNull);
		if(isNull)
		{
			heap_endscan(relScan);
			heap_close(relJob, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "monitor_job")
				, errmsg("column command is null")));
		}
		command = TextDatumGetCString(datumCommand);
		Assert(command);
		pstr = NULL;
		pstr = strcasestr(command, subjobstr);
		if (pstr != NULL)
		{
			res = true;
			monitor_job = (Form_monitor_job)GETSTRUCT(tuple);
			namestrcpy(&jobname, NameStr(monitor_job->name));
			break;
		}

	}
	heap_endscan(relScan);
	heap_close(relJob, AccessShareLock);
	
	if (res)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("on job table, the content of job \"%s\" includes \"%s\" string and its status is \"on\"; you need do \"ALTER JOB \"%s\" (STATUS=false);\" to alter its status to \"off\" or set \"adbmonitor=off\" in postgresql.conf of ADBMGR to turn all job off which can be made effect by mgr_ctl reload", jobname.data, subjobstr, jobname.data)
				,errhint("try \"list job\" for more information")));
	}

	return res;
}

/*
* check the datanode master running status, when find the master not running normal,
* inform the adbmgr to do "failover datanode" command.
*/

Datum monitor_handle_datanode(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapScanDesc relScan;
	HeapTuple tuple;
	HeapTuple tupResult;
	Form_mgr_node mgr_node;
	ScanKeyData key[3];
	StringInfoData cmdstrmsg;
	NameData masterName;
	int nargs;
	int nodePort;
	int nmasterNum = 0;
	int nslaveNum = 0;
	int createFdNum = 0;
	int i = 0;
	char *address;
	bool bnameNull = false;
	bool res = true;
	bool bexec = false;
	int ret = 0;
	fdCtl *fdHandle = NULL;
	struct sockaddr_in *serv_addr = NULL;
	handleDnGtmArg nodeArg;

	/* default failover used "force" */
	nodeArg.bforce = true;
	/* default check number */
	nodeArg.reconnect_attempts = 3;
	/* default check interval time, unit : S */
	nodeArg.reconnect_interval = 2;
	/* default select timeout, unit : S */
	nodeArg.select_timeout = 15;

	/* check the argv num */
	namestrcpy(&masterName, "");
	nargs = PG_NARGS();
	switch(nargs)
	{
		case 5:
			nodeArg.select_timeout= PG_GETARG_UINT32(4);
		case 4:
			nodeArg.reconnect_interval = PG_GETARG_UINT32(3);
		case 3:
			nodeArg.reconnect_attempts = PG_GETARG_UINT32(2);
		case 2:
			nodeArg.bforce = PG_GETARG_BOOL(1);
		case 1:
			namestrcpy(&masterName, PG_GETARG_CSTRING(0));
		case 0:
			break;
		default:
			/* error */
			break;
	}

	if (masterName.data[0] == '\0')
		bnameNull = true;

	/* check the nodename exist */
	if ((!bnameNull) && (!mgr_check_node_exist_incluster(&masterName, true)))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("datanode master \"%s\" does not exist in cluster", NameStr(masterName))));

	/* check the input value */
	mgr_check_handle_node_func_arg(&nodeArg);

	relNode = heap_open(NodeRelationId, AccessShareLock);
	/* get total datanode master num */
	if (!bnameNull)
		nmasterNum = 1;
	else
		nmasterNum = mgr_nodeType_Num_incluster(0, relNode, CNDN_TYPE_DATANODE_MASTER);

	PG_TRY();
	{
		if (nmasterNum < 1)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("there is not any datanode master in cluster")));

		fdHandle = (fdCtl *)palloc0(sizeof(fdCtl) * nmasterNum);
		if (!fdHandle)
			ereport(ERROR, (errmsg("malloc %lu byte fail: %s", sizeof(fdCtl) * nmasterNum, strerror(errno))));

		serv_addr = (struct sockaddr_in *)palloc0(sizeof(struct sockaddr_in) * nmasterNum);
		if (!serv_addr)
			ereport(ERROR, (errmsg("malloc %lu byte fail: %s", sizeof(struct sockaddr_in) * nmasterNum, strerror(errno))));

		/* create nmasterNum socket noblock fd */
		createFdNum = mgr_get_fd_noblock(fdHandle, nmasterNum);
		if (createFdNum != nmasterNum)
			ereport(ERROR, (errmsg("create noblock socket fd fail: %s", strerror(errno))));

		/* traversal the datanode master */
		i = 0;
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		ScanKeyInit(&key[1]
			,Anum_mgr_node_nodeinited
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		ScanKeyInit(&key[2]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
		relScan = heap_beginscan_catalog(relNode, 3, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(!bnameNull)
				if (strcmp(masterName.data, NameStr(mgr_node->nodename)) !=0)
					continue;
			nodePort = mgr_node->nodeport;
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			namestrcpy(&(fdHandle[i].nodename), NameStr(mgr_node->nodename));
			fdHandle[i].port = mgr_node->nodeport;
			fdHandle[i].address = address;

			serv_addr[i].sin_family = AF_INET;
			serv_addr[i].sin_port = htons(mgr_node->nodeport);
			serv_addr[i].sin_addr.s_addr = inet_addr(address);
			i++;
		}
		heap_endscan(relScan);

		i = 0;
		while (i < nmasterNum)
		{
			fdHandle[i].connected = connect(fdHandle[i].fd, (struct sockaddr *)&serv_addr[i], sizeof(struct sockaddr_in));
			fdHandle[i].connectedError = errno;
			i++;
		}

		i = 0;
		while (i<nodeArg.reconnect_attempts)
		{
			ret = mgr_get_async_connect_result(fdHandle, nmasterNum, nodeArg.select_timeout);
			if (ret)
				pg_usleep(nodeArg.reconnect_interval*1000000L);
			else
				break;
			i++;
		}

		if (ret)
		{
			ereport(WARNING, (errmsg("the total number of datanode master which is not running normal : %d" , ret)));
			/* do fail command */
			i = 0;
			/* check the datanode master has slave node */
			relScan = heap_beginscan_catalog(relNode, 3, key);
			while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
			{
				mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
				Assert(mgr_node);
				i++;
				if (fdHandle[i-1].connectStatus)
					continue;
				nslaveNum = mgr_nodeType_Num_incluster(HeapTupleGetOid(tuple), relNode, CNDN_TYPE_DATANODE_SLAVE);
				if (!nslaveNum)
				{
					ereport(WARNING, (errmsg("the datanode master \"%s\" is not running normal and has no slave"
						, NameStr(mgr_node->nodename))));
					bexec = false;
				}
				else
				{
					initStringInfo(&cmdstrmsg);
					appendStringInfo(&cmdstrmsg, "failover datanode %s %s", NameStr(mgr_node->nodename), nodeArg.bforce ? "FORCE":"");
					ereport(WARNING, (errmsg("the datanode master \"%s\" is not running normal and will notice ADBMGR to do \"%s\" command" 
					, NameStr(mgr_node->nodename), cmdstrmsg.data)));
					bexec = true;
					/* do failover command */
					res = DirectFunctionCall2(mgr_failover_one_dn, CStringGetDatum(NameStr(mgr_node->nodename)), BoolGetDatum(nodeArg.bforce));
					if (!res)
						ereport(WARNING, (errmsg("on ADBMGR do command \"%s\" fail, check the log" , cmdstrmsg.data)));
					pfree(cmdstrmsg.data);
					break;
				}

			}

			heap_endscan(relScan);
		}
	}PG_CATCH();
	{
		i = 0;
		if (fdHandle)
		{
			while (i < createFdNum)
			{
				if (fdHandle[i].fd == -1)
					break;
				close(fdHandle[i].fd);
				i++;
			}

			i = 0;
			if (createFdNum == nmasterNum)
			{
				while (i < nmasterNum)
				{
					pfree(fdHandle[i].address);
					i++;
				}
			}
			pfree(fdHandle);
		}
		if (serv_addr)
			pfree(serv_addr);
		heap_close(relNode, AccessShareLock);
		PG_RE_THROW();
	}PG_END_TRY();

	pfree(serv_addr);
	i = 0;
	while (i < nmasterNum)
	{
		close(fdHandle[i].fd);
		pfree(fdHandle[i].address);
		i++;
	}
	pfree(fdHandle);
	heap_close(relNode, AccessShareLock);

	if (bnameNull)
		namestrcpy(&masterName, "datanode master");
	if (!ret)
	{
		/* all datanode master running normal */
		if (bnameNull)
			tupResult = build_common_command_tuple(&masterName, true, "all datanode master in cluster are running normal");
		else
			tupResult = build_common_command_tuple(&masterName, true, "the datanode master in cluster is running normal");
	}
	else
	{
		if (bexec)
			tupResult = build_common_command_tuple(&masterName, res,  res ? "execute failover command success" : "execute failover command fail");
		else
			ereport(ERROR, (errmsg("all of the datanode master which are not running normal have no slave node")));
	}

	/* record the result */
	if (bexec)
		ereport(LOG, (errmsg("execute the command for monitor_handle_datanode : \n nodename:%s, \n result:%s, \n description:%s"
			, NameStr(masterName)
			, res ? "true" : "false"
			, (res ? "execute failover command success" : "execute failover command fail"))));
	else
		ereport(DEBUG1, (errmsg("the command for monitor_handle_datanode : \n nodename:%s, \n result:%s, \n description:%s"
			, NameStr(masterName)
			, (!ret) ? "true" : (bexec ? (res ? "true" : "false") : "false")
			, (!ret) ? (bnameNull? "all datanode master in cluster are running normal" 
				: "the datanode master in cluster is running normal") 
				: (bexec ? (res ? "execute failover command success" : "execute failover command fail") 
				: "all of the datanode master which are not running normal have no slave node"))));

	return HeapTupleGetDatum(tupResult);
}

/*
* calculate the total number of slave in cluster for given node type and its master's oid
*
*/

static int mgr_nodeType_Num_incluster(const int masterNodeOid, Relation relNode, const char nodeType)
{
	HeapScanDesc relScan;
	HeapTuple tuple;
	ScanKeyData key[4];
	int nSlaveNum = 0;

	/* traversal the slave node */
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeinited
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodeType));
	ScanKeyInit(&key[3]
		,Anum_mgr_node_nodemasternameOid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(masterNodeOid));
	relScan = heap_beginscan_catalog(relNode, 4, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		nSlaveNum++;
	}

	heap_endscan(relScan);

	return nSlaveNum;
}


/*
* async connect result, return the number which fd is not connect normal
*/

static int mgr_get_async_connect_result(fdCtl *fdHandle, int totalFd, int selectTimeOut)
{
	int m = 0;
	int s = 0;
	int i = 0;
	int maxFd = 0;
	int res;
	fd_set rset;
	fd_set wset;
	struct timeval tval;

	while (m++ < totalFd)
	{
		tval.tv_sec = selectTimeOut;
		tval.tv_usec = 0;
		FD_ZERO(&rset);
		FD_ZERO(&wset);
		i = 0;
		maxFd = 0;
		s = 0;

		/* fd set */
		while (i < totalFd)
		{
			if (fdHandle[i].connected == 0)
			{
				fdHandle[i].bchecked = true;
				fdHandle[i].connectStatus = true;
			}
			else if (fdHandle[i].connected == -1 && fdHandle[i].connectedError == EINPROGRESS)
			{
				if (!(fdHandle[i].bchecked))
				{
					FD_SET(fdHandle[i].fd, &rset);
					FD_SET(fdHandle[i].fd, &wset);
					s++;
				}
				if (fdHandle[i].fd > maxFd)
					maxFd = fdHandle[i].fd;
			}

			i++;
		}

		if (!s)
			break;

		res = select(maxFd + 1, &rset, &wset, NULL, &tval);
		if (res < 0)
		{
			ereport(ERROR, (errmsg("network error in connect : %s\n", strerror(errno))));
		}
		else if (res == 0)
		{
			/* connect timeout, do nothing */
		}
		else
		{
			/* check connect status */
			i = 0;
			while (i < totalFd)
			{
				if (fdHandle[i].bchecked == false)
				{
					int r1 = FD_ISSET(fdHandle[i].fd, &rset);
					int w1 = FD_ISSET(fdHandle[i].fd, &wset);
					if ( r1 || w1 )
					{
						int err = 0;
						int len = sizeof(err);
						getsockopt(fdHandle[i].fd, SOL_SOCKET, SO_ERROR,&err,(socklen_t *)&len);
						fdHandle[i].bchecked = true;
						if (!err)
							fdHandle[i].connectStatus = true;
						else
							fdHandle[i].connectStatus = false;
					}
				}
				i++;
			}
		}
	}

	/*get the num fd which connect not right */
	i = 0;
	s = 0;
	while (i<totalFd)
	{
		if (!fdHandle[i].connectStatus)
			s++;
		i++;
	}

	return s;
}

/*
* check the gtm master running status, when find the master not running normal,
* inform the adbmgr to do "failover gtm" command.
*/

Datum monitor_handle_gtm(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapScanDesc relScan;
	HeapTuple tuple;
	HeapTuple tupResult;
	Form_mgr_node mgr_node;
	ScanKeyData key[3];
	StringInfoData cmdstrmsg;
	NameData masterName;
	int nargs;
	int nodePort;
	int nmasterNum = 0;
	int nslaveNum = 0;
	int createFdNum = 0;
	int i = 0;
	char *address;
	bool bnameNull = false;
	bool res = true;
	bool bexec = false;
	int ret = 0;
	fdCtl *fdHandle = NULL;
	struct sockaddr_in *serv_addr = NULL;
	handleDnGtmArg nodeArg;

	/* default failover used "force" */
	nodeArg.bforce = true;
	/* default check number */
	nodeArg.reconnect_attempts = 3;
	/* default check interval time, unit : S */
	nodeArg.reconnect_interval = 2;
	/* default select timeout, unit : S */
	nodeArg.select_timeout = 15;

	/* check the argv num */
	namestrcpy(&masterName, "");
	nargs = PG_NARGS();
	switch(nargs)
	{
		case 5:
			nodeArg.select_timeout = PG_GETARG_UINT32(4);
		case 4:
			nodeArg.reconnect_interval = PG_GETARG_UINT32(3);
		case 3:
			nodeArg.reconnect_attempts = PG_GETARG_UINT32(2);
		case 2:
			nodeArg.bforce = PG_GETARG_BOOL(1);
		case 1:
			namestrcpy(&masterName, PG_GETARG_CSTRING(0));
		case 0:
			break;
		default:
			/* error */
			break;
	}

	if (masterName.data[0] == '\0')
		bnameNull = true;

	/* check the nodename exist */
	if ((!bnameNull) && (!mgr_check_node_exist_incluster(&masterName, true)))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("gtm master \"%s\" does not exist in cluster", NameStr(masterName))));
	/* check the input value */
	mgr_check_handle_node_func_arg(&nodeArg);

	relNode = heap_open(NodeRelationId, AccessShareLock);
	/* get total gtm master num */
	if (!bnameNull)
		nmasterNum = 1;
	else
		nmasterNum = mgr_nodeType_Num_incluster(0, relNode, GTM_TYPE_GTM_MASTER);

	PG_TRY();
	{
		if (nmasterNum != 1)
			ereport(ERROR, (errmsg("check the node table ,there is not one gtm master in cluster, but is %d", nmasterNum)));

		fdHandle = (fdCtl *)palloc0(sizeof(fdCtl) * nmasterNum);
		if (!fdHandle)
			ereport(ERROR, (errmsg("malloc %lu byte fail: %s", sizeof(fdCtl) * nmasterNum, strerror(errno))));

		serv_addr = (struct sockaddr_in *)palloc0(sizeof(struct sockaddr_in) * nmasterNum);
		if (!serv_addr)
			ereport(ERROR, (errmsg("malloc %lu byte fail: %s", sizeof(struct sockaddr_in) * nmasterNum, strerror(errno))));

		/* create 1 socket fd */
		createFdNum = mgr_get_fd_noblock(fdHandle, 1);
		if (!createFdNum)
			ereport(ERROR, (errmsg("create noblock socket fd fail : %s", strerror(errno))));

		/* traversal the gtm master */
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		ScanKeyInit(&key[1]
			,Anum_mgr_node_nodeinited
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		ScanKeyInit(&key[2]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(GTM_TYPE_GTM_MASTER));
		relScan = heap_beginscan_catalog(relNode, 3, key);
		if((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			nodePort = mgr_node->nodeport;
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			namestrcpy(&(fdHandle[0].nodename), NameStr(mgr_node->nodename));
			fdHandle[0].port = mgr_node->nodeport;
			fdHandle[0].address = address;

			serv_addr[0].sin_family = AF_INET;
			serv_addr[0].sin_port = htons(mgr_node->nodeport);
			serv_addr[0].sin_addr.s_addr = inet_addr(address);
		}
		else
		{
			heap_endscan(relScan);
			ereport(ERROR, (errmsg("get information of gtm master in node table fail")));
		}
		heap_endscan(relScan);

		fdHandle[0].connected = connect(fdHandle[0].fd, (struct sockaddr *)&serv_addr[0], sizeof(struct sockaddr_in));
		fdHandle[0].connectedError = errno;

		i = 0;
		while (i<nodeArg.reconnect_attempts)
		{
			ret = mgr_get_async_connect_result(fdHandle, 1, nodeArg.select_timeout);
			if (ret)
				pg_usleep(nodeArg.reconnect_interval*1000000L);
			else
				break;
			i++;
		}

		if (ret)
		{
			Assert(!fdHandle[0].connectStatus);
			ereport(WARNING, (errmsg("the number of gtm master which is not running normal : %d" , ret)));
			/* check the gtm master has slave node */
			relScan = heap_beginscan_catalog(relNode, 3, key);
			if((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
			{
				mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
				Assert(mgr_node);;
				nslaveNum = mgr_nodeType_Num_incluster(HeapTupleGetOid(tuple), relNode, GTM_TYPE_GTM_SLAVE);
				if (!nslaveNum)
				{
					ereport(WARNING, (errmsg("the gtm master \"%s\" is not running normal and has no slave"
						, NameStr(mgr_node->nodename))));
					bexec = false;
				}
				else
				{
					initStringInfo(&cmdstrmsg);
					appendStringInfo(&cmdstrmsg, "failover gtm %s %s", NameStr(mgr_node->nodename), nodeArg.bforce ? "FORCE":"");
					ereport(WARNING, (errmsg("the gtm master \"%s\" is not running normal and will notice ADBMGR to do \"%s\" command" 
					, NameStr(mgr_node->nodename), cmdstrmsg.data)));
					bexec = true;
					/* do failover command */
					res = DirectFunctionCall2(mgr_failover_gtm, CStringGetDatum(NameStr(mgr_node->nodename)), BoolGetDatum(nodeArg.bforce));
					if (!res)
						ereport(WARNING, (errmsg("on ADBMGR do command \"%s\" fail, check the log" , cmdstrmsg.data)));
					pfree(cmdstrmsg.data);
				}

			}
			else
			{
				heap_endscan(relScan);
				ereport(ERROR, (errmsg("get information of gtm master in node table fail")));
			}

			heap_endscan(relScan);
		}
	}PG_CATCH();
	{
		if (fdHandle)
		{
			if (createFdNum)
				pfree(fdHandle[0].address);
			pfree(fdHandle);
		}
		if (serv_addr)
			pfree(serv_addr);
		heap_close(relNode, AccessShareLock);
		PG_RE_THROW();
	}PG_END_TRY();

	pfree(serv_addr);
	close(fdHandle[0].fd);
	pfree(fdHandle[0].address);
	pfree(fdHandle);
	heap_close(relNode, AccessShareLock);

	namestrcpy(&masterName, "gtm master");
	if (!ret)
	{
		tupResult = build_common_command_tuple(&masterName, true, "the gtm master in cluster is running normal");
	}
	else
	{
		if (bexec)
			tupResult = build_common_command_tuple(&masterName, res,  res ? "execute failover command success" : "execute failover command fail");
		else
			ereport(ERROR, (errmsg("the gtm master which is not running normal has no slave node")));
	}

	/* record the result */
	if (bexec)
		ereport(LOG, (errmsg("execute the command for monitor_handle_gtm : \n nodename:%s, \n result:%s, \n description:%s"
		, NameStr(masterName)
		, res ? "true":"false"
		, res ? "execute failover command success" : "execute failover command fail")));
	else
		ereport(DEBUG1, (errmsg("the command for monitor_handle_gtm : \n nodename:%s, \n result:%s, \n description:%s"
			, NameStr(masterName)
			, (!ret) ? "true" : (bexec ? (res ? "true":"false") : "false")
			, (!ret) ? "the gtm master in cluster is running normal" : 
				(bexec ? (res ? "execute failover command success" : "execute failover command fail") 
				: "the gtm master which is not running normal has no slave node"))));
	return HeapTupleGetDatum(tupResult);
}

static int mgr_get_fd_noblock(fdCtl *fdHandle, int totalNum)
{
	int i = 0;
	int flags;

	while (i < totalNum)
	{
		fdHandle[i].fd = socket(AF_INET, SOCK_STREAM, 0);
		fdHandle[i].bchecked = false;
		fdHandle[i].connectStatus = false;
		if (fdHandle[i].fd == -1)
			return i;
		/* set noblock */
		flags = fcntl(fdHandle[i].fd, F_GETFL, 0);
		if (flags < 0)
		{
			ereport(WARNING, (errmsg("get the file handle status fail : %s", strerror(errno))));
			return i;
		}
		if (fcntl(fdHandle[i].fd, F_SETFL, flags | O_NONBLOCK) == -1)
		{
			return i;
			ereport(WARNING, (errmsg("set the file handle noblock fail : %s", strerror(errno))));
		}
		i++;
	}

	return i;
}

static void mgr_check_handle_node_func_arg(handleDnGtmArg *nodeArg)
{
	Assert(nodeArg);
	if (nodeArg->reconnect_attempts < 2 || nodeArg->reconnect_attempts > 60)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
			,errmsg("the third input parameter reconnect_attempts = %d, not in the limit 2<=reconnect_attempts<=60, default value is 2"
			, nodeArg->reconnect_attempts), errhint("try \"add job\" for more information")));
	if (nodeArg->reconnect_interval < 2 || nodeArg->reconnect_interval > 120)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
			,errmsg("the fourth input parameter reconnect_interval = %d, not in the limit 2<=reconnect_interval<=120, default value is 3"
			, nodeArg->reconnect_interval), errhint("try \"add job\" for more information")));
	if (nodeArg->select_timeout < 2 || nodeArg->select_timeout > 120)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
			,errmsg("the fifth input parameter select_timeout = %d, not in the limit 2<=select_timeout<=120, default value is 15"
			, nodeArg->select_timeout), errhint("try \"add job\" for more information")));

}
