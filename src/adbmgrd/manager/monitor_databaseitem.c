/*
 * commands of node
 */
#include <stdint.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "../../interfaces/libpq/libpq-fe.h"
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_node.h"
#include "catalog/monitor_databaseitem.h"
#include "catalog/monitor_databasetps.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "funcapi.h"
#include "utils/lsyscache.h"
#include "access/xact.h"
#include "utils/date.h"

static void monitor_get_sum_all_onetypenode_onedb(Relation rel_node, char *sqlstr, char *dbname
								, char nodetype, int64 iarray[], int len);
static int64 monitor_standbydelay(char nodetype);
static int64 monitor_all_typenode_usedbname_locksnum(Relation rel_node, char *sqlstr, char *dbname
		, char nodetype, int gettype);

#define SQLSTRSTANDBYDELAY  "select CASE WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0 ELSE  " \
	"round(EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp())) end;"

typedef enum ResultChoice
{
	GET_MIN = 0,
	GET_MAX,
	GET_SUM
}ResultChoice;

/*see the content of adbmgr_init.sql: "insert into pg_catalog.monitor_host_threshold"
* the values are the same in adbmgr_init.sql for given items
*/
typedef enum ThresholdItem
{
	TPS_TIMEINTERVAL = 31,
	LONGTRANS_MINTIME = 32
}ThresholdItem;
/*
* get one value from the given sql
*/
int64 monitor_get_onesqlvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname)
{
	int result = -1;
	StringInfoData resultstrdata;

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, sqlstr, user, address, nodeport, dbname, &resultstrdata);
	if (resultstrdata.len != 0)
	{
		result = atoi(resultstrdata.data);
	}
	pfree(resultstrdata.data);

	return result;
}

/*
* get sql'result just need execute on one coordinator
*/
int64 monitor_get_result_one_node(Relation rel_node, char *sqlstr, char *dbname, char nodetype)
{
	int coordport;
	int agentport;
	int64 ret;
	char *hostaddress = NULL;
	char *user = NULL;

	monitor_get_one_node_user_address_port(rel_node, &agentport, &user, &hostaddress, &coordport, nodetype);
	if (!user)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
			,errmsg("cannot get user name in host table")));
	if (!hostaddress)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
			,errmsg("cannot get IP address in host table")));
	ret = monitor_get_onesqlvalue_one_node(agentport, sqlstr, user, hostaddress, coordport, dbname);
	pfree(user);
	pfree(hostaddress);

	return ret;
}

int64 monitor_get_sqlres_all_typenode_usedbname(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int gettype)
{
	/*get datanode master user, port*/
	TableScanDesc rel_scan;
	ScanKeyData key[4];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	char *user;
	char *address;
	int port;
	int64 result = 0;
	int64 resulttmp = 0;
	int agentport;
	bool bfirst = true;

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
	ScanKeyInit(&key[3]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	rel_scan = table_beginscan_catalog(rel_node, 4, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		port = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		/*get agent port*/
		tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
		if(!(HeapTupleIsValid(tup)))
		{
			ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errcode(ERRCODE_INTERNAL_ERROR)));
		}
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		agentport = mgr_host->hostagentport;
		ReleaseSysCache(tup);
		resulttmp = monitor_get_onesqlvalue_one_node(agentport, sqlstr, user, address, port, dbname);
		if(bfirst && gettype==GET_MIN) result = resulttmp;
		bfirst = false;
		switch(gettype)
		{
			case GET_MIN:
				if(resulttmp < result) result = resulttmp;
				break;
			case GET_MAX:
				if(resulttmp>result) result = resulttmp;
				break;
			case GET_SUM:
				result = result + resulttmp;
				break;
			default:
				result = 0;
				break;
		};
		pfree(user);
		pfree(address);
	}
	heap_endscan(rel_scan);

	return result;
}

Datum monitor_databaseitem_insert_data(PG_FUNCTION_ARGS)
{
	char *user = NULL;
	char *hostaddress = NULL;
	char *dbname;
	int coordport = 0;
	int64 dbsize = 0;
	int64 heaphit = 0;
	int64 heapread = 0;
	int64 commit = 0;
	int64 rollback = 0;
	int64 preparenum = 0;
	int64 unusedindexnum = 0;
	int64 locksnum = 0;
	int64 longquerynum = 0;
	int64 idlequerynum = 0;
	int64 connectnum = 0;
	bool bautovacuum = false;
	bool barchive = false;
	bool bfrist = true;
	int64 dbage = 0;
	int64 standbydelay = 0;
	int64 indexsize = 0;
	int longtransmintime = 100;
	int iloop = 0;
	int64 iarray_heaphit_read_indexsize[3] = {0,0,0};
	int64 iarray_commit_connect_longidle_prepare[6] = {0, 0, 0, 0, 0, 0};
	int agentport;
	float heaphitrate = 0;
	float commitrate = 0;
	List *dbnamelist = NIL;
	ListCell *cell;
	HeapTuple tuple;
	TimestampTz time;
	Relation rel;
	Relation rel_node;
	StringInfoData sqldbsizeStrData;
	StringInfoData sqldbageStrData;
	StringInfoData sqllocksStrData;
	StringInfoData sqlstr_heaphit_read_indexsize;
	StringInfoData sqlstr_commit_connect_longidle_prepare;
	char *sqlunusedindex = "select count(*) from  pg_stat_user_indexes where idx_scan = 0";
	const char *clustertime = NULL;
	Monitor_Threshold monitor_threshold;

	rel = table_open(MdatabaseitemRelationId, RowExclusiveLock);
	rel_node = table_open(NodeRelationId, RowExclusiveLock);
	/*get database list*/
	monitor_get_one_node_user_address_port(rel_node, &agentport, &user, &hostaddress, &coordport, CNDN_TYPE_GTM_COOR_MASTER);
	if (!user)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
			,errmsg("cannot get user name in host table")));
	if (!hostaddress)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
			,errmsg("cannot get IP address in host table")));
	dbnamelist = monitor_get_dbname_list(user, hostaddress, coordport);
	if(dbnamelist == NULL)
	{
		pfree(user);
		pfree(hostaddress);
		table_close(rel, RowExclusiveLock);
		table_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get database namelist error")));
	}
	time = GetCurrentTimestamp();
	/*get long transaction min time from table: MonitorHostThresholdRelationId*/
	get_threshold(LONGTRANS_MINTIME, &monitor_threshold);
	if (monitor_threshold.threshold_warning !=0)
	{
		longtransmintime = monitor_threshold.threshold_warning;
	}
	initStringInfo(&sqldbsizeStrData);
	initStringInfo(&sqldbageStrData);
	initStringInfo(&sqllocksStrData);
	initStringInfo(&sqlstr_heaphit_read_indexsize);
	initStringInfo(&sqlstr_commit_connect_longidle_prepare);
	foreach(cell, dbnamelist)
	{
		dbname = (char *)(lfirst(cell));
		/* get database size on coordinator*/
		appendStringInfo(&sqldbsizeStrData, "select round(pg_database_size(datname)::numeric(18,4)/1024/1024) from pg_database where datname=\'%s\';", dbname);
		/*dbsize, unit MB*/
		dbsize = monitor_get_result_one_node(rel_node, sqldbsizeStrData.data, DEFAULT_DB, CNDN_TYPE_GTM_COOR_MASTER);
		for (iloop=0; iloop<3; iloop++)
		{
			iarray_heaphit_read_indexsize[iloop] = 0;
		}
		/*heaphit, heapread, indexsize*/
		/*indexsize: now user "select 1.0" instead of right sql as below because the index
		*size is too large and used too much time more then ten minutes.
		*
		* select round(sum(pg_catalog.pg_indexes_size(c.oid))::numeric(18,4)/1024/1024) from
		*pg_catalog.pg_class c  WHERE c.relkind = 'r' or c.relkind = 't';
		*/
		appendStringInfoString(&sqlstr_heaphit_read_indexsize, "select sum(heap_blks_hit/100.0)::bigint from pg_statio_user_tables union all select sum(heap_blks_read/100.0)::bigint from pg_statio_user_tables union all select 1.0");
		monitor_get_sum_all_onetypenode_onedb(rel_node, sqlstr_heaphit_read_indexsize.data, dbname, CNDN_TYPE_DATANODE_MASTER, iarray_heaphit_read_indexsize, 3);

		heaphit = iarray_heaphit_read_indexsize[0];
		heapread = iarray_heaphit_read_indexsize[1];
		if((heaphit + heapread) == 0)
			heaphitrate = 100;
		else
			heaphitrate = (heaphit*1.0/(heaphit + heapread))*100.0;
		/*the database index size, unit: MB */
		indexsize = iarray_heaphit_read_indexsize[2];

		clustertime = timestamptz_to_str(GetCurrentTimestamp());
		mthreshold_levelvalue_impositiveseq(OBJECT_CLUSTER_HEAPHIT, MONITOR_CLUSTERSTR, clustertime
				, heaphitrate, "heaphit rate");

		/*get all coordinators' result then sum them*/
		/*
		* xact_commit, xact_rollback, numbackends, longquerynum, idlequerynum, preparednum
		*/
		for (iloop=0; iloop<6; iloop++)
		{
			iarray_commit_connect_longidle_prepare[iloop] = 0;
		}
		appendStringInfo(&sqlstr_commit_connect_longidle_prepare,"select (xact_commit/100.0)::bigint from pg_stat_database where datname = \'%s\' union all select (xact_rollback/100.0)::bigint from pg_stat_database where datname = \'%s\' union all select numbackends from pg_stat_database where datname = \'%s\' union all select count(*) from  pg_stat_activity where extract(epoch from (query_start-now())) > %d and datname=\'%s\' union all select count(*) from pg_stat_activity where state='idle' and datname = \'%s\' union all select count(*) from pg_prepared_xacts where database= \'%s\';", dbname, dbname, dbname, longtransmintime, dbname, dbname, dbname);
		monitor_get_sum_all_onetypenode_onedb(rel_node, sqlstr_commit_connect_longidle_prepare.data, dbname, CNDN_TYPE_GTM_COOR_MASTER, iarray_commit_connect_longidle_prepare, 6);

		/*xact_commit_rate on coordinator*/
		commit = iarray_commit_connect_longidle_prepare[0];
		rollback = iarray_commit_connect_longidle_prepare[1];
		if((commit + rollback) == 0)
			commitrate = 100;
		else
			commitrate = (commit*1.0/(commit + rollback))*100.0;
		/*connect num*/
		connectnum = iarray_commit_connect_longidle_prepare[2];
		/*get long query num on coordinator*/
		longquerynum = iarray_commit_connect_longidle_prepare[3];
		idlequerynum = iarray_commit_connect_longidle_prepare[4];
		/*prepare query num on coordinator*/
		preparenum = iarray_commit_connect_longidle_prepare[5];


		/*unused index on datanode master, get min
		* " select count(*) from  pg_stat_user_indexes where idx_scan = 0"  on one database, get min on every dn master
		*/
		unusedindexnum = monitor_get_sqlres_all_typenode_usedbname(rel_node, sqlunusedindex
				, dbname, CNDN_TYPE_DATANODE_MASTER, GET_MIN);

		/*get locks on coordinator, get max*/
		appendStringInfo(&sqllocksStrData, "select count(*) from pg_locks ,pg_database where pg_database.Oid = pg_locks.database and pg_database.datname=\'%s\';", dbname);
		locksnum = monitor_all_typenode_usedbname_locksnum(rel_node, sqllocksStrData.data
				, dbname, CNDN_TYPE_GTM_COOR_MASTER, GET_MAX);

		/* check warning threshold */
		clustertime = timestamptz_to_str(GetCurrentTimestamp());
		mthreshold_levelvalue_impositiveseq(OBJECT_CLUSTER_COMMITRATE, MONITOR_CLUSTERSTR
				, clustertime, commitrate, "commit rate");
		mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LONGTRANS, MONITOR_CLUSTERSTR, clustertime
				, longquerynum, "long transactions");
		mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LONGTRANS, MONITOR_CLUSTERSTR, clustertime
				, idlequerynum, "idle transactions");
		mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_CONNECT, MONITOR_CLUSTERSTR, clustertime
				, connectnum, "connect");
		mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LOCKS, MONITOR_CLUSTERSTR, clustertime
				, locksnum, "locks");
		mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_UNUSEDINDEX, MONITOR_CLUSTERSTR, clustertime
				, unusedindexnum, "unused index");
		mthreshold_levelvalue_positiveseq(OBJECT_CLUSTER_LOCKS, MONITOR_CLUSTERSTR, clustertime
				, locksnum, "locks");

		/*autovacuum*/
		if(bfrist)
		{
			/*these vars just need get one time, from coordinator*/
			char *sqlstr_vacuum_archive_dbage = "select case when setting = \'on\' then 1 else 0 end from pg_settings where name=\'autovacuum\' union all select case when setting = \'on\' then 1 else 0 end from pg_settings where name=\'archive_mode\'";
			int64 iarray_vacuum_archive_dbage[2] = {0,0};
			monitor_get_sqlvalues_one_node(agentport, sqlstr_vacuum_archive_dbage, user, hostaddress, coordport,DEFAULT_DB, iarray_vacuum_archive_dbage, 2);

			bautovacuum = (iarray_vacuum_archive_dbage[0] == 0 ? false:true);
			barchive = (iarray_vacuum_archive_dbage[1] == 0 ? false:true);

			/*standby delay*/
			standbydelay = monitor_standbydelay(CNDN_TYPE_DATANODE_SLAVE);
		}

		/* get database transaction age on coordinator*/
		appendStringInfo(&sqldbageStrData, "select age(datfrozenxid) from pg_database where datname=\'%s\';", dbname);
		/*dbsize, unit MB*/
		dbage = monitor_get_result_one_node(rel_node, sqldbageStrData.data, DEFAULT_DB, CNDN_TYPE_GTM_COOR_MASTER);

		/*build tuple*/
		tuple = monitor_build_database_item_tuple(rel, time, dbname, dbsize, barchive, bautovacuum, heaphitrate, commitrate, dbage, connectnum, standbydelay, locksnum, longquerynum, idlequerynum, preparenum, unusedindexnum, indexsize);
		CatalogTupleInsert(rel, tuple);
		heap_freetuple(tuple);
		resetStringInfo(&sqldbsizeStrData);
		resetStringInfo(&sqldbageStrData);
		resetStringInfo(&sqllocksStrData);
		resetStringInfo(&sqlstr_heaphit_read_indexsize);
		resetStringInfo(&sqlstr_commit_connect_longidle_prepare);
		bfrist = false;
	}
	pfree(user);
	pfree(hostaddress);
	pfree(sqldbsizeStrData.data);
	pfree(sqllocksStrData.data);
	pfree(sqlstr_heaphit_read_indexsize.data);
	pfree(sqlstr_commit_connect_longidle_prepare.data);
	list_free(dbnamelist);
	table_close(rel, RowExclusiveLock);
	table_close(rel_node, RowExclusiveLock);
	PG_RETURN_TEXT_P(cstring_to_text("insert_data"));
}

/*
* build tuple for table: monitor_databasetps, see: monitor_databasetps.h
*/
HeapTuple monitor_build_database_item_tuple(Relation rel, const TimestampTz time, char *dbname
			, int64 dbsize, bool archive, bool autovacuum, float heaphitrate,  float commitrate, int64 dbage, int64 connectnum, int64 standbydelay, int64 locksnum, int64 longquerynum, int64 idlequerynum, int64 preparenum, int64 unusedindexnum, int64 indexsize)
{
	Datum datums[16];
	bool nulls[16];
	TupleDesc desc;
	NameData name;
	int idex = 0;

	desc = RelationGetDescr(rel);
	namestrcpy(&name, dbname);
	AssertArg(desc && desc->natts == 16
		&& TupleDescAttr(desc, 0)->atttypid == TIMESTAMPTZOID
		&& TupleDescAttr(desc, 1)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 2)->atttypid == INT8OID
		&& TupleDescAttr(desc, 3)->atttypid == BOOLOID
		&& TupleDescAttr(desc, 4)->atttypid == BOOLOID
		&& TupleDescAttr(desc, 5)->atttypid == FLOAT4OID
		&& TupleDescAttr(desc, 6)->atttypid == FLOAT4OID
		&& TupleDescAttr(desc, 7)->atttypid == INT8OID
		&& TupleDescAttr(desc, 8)->atttypid == INT8OID
		&& TupleDescAttr(desc, 9)->atttypid == INT8OID
		&& TupleDescAttr(desc, 10)->atttypid == INT8OID
		&& TupleDescAttr(desc, 11)->atttypid == INT8OID
		&& TupleDescAttr(desc, 12)->atttypid == INT8OID
		&& TupleDescAttr(desc, 13)->atttypid == INT8OID
		&& TupleDescAttr(desc, 14)->atttypid == INT8OID
		&& TupleDescAttr(desc, 15)->atttypid == INT8OID
		);
	memset(datums, 0, sizeof(datums));
	memset(nulls, 0, sizeof(nulls));
	datums[0] = TimestampTzGetDatum(time);
	datums[1] = NameGetDatum(&name);
	datums[2] = Int64GetDatum(dbsize);
	datums[3] = BoolGetDatum(archive);
	datums[4] = BoolGetDatum(autovacuum);
	datums[5] = Float4GetDatum(heaphitrate);
	datums[6] = Float4GetDatum(commitrate);
	datums[7] = Int64GetDatum(dbage);
	datums[8] = Int64GetDatum(connectnum);
	datums[9] = Int64GetDatum(standbydelay);
	datums[10] = Int64GetDatum(locksnum);
	datums[11] = Int64GetDatum(longquerynum);
	datums[12] = Int64GetDatum(idlequerynum);
	datums[13] = Int64GetDatum(preparenum);
	datums[14] = Int64GetDatum(unusedindexnum);
	datums[15] = Int64GetDatum(indexsize);

	for (idex=0; idex<sizeof(nulls); idex++)
		nulls[idex] = false;

	return heap_form_tuple(desc, datums, nulls);
}


/*for table: monitor_databasetps, see monitor_databasetps.h*/


/*
* get database tps, qps in cluster then insert them to table
*/
Datum monitor_databasetps_insert_data(PG_FUNCTION_ARGS)
{
	Relation rel;
	Relation rel_node;
	TimestampTz time;
	int64 pgdbruntime;
	HeapTuple tup_result;
	List *dbnamelist = NIL;
	ListCell *cell;
	int64 **dbtps = NULL;
	int64 **dbqps = NULL;
	int64 tps = 0;
	int64 qps = 0;
	int64 dbnum = 0;
	int coordport = 0;
	int iloop = 0;
	int idex = 0;
	int sleepTime = 3;
	int agentport = 0;
	const int ncol = 2;
	char *user = NULL;
	char *hostaddress = NULL;
	char *dbname = NULL;
	StringInfoData sqltpsStrData;
	StringInfoData sqlqpsStrData;
	StringInfoData sqldbruntimeStrData;
	Monitor_Threshold monitor_threshold;

	rel = table_open(MdatabasetpsRelationId, RowExclusiveLock);
	rel_node = table_open(NodeRelationId, RowExclusiveLock);
	/*get user, address, port of coordinator*/
	monitor_get_one_node_user_address_port(rel_node, &agentport, &user, &hostaddress, &coordport, CNDN_TYPE_GTM_COOR_MASTER);
	if (!user)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
			,errmsg("cannot get user name in host table")));
	if (!hostaddress)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
			,errmsg("cannot get IP address in host table")));
	/*get database namelist*/
	dbnamelist = monitor_get_dbname_list(user, hostaddress, coordport);
	pfree(user);
	pfree(hostaddress);
	if(dbnamelist == NULL)
	{
		table_close(rel, RowExclusiveLock);
		table_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get database namelist error")));
	}
	dbnum = list_length(dbnamelist);
	Assert(dbnum > 0);
	dbtps = (int64 **)palloc(sizeof(int64 *)*dbnum);
	dbqps = (int64 **)palloc(sizeof(int64 *)*dbnum);
	iloop = 0;
	while(iloop < dbnum)
	{
		dbtps[iloop] = (int64 *)palloc(sizeof(int64)*ncol);
		dbqps[iloop] = (int64 *)palloc(sizeof(int64)*ncol);
		iloop++;
	}

	initStringInfo(&sqltpsStrData);
	initStringInfo(&sqlqpsStrData);
	initStringInfo(&sqldbruntimeStrData);
	time = GetCurrentTimestamp();

	iloop = 0;
	/*get tps timeinterval from table:MonitorHostThresholdRelationId*/
	get_threshold(TPS_TIMEINTERVAL, &monitor_threshold);
	if(monitor_threshold.threshold_warning != 0)
		sleepTime = monitor_threshold.threshold_warning;
	while(iloop<ncol)
	{
		idex = 0;
		foreach(cell, dbnamelist)
		{
			dbname = (char *)(lfirst(cell));
			appendStringInfo(&sqltpsStrData, "select xact_commit+xact_rollback from pg_stat_database where datname = \'%s\';",  dbname);
			appendStringInfo(&sqlqpsStrData, "select sum(calls)from pg_stat_statements, pg_database where dbid = pg_database.oid and pg_database.datname=\'%s\';",  dbname);
			/*get given database tps first*/
			dbtps[idex][iloop] = (monitor_get_sqlres_all_typenode_usedbname(rel_node, sqltpsStrData.data, DEFAULT_DB, CNDN_TYPE_GTM_COOR_MASTER, GET_SUM)
								+ monitor_get_sqlres_all_typenode_usedbname(rel_node, sqltpsStrData.data, DEFAULT_DB, CNDN_TYPE_COORDINATOR_MASTER, GET_SUM));
			/*get given database qps first*/
			dbqps[idex][iloop] = (monitor_get_sqlres_all_typenode_usedbname(rel_node, sqlqpsStrData.data, DEFAULT_DB, CNDN_TYPE_GTM_COOR_MASTER, GET_SUM)
								+ monitor_get_sqlres_all_typenode_usedbname(rel_node, sqlqpsStrData.data, DEFAULT_DB, CNDN_TYPE_COORDINATOR_MASTER, GET_SUM));;
			resetStringInfo(&sqltpsStrData);
			resetStringInfo(&sqlqpsStrData);
			idex++;
		}
		iloop++;
		if(iloop < ncol)
			sleep(sleepTime);
	}
	/*insert data*/
	idex = 0;
	foreach(cell, dbnamelist)
	{
		dbname = (char *)(lfirst(cell));
		tps = labs(dbtps[idex][1] - dbtps[idex][0])/sleepTime;
		qps = labs(dbqps[idex][1] - dbqps[idex][0])/sleepTime;
		appendStringInfo(&sqldbruntimeStrData, "select case when  stats_reset IS NULL then  0 else  round(abs(extract(epoch from now())- extract(epoch from  stats_reset))) end from pg_stat_database where datname = \'%s\';", dbname);
		pgdbruntime = monitor_get_result_one_node(rel_node, sqldbruntimeStrData.data, DEFAULT_DB, CNDN_TYPE_GTM_COOR_MASTER);
		tup_result = monitor_build_databasetps_qps_tuple(rel, time, dbname, tps, qps, pgdbruntime);
		CatalogTupleInsert(rel, tup_result);
		heap_freetuple(tup_result);
		resetStringInfo(&sqldbruntimeStrData);
		idex++;
	}
	/*pfree dbtps, dbqps*/
	iloop = 0;
	while(iloop < dbnum)
	{
		pfree((int *)dbtps[iloop]);
		pfree((int *)dbqps[iloop]);
		iloop++;
	}
	pfree(dbtps);
	pfree(dbqps);
	pfree(sqltpsStrData.data);
	pfree(sqlqpsStrData.data);
	pfree(sqldbruntimeStrData.data);
	list_free(dbnamelist);
	table_close(rel, RowExclusiveLock);
	table_close(rel_node, RowExclusiveLock);
	PG_RETURN_TEXT_P(cstring_to_text("insert_data"));
}
/*
* build tuple for table: monitor_databasetps, see: monitor_databasetps.h
*/
HeapTuple monitor_build_databasetps_qps_tuple(Relation rel, const TimestampTz time, const char *dbname, const int64 tps, const int64 qps, int64 pgdbruntime)
{
	Datum datums[5];
	bool nulls[5];
	TupleDesc desc;
	NameData name;

	desc = RelationGetDescr(rel);
	namestrcpy(&name, dbname);
	AssertArg(desc && desc->natts == 5
		&& TupleDescAttr(desc, 0)->atttypid == TIMESTAMPTZOID
		&& TupleDescAttr(desc, 1)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 2)->atttypid == INT8OID
		&& TupleDescAttr(desc, 3)->atttypid == INT8OID
		&& TupleDescAttr(desc, 4)->atttypid == INT8OID
		);
	memset(datums, 0, sizeof(datums));
	memset(nulls, 0, sizeof(nulls));
	datums[0] = TimestampTzGetDatum(time);
	datums[1] = NameGetDatum(&name);
	datums[2] = Int64GetDatum(tps);
	datums[3] = Int64GetDatum(qps);
	datums[4] = Int64GetDatum(pgdbruntime);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = false;

	return heap_form_tuple(desc, datums, nulls);
}
/*get the sqlstr values from given sql on given typenode, then sum them
* for example: we want to get caculate cmmitrate, which on coordinators, we need get sum
* commit on all coordinators and sum rollback on all coordinators. the input parameters: len
* is the num we want, iarray is the values restore.
*/
static void monitor_get_sum_all_onetypenode_onedb(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int64 iarray[], int len)
{
	/*get node user, port*/
	TableScanDesc rel_scan = NULL;
	ScanKeyData key[4];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	char *user = NULL;
	char *address = NULL;
	char *nodetime = NULL;
	int port;
	int agentport = 0;
	int64 *iarraytmp;
	int iloop = 0;
	bool bfirst = true;

	iarraytmp = (int64 *)palloc(sizeof(int64)*len);

	PG_TRY();
	{
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
		ScanKeyInit(&key[3]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(mgr_zone));
		rel_scan = table_beginscan_catalog(rel_node, 4, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			port = mgr_node->nodeport;
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			if(bfirst)
			{
				user = get_hostuser_from_hostoid(mgr_node->nodehost);
			}
			bfirst = false;
			memset(iarraytmp, 0, len*sizeof(int64));
			/*get agent port*/
			tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
			if(!(HeapTupleIsValid(tup)))
			{
				ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
					, errcode(ERRCODE_INTERNAL_ERROR)));
			}
			mgr_host = (Form_mgr_host)GETSTRUCT(tup);
			Assert(mgr_host);
			agentport = mgr_host->hostagentport;
			ReleaseSysCache(tup);
			monitor_get_sqlvalues_one_node(agentport, sqlstr, user, address, port,dbname, iarraytmp, len);
			for(iloop=0; iloop<len; iloop++)
			{
				iarray[iloop] += iarraytmp[iloop];
			}

			/* check warn threshold */
			if ((nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_GTM_COOR_MASTER) && len == 6)
			{
				nodetime = monitor_get_timestamptz_onenode(agentport, user, address, port);
				if(nodetime == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
						,errmsg("get time from node error,")));
				}
				mthreshold_levelvalue_impositiveseq(OBJECT_NODE_COMMITRATE, address, nodetime
					, iarraytmp[0]+iarraytmp[1] == 0 ? 100 : (iarraytmp[0]*1.0/(iarraytmp[0]+iarraytmp[1])*100.0)
					,  "commit rate");
				mthreshold_levelvalue_positiveseq(OBJECT_NODE_CONNECT, address, nodetime
					, iarraytmp[2], "connect");
				mthreshold_levelvalue_positiveseq(OBJECT_NODE_LONGTRANS, address, nodetime
					, iarraytmp[3], "long transactions");
				mthreshold_levelvalue_positiveseq(OBJECT_NODE_LONGTRANS, address, nodetime
					, iarraytmp[4], "idle transactions");
				pfree(nodetime);
			}
			else if (nodetype == CNDN_TYPE_DATANODE_MASTER && len == 3)
			{
				nodetime = monitor_get_timestamptz_onenode(agentport, user, address, port);
				if(nodetime == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
						,errmsg("get time from node error,")));
				}
				mthreshold_levelvalue_impositiveseq(OBJECT_NODE_HEAPHIT, address, nodetime
					, iarraytmp[0]+iarraytmp[1] == 0 ? 100 : (iarraytmp[0]*1.0/(iarraytmp[0]+iarraytmp[1])*100.0)
					,  "heaphit rate");
				pfree(nodetime);
			}

			pfree(address);
		}
	}PG_CATCH();
	{
		if(user)
			pfree(user);
		if (rel_scan)
			heap_endscan(rel_scan);
		pfree(iarraytmp);

		PG_RE_THROW();
	}PG_END_TRY();

	if(user)
	{
		pfree(user);
	}
	heap_endscan(rel_scan);
	pfree(iarraytmp);
}

void monitor_get_stringvalues(char cmdtype, int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname, StringInfo resultstrdata)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg;
	StringInfoData buf;

	resetStringInfo(resultstrdata);
	initStringInfo(&sendstrmsg);
	/*sequence:user port dbname sqlstr, delimiter by '\0'*/
	/*user*/
	appendStringInfoString(&sendstrmsg, user);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	/*port*/
	appendStringInfo(&sendstrmsg, "%d", nodeport);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	/*dbname*/
	appendStringInfoString(&sendstrmsg, dbname);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	/*sqlstring*/
	appendStringInfoString(&sendstrmsg, sqlstr);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	ma = ma_connect(address, (unsigned short)agentport);
	if(!ma_isconnected(ma))
	{
		/*report error message */
		ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION)
			,errmsg("%s", ma_last_error_msg(ma))));
		ma_close(ma);
		return;
	}
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION)
			,errmsg("%s", ma_last_error_msg(ma))));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	mgr_recv_sql_stringvalues_msg(ma, resultstrdata);
	ma_close(ma);
	if (resultstrdata->data == NULL)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("get sqlstr:%s \n\tresult fail", sqlstr)));
		return;
	}
}

/*
 * get the largest delay time from given standby nodes
 */
static int64 monitor_standbydelay(char nodetype)
{
	Relation hostrel;
	Relation noderel;
	TableScanDesc hostrel_scan;
	TableScanDesc noderel_scan;
	Form_mgr_host mgr_host;
	Form_mgr_node mgr_node;
	HeapTuple hosttuple;
	HeapTuple nodetuple;
	HeapTuple tup;
	bool isNull = false;
	Datum datumaddress;
	Oid hostoid;
	ScanKeyData key[2];
	int64 nodedelay = 0;
	int64 maxdelay = 0;
	int clusterstandbydelay = 0;
	int port = 0;
	int agentport = 0;
	NameData ndatauser;
	char *address;
	char *nodetime;
	const char *clustertime;
	//bool getnode = false;

	hostrel = table_open(HostRelationId, AccessShareLock);
	hostrel_scan = table_beginscan_catalog(hostrel, 0, NULL);
	noderel = table_open(NodeRelationId, AccessShareLock);

	PG_TRY();
	{
		while((hosttuple = heap_getnext(hostrel_scan, ForwardScanDirection)) != NULL)
		{
			//getnode = false;
			mgr_host = (Form_mgr_host)GETSTRUCT(hosttuple);
			Assert(mgr_host);
			datumaddress = heap_getattr(hosttuple, Anum_mgr_host_hostaddr, RelationGetDescr(hostrel), &isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
					, errmsg("column hostaddress is null")));
			}
			address = TextDatumGetCString(datumaddress);
			namestrcpy(&ndatauser, NameStr(mgr_host->hostuser));
			hostoid = mgr_host->oid;
			/*find datanode master in node systbl, which hosttuple's nodehost is hostoid*/
			ScanKeyInit(&key[0]
				,Anum_mgr_node_nodehost
				,BTEqualStrategyNumber, F_OIDEQ
				,ObjectIdGetDatum(hostoid));
			ScanKeyInit(&key[1]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
			noderel_scan = table_beginscan_catalog(noderel, 2, key);
			while((nodetuple = heap_getnext(noderel_scan, ForwardScanDirection)) != NULL)
			{
				mgr_node = (Form_mgr_node)GETSTRUCT(nodetuple);
				Assert(mgr_node);
				/*check the nodetype*/
				if (mgr_node->nodetype != CNDN_TYPE_DATANODE_SLAVE)
					continue;
				/*get port*/
				port = mgr_node->nodeport;
				/*get agent port*/
				tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
				if(!(HeapTupleIsValid(tup)))
				{
					ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
						, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
						, errcode(ERRCODE_INTERNAL_ERROR)));
				}
				mgr_host = (Form_mgr_host)GETSTRUCT(tup);
				Assert(mgr_host);
				agentport = mgr_host->hostagentport;
				ReleaseSysCache(tup);
				nodedelay = monitor_get_onesqlvalue_one_node(agentport, SQLSTRSTANDBYDELAY, ndatauser.data
									, address, port, DEFAULT_DB);
				if (nodedelay > maxdelay)
					maxdelay = nodedelay;
				clusterstandbydelay = clusterstandbydelay + nodedelay;

				nodetime = monitor_get_timestamptz_onenode(agentport, ndatauser.data, address, port);
				if(nodetime == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
						,errmsg("get time from node error,")));
				}
				mthreshold_levelvalue_positiveseq(OBJECT_NODE_STANDBYDELAY, address, nodetime
						, nodedelay, "standby delay");
				pfree(nodetime);
			}
			heap_endscan(noderel_scan);
		}
	}PG_CATCH();
	{
		heap_endscan(hostrel_scan);
		table_close(hostrel, AccessShareLock);
		table_close(noderel, AccessShareLock);

		PG_RE_THROW();
	}PG_END_TRY();

	heap_endscan(hostrel_scan);
	table_close(hostrel, AccessShareLock);
	table_close(noderel, AccessShareLock);
	/*check cluster*/
	clustertime = timestamptz_to_str(GetCurrentTimestamp());
	mthreshold_levelvalue_positiveseq(OBJECT_NODE_STANDBYDELAY, MONITOR_CLUSTERSTR, clustertime
				, clusterstandbydelay, "standby delay");

	return maxdelay;
}

static int64 monitor_all_typenode_usedbname_locksnum(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int gettype)
{
	/*get datanode master user, port*/
	TableScanDesc rel_scan;
	ScanKeyData key[4];
	HeapTuple tuple;
	HeapTuple tup;
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	char *user;
	char *address;
	char *nodetime;
	int port;
	int64 result = 0;
	int64 resulttmp = 0;
	int agentport;
	bool bfirst = true;

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
	ScanKeyInit(&key[3]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	rel_scan = table_beginscan_catalog(rel_node, 4, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		port = mgr_node->nodeport;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		/*get agent port*/
		tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node->nodehost));
		if(!(HeapTupleIsValid(tup)))
		{
			heap_endscan(rel_scan);
			ereport(ERROR, (errmsg("host oid \"%u\" not exist", mgr_node->nodehost)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errcode(ERRCODE_INTERNAL_ERROR)));
		}
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		agentport = mgr_host->hostagentport;
		ReleaseSysCache(tup);
		resulttmp = monitor_get_onesqlvalue_one_node(agentport, sqlstr, user, address, port, dbname);

		/* check warning threshold */
		nodetime = monitor_get_timestamptz_onenode(agentport, user, address, port);
		if(nodetime == NULL)
		{
			heap_endscan(rel_scan);
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("get time from node error,")));
		}
		mthreshold_levelvalue_positiveseq(OBJECT_NODE_LOCKS, address, nodetime, resulttmp, "locks");
		pfree(nodetime);

		if(bfirst && gettype==GET_MIN) result = resulttmp;
		bfirst = false;
		switch(gettype)
		{
			case GET_MIN:
				if(resulttmp < result) result = resulttmp;
				break;
			case GET_MAX:
				if(resulttmp>result) result = resulttmp;
				break;
			case GET_SUM:
				result = result + resulttmp;
				break;
			default:
				result = 0;
				break;
		};
		pfree(user);
		pfree(address);
	}
	heap_endscan(rel_scan);

	return result;
}
