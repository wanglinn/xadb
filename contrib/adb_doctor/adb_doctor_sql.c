/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/builtins.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "adb_doctor_sql.h"
#include "utils/formatting.h"

static void wrapMgrNode(HeapTuple tuple, TupleDesc tupdesc, AdbMgrNodeWrapper *wrapper);
static void wrapMgrHost(HeapTuple tuple, TupleDesc tupdesc, AdbMgrHostWrapper *wrapper);

void SPI_updateAdbDoctorConf(char *key, char *value)
{
	StringInfoData buf;
	int ret;
	char *key_lower;
	/* k must be lowercase string */
	key_lower = asc_tolower(key, strlen(key));
	initStringInfo(&buf);
	appendStringInfo(&buf, "update %s.%s set %s = '%s' where %s = '%s'",
					 ADB_DOCTOR_SCHEMA,
					 ADB_DOCTOR_CONF_RELNAME,
					 ADB_DOCTOR_CONF_ATTR_VALUE,
					 value,
					 ADB_DOCTOR_CONF_ATTR_KEY,
					 key_lower);
	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);
	pfree(key_lower);

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));

	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: expected the number of rows:%d, but actual:%lu", 1, SPI_processed)));
}

/*
 * The result is returned in memory allocated using palloc.
 * You can use pfree to release the memory when you don't need it anymore.
 */
char *SPI_selectAdbDoctConfByKey(char *key)
{
	char *v;
	StringInfoData buf;
	int ret;
	uint64 rows;

	initStringInfo(&buf);
	appendStringInfo(&buf, "select %s from %s.%s where %s = '%s'",
					 ADB_DOCTOR_CONF_ATTR_VALUE,
					 ADB_DOCTOR_SCHEMA,
					 ADB_DOCTOR_CONF_RELNAME,
					 ADB_DOCTOR_CONF_ATTR_KEY,
					 key);
	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));
	rows = SPI_processed;
	if (rows == 1 && SPI_tuptable != NULL)
	{
		v = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		return v;
	}
	else
	{
		return NULL;
	}
}
static int SPI_selectAdbDoctorConfInt(char *key)
{
	char *valueStr = SPI_selectAdbDoctConfByKey(key);
	int value = pg_atoi(valueStr, sizeof(int), 0);
	pfree(valueStr);
	return value;
}

void SPI_selectAdbDoctorConfAll(AdbDoctorConf **confP, MemoryContext ctx)
{
	int datalevel = SPI_selectAdbDoctorConfInt(ADB_DOCTOR_CONF_KEY_DATALEVEL);
	int probeinterval = SPI_selectAdbDoctorConfInt(ADB_DOCTOR_CONF_KEY_PROBEINTERVAL);
	int agentdeadline = SPI_selectAdbDoctorConfInt(ADB_DOCTOR_CONF_KEY_AGENTDEADLINE);

	MemoryContext oldCtx = MemoryContextSwitchTo(ctx);

	/* do outside the spi memory context because the spi will be freed. */
	AdbDoctorConf *conf = palloc0(sizeof(AdbDoctorConf));
	conf->datalevel = datalevel;
	conf->probeinterval = probeinterval;
	conf->agentdeadline = agentdeadline;
	safeAdbDoctorConf(conf);
	*confP = conf;

	MemoryContextSwitchTo(oldCtx);
}

AdbDoctorList *SPI_selectMgrNodeForMonitor(MemoryContext ctx)
{
	AdbDoctorList *list;
	AdbDoctorLink *link;
	AdbDoctorNodeData *data;
	AdbMgrNodeWrapper *wrapper;
	uint64 rows, j;
	int ret;
	HeapTuple tuple;
	TupleDesc tupdesc;
	StringInfoData buf;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT t1.*,t1.oid,t2.hostuser,t2.hostaddr \n"
					 " FROM pg_catalog.mgr_node t1 \n"
					 " LEFT JOIN pg_catalog.mgr_host t2 ON t1.nodehost = t2.oid \n"
					 " WHERE t1.nodeinited = %d::boolean \n"
					 " AND t1.nodeincluster = %d::boolean \n"
					 " AND t1.allowcure = %d::boolean \n"
					 " AND t1.curestatus in ('%s', '%s') \n",
					 true,
					 true,
					 true,
					 CURE_STATUS_NORMAL,
					 CURE_STATUS_CURING);

	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));

	oldCtx = MemoryContextSwitchTo(ctx);

	rows = SPI_processed;
	if (rows > 0 && SPI_tuptable != NULL)
	{
		/* must palloc here, do not palloc in spi context. */
		list = newAdbDoctorList();

		tupdesc = SPI_tuptable->tupdesc;
		for (j = 0; j < rows; j++)
		{
			tuple = SPI_tuptable->vals[j];

			wrapper = palloc0(sizeof(AdbMgrNodeWrapper));
			wrapMgrNode(tuple, tupdesc, wrapper);

			data = palloc0(sizeof(AdbDoctorNodeData));
			data->header.type = ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR;
			data->wrapper = wrapper;

			link = newAdbDoctorLink(data, (void (*)(void *))pfreeAdbDoctorBgworkerData);
			dlist_push_tail(&list->head, &link->wi_links);
			list->num++;
		}
	}
	else
	{
		list = NULL;
	}

	MemoryContextSwitchTo(oldCtx);

	return list;
}

AdbDoctorList *SPI_selectMgrNodeForSwitcher(MemoryContext ctx)
{
	AdbDoctorList *list;
	AdbDoctorLink *link;
	AdbDoctorSwitcherData *data;
	AdbMgrNodeWrapper *wrapper;
	uint64 rows, j;
	int ret;
	HeapTuple tuple;
	TupleDesc tupdesc;
	StringInfoData buf;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT t1.*,t1.oid,t2.hostuser,t2.hostaddr \n"
					 " FROM pg_catalog.mgr_node t1 \n"
					 " LEFT JOIN pg_catalog.mgr_host t2 ON t1.nodehost = t2.oid \n"
					 " WHERE t1.nodeinited = %d::boolean \n"
					 " AND t1.nodeincluster = %d::boolean \n"
					 " AND t1.allowcure = %d::boolean \n"
					 " AND t1.curestatus in ('%s', '%s') \n"
					 " AND t1.nodetype in ('%c', '%c', '%c') \n",
					 true,
					 true,
					 true,
					 CURE_STATUS_WAIT_SWITCH,
					 CURE_STATUS_SWITCHING,
					 CNDN_TYPE_COORDINATOR_MASTER,
					 CNDN_TYPE_DATANODE_MASTER,
					 GTM_TYPE_GTM_MASTER);

	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));

	oldCtx = MemoryContextSwitchTo(ctx);

	rows = SPI_processed;
	if (rows > 0 && SPI_tuptable != NULL)
	{
		/* must palloc here, do not palloc in spi context. */
		list = newAdbDoctorList();

		tupdesc = SPI_tuptable->tupdesc;
		for (j = 0; j < rows; j++)
		{
			tuple = SPI_tuptable->vals[j];

			wrapper = palloc0(sizeof(AdbMgrNodeWrapper));
			wrapMgrNode(tuple, tupdesc, wrapper);

			data = palloc0(sizeof(AdbDoctorSwitcherData));
			data->header.type = ADB_DOCTOR_BGWORKER_TYPE_SWITCHER;
			data->wrapper = wrapper;

			link = newAdbDoctorLink(data, (void (*)(void *))pfreeAdbDoctorBgworkerData);
			dlist_push_tail(&list->head, &link->wi_links);
			list->num++;
		}
	}
	else
	{
		list = NULL;
	}

	MemoryContextSwitchTo(oldCtx);

	return list;
}

AdbDoctorHostData *SPI_selectMgrHostForMonitor(MemoryContext ctx)
{
	AdbDoctorHostData *hostData;
	AdbDoctorList *list;
	StringInfoData buf;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "select *  \n"
					 "from pg_catalog.mgr_host \n"
					 "WHERE allowcure = %d::boolean",
					 true);

	list = SPI_selectMgrHost(ctx, buf.data);
	pfree(buf.data);

	oldCtx = MemoryContextSwitchTo(ctx);

	if (list != NULL)
	{
		/* must palloc here, do not palloc in spi context. */
		hostData = palloc0(sizeof(AdbDoctorHostData));
		hostData->header.type = ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR;
		hostData->list = list;
	}
	else
	{
		hostData = NULL;
	}

	MemoryContextSwitchTo(oldCtx);

	return hostData;
}

AdbMgrHostWrapper *SPI_selectMgrHostByOid(MemoryContext ctx, Oid oid)
{
	AdbMgrHostWrapper *wrapper;
	StringInfoData buf;
	HeapTuple tuple;
	TupleDesc tupdesc;
	uint64 rows;
	int ret;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "select *  \n"
					 "from pg_catalog.mgr_host \n"
					 "WHERE oid = %u",
					 oid);

	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));

	rows = SPI_processed;

	oldCtx = MemoryContextSwitchTo(ctx);

	tupdesc = SPI_tuptable->tupdesc;
	if (rows == 1 && SPI_tuptable != NULL)
	{
		tuple = SPI_tuptable->vals[0];
		wrapper = palloc0(sizeof(AdbMgrHostWrapper));
		wrapMgrHost(tuple, tupdesc, wrapper);
	}
	else
	{
		wrapper = NULL;
	}

	MemoryContextSwitchTo(oldCtx);

	return wrapper;
}
/*
 * The result is returned in memory allocated using palloc.
 * You can use pfree to release the memory when you don't need it anymore.
 * you should use pfreeAdbMgrNodeWrapper().
 */
AdbDoctorList *SPI_selectMgrNode(MemoryContext ctx, char *sql)
{
	AdbDoctorList *list;
	AdbDoctorLink *link;
	AdbMgrNodeWrapper *wrapper;
	uint64 rows, j;
	int ret;
	HeapTuple tuple;
	TupleDesc tupdesc;
	MemoryContext oldCtx;

	ret = SPI_execute(sql, false, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));

	rows = SPI_processed;

	oldCtx = MemoryContextSwitchTo(ctx);

	if (rows > 0 && SPI_tuptable != NULL)
	{
		/* must palloc here, do not palloc in spi context. */
		list = newAdbDoctorList();

		tupdesc = SPI_tuptable->tupdesc;
		for (j = 0; j < rows; j++)
		{
			tuple = SPI_tuptable->vals[j];

			wrapper = palloc0(sizeof(AdbMgrNodeWrapper));
			wrapMgrNode(tuple, tupdesc, wrapper);

			link = newAdbDoctorLink(wrapper, (void (*)(void *))pfreeAdbMgrNodeWrapper);
			dlist_push_tail(&list->head, &link->wi_links);
			list->num++;
		}
	}
	else
	{
		list = NULL;
	}

	MemoryContextSwitchTo(oldCtx);

	return list;
}

/*
 * The result is returned in memory allocated using palloc.
 * You can use pfree to release the memory when you don't need it anymore.
 */
AdbDoctorList *SPI_selectMgrHost(MemoryContext ctx, char *sql)
{
	AdbDoctorList *list;
	AdbDoctorLink *link;
	AdbMgrHostWrapper *wrapper;
	uint64 rows, j;
	int ret;
	HeapTuple tuple;
	TupleDesc tupdesc;
	MemoryContext oldCtx;

	ret = SPI_execute(sql, false, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d", ret)));

	rows = SPI_processed;

	oldCtx = MemoryContextSwitchTo(ctx);

	if (rows > 0 && SPI_tuptable != NULL)
	{
		/* must palloc here, do not palloc in spi context. */
		list = newAdbDoctorList();

		tupdesc = SPI_tuptable->tupdesc;
		for (j = 0; j < rows; j++)
		{
			tuple = SPI_tuptable->vals[j];

			wrapper = palloc0(sizeof(AdbMgrHostWrapper));
			wrapMgrHost(tuple, tupdesc, wrapper);

			link = newAdbDoctorLink(wrapper, (void (*)(void *))pfreeAdbMgrHostWrapper);
			dlist_push_tail(&list->head, &link->wi_links);
			list->num++;
		}
	}
	else
	{
		list = NULL;
	}

	MemoryContextSwitchTo(oldCtx);

	return list;
}

static void wrapMgrNode(HeapTuple tuple, TupleDesc tupdesc, AdbMgrNodeWrapper *wrapper)
{
	Datum datum;
	bool isNull;

	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodename, &isNull);
	if (!isNull)
		wrapper->fdmn.nodename = *DatumGetName(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodehost, &isNull);
	if (!isNull)
		wrapper->fdmn.nodehost = DatumGetObjectId(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodetype, &isNull);
	if (!isNull)
		wrapper->fdmn.nodetype = DatumGetChar(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodesync, &isNull);
	if (!isNull)
		wrapper->fdmn.nodesync = *DatumGetName(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodeport, &isNull);
	if (!isNull)
		wrapper->fdmn.nodeport = DatumGetInt32(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodeinited, &isNull);
	if (!isNull)
		wrapper->fdmn.nodeinited = DatumGetBool(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodemasternameoid, &isNull);
	if (!isNull)
		wrapper->fdmn.nodemasternameoid = DatumGetObjectId(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodeincluster, &isNull);
	if (!isNull)
		wrapper->fdmn.nodeincluster = DatumGetBool(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodereadonly, &isNull);
	if (!isNull)
		wrapper->fdmn.nodereadonly = DatumGetBool(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodezone, &isNull);
	if (!isNull)
		wrapper->fdmn.nodezone = *DatumGetName(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_allowcure, &isNull);
	if (!isNull)
		wrapper->fdmn.allowcure = DatumGetBool(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_curestatus, &isNull);
	if (!isNull)
		wrapper->fdmn.curestatus = *DatumGetName(datum);
	datum = SPI_getbinval(tuple, tupdesc, Anum_mgr_node_nodepath, &isNull);
	if (!isNull)
		wrapper->nodepath = TextDatumGetCString(datum);
	else
		wrapper->nodepath = palloc0(1);
	datum = SPI_getbinval(tuple, tupdesc, Natts_mgr_node + 1, &isNull);
	if (!isNull)
		wrapper->oid = DatumGetObjectId(datum);
	datum = SPI_getbinval(tuple, tupdesc, Natts_mgr_node + 2, &isNull);
	if (!isNull)
		wrapper->hostuser = *DatumGetName(datum);
	datum = SPI_getbinval(tuple, tupdesc, Natts_mgr_node + 3, &isNull);
	if (!isNull)
		wrapper->hostaddr = TextDatumGetCString(datum);
	else
		wrapper->hostaddr = palloc0(1);
}

static void wrapMgrHost(HeapTuple tuple, TupleDesc tupdesc, AdbMgrHostWrapper *wrapper)
{
	Datum datum;
	bool isNull;
	Oid oid;
	Form_mgr_host tmp;

	oid = HeapTupleGetOid(tuple);
	wrapper->oid = oid;
	tmp = (Form_mgr_host)GETSTRUCT(tuple);
	wrapper->fdmh = *tmp;
	datum = heap_getattr(tuple, Anum_mgr_host_hostaddr, tupdesc, &isNull);
	if (!isNull)
		wrapper->hostaddr = TextDatumGetCString(datum);
	else
		wrapper->hostaddr = palloc0(1);
	datum = heap_getattr(tuple, Anum_mgr_host_hostadbhome, tupdesc, &isNull);
	if (!isNull)
		wrapper->hostadbhome = TextDatumGetCString(datum);
	else
		wrapper->hostadbhome = palloc0(1);
}