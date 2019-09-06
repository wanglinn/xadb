#include "postgres.h"
#include "nodes/params.h"
#include "tcop/dest.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "mgr/mgr_agent.h"
#include "utils/timestamp.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "catalog/mgr_node.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "executor/spi.h"
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "parser/mgr_node.h"
#include "commands/defrem.h"
#include "mgr/mgr_cmds.h"

#define xpstrdup(tgtvar_, srcvar_)      \
	do                                  \
	{                                   \
		if (srcvar_)                    \
			tgtvar_ = pstrdup(srcvar_); \
		else                            \
			tgtvar_ = NULL;             \
	} while (0)

static Datum mgr_doctor_list_internal(PG_FUNCTION_ARGS, char *sql);

Datum mgr_doctor_start(PG_FUNCTION_ARGS)
{
	int ret;
	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("start doctor failed, SPI_connect returned %d",
						ret)));

	ret = SPI_execute("select adb_doctor.adb_doctor_start()", 0, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		/* internal error */
		ereport(ERROR,
				(errmsg("start doctor failed, SPI_execute returned %d",
						ret)));
	}

	SPI_finish();

	PG_RETURN_BOOL(true);
}

Datum mgr_doctor_stop(PG_FUNCTION_ARGS)
{
	int ret;
	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("stop doctor failed, SPI_connect returned %d",
						ret)));

	ret = SPI_execute("select adb_doctor.adb_doctor_stop()", 0, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		/* internal error */
		ereport(ERROR,
				(errmsg("stop doctor failed, SPI_execute returned %d",
						ret)));
	}

	SPI_finish();

	PG_RETURN_BOOL(true);
}

void mgr_doctor_set_param(MGRDoctorSet *node, ParamListInfo params, DestReceiver *dest)
{
	DirectFunctionCall1(mgr_doctor_param, PointerGetDatum(node));
	return;
}

Datum mgr_doctor_param(PG_FUNCTION_ARGS)
{
	int ret;
	ListCell *lc;
	DefElem *def;
	char *k;
	char *v;
	StringInfoData buf;
	MGRDoctorSet *mgrDoctorSet;

	mgrDoctorSet = (MGRDoctorSet *)PG_GETARG_POINTER(0);
	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("set doctor failed, SPI_connect returned %d",
						ret)));

	initStringInfo(&buf);
	if (mgrDoctorSet->options)
	{
		foreach (lc, mgrDoctorSet->options)
		{
			def = lfirst(lc);
			Assert(def && IsA(def, DefElem));
			k = def->defname;
			v = defGetString(def);
			if (v == NULL || strlen(v) == 0)
			{
				SPI_finish();
				ereport(ERROR,
						(errmsg("set doctor parameter failed, nothing to set!")));
			}
			resetStringInfo(&buf);
			appendStringInfo(&buf, "select adb_doctor.adb_doctor_param('%s', '%s')",
							 k, v);
			ret = SPI_execute(buf.data, false, 0);
			if (ret != SPI_OK_SELECT)
			{
				SPI_finish();
				ereport(ERROR,
						(errmsg("set doctor parameter failed, SPI_execute returned %d",
								ret)));
			}
		}
	}
	if (mgrDoctorSet->nodename)
	{
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "update pg_catalog.mgr_node  "
						 "set allowcure = %d::boolean "
						 "where nodename = '%s' ",
						 mgrDoctorSet->enable,
						 mgrDoctorSet->nodename);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UPDATE)
		{
			SPI_finish();
			ereport(ERROR,
					(errmsg("set doctor node failed, SPI_execute returned %d",
							ret)));
		}
		if (SPI_processed == 0)
		{
			SPI_finish();
			ereport(ERROR,
					(errmsg("set doctor node failed, the node %s may not exists.",
							mgrDoctorSet->nodename)));
		}
	}
	if (mgrDoctorSet->hostname)
	{
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "update pg_catalog.mgr_host  "
						 "set allowcure = %d::boolean "
						 "where hostname = '%s' ",
						 mgrDoctorSet->enable,
						 mgrDoctorSet->hostname);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UPDATE)
		{
			SPI_finish();
			ereport(ERROR,
					(errmsg("set doctor host failed, SPI_execute returned %d",
							ret)));
		}
		if (SPI_processed == 0)
		{
			SPI_finish();
			ereport(ERROR,
					(errmsg("set doctor host failed, the host %s may not exists.",
							mgrDoctorSet->hostname)));
		}
	}
	pfree(buf.data);

	SPI_finish();

	PG_RETURN_BOOL(true);
}

Datum mgr_doctor_list(PG_FUNCTION_ARGS)
{
	char *sql = "select * from mgr_doctor_list_param() "
				"union all "
				"select * from mgr_doctor_list_node() "
				"union all "
				"select * from mgr_doctor_list_host() ";
	return mgr_doctor_list_internal(fcinfo, sql);
}

Datum mgr_doctor_list_param(PG_FUNCTION_ARGS)
{
	char *sql = "select 'PARAMETER' as type, "
				"'--' as subtype, "
				"k as key, "
				"v as value, "
				"comment "
				"from adb_doctor.adb_doctor_list();";
	return mgr_doctor_list_internal(fcinfo, sql);
}

Datum mgr_doctor_list_node(PG_FUNCTION_ARGS)
{
	Datum ret;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "select 'NODE' as type, "
					 "case nodetype "
					 " WHEN '%c' then 'gtmcoord master' "
					 " WHEN '%c' then 'gtmcoord slave' "
					 " WHEN '%c' then 'coordinator' "
					 " WHEN '%c' then 'coordinator' "
					 " WHEN '%c' then 'datanode master' "
					 " WHEN '%c' then 'datanode slave' END as subtype, "
					 "nodename as key, "
					 "allowcure as value, "
					 "CASE allowcure "
					 " WHEN 't' THEN 'enable doctor' "
					 " WHEN 'f' THEN 'disable doctor' END AS comment "
					 "from pg_catalog.mgr_node;",
					 CNDN_TYPE_GTM_COOR_MASTER,
					 CNDN_TYPE_GTM_COOR_SLAVE,
					 CNDN_TYPE_COORDINATOR_MASTER,
					 CNDN_TYPE_COORDINATOR_SLAVE,
					 CNDN_TYPE_DATANODE_MASTER,
					 CNDN_TYPE_DATANODE_SLAVE);
	ret = mgr_doctor_list_internal(fcinfo, sql.data);
	pfree(sql.data);
	return ret;
}

Datum mgr_doctor_list_host(PG_FUNCTION_ARGS)
{
	Datum ret;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "select 'HOST' as type, "
					 "'--' as subtype, "
					 "hostname as key, "
					 "allowcure as value, "
					 "CASE allowcure "
					 " WHEN 't' THEN 'enable doctor' "
					 " WHEN 'f' THEN 'disable doctor' END AS comment "
					 "from pg_catalog.mgr_host;");
	ret = mgr_doctor_list_internal(fcinfo, sql.data);
	pfree(sql.data);
	return ret;
}

static Datum mgr_doctor_list_internal(PG_FUNCTION_ARGS, char *sql)
{
	int ret;
	int i;
	int natts;
	uint64 call_cntr;
	uint64 max_calls;
	SPITupleTable *spi_tuptable;
	HeapTuple spi_tuple;
	TupleDesc spi_tupdesc;
	HeapTuple ret_tuple;
	TupleDesc ret_tupdesc;
	AttInMetadata *attinmeta;
	char **values;
	Tuplestorestate *tupstore;
	ReturnSetInfo *rsinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	oldcontext = CurrentMemoryContext;
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

	/* get a tuple descriptor for our result type */
	switch (get_call_result_type(fcinfo, NULL, &ret_tupdesc))
	{
	case TYPEFUNC_COMPOSITE:
		/* success */
		break;
	case TYPEFUNC_RECORD:
		/* failed to determine actual type of RECORD */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
		break;
	default:
		/* result type isn't composite */
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("return type must be a row type")));
		break;
	}

	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("list doctor failed, SPI_connect returned %d",
						ret)));

	ret = SPI_execute(sql, false, 0);
	max_calls = SPI_processed;
	/* If no qualifying tuples, fall out early */
	if (ret != SPI_OK_SELECT || max_calls == 0)
	{
		SPI_finish();
		rsinfo->isDone = ExprEndResult;
		PG_RETURN_NULL();
	}
	spi_tuptable = SPI_tuptable;
	spi_tupdesc = spi_tuptable->tupdesc;
	/* The above spi query must always return three columns.
	 * k, v, comment */
	if (spi_tupdesc->natts != 5)
		ereport(ERROR,
				(errmsg("list doctor failed, The provided query must return 3 columns, "
						"but actually returns %d columns",
						spi_tupdesc->natts)));

	/* switch to long-lived memory context */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	/* make sure we have a persistent copy of the result tupdesc */
	ret_tupdesc = CreateTupleDescCopy(ret_tupdesc);
	/* initialize our tuplestore in long-lived context */
	tupstore = tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random,
									 false, work_mem);
	MemoryContextSwitchTo(oldcontext);
	/*
	 * Generate attribute metadata needed later to produce tuples from raw C
	 * strings
	 */
	attinmeta = TupleDescGetAttInMetadata(ret_tupdesc);
	MemoryContextSwitchTo(oldcontext);

	/* total columns from tuples to be returned */
	natts = ret_tupdesc->natts;
	/* allocate and zero space */
	values = (char **)palloc0((natts) * sizeof(char *));
	for (call_cntr = 0; call_cntr < max_calls; call_cntr++)
	{
		/* get the next sql result tuple */
		spi_tuple = spi_tuptable->vals[call_cntr];
		/* get the rowid from the current sql result tuple */
		for (i = 0; i < natts; i++)
		{
			xpstrdup(values[i], SPI_getvalue(spi_tuple, spi_tupdesc, i + 1));
		}
		/* build the tuple and store it */
		ret_tuple = BuildTupleFromCStrings(attinmeta, values);
		tuplestore_puttuple(tupstore, ret_tuple);
		heap_freetuple(ret_tuple);
		/* Clean up */
		for (i = 0; i < natts; i++)
			if (values[i])
				pfree(values[i]);
	}
	pfree(values);
	/* let the caller know we're sending back a tuplestore */
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = ret_tupdesc;

	/* release SPI related resources (and return to caller's context) */
	SPI_finish();

	return (Datum)0;
}