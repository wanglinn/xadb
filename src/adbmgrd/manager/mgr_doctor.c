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

static bool
compatTupleDescs(TupleDesc ret_tupdesc, TupleDesc sql_tupdesc);

Datum mgr_doctor_start(PG_FUNCTION_ARGS)
{
	int ret;
	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("mgr_doctor_start: SPI_connect returned %d",
						ret)));

	ret = SPI_execute("select adb_doctor.adb_doctor_start()", 0, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		/* internal error */
		ereport(ERROR,
				(errmsg("mgr_doctor_start: SPI_execute returned %d",
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
				(errmsg("mgr_doctor_stop: SPI_connect returned %d",
						ret)));

	ret = SPI_execute("select adb_doctor.adb_doctor_stop()", 0, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		/* internal error */
		ereport(ERROR,
				(errmsg("mgr_doctor_stop: SPI_execute returned %d",
						ret)));
	}

	SPI_finish();

	PG_RETURN_BOOL(true);
}

void mgr_doctor_set_param(MGRDoctorSet *node, ParamListInfo params, DestReceiver *dest)
{
	DirectFunctionCall1(mgr_doctor_param, PointerGetDatum(node->options));
	return;
}

Datum mgr_doctor_param(PG_FUNCTION_ARGS)
{
	int ret;
	List *options;
	ListCell *lc;
	DefElem *def;
	char *k;
	char *v;
	StringInfoData buf;

	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("mgr_doctor_param: SPI_connect returned %d",
						ret)));

	initStringInfo(&buf);
	options = (List *)PG_GETARG_POINTER(0);
	foreach (lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		k = def->defname;
		v = defGetString(def);
		if (v == NULL || strlen(v) == 0)
		{
			ereport(ERROR,
					(errmsg("Failed to set doctor parameters, nothing to set!")));
		}
		resetStringInfo(&buf);
		appendStringInfo(&buf, "select adb_doctor.adb_doctor_param('%s', '%s')",
						 k, v);
		ret = SPI_execute(buf.data, false, 0);
		pfree(buf.data);
		if (ret != SPI_OK_SELECT)
		{
			SPI_finish();
			/* internal error */
			ereport(ERROR,
					(errmsg("mgr_doctor_param: SPI_execute returned %d",
							ret)));
		}
	}

	SPI_finish();

	PG_RETURN_BOOL(true);
}

Datum mgr_doctor_list(PG_FUNCTION_ARGS)
{
	int ret;
	int i;
	int natts;
	uint64 proc;
	uint64 call_cntr;
	uint64 max_calls;
	StringInfoData buf;
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

	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		/* internal error */
		ereport(ERROR,
				(errmsg("mgr_doctor_list: SPI_connect returned %d",
						ret)));

	/* Retrieve the desired rows */
	initStringInfo(&buf);
	appendStringInfo(&buf, "select * from adb_doctor.adb_doctor_list()");
	ret = SPI_execute(buf.data, false, 0);
	proc = SPI_processed;
	pfree(buf.data);

	/* If no qualifying tuples, fall out early */
	if (ret != SPI_OK_SELECT || proc == 0)
	{
		SPI_finish();
		rsinfo->isDone = ExprEndResult;
		PG_RETURN_NULL();
	}

	spi_tuptable = SPI_tuptable;
	spi_tupdesc = spi_tuptable->tupdesc;

	/* The above spi query must always return three columns.
	 * k, v, comment */
	if (spi_tupdesc->natts != 3)
		ereport(ERROR,
				(errmsg("mgr_doctor_list: The provided query must return 3 columns, "
						"but actually returns %d columns",
						spi_tupdesc->natts)));

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

	/*
	 * Check that return tupdesc is compatible with the data we got from SPI,
	 * at least based on number and type of attributes
	 */
	if (!compatTupleDescs(ret_tupdesc, spi_tupdesc))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("return and sql tuple descriptions are "
						"incompatible")));

	/*
	 * switch to long-lived memory context
	 */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* make sure we have a persistent copy of the result tupdesc */
	ret_tupdesc = CreateTupleDescCopy(ret_tupdesc);

	/* initialize our tuplestore in long-lived context */
	tupstore =
		tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Generate attribute metadata needed later to produce tuples from raw C
	 * strings
	 */
	attinmeta = TupleDescGetAttInMetadata(ret_tupdesc);

	MemoryContextSwitchTo(oldcontext);

	/* total number of tuples to be examined */
	max_calls = proc;
	/* total columns from tuples to be returned */
	natts = ret_tupdesc->natts;

	for (call_cntr = 0; call_cntr < max_calls; call_cntr++)
	{
		/* allocate and zero space */
		values = (char **)palloc0((natts) * sizeof(char *));
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
		pfree(values);
	}

	/* let the caller know we're sending back a tuplestore */
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = ret_tupdesc;

	/* release SPI related resources (and return to caller's context) */
	SPI_finish();

	return (Datum)0;
}

/*
 * Check if two tupdescs match in type of attributes
 */
static bool
compatTupleDescs(TupleDesc ret_tupdesc, TupleDesc sql_tupdesc)
{
	int i;
	Form_pg_attribute ret_attr;
	Form_pg_attribute sql_attr;

	if (ret_tupdesc->natts != sql_tupdesc->natts)
		return false;

	for (i = 0; i < ret_tupdesc->natts; i++)
	{
		sql_attr = TupleDescAttr(sql_tupdesc, i);
		ret_attr = TupleDescAttr(ret_tupdesc, i);

		if (ret_attr->atttypid != sql_attr->atttypid)
			return false;
	}

	/* OK, the two tupdescs are compatible for our purposes */
	return true;
}