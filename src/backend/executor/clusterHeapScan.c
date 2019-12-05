#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/tuptypeconvert.h"
#include "access/sysattr.h"
#include "access/skey.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "executor/clusterHeapScan.h"
#include "executor/clusterReceiver.h"
#include "executor/executor.h"
#include "executor/execCluster.h"
#include "executor/clusterHeapScan.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "storage/mem_toc.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define REMOTE_KEY_HEAP_SCAN_INFO	1

static void index_scan_send(IndexScanDesc scan, ProjectionInfo *project, TupleTypeConvert *convert, TupleTableSlot *convert_slot, StringInfo buf);
static List* make_tlist_from_bms(Relation rel, Bitmapset *bms);
static Relation find_index_for_attno(Relation rel, AttrNumber attno, LOCKMODE lockmode);

List* ExecClusterHeapScan(List *rnodes, Relation rel, Bitmapset *ret_attnos,
						  AttrNumber eq_attr, Datum *datums, int count, bool test_null)
{
	List *result;
	ArrayType *array;
	Form_pg_attribute attr;
	StringInfoData msg;
	Oid array_type;

	initStringInfo(&msg);

	attr = TupleDescAttr(RelationGetDescr(rel), eq_attr-1);
	array = construct_array(datums,
							count,
							attr->atttypid,
							attr->attlen,
							attr->attbyval,
							attr->attalign);
	array_type = get_array_type(attr->atttypid);
	if (!OidIsValid(array_type))
		array_type = ANYARRAYOID;

	ClusterTocSetCustomFun(&msg, DoClusterHeapScan);

	begin_mem_toc_insert(&msg, REMOTE_KEY_HEAP_SCAN_INFO);
	save_oid_class(&msg, RelationGetRelid(rel));						/* relation */
	appendBinaryStringInfo(&msg, (char*)&eq_attr, sizeof(eq_attr));		/* eq_attr */
	do_datum_convert_out(&msg, array_type, PointerGetDatum(array));		/* datums */
	save_node_bitmapset(&msg, ret_attnos);								/* ret_attnos */
	appendStringInfoChar(&msg, (char)test_null);						/* test_null */
	end_mem_toc_insert(&msg, REMOTE_KEY_HEAP_SCAN_INFO);

	result = ExecClusterCustomFunction(rnodes, &msg, EXEC_CLUSTER_FLAG_READ_ONLY);

	pfree(msg.data);
	pfree(array);

	return result;
}

void DoClusterHeapScan(StringInfo mem_toc)
{
	MemoryContext mcxt = AllocSetContextCreate(CurrentMemoryContext,
											   "ExecClusterHeapScan",
											   ALLOCSET_DEFAULT_SIZES);
	MemoryContext old_context = MemoryContextSwitchTo(mcxt);
	Relation				rel;
	Relation				index_rel;
	HeapTuple				tup;
	Form_pg_attribute		attr;
	ArrayType			   *array;
	Bitmapset			   *ret_attnos;
	List				   *tlist;
	ExprContext			   *econtext;
	TupleTableSlot		   *scan_slot;
	TupleTableSlot		   *result_slot;
	TupleTableSlot		   *convert_slot;
	TupleTypeConvert	   *convert;
	ProjectionInfo		   *project;
	IndexScanDesc			index_scan;
	TupleDesc				result_desc;
	ScanKeyData				key;
	StringInfoData			buf;
	Oid						array_type;
	Oid						opno;
	StrategyNumber			stragegy;
	AttrNumber				eq_attr;
	bool					test_null;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_HEAP_SCAN_INFO, &buf.len);
	if (buf.data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not found heap scan info")));
	buf.maxlen = buf.len;
	buf.cursor = 0;

	/* relation */
	rel = heap_open(load_oid_class(&buf), AccessShareLock);
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table", RelationGetRelationName(rel))));

	/* eq_attr */
	pq_copymsgbytes(&buf, (char*)&eq_attr, sizeof(eq_attr));
	if (eq_attr <= 0 ||
		eq_attr > RelationGetDescr(rel)->natts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("invalid attribute %d for relation \"%s\"",
				 		eq_attr,
						RelationGetRelationName(rel))));
	}
	attr = TupleDescAttr(RelationGetDescr(rel), eq_attr-1);
	array_type = get_array_type(attr->atttypid);
	if (!OidIsValid(array_type))
		array_type = ANYARRAYOID;

	/* datums */
	array = DatumGetArrayTypeP(do_datum_convert_in(&buf, array_type));

	ret_attnos = load_Bitmapset(&buf);				/* ret_attnos */
	test_null = (bool)pq_getmsgbyte(&buf);			/* test_null */

	/* make target list */
	tlist = make_tlist_from_bms(rel, ret_attnos);

	/* find index */
	index_rel = find_index_for_attno(rel, eq_attr, AccessShareLock);
	if (index_rel == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Can not found a index for relation \"%s\"", RelationGetRelationName(rel))));
	}

	/* initialize message buffer */
	initStringInfo(&buf);

	/* create target list status */
	econtext = CreateStandaloneExprContext();

	/* create slot and convert */
	scan_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	result_desc = ExecTypeFromTL(tlist, false);
	result_slot = MakeSingleTupleTableSlot(result_desc);
	convert = create_type_convert(result_desc, true, false);
	if (convert)
	{
		convert_slot = MakeSingleTupleTableSlot(convert->out_desc);
		serialize_slot_convert_head(&buf, convert->out_desc);
	}else
	{
		convert_slot = NULL;
		serialize_slot_head_message(&buf, result_desc);
	}
	/* send tupedesc message */
	pq_putmessage('d', buf.data, buf.len);
	resetStringInfo(&buf);

	/* create project info */
	project = ExecBuildProjectionInfo(tlist,
									  econtext,
									  result_slot,
									  NULL,
									  RelationGetDescr(rel));

	/* get equal operator id */
	attr = TupleDescAttr(RelationGetDescr(rel), eq_attr-1);
	tup = oper(NULL, SystemFuncName("="), attr->atttypid, attr->atttypid, false, -1);
	opno = oprid(tup);
	ReleaseSysCache(tup);

	stragegy = get_op_opfamily_strategy(opno, index_rel->rd_opfamily[0]);
	/* check type */
	if (index_rel->rd_opcintype[0] == ANYARRAYOID &&
		 type_is_array(attr->atttypid))
	{
		/* nothing to do */
	}else if (attr->atttypid != index_rel->rd_opcintype[0])
	{
		Oid funcid;
		CoercionPathType path_type = find_coercion_pathway(index_rel->rd_opcintype[0],
														   attr->atttypid,
														   COERCION_IMPLICIT,
														   &funcid);
		if (path_type != COERCION_PATH_RELABELTYPE)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("Only support binary-compatible type index scan")));
	}

	/* index scan */
	if (index_rel->rd_amroutine->amsearcharray)
	{
		ScanKeyEntryInitialize(&key,
							   SK_SEARCHARRAY,
							   1,
							   stragegy,
							   index_rel->rd_opcintype[0],
							   attr->attcollation,
							   get_opcode(opno),
							   PointerGetDatum(array));
		index_scan = index_beginscan(rel, index_rel, GetActiveSnapshot(), 1, 0);
		index_rescan(index_scan, &key, 1, NULL, 0);
		econtext->ecxt_scantuple = scan_slot;
		index_scan_send(index_scan, project, convert, convert_slot, &buf);
	}else
	{
		Datum *datums;
		int count;
		int i;
		RegProcedure code;
		deconstruct_array(array,
						  index_rel->rd_opcintype[0],
						  attr->attlen,
						  attr->attbyval,
						  attr->attalign,
						  &datums,
						  NULL,
						  &count);
		index_scan = index_beginscan(rel, index_rel, GetActiveSnapshot(), 1, 0);
		stragegy = get_op_opfamily_strategy(opno, index_rel->rd_opfamily[0]);
		code = get_opcode(opno);
		for(i=0;i<count;++i)
		{
			ScanKeyInit(&key,
						1,
						stragegy,
						code,
						datums[i]);
			index_rescan(index_scan, &key, 1, NULL, 0);
			index_scan_send(index_scan, project, convert, convert_slot, &buf);
		}
	}
	if (test_null)
	{
		ScanKeyEntryInitialize(&key,
							   SK_ISNULL|SK_SEARCHNULL,
							   1,
							   InvalidStrategy,
							   InvalidOid,
							   InvalidOid,
							   InvalidOid,
							   (Datum)0);
		index_rescan(index_scan, &key, 1, NULL, 0);
		index_scan_send(index_scan, project, convert, convert_slot, &buf);
	}
	index_endscan(index_scan);
	pq_flush();

	/* hold lock until end of transaction */
	index_close(index_rel, NoLock);
	relation_close(rel, NoLock);

	/* clean up */
	if (convert)
	{
		free_type_convert(convert);
		ExecDropSingleTupleTableSlot(convert_slot);
	}
	ExecDropSingleTupleTableSlot(result_slot);
	FreeTupleDesc(result_desc);
	ExecDropSingleTupleTableSlot(scan_slot);
	FreeExprContext(econtext, true);
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(mcxt);
}

static void index_scan_send(IndexScanDesc scan, ProjectionInfo *project, TupleTypeConvert *convert, TupleTableSlot *convert_slot, StringInfo buf)
{
	HeapTuple tup;
	ExprContext *econtext = project->pi_exprContext;
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	while ((tup = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		resetStringInfo(buf);
		ExecStoreTuple(tup, slot, InvalidBuffer, false);
		MemoryContextReset(econtext->ecxt_per_tuple_memory);
		ExecProject(project);
		if (convert)
		{
			do_type_convert_slot_out(convert, slot, convert_slot, false);
			serialize_slot_message(buf, convert_slot, CLUSTER_MSG_CONVERT_TUPLE);
		}else
		{
			serialize_slot_message(buf, slot, CLUSTER_MSG_TUPLE_DATA);
		}
		pq_putmessage('d', buf->data, buf->len);
	}
}

static List* make_tlist_from_bms(Relation rel, Bitmapset *bms)
{
	Var				   *var;
	TargetEntry		   *te;
	List			   *tlist = NIL;
	Form_pg_attribute	attr;
	int					x = -1;
	int					resno = 0;
	AttrNumber			attno;

	while((x=bms_next_member(bms, x)) >= 0)
	{
		attno = x + FirstLowInvalidHeapAttributeNumber;
		if (!AttributeNumberIsValid(attno) ||
			attno > RelationGetDescr(rel)->natts)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("invalid result attribute %d for relation \"%s\"",
					 		attno,
							RelationGetRelationName(rel))));
		if (attno < 0)
			attr = SystemAttributeDefinition(attno,
											 RelationGetForm(rel)->relhasoids);
		else
			attr = TupleDescAttr(RelationGetDescr(rel), attno-1);

		var = makeVar(1, attno, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
		te = makeTargetEntry((Expr*)var, ++resno, pstrdup(NameStr(attr->attname)), false);
		tlist = lappend(tlist, te);
	}

	return tlist;
}

static Relation find_index_for_attno(Relation rel, AttrNumber attno, LOCKMODE lockmode)
{
	Relation indexRelation;
	Form_pg_index index;
	ListCell *lc;
	Relation result = NULL;
	List *list = RelationGetIndexList(rel);
	foreach(lc, list)
	{
		indexRelation = index_open(lfirst_oid(lc), lockmode);
		index = indexRelation->rd_index;

		/*
		 * Ignore invalid indexes, since they can't safely be used for
		 * queries.  Note that this is OK because the data structure we
		 * are constructing is only used by the planner --- the executor
		 * still needs to insert into "invalid" indexes, if they're marked
		 * IndexIsReady.
		 */
		if (!IndexIsValid(index))
		{
			index_close(indexRelation, NoLock);
			continue;
		}

		/*
		 * If the index is valid, but cannot yet be used, ignore it;
		 */
		if (index->indcheckxmin &&
			!TransactionIdPrecedes(HeapTupleHeaderGetXmin(indexRelation->rd_indextuple->t_data),
									TransactionXmin))
		{
			index_close(indexRelation, NoLock);
			continue;
		}

		if (index->indkey.values[0] == attno)
		{
			if (indexRelation->rd_amroutine->amsearcharray)
			{
				if (result)
					index_close(result, NoLock);
				result = indexRelation;
				break;
			}else if(result == NULL)
			{
				result = indexRelation;
			}
		}

		if (result != indexRelation)
			index_close(indexRelation, NoLock);
	}
	list_free(list);

	return result;
}
