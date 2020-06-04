/*-------------------------------------------------------------------------
 *
 * ora_convert.c
 * 
 *		Oracle implicit conversion
 *		Management of implicit conversion of different functions or 
 *		expression value parameters in Oracle syntax.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/relscan.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/ora_convert.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#ifdef ADB
#include "access/heapam.h"
#include "catalog/pgxc_node.h"
#include "executor/execCluster.h"
#include "libpq/libpq-node.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "storage/mem_toc.h"

#define REMOTE_KEY_IMPLICIT_CONVERT		1
#endif /* ADB */


void ExecImplicitConvert(OraImplicitConvertStmt *stmt);
static void ExecImplicitConvertLocal(OraImplicitConvertStmt *stmt);

#ifdef ADB
void ClusterExecImplicitConvert(StringInfo mem_toc);
static List* getMasterNodeOid(void);
#endif	/* ADB */


/* 
 * Oracle type implicit conversion creation.
 */
void ExecImplicitConvert(OraImplicitConvertStmt *stmt)
#ifdef ADB
{
	ListCell   *lc;
	List	   *nodeOids;
	List	   *remoteList;
	Oid			oid;
	bool		include_myself = false;

	if (stmt->node_list == NIL)
	{
		include_myself = true;
		stmt->node_list = getMasterNodeOid();
	}

	nodeOids = NIL;
	foreach(lc, stmt->node_list)
	{
		oid = lfirst_oid(lc);
		if (oid == PGXCNodeOid)
		{
			include_myself = true;
			continue;
		}
		
		if (IsConnFromApp())
			nodeOids = list_append_unique_oid(nodeOids, oid);
	}

	/* Execute settings on other nodes of the cluster */
	remoteList = NIL;
	if (nodeOids != NIL)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ClusterTocSetCustomFun(&msg, ClusterExecImplicitConvert);

		begin_mem_toc_insert(&msg, REMOTE_KEY_IMPLICIT_CONVERT);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_IMPLICIT_CONVERT);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	/* Create an implicit transform in local */
	if (include_myself)
		ExecImplicitConvertLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
}
#else
{
	ExecImplicitConvertLocal(stmt);
}
#endif	/* ADB */

/* 
 * Execute local Oracle type implicit conversion creation.
 */
static void ExecImplicitConvertLocal(OraImplicitConvertStmt *stmt)
{
	Relation			convert_rel;
	TupleDesc			rel_dsc;
	TableScanDesc		rel_scan;
	ScanKeyData			key[2];
	HeapTuple			tuple, newtuple;
	Form_ora_convert	ora_convert;
	oidvector			*oldcvtfrom;
	oidvector			*oldcvtto;
	oidvector			*newcvtfrom;
	oidvector			*newcvtto;
	int					cvtfromList_count = 0;
	int					cvttoList_count = 0;
	Datum				cvttoDatum;
	bool				isNull;
	int					i;
	bool				compareCvtfrom = true;
	bool				compareCvtto = true;


	cvtfromList_count = list_length(stmt->cvtfrom);
	
	if (stmt->cvtto)
		cvttoList_count = list_length(stmt->cvtto);
	
	if (cvtfromList_count < 1  || (stmt->action != ICONVERT_DELETE && cvttoList_count < 1))
		elog(ERROR, "The number of unqualified parameters or inconsistent parameters.");

	/* Check parameter */
	if (stmt->action != ICONVERT_DELETE && cvtfromList_count != cvttoList_count && 
		(stmt->cvtkind == ORA_CONVERT_KIND_FUNCTION || stmt->cvtkind == ORA_CONVERT_KIND_OPERATOR))
		elog(ERROR, "The number of unqualified parameters or inconsistent parameters.");
	else
	{
		Oid			*fromOids, *toOids;
		ListCell	*cell;

		fromOids = palloc(cvtfromList_count * sizeof(Oid));
		cell = list_head(stmt->cvtfrom);
		for (i = 0; i < cvtfromList_count; i++)
		{
			Assert(nodeTag(lfirst(cell)) == T_String);
			fromOids[i] = TypenameGetTypid(strVal(lfirst(cell)));
			if (fromOids[i] == InvalidOid)
				elog(ERROR, "Added data type not included.");
			cell = lnext(cell);
		}
		newcvtfrom = buildoidvector(fromOids, cvtfromList_count);
		pfree(fromOids);

		if (stmt->cvtto)
		{
			toOids = palloc(cvttoList_count * sizeof(Oid));
			cell = list_head(stmt->cvtto);
			for (i = 0; i < cvttoList_count; i++)
			{
				Assert(nodeTag(lfirst(cell)) == T_String);
				toOids[i] = TypenameGetTypid(strVal(lfirst(cell)));
				if (toOids[i] == InvalidOid)
					elog(ERROR, "Added data type not included.");
				cell = lnext(cell);
			}
			newcvtto = buildoidvector(toOids, cvttoList_count);
			pfree(toOids);
		}
	}

	Assert(stmt->cvtname);
	ScanKeyInit(&key[0]
			,Anum_ora_convert_cvtkind
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(stmt->cvtkind));
	ScanKeyInit(&key[1]
			,Anum_ora_convert_cvtname
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(stmt->cvtname));
			
	convert_rel = table_open(OraConvertRelationId, RowExclusiveLock);
	rel_scan = table_beginscan_catalog(convert_rel, 2, key);
	rel_dsc = RelationGetDescr(convert_rel);

	
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		ora_convert= (Form_ora_convert)GETSTRUCT(tuple);
		Assert(ora_convert);

		/* get tuple cvtfrom */
		oldcvtfrom = &(ora_convert->cvtfrom);

		/* get tuple cvtto */
		cvttoDatum = heap_getattr(tuple, Anum_ora_convert_cvtto, rel_dsc, &isNull);
		if (isNull)
		{
			heap_endscan(rel_scan);
			heap_close(convert_rel, RowExclusiveLock);
			elog(ERROR, "column cvtto is null.");
			return;
		}
		oldcvtto = (oidvector *) DatumGetPointer(cvttoDatum);

		/* compare cvtfrom */
		if (newcvtfrom->dim1 == oldcvtfrom->dim1)
		{
			for (i = 0; i < cvtfromList_count; i++)
			{
				if (oid_cmp(&(newcvtfrom->values[i]), &(oldcvtfrom->values[i])) != 0)
				{
					compareCvtfrom = false;
					break;
				}
			}
		}
		else
			compareCvtfrom = false;
		
		if (stmt->cvtto)
		{
			/* compare cvtto */
			if (newcvtto->dim1 == oldcvtto->dim1)
			{
				for (i = 0; i < cvttoList_count; i++)
				{
					if (oid_cmp(&(newcvtto->values[i]), &(oldcvtto->values[i])) != 0)
					{
						compareCvtto = false;
						break;
					}
				}
			}
			else
				compareCvtto = false;
		}

		/* 
		 * we think that the 'cvtfrom' value of the same function name is unique.
		 * Record found ?
		 */
		if (compareCvtfrom)
			break;
		else
			compareCvtfrom = compareCvtto = true;
	}
	
	switch (stmt->action)
	{
		case ICONVERT_CREATE:
			{
				Datum		datum[Natts_ora_convert];
				bool		nulls[Natts_ora_convert];

				datum[Anum_ora_convert_cvtkind -1] = CharGetDatum(stmt->cvtkind);
				datum[Anum_ora_convert_cvtname -1] = CharGetDatum(stmt->cvtname);
				datum[Anum_ora_convert_cvtfrom -1] = PointerGetDatum(newcvtfrom);
				datum[Anum_ora_convert_cvtto -1] = PointerGetDatum(newcvtto);
				nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
				newtuple = heap_form_tuple(rel_dsc, datum, nulls);
				CatalogTupleInsert(convert_rel, newtuple);
				heap_freetuple(newtuple);
			}
			break;
		case ICONVERT_UPDATE:
			if (tuple && compareCvtfrom)
			{
				Datum		datum[Natts_ora_convert];
				bool		nulls[Natts_ora_convert];
				
				datum[Anum_ora_convert_cvtkind -1] = CharGetDatum(stmt->cvtkind);
				datum[Anum_ora_convert_cvtname -1] = CharGetDatum(stmt->cvtname);
				datum[Anum_ora_convert_cvtfrom -1] = PointerGetDatum(newcvtfrom);
				datum[Anum_ora_convert_cvtto -1] = PointerGetDatum(newcvtto);
				nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
				newtuple = heap_form_tuple(rel_dsc, datum, nulls);
				CatalogTupleUpdate(convert_rel, &tuple->t_self, newtuple);
				heap_freetuple(newtuple);
			}
			else
			{
				elog(ERROR, "UPDATE 0");
			}
			
			break;
		case ICONVERT_DELETE:
			if (tuple && compareCvtfrom)
			{
				CatalogTupleDelete(convert_rel, &tuple->t_self);
			}
			else
			{
				if (!stmt->if_exists)
				{
					elog(ERROR, "DELETE 0");
				}
			}
			
			break;
		default:
			elog(WARNING, "unrecognized commandType: %d",
					(int) stmt->action);
			break;
	}
	table_endscan(rel_scan);
	table_close(convert_rel, RowExclusiveLock);
}

#ifdef ADB
/* 
 * Execute cluster Oracle type implicit conversion creation.
 */
void ClusterExecImplicitConvert(StringInfo mem_toc)
{
	OraImplicitConvertStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_IMPLICIT_CONVERT, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found CreateNodeStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(OraImplicitConvertStmt, loadNode(&buf));

	ExecImplicitConvertLocal(stmt);
}


/*
 * Get the oid of all coordinators or datanode masters 
 * that do not include this node in the cluster.
 */
static List*
getMasterNodeOid(void)
{
	HeapTuple		tuple;
	Relation		rel;
	TableScanDesc	scan;
	Form_pgxc_node	xc_node;
	List 			*nodeOid_list = NIL;

	rel = table_open(PgxcNodeRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while ((tuple=heap_getnext(scan, ForwardScanDirection)) != NULL)
	{	
		xc_node = (Form_pgxc_node)GETSTRUCT(tuple);
		if (xc_node->node_type == PGXC_NODE_COORDINATOR || 
			xc_node->node_type == PGXC_NODE_DATANODE)
		{
			if (xc_node->oid == PGXCNodeOid)
				continue;
			nodeOid_list = list_append_unique_oid(nodeOid_list, xc_node->oid);
		}
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	return nodeOid_list;
}
#endif 