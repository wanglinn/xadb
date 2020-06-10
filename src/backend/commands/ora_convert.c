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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/pg_type_d.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/ora_convert.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#ifdef ADB
#include "catalog/pgxc_node.h"
#include "executor/execCluster.h"
#include "libpq/libpq-node.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "storage/mem_toc.h"

#define REMOTE_KEY_IMPLICIT_CONVERT		1
#define REMOTE_KEY_SOURCE_STRING		2
#endif /* ADB */


void ExecImplicitConvert(OraImplicitConvertStmt *stmt, ParseState *pstate);
static void ExecImplicitConvertLocal(OraImplicitConvertStmt *stmt, ParseState *pstate);
Oid TypenameGetTypOid(TypeName *typname, bool *find);

#ifdef ADB
void ClusterExecImplicitConvert(StringInfo mem_toc);
static List* getMasterNodeOid(void);
#endif	/* ADB */


/* 
 * Oracle type implicit conversion creation.
 */
void ExecImplicitConvert(OraImplicitConvertStmt *stmt, ParseState *pstate)
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

		begin_mem_toc_insert(&msg, REMOTE_KEY_SOURCE_STRING);
		save_node_string(&msg, pstate->p_sourcetext);
		end_mem_toc_insert(&msg, REMOTE_KEY_SOURCE_STRING);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	/* Create an implicit transform in local */
	if (include_myself)
		ExecImplicitConvertLocal(stmt, pstate);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
}
#else
{
	ExecImplicitConvertLocal(stmt, pstate);
}
#endif	/* ADB */

/* 
 * Execute local Oracle type implicit conversion creation.
 */
static void ExecImplicitConvertLocal(OraImplicitConvertStmt *stmt, ParseState *pstate)
{
	Relation			convert_rel;
	HeapTuple			tuple, newtuple;
	oidvector		   *cvtfrom;
	oidvector		   *cvtto;
	TypeName		   *typeName;
	int					cvtfromList_count = 0;
	int					cvttoList_count = 0;
	int					i;
	bool				find_oid;


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
			typeName = lfirst_node(TypeName, cell);
			fromOids[i] = TypenameGetTypOid(typeName, &find_oid);
			if (!find_oid)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("Added data type not included."),
						 parser_errposition(pstate, typeName->location)));
			cell = lnext(cell);
		}
		cvtfrom = buildoidvector(fromOids, cvtfromList_count);
		pfree(fromOids);

		if (stmt->cvtto)
		{
			toOids = palloc(cvttoList_count * sizeof(Oid));
			cell = list_head(stmt->cvtto);
			for (i = 0; i < cvttoList_count; i++)
			{
				typeName = lfirst_node(TypeName, cell);
				toOids[i] = TypenameGetTypOid(typeName, &find_oid);
				if (!find_oid)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Added data type not included."),
							 parser_errposition(pstate, typeName->location)));
				cell = lnext(cell);
			}
			cvtto = buildoidvector(toOids, cvttoList_count);
			pfree(toOids);
		}
	}

	Assert(stmt->cvtname);
	convert_rel = table_open(OraConvertRelationId, RowExclusiveLock);
	tuple = SearchSysCache3(ORACONVERTSCID,
							CharGetDatum(stmt->cvtkind),
							CStringGetDatum(stmt->cvtname),
							PointerGetDatum(cvtfrom));

	if (stmt->action == ICONVERT_CREATE)
	{
		if (cvtto == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("convert to can not empty"),
					 parser_errposition(pstate, stmt->location)));
		}
		if (HeapTupleIsValid(tuple))
		{
			Datum		datum[Natts_ora_convert];
			bool		nulls[Natts_ora_convert];
			bool		reps[Natts_ora_convert];
			if (stmt->replace == false)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("convert already exists"),
						 parser_errposition(pstate, stmt->location)));
			
			MemSet(reps, false, sizeof(reps));
			datum[Anum_ora_convert_cvtto - 1] = PointerGetDatum(cvtto);
			nulls[Anum_ora_convert_cvtto - 1] = false;
			reps[Anum_ora_convert_cvtto - 1] = true;
			newtuple = heap_modify_tuple(tuple, RelationGetDescr(convert_rel), datum, nulls, reps);
			CatalogTupleUpdate(convert_rel, &tuple->t_self, newtuple);
			heap_freetuple(newtuple);
		}else
		{
			Relation		index_rel = index_open(OraConvertIdIndexId, AccessShareLock);
			IndexScanDesc	scan = index_beginscan(convert_rel, index_rel, SnapshotAny, 0, 0);
			TupleTableSlot *slot = table_slot_create(convert_rel, NULL);
			Datum		datum[Natts_ora_convert];
			bool		nulls[Natts_ora_convert];

			MemSet(nulls, true, sizeof(nulls));

			/* generate a new id */
			index_rescan(scan, NULL, 0, NULL, 0);
			if (index_getnext_slot(scan, BackwardScanDirection, slot) == false)
			{
				datum[Anum_ora_convert_cvtid - 1] = 1;
				nulls[Anum_ora_convert_cvtid-1] = false;
			}else
			{
				Datum d = slot_getattr(slot,
									  Anum_ora_convert_cvtid,
									  &nulls[Anum_ora_convert_cvtid-1]);
				Assert(nulls[Anum_ora_convert_cvtid-1] == false);
				datum[Anum_ora_convert_cvtid-1] = ObjectIdGetDatum(DatumGetObjectId(d) + 1);
			}
			ExecDropSingleTupleTableSlot(slot);
			index_endscan(scan);
			index_close(index_rel, NoLock);	/* lock until end of transaction */

			datum[Anum_ora_convert_cvtkind - 1] = CharGetDatum(stmt->cvtkind);
			nulls[Anum_ora_convert_cvtkind - 1] = false;
			datum[Anum_ora_convert_cvtname - 1] = CStringGetDatum(stmt->cvtname);
			nulls[Anum_ora_convert_cvtname - 1] = false;
			datum[Anum_ora_convert_cvtfrom - 1] = PointerGetDatum(cvtfrom);
			nulls[Anum_ora_convert_cvtfrom - 1] = false;
			datum[Anum_ora_convert_cvtto - 1] = PointerGetDatum(cvtto);
			nulls[Anum_ora_convert_cvtto - 1] = false;
			tuple = heap_form_tuple(RelationGetDescr(convert_rel), datum, nulls);
			CatalogTupleInsert(convert_rel, tuple);
			heap_freetuple(tuple);
			tuple = NULL;
		}
	}else if (stmt->action == ICONVERT_DELETE)
	{
		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("convert not exist"),
					 parser_errposition(pstate, stmt->location)));
		CatalogTupleDelete(convert_rel, &tuple->t_self);
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown convert command type %d", stmt->action),
				 parser_errposition(pstate, stmt->location)));
	}

	if (HeapTupleIsValid(tuple))
		ReleaseSysCache(tuple);

	table_close(convert_rel, NoLock);	/* lock table until end of transaction */
}

#ifdef ADB
/* 
 * Execute cluster Oracle type implicit conversion creation.
 */
void ClusterExecImplicitConvert(StringInfo mem_toc)
{
	OraImplicitConvertStmt *stmt;
	ParseState *pstate;
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

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = mem_toc_lookup(mem_toc, REMOTE_KEY_SOURCE_STRING, NULL);

	ExecImplicitConvertLocal(stmt, pstate);
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

Oid
TypenameGetTypOid(TypeName *typname, bool *find)
{
	char		*typeName;
	char		*schemaname;
	Oid			typeNamespace;
	Oid			typoid;


	/* deconstruct the name list */
	DeconstructQualifiedName(typname->names, &schemaname, &typeName);

	if (strcmp(typeName, "any") == 0)
	{
		*find = true;
		return (Oid) 0;
	}

	if (schemaname)
	{
		/* Convert list of names to a name and namespace */
		typeNamespace = QualifiedNameGetCreationNamespace(typname->names, &typeName);

		typoid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
								 CStringGetDatum(typeName),
								 ObjectIdGetDatum(typeNamespace));
	}
	else
	{
		/* Unqualified type name, so search the search path */
		typoid = TypenameGetTypid(typeName);
	}

	if (OidIsValid(typoid) && get_typisdefined(typoid))
	{
		*find = true;
		return typoid;
	}
	else
	{
		*find = false;
		return InvalidOid;
	}
	
}