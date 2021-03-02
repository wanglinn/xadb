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
#include "catalog/dependency.h"
#include "catalog/pg_type_d.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/ora_convert.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
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

#define REMOTE_KEY_CREATE_CONVERT		1
#define REMOTE_KEY_SOURCE_STRING		2
#define REMOTE_KEY_DROP_CONVERT			3
#endif /* ADB */


static void CreateOracleConvertLocal(CreateOracleConvertStmt *stmt, ParseState *pstate);
static void DropOracleConvertLocal(DropStmt *stmt);
static oidvector* GetTypeOidVector(List *list, ListCell *lc, int count, bool missing_ok, ParseState *pstate);

#ifdef ADB
void ClusterCreateOracleConvert(StringInfo mem_toc);
void ClusterDropOracleConvert(StringInfo mem_toc);
static List* getMasterNodeOid(void);
#endif	/* ADB */


/* 
 * Oracle type implicit conversion creation.
 */
void CreateOracleConvert(CreateOracleConvertStmt *stmt, ParseState *pstate)
{
#ifdef ADB
	ListCell   *lc;
	List	   *master_node_oid;
	List	   *nodeOids;
	List	   *remoteList;
	Oid			oid;
	bool		include_myself = false;

	master_node_oid = getMasterNodeOid();

	nodeOids = NIL;
	foreach(lc, master_node_oid)
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

		ClusterTocSetCustomFun(&msg, ClusterCreateOracleConvert);

		begin_mem_toc_insert(&msg, REMOTE_KEY_CREATE_CONVERT);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_CREATE_CONVERT);

		begin_mem_toc_insert(&msg, REMOTE_KEY_SOURCE_STRING);
		save_node_string(&msg, pstate->p_sourcetext);
		end_mem_toc_insert(&msg, REMOTE_KEY_SOURCE_STRING);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	/* Create an implicit transform in local */
	if (include_myself)
		CreateOracleConvertLocal(stmt, pstate);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
#else
	CreateOracleConvertLocal(stmt, pstate);
#endif	/* ADB */
}

/* 
 * Execute local Oracle type implicit conversion creation.
 */
static void CreateOracleConvertLocal(CreateOracleConvertStmt *stmt, ParseState *pstate)
{
	Relation			convert_rel;
	HeapTuple			tuple, newtuple;
	oidvector		   *cvtfrom;
	oidvector		   *cvtto;
	int					cvtfromList_count = 0;
	int					cvttoList_count = 0;
	int					i;
	ObjectAddress		myself;
	ObjectAddress		referenced;

	if (stmt->cvtto == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("convert to can not empty"),
				 parser_errposition(pstate, stmt->location)));

	cvtfromList_count = list_length(stmt->cvtfrom);
	cvttoList_count = list_length(stmt->cvtto);
	
	if (cvtfromList_count < 1)
		elog(ERROR, "The number of unqualified parameters or inconsistent parameters.");

	/* Check parameter */
	if (cvtfromList_count != cvttoList_count && 
		(stmt->cvtkind == ORA_CONVERT_KIND_FUNCTION || stmt->cvtkind == ORA_CONVERT_KIND_OPERATOR))
		elog(ERROR, "The number of unqualified parameters or inconsistent parameters.");

	cvtfrom = GetTypeOidVector(stmt->cvtfrom, list_head(stmt->cvtfrom), cvtfromList_count, false, pstate);
	cvtto = GetTypeOidVector(stmt->cvtto, list_head(stmt->cvtto), cvttoList_count, false, pstate);

	Assert(stmt->cvtname);
	convert_rel = table_open(OraConvertRelationId, RowExclusiveLock);
	tuple = SearchSysCache3(ORACONVERTSCID,
							CharGetDatum(stmt->cvtkind),
							CStringGetDatum(stmt->cvtname),
							PointerGetDatum(cvtfrom));

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

		myself.objectId = ((Form_ora_convert)GETSTRUCT(tuple))->cvtid;
		deleteDependencyRecordsFor(OraConvertRelationId, myself.objectId, false);

		MemSet(reps, false, sizeof(reps));
		datum[Anum_ora_convert_cvtto - 1] = PointerGetDatum(cvtto);
		nulls[Anum_ora_convert_cvtto - 1] = false;
		reps[Anum_ora_convert_cvtto - 1] = true;
		newtuple = heap_modify_tuple(tuple, RelationGetDescr(convert_rel), datum, nulls, reps);
		CatalogTupleUpdate(convert_rel, &tuple->t_self, newtuple);
		heap_freetuple(newtuple);
		ReleaseSysCache(tuple);
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

			myself.objectId = 1;
		}else
		{
			Datum d = slot_getattr(slot,
								  Anum_ora_convert_cvtid,
								  &nulls[Anum_ora_convert_cvtid-1]);
			Assert(nulls[Anum_ora_convert_cvtid-1] == false);
			datum[Anum_ora_convert_cvtid-1] = ObjectIdGetDatum(DatumGetObjectId(d) + 1);

			myself.objectId = DatumGetObjectId(d) + 1;
			datum[Anum_ora_convert_cvtid-1] = ObjectIdGetDatum(myself.objectId);
		}
		ExecDropSingleTupleTableSlot(slot);
		index_endscan(scan);
		index_close(index_rel, AccessShareLock);

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
	}

	/* make dependency entries */
	myself.classId = OraConvertRelationId;
	Assert(OidIsValid(myself.objectId));
	myself.objectSubId = 0;

	for (i = 0; i < cvtfromList_count; i++)
	{
		/* dependency on convert from type */
		referenced.classId = TypeRelationId;
		referenced.objectId = cvtfrom->values[i];
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	for (i = 0; i < cvttoList_count; i++)
	{
		/* dependency on convert to type */
		referenced.classId = TypeRelationId;
		referenced.objectId = cvtto->values[i];
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);
	/* Post creation hook for new convert */
	InvokeObjectPostCreateHook(OraConvertRelationId, myself.objectId, 0);

	table_close(convert_rel, RowExclusiveLock);
}

Oid GetOracleConvertOid(List *objects, bool missing_ok)
{
	ListCell   *lc;
	oidvector  *cvtfrom;
	Datum		cvtkind;
	Datum		cvtname;
	Oid			result;

	lc = list_head(objects);
	Assert(IsA(lfirst(lc), Integer));
	cvtkind = CharGetDatum(intVal(lfirst(lc)));

	lc = lnext(objects, lc);
	Assert(IsA(lfirst(lc), String));
	cvtname = CStringGetDatum(strVal(lfirst(lc)));

	lc = lnext(objects, lc);
	cvtfrom = GetTypeOidVector(objects, lc, list_length(objects)-2, missing_ok, NULL);
	if (cvtfrom == NULL)
	{
		Assert(missing_ok);
		return InvalidOid;
	}

	result = GetSysCacheOid3(ORACONVERTSCID, Anum_ora_convert_cvtid,
							 cvtkind,
							 cvtname,
							 PointerGetDatum(cvtfrom));
	pfree(cvtfrom);
	if (!OidIsValid(result) &&
		missing_ok == false)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("convert not exist")));

	return result;
}

void DropOracleConvertById(Oid convertid)
{
	Relation	rel;
	ScanKeyData	key;
	SysScanDesc	scan;
	HeapTuple	tuple;

	rel = table_open(OraConvertRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_ora_convert_cvtid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(convertid));
	scan = systable_beginscan(rel, OraConvertIdIndexId, true, NULL, 1, &key);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for oracle convert %u", convertid);
	CatalogTupleDelete(rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

void DropOracleConvert(DropStmt *stmt)
{
#ifdef ADB
	ListCell   *lc;
	List	   *master_node_oid;
	List	   *nodeOids;
	List	   *remoteList;
	Oid			oid;
	bool		include_myself = false;

	master_node_oid = getMasterNodeOid();

	nodeOids = NIL;
	foreach(lc, master_node_oid)
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

		ClusterTocSetCustomFun(&msg, ClusterDropOracleConvert);

		begin_mem_toc_insert(&msg, REMOTE_KEY_DROP_CONVERT);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_DROP_CONVERT);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	/* Create an implicit transform in local */
	if (include_myself)
		DropOracleConvertLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
#else
	DropOracleConvertLocal(stmt);
#endif	/* ADB */
}

static void DropOracleConvertLocal(DropStmt *stmt)
{
	Relation		convert_rel;
	HeapTuple		tuple;
	ListCell	   *lc;
	Datum			cvtkind;
	Datum			cvtname;
	oidvector	   *cvtfrom;

	lc = list_head(stmt->objects);
	Assert(IsA(lfirst(lc), Integer));
	cvtkind = CharGetDatum(intVal(lfirst(lc)));

	lc = lnext(stmt->objects, lc);
	Assert(IsA(lfirst(lc), String));
	cvtname = CStringGetDatum(strVal(lfirst(lc)));

	lc = lnext(stmt->objects, lc);
	cvtfrom = GetTypeOidVector(stmt->objects, lc, list_length(stmt->objects)-2, true, NULL);

	convert_rel = table_open(OraConvertRelationId, RowExclusiveLock);
	tuple = SearchSysCache3(ORACONVERTSCID,
							cvtkind,
							cvtname,
							PointerGetDatum(cvtfrom));
	if (HeapTupleIsValid(tuple))
	{
		CatalogTupleDelete(convert_rel, &tuple->t_self);
		ReleaseSysCache(tuple);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("oracle convert not exist.")));

	table_close(convert_rel, NoLock);	/* lock table until end of transaction */
}


#ifdef ADB
/* 
 * Execute cluster Oracle type implicit conversion creation.
 */
void ClusterCreateOracleConvert(StringInfo mem_toc)
{
	CreateOracleConvertStmt *stmt;
	ParseState *pstate;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_CREATE_CONVERT, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found CreateNodeStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(CreateOracleConvertStmt, loadNode(&buf));

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = mem_toc_lookup(mem_toc, REMOTE_KEY_SOURCE_STRING, NULL);

	CreateOracleConvertLocal(stmt, pstate);
}


void ClusterDropOracleConvert(StringInfo mem_toc)
{
	DropStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_DROP_CONVERT, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found DropConvertStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(DropStmt, loadNode(&buf));

	DropOracleConvertLocal(stmt);
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

static Oid
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

static oidvector* GetTypeOidVector(List *list, ListCell *lc, int count, bool missing_ok, ParseState *pstate)
{
	bool		find_oid;
	int			i;
	Oid			oid;
	oidvector  *ov = buildoidvector(NULL, count);
	TypeName   *typeName;

	for (i=0;i<count;++i)
	{
		typeName = lfirst_node(TypeName, lc);
		oid = TypenameGetTypOid(typeName, &find_oid);
		if (!find_oid)
		{
			if (missing_ok)
			{
				pfree(ov);
				return NULL;
			}
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("type \"%s\" does not exist",TypeNameToString(typeName)),
					 parser_errposition(pstate, typeName->location)));
		}
		ov->values[i] = oid;
		lc = lnext(list, lc);
	}

	return ov;
}