#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_aux_class.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-node.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/ruleutils.h"
#include "pgxc/pgxc.h"
#include "storage/mem_toc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

extern bool enable_aux_dml;

static char *ChooseAuxTableName(const char *name1, const char *name2,
								const char *label, Oid namespaceid);
static List *MakeAuxTableColumns(Form_pg_attribute auxcolumn, Relation rel);
static PaddingAuxDataStmt *AnalyzeRewriteCreateAuxStmt(CreateAuxStmt *auxstmt);
static void TruncateAuxRelation(Relation rel);

/*
 * InsertAuxClassTuple
 *
 * add record for pg_aux_class
 */
void
InsertAuxClassTuple(Oid auxrelid, Oid relid, AttrNumber attnum)
{
	Relation		auxrelation;
	HeapTuple		tuple;
	Datum			values[Natts_pg_aux_class];
	bool			nulls[Natts_pg_aux_class];
	ObjectAddress	myself,
					referenced;

	/* Sanity check */
	Assert(OidIsValid(auxrelid));
	Assert(OidIsValid(relid));
	Assert(AttrNumberIsForUserDefinedAttr(attnum));

	MemSet(nulls, 0, sizeof(nulls));
	MemSet(values, 0, sizeof(values));

	values[Anum_pg_aux_class_auxrelid - 1] = ObjectIdGetDatum(auxrelid);
	values[Anum_pg_aux_class_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_aux_class_attnum - 1] = Int16GetDatum(attnum);

	auxrelation = heap_open(AuxClassRelationId, RowExclusiveLock);
	tuple = heap_form_tuple(RelationGetDescr(auxrelation), values, nulls);
	CatalogTupleInsert(auxrelation, tuple);
	heap_freetuple(tuple);
	heap_close(auxrelation, RowExclusiveLock);

	/* reference object address */
	ObjectAddressSubSet(referenced, RelationRelationId, relid, attnum);

	/* Make pg_aux_class object depend entry */
	ObjectAddressSet(myself, AuxClassRelationId, auxrelid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/* Make pg_class object depend entry */
	ObjectAddressSet(myself, RelationRelationId, auxrelid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
}

/*
 * RemoveAuxClassTuple
 *
 * remove record from pg_aux_class by "auxrelid" if valid
 * or "auxrelid" and "attnum".
 */
void
RemoveAuxClassTuple(Oid auxrelid, Oid relid, AttrNumber attnum)
{
	Relation	auxrelation;
	HeapTuple	tuple;

	auxrelation = heap_open(AuxClassRelationId, RowExclusiveLock);

	if (OidIsValid(auxrelid))
		tuple = SearchSysCache1(AUXCLASSIDENT,
								ObjectIdGetDatum(auxrelid));
	else
		tuple = SearchSysCache2(AUXCLASSRELIDATT,
								ObjectIdGetDatum(relid),
								Int16GetDatum(attnum));

	if (!HeapTupleIsValid(tuple))
		return ;

	simple_heap_delete(auxrelation, &(tuple->t_self));
	ReleaseSysCache(tuple);
	heap_close(auxrelation, RowExclusiveLock);
}

/*
 * LookupAuxRelation
 *
 * find out the auxiliary relation by the Oid of master
 * relation and relevant attribute number.
 */
Oid
LookupAuxRelation(Oid relid, AttrNumber attnum)
{
	HeapTuple				tuple;
	Form_pg_aux_class		auxtup;
	Oid						auxrelid;

	/*
	 * Sanity check
	 */
	if (!OidIsValid(relid) ||
		!AttrNumberIsForUserDefinedAttr(attnum))
		return InvalidOid;

	tuple = SearchSysCache2(AUXCLASSRELIDATT,
							ObjectIdGetDatum(relid),
							Int16GetDatum(attnum));
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;

	auxtup = (Form_pg_aux_class) GETSTRUCT(tuple);
	auxrelid = auxtup->auxrelid;

	ReleaseSysCache(tuple);

	return auxrelid;
}

/*
 * LookupAuxMasterRel
 *
 * find out Oid of the master relation by the specified
 * Oid of auxiliary relation.
 */
Oid
LookupAuxMasterRel(Oid auxrelid, AttrNumber *attnum)
{
	HeapTuple				tuple;
	Form_pg_aux_class		auxtup;
	Oid						master_relid;

	if (attnum)
		*attnum = InvalidAttrNumber;

	if (!OidIsValid(auxrelid))
		return InvalidOid;

	tuple = SearchSysCache1(AUXCLASSIDENT,
							ObjectIdGetDatum(auxrelid));
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;

	auxtup = (Form_pg_aux_class) GETSTRUCT(tuple);
	master_relid = auxtup->relid;
	if (attnum)
		*attnum = auxtup->attnum;

	Assert(AttributeNumberIsValid(auxtup->attnum));

	ReleaseSysCache(tuple);

	return master_relid;
}

/*
 * RelationIdGetAuxAttnum
 *
 * is it an auxiliary relation? retrun auxiliary attribute
 * number if true.
 */
bool
RelationIdGetAuxAttnum(Oid auxrelid, AttrNumber *attnum)
{
	HeapTuple			tuple;
	Form_pg_aux_class	auxtup;

	if (attnum)
		*attnum = InvalidAttrNumber;

	if (!OidIsValid(auxrelid))
		return false;

	tuple = SearchSysCache1(AUXCLASSIDENT,
							ObjectIdGetDatum(auxrelid));
	if (!HeapTupleIsValid(tuple))
		return false;

	auxtup = (Form_pg_aux_class) GETSTRUCT(tuple);
	if (attnum)
		*attnum = auxtup->attnum;

	Assert(AttributeNumberIsValid(auxtup->attnum));

	ReleaseSysCache(tuple);

	return true;
}

static char *
ChooseAuxTableName(const char *name1, const char *name2,
				   const char *label, Oid namespaceid)
{
	int 		pass = 0;
	char	   *relname = NULL;
	char		modlabel[NAMEDATALEN];

	/* try the unmodified label first */
	StrNCpy(modlabel, label, sizeof(modlabel));

	for (;;)
	{
		relname = makeObjectName(name1, name2, modlabel);

		if (!OidIsValid(get_relname_relid(relname, namespaceid)))
			break;

		/* found a conflict, so try a new name component */
		pfree(relname);
		snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
	}

	return relname;
}

bool HasAuxRelation(Oid relid)
{
	HeapTuple		tuple;
	ScanKeyData		skey;
	SysScanDesc		auxscan;
	Relation		auxrel;
	bool			result;

	if (relid < FirstNormalObjectId)
		return false;

	ScanKeyInit(&skey,
				Anum_pg_aux_class_relid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	auxrel = heap_open(AuxClassRelationId, AccessShareLock);
	auxscan = systable_beginscan(auxrel,
								 AuxClassRelidAttnumIndexId,
								 true,
								 NULL,
								 1,
								 &skey);
	tuple = systable_getnext(auxscan);
	result = HeapTupleIsValid(tuple);
	systable_endscan(auxscan);
	heap_close(auxrel, AccessShareLock);

	return result;
}

static List *
MakeAuxTableColumns(Form_pg_attribute auxcolumn, Relation rel)
{
	ColumnDef		   *coldef;
	List			   *tableElts = NIL;
	List			   *arrayBounds = NIL;
	Constraint		   *n;
	int					i;

	Assert(auxcolumn && rel);

	n = makeNode(Constraint);
	n->contype = CONSTR_NOTNULL;
	n->location = -1;

#if (Anum_aux_table_auxnodeid == 1)
	/* 1. additional fixed columns -- auxnodeid */
	coldef = makeColumnDef("auxnodeid",
						   INT4OID,
						   -1,
						   InvalidOid);
	coldef->constraints = lappend(coldef->constraints, n);
	tableElts = lappend(tableElts, coldef);
#else
#error need change var list order
#endif

#if (Anum_aux_table_auxctid == 2)
	/* 2. additional fixed columns -- auxctid */
	coldef = makeColumnDef("auxctid",
						   TIDOID,
						   -1,
						   InvalidOid);
	coldef->constraints = lappend(coldef->constraints, copyObject(n));
	tableElts = lappend(tableElts, coldef);
#else
#error need change var list order
#endif

#if (Anum_aux_table_key == 3)
	/* 3. auxiliary column */
	coldef = makeColumnDef(NameStr(auxcolumn->attname),
						   auxcolumn->atttypid,
						   auxcolumn->atttypmod,
						   auxcolumn->attcollation);
	/* is it an array column? */
	for (i = 0; i < auxcolumn->attndims; i++)
		arrayBounds = lappend(arrayBounds, makeInteger(-1));
	coldef->typeName->arrayBounds = arrayBounds;
	/* does it have not null constraint? */
	if (auxcolumn->attnotnull)
		coldef->constraints = lappend(coldef->constraints, copyObject(n));
	tableElts = lappend(tableElts, coldef);
#else
#error need change var list order
#endif

	return tableElts;
}

/*
 * AnalyzeRewriteCreateAuxStmt
 *
 * Do some necessary checkups and rewrite CreateAuxStmt.
 */
static PaddingAuxDataStmt *
AnalyzeRewriteCreateAuxStmt(CreateAuxStmt *auxstmt)
{
	PaddingAuxDataStmt *padding_stmt;
	CreateStmt		   *create_stmt;
	IndexStmt		   *index_stmt;
	HeapTuple			atttuple;
	Form_pg_attribute	auxattform;
	Relation			master_relation;
	Oid 				master_nspid;
	Oid 				master_relid;
	RelationLocInfo    *master_reloc;

	create_stmt = (CreateStmt *)auxstmt->create_stmt;
	index_stmt = (IndexStmt *) auxstmt->index_stmt;

	/* Sanity check */
	Assert(create_stmt && index_stmt);
	Assert(auxstmt->master_relation);
	Assert(auxstmt->aux_column);
	Assert(create_stmt->master_relation);
	Assert(create_stmt->tableElts == NIL);

	/* Master relation check */
	master_relid = RangeVarGetRelidExtended(auxstmt->master_relation,
											ShareLock,
											0,
											RangeVarCallbackOwnsRelation,
											NULL);
	master_relation = relation_open(master_relid, NoLock);

	/* cannot build auxiliary table on non-table relation */
	if (master_relation->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot build an auxiliary table on non-table relation \"%s\"",
				 		RelationGetRelationName(master_relation))));

	/* cannot build auxiliary table on auxiliary table */
	if (RelationIdIsAuxiliary(master_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot build an auxiliary table on auxiliary table \"%s\"",
				 		RelationGetRelationName(master_relation))));

	/* cannot build auxiliary table on local/temporary table */
	if (RelationGetLocInfo(master_relation) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot build an auxiliary table on local or temporary table \"%s\"",
				 		RelationGetRelationName(master_relation))));

	master_reloc = RelationGetLocInfo(master_relation);
	master_nspid = RelationGetNamespace(master_relation);
	switch (master_reloc->locatorType)
	{
		case LOCATOR_TYPE_REPLICATED:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("no need to build auxiliary table for replication table")));
			break;
		case LOCATOR_TYPE_RANDOM:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot build auxiliary table for roundrobin table")));
			break;
		case LOCATOR_TYPE_HASH:
		case LOCATOR_TYPE_MODULO:
		case LOCATOR_TYPE_HASHMAP:
			/* it is OK */
			break;
		case LOCATOR_TYPE_CUSTOM:
		case LOCATOR_TYPE_RANGE:
			/* not support yet */
			break;
		case LOCATOR_TYPE_NONE:
		case LOCATOR_TYPE_DISTRIBUTED:
		default:
			/* should not reach here */
			Assert(false);
			break;
	}

	/* Auxiliary column check */
	atttuple = SearchSysCacheAttName(master_relid, auxstmt->aux_column);
	if (!HeapTupleIsValid(atttuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist",
						auxstmt->aux_column)));
	auxattform = (Form_pg_attribute) GETSTRUCT(atttuple);
	if (!AttrNumberIsForUserDefinedAttr(auxattform->attnum))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("auxiliary table on system column \"%s\" is not supported",
				 auxstmt->aux_column)));
	if (IsDistribColumn(master_relid, auxattform->attnum))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("no need to build auxiliary table for distribute column \"%s\"",
				 auxstmt->aux_column)));

	/* choose auxiliary table name */
	if (create_stmt->relation == NULL)
	{
		char *nspname = get_namespace_name(RelationGetNamespace(master_relation));
		char *relname = ChooseAuxTableName(RelationGetRelationName(master_relation),
										   NameStr(auxattform->attname),
										   "aux", master_nspid);
		create_stmt->relation = makeRangeVar(nspname, relname, -1);
		index_stmt->relation = makeRangeVar(nspname, relname, -1);
	}

	/* keep the same persistence with master relation */
	create_stmt->relation->relpersistence = master_relation->rd_rel->relpersistence;

	padding_stmt = makeNode(PaddingAuxDataStmt);
	padding_stmt->masterrv = auxstmt->master_relation;
	padding_stmt->truncaux = false;
	padding_stmt->auxrvlist = list_make1(create_stmt->relation);

	/* makeup table elements */
	create_stmt->tableElts = MakeAuxTableColumns(auxattform, master_relation);
	create_stmt->aux_attnum = auxattform->attnum;
	if (create_stmt->distributeby == NULL)
	{
		create_stmt->distributeby = makeNode(DistributeBy);
		create_stmt->distributeby->disttype = LOCATOR_TYPE_HASH;
		create_stmt->distributeby->colname = pstrdup(NameStr(auxattform->attname));
	}

	ReleaseSysCache(atttuple);
	relation_close(master_relation, NoLock);

	return padding_stmt;
}

static void
TruncateAuxRelation(Relation rel)
{
	if (!rel || !RelationIsAuxiliary(rel))
		return ;

	TruncateRelation(rel, GetCurrentSubTransactionId());
}

void
ExecPaddingAuxDataStmt(PaddingAuxDataStmt *stmt, StringInfo msg)
{
	List			   *rnodes;
	AuxiliaryRelCopy   *auxcopy;
	Relation			master = NULL;
	Relation			auxrel = NULL;
	List			   *auxcopylist = NIL;
	ListCell		   *lc;

	Assert(stmt);
	Assert(stmt->masterrv);
	Assert(stmt->auxrvlist);

	if (IsCnMaster())
	{
		List		   *rconns = NIL;
		List		   *mnodes = NIL;
		List		   *auxrellist = NIL;
		int				auxid = 1;
		uint32			flags = EXEC_CLUSTER_FLAG_USE_SELF_AND_MEM_REDUCE;
		StringInfoData	buf;

		/* AnalyzeRewriteCreateAuxStmt has LOCKed already */
		master = heap_openrv(stmt->masterrv, NoLock);
		rnodes = NIL;
		if (master->rd_locator_info)
			rnodes = list_copy(master->rd_locator_info->nodeids);
		foreach (lc, stmt->auxrvlist)
		{
			auxrel = heap_openrv((RangeVar *) lfirst(lc), AccessExclusiveLock);
			if (RELATION_IS_OTHER_TEMP(auxrel))
			{
				heap_close(auxrel, AccessExclusiveLock);
				continue;
			}
			if (stmt->truncaux)
				TruncateAuxRelation(auxrel);
			auxrellist = lappend(auxrellist, auxrel);
			if (auxrel->rd_locator_info)
				rnodes = list_concat_unique_oid(rnodes, auxrel->rd_locator_info->nodeids);
			auxcopy = MakeAuxRelCopyInfoFromMaster(master, auxrel, auxid++);
			auxcopylist = lappend(auxcopylist, auxcopy);
		}

		initStringInfo(&buf);

		begin_mem_toc_insert(&buf, AUX_REL_COPY_INFO);
		SerializeAuxRelCopyInfo(&buf, auxcopylist);
		end_mem_toc_insert(&buf, AUX_REL_COPY_INFO);

		mnodes = list_copy(rnodes);
		begin_mem_toc_insert(&buf, AUX_REL_MAIN_NODES);
		if (flags & EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE)
			mnodes = lappend_oid(mnodes, PGXCNodeOid);
		saveNode(&buf, (const Node *) mnodes);
		end_mem_toc_insert(&buf, AUX_REL_MAIN_NODES);

		rconns = ExecStartClusterAuxPadding(rnodes,
											(Node *) stmt,
											&buf,
											flags);

		if (flags & EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE)
		{
			ListCell *lc1, *lc2;

			forboth(lc1, auxrellist, lc2, auxcopylist)
			{
				auxrel = (Relation) lfirst(lc1);
				auxcopy = (AuxiliaryRelCopy *) lfirst(lc2);

				DoPaddingDataForAuxRel(master,
									   auxrel,
									   mnodes,
									   auxcopy);

				heap_close(auxrel, NoLock);
			}
			list_free(auxrellist);
			list_free(auxcopylist);
		}
		list_free(mnodes);

		heap_close(master, NoLock);

		/* cleanup */
		if (rconns)
		{
			ListCell	   *lc;
			PGconn		   *conn;
			PGresult	   *res;
			ExecStatusType	rst;

			foreach(lc, rconns)
			{
				conn = lfirst(lc);
				if (PQisCopyInState(conn))
					PQputCopyEnd(conn, NULL);
			}

			foreach(lc, rconns)
			{
				for(;;)
				{
					CHECK_FOR_INTERRUPTS();
					res = PQgetResult(conn);
					if (res == NULL)
						break;
					rst = PQresultStatus(res);
					switch(rst)
					{
					case PGRES_EMPTY_QUERY:
					case PGRES_COMMAND_OK:
						break;
					case PGRES_TUPLES_OK:
					case PGRES_SINGLE_TUPLE:
						PQclear(res);
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("datanode padding auxiliary command result tuples"),
								 errnode(PQNConnectName(conn))));
						break;
					case PGRES_COPY_OUT:
						{
							const char *msg;
							int len;

							PQclear(res);
							res = NULL;
							len = PQgetCopyDataBuffer(conn, &msg, false);
							if (len > 0)
								clusterRecvTuple(NULL, msg, len, NULL, conn);
						}
						break;
					case PGRES_COPY_IN:
					case PGRES_COPY_BOTH:
						/* copy in should not happen */
						PQputCopyEnd(conn, NULL);
						break;
					case PGRES_NONFATAL_ERROR:
						PQNReportResultError(res, conn, NOTICE, false);
						break;
					case PGRES_BAD_RESPONSE:
					case PGRES_FATAL_ERROR:
						PQNReportResultError(res, conn, ERROR, true);
						break;
					}
					PQclear(res);
				}
			}
		}
	} else
	{
		ListCell	   *lc;
		StringInfoData	buf;
		RangeVar	   *auxrv;

		Assert(msg != NULL);
		buf.data = mem_toc_lookup(msg, AUX_REL_COPY_INFO, &buf.len);
		Assert(buf.data != NULL && buf.len > 0);
		buf.maxlen = buf.len;
		buf.cursor = 0;
		auxcopylist = RestoreAuxRelCopyInfo(&buf);

		buf.data = mem_toc_lookup(msg, AUX_REL_MAIN_NODES, &buf.len);
		Assert(buf.data != NULL);
		buf.maxlen = buf.len;
		buf.cursor = 0;
		rnodes = (List*)loadNode(&buf);

		master = heap_openrv_extended(stmt->masterrv, ShareLock, true);
		auxrv = makeNode(RangeVar);
		foreach (lc, auxcopylist)
		{
			auxcopy = (AuxiliaryRelCopy *) lfirst(lc);
			auxrv->schemaname = auxcopy->schemaname;
			auxrv->relname = auxcopy->relname;
			auxrel = heap_openrv_extended(auxrv, AccessExclusiveLock, true);
			if (stmt->truncaux)
				TruncateAuxRelation(auxrel);

			DoPaddingDataForAuxRel(master,
								   auxrel,
								   rnodes,
								   auxcopy);

			if (auxrel)
				heap_close(auxrel, NoLock);
		}
		pfree(auxrv);

		if (master)
			heap_close(master, NoLock);
	}
}

void
ExecCreateAuxStmt(CreateAuxStmt *auxstmt,
				  const char *queryString,
				  ProcessUtilityContext context,
				  DestReceiver *dest,
				  bool sentToRemote,
				  char *completionTag)
{
	PaddingAuxDataStmt *padding_stmt;
	PlannedStmt *stmt;

	if (!IsCnMaster())
		return ;

	padding_stmt = AnalyzeRewriteCreateAuxStmt(auxstmt);

	stmt = makeNode(PlannedStmt);
	stmt->commandType = CMD_UTILITY;
	stmt->utilityStmt = auxstmt->create_stmt;

	/*
	 * Process create auxiliary table
	 */
	ProcessUtility(stmt,
				   queryString,
				   context,
				   NULL,
				   NULL,
				   dest,
				   sentToRemote,
				   completionTag);

	/* Padding data for auxiliary data */
	ExecPaddingAuxDataStmt(padding_stmt, NULL);

	stmt->utilityStmt = auxstmt->index_stmt;

	/* create index for auxiliary table */
	ProcessUtility(stmt,
				   queryString,
				   PROCESS_UTILITY_SUBCOMMAND,
				   NULL,
				   NULL,
				   None_Receiver,
				   false,
				   NULL);
}

void
PaddingAuxDataOfMaster(Relation master)
{
	Oid				  auxrelid;
	ListCell		 *lc;
	PaddingAuxDataStmt *stmt;

	if (!IsCnMaster() ||
		!master->rd_auxlist)
		return ;

	stmt = makeNode(PaddingAuxDataStmt);
	stmt->masterrv = makeRangeVar(get_namespace_name(RelationGetNamespace(master)),
								  RelationGetRelationName(master),
								  -1);
	stmt->truncaux = true;
	foreach (lc, master->rd_auxlist)
	{
		auxrelid = lfirst_oid(lc);
		stmt->auxrvlist = lappend(stmt->auxrvlist,
								  makeRangeVar(get_namespace_name(get_rel_namespace(auxrelid)),
								  get_rel_name(auxrelid), -1));
	}
	ExecPaddingAuxDataStmt(stmt, NULL);
}

void RelationBuildAuxiliary(Relation rel)
{
	HeapTuple				tuple;
	Form_pg_aux_class		form_aux;
	Relation				auxrel;
	ScanKeyData				skey;
	SysScanDesc				auxscan;
	List				   *auxlist;
	Bitmapset			   *auxatt;
	MemoryContext			old_context;
	AssertArg(rel);

	if (RelationGetRelid(rel) < FirstNormalObjectId)
	{
		rel->rd_auxlist = NIL;
		rel->rd_auxatt = NULL;
		return;
	}

	ScanKeyInit(&skey,
				Anum_pg_aux_class_relid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));

	auxrel = heap_open(AuxClassRelationId, AccessShareLock);
	auxscan = systable_beginscan(auxrel,
								 AuxClassRelidAttnumIndexId,
								 true,
								 NULL,
								 1,
								 &skey);

	old_context = MemoryContextSwitchTo(CacheMemoryContext);
	auxlist = NIL;
	auxatt = NULL;
	while (HeapTupleIsValid(tuple = systable_getnext(auxscan)))
	{
		form_aux = (Form_pg_aux_class) GETSTRUCT(tuple);
		auxlist = lappend_oid(auxlist, form_aux->auxrelid);
		auxatt = bms_add_member(auxatt, form_aux->attnum);
	}
	rel->rd_auxlist = auxlist;
	rel->rd_auxatt = auxatt;
	MemoryContextSwitchTo(old_context);

	systable_endscan(auxscan);
	heap_close(auxrel, AccessShareLock);
}

Bitmapset *MakeAuxMainRelResultAttnos(Relation rel)
{
	Bitmapset *attr;
	int x;
	Assert(rel->rd_auxatt && rel->rd_locator_info);

	/* system attrs */
	attr = bms_make_singleton(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber);
	attr = bms_add_member(attr, XC_NodeIdAttributeNumber - FirstLowInvalidHeapAttributeNumber);

	/* auxiliary columns */
	x = -1;
	while ((x=bms_next_member(rel->rd_auxatt, x)) >= 0)
		attr = bms_add_member(attr, x - FirstLowInvalidHeapAttributeNumber);

	return attr;
}

List *MakeMainRelTargetForAux(Relation main_rel, Relation aux_rel, Index relid, bool target_entry)
{
	Form_pg_attribute	main_attr;
	Form_pg_attribute	aux_attr;
	TupleDesc			main_desc = RelationGetDescr(main_rel);
	TupleDesc			aux_desc = RelationGetDescr(aux_rel);
	Var				   *var;
	TargetEntry		   *te;
	List			   *result = NIL;
	char			   *attname;
	int					anum;
	int					i,j;

	for(i=anum=0;i<aux_desc->natts;++i)
	{
		aux_attr = TupleDescAttr(aux_desc, i);
		if (aux_attr->attisdropped)
			continue;

		++anum;
		attname = NameStr(aux_attr->attname);
		if (anum == Anum_aux_table_auxnodeid)
		{
			main_attr = SystemAttributeDefinition(XC_NodeIdAttributeNumber,
												  RelationGetForm(main_rel)->relhasoids);
		}else if (anum == Anum_aux_table_auxctid)
		{
			main_attr = SystemAttributeDefinition(SelfItemPointerAttributeNumber,
												  RelationGetForm(main_rel)->relhasoids);
		}else
		{
			for(j=0;j<main_desc->natts;++j)
			{
				main_attr = TupleDescAttr(main_desc, j);
				if (main_attr->attisdropped)
					continue;

				if (strcmp(attname, NameStr(main_attr->attname)) == 0)
					break;
			}
			if (j >= main_desc->natts)
				main_attr = NULL;
		}

		if (main_attr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("Can not found column \"%s\" in relation \"%s\" for auxiliary table \"%s\"",
					 		attname, RelationGetRelationName(main_rel), RelationGetRelationName(aux_rel)),
					 err_generic_string(PG_DIAG_SCHEMA_NAME, get_namespace_name(RelationGetNamespace(main_rel))),
					 err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(main_rel)),
					 err_generic_string(PG_DIAG_COLUMN_NAME, attname)));

		if (main_attr->atttypid != aux_attr->atttypid ||
			main_attr->atttypmod != aux_attr->atttypmod)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("Column \"%s\" in relation \"%s\" off type %s does not match auxiliary column of type %s.",
					 		NameStr(main_attr->attname),
							RelationGetRelationName(main_rel),
							format_type_with_typemod(main_attr->atttypid, main_attr->atttypmod),
							format_type_with_typemod(aux_attr->atttypid, aux_attr->atttypmod)),
					 err_generic_string(PG_DIAG_SCHEMA_NAME, get_namespace_name(RelationGetNamespace(main_rel))),
					 err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(main_rel)),
					 err_generic_string(PG_DIAG_COLUMN_NAME, attname)));

		var = makeVar(relid, main_attr->attnum, main_attr->atttypid, main_attr->atttypmod, main_attr->attcollation, 0);
		if (target_entry)
		{
			te = makeTargetEntry((Expr*)var, (AttrNumber)anum, pstrdup(NameStr(main_attr->attname)), false);
			result = lappend(result, te);
		}else
		{
			result = lappend(result, var);
		}
	}

	return result;
}
