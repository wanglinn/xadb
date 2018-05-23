#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_aux_class.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

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
	simple_heap_insert(auxrelation, tuple);
	/* keep the catalog indexes up to date */
	CatalogUpdateIndexes(auxrelation, tuple);
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

Oid
LookupAuxMasterRel(Oid auxrelid, AttrNumber *attnum)
{
	HeapTuple				tuple;
	Form_pg_aux_class		auxtup;
	Oid						master_relid;

	if (!OidIsValid(auxrelid))
	{
		if (attnum)
			*attnum = InvalidAttrNumber;
		return InvalidOid;
	}

	tuple = SearchSysCache1(AUXCLASSIDENT,
							ObjectIdGetDatum(auxrelid));
	if (!HeapTupleIsValid(tuple))
	{
		if (attnum)
			*attnum = InvalidAttrNumber;
		return InvalidOid;
	}

	auxtup = (Form_pg_aux_class) GETSTRUCT(tuple);
	master_relid = auxtup->relid;
	if (attnum)
		*attnum = auxtup->attnum;

	ReleaseSysCache(tuple);

	return master_relid;
}

bool
IsAuxRelation(Oid auxrelid)
{
	HeapTuple				tuple;

	if (!OidIsValid(auxrelid))
		return false;

	tuple = SearchSysCache1(AUXCLASSIDENT,
							ObjectIdGetDatum(auxrelid));
	if (!HeapTupleIsValid(tuple))
		return false;

	ReleaseSysCache(tuple);

	return true;
}

static List *
MakeAuxTableColumns(Form_pg_attribute auxcolumn, Relation rel, AttrNumber *distattnum)
{
	ColumnDef		   *coldef;
	List			   *tableElts = NIL;
	RelationLocInfo	   *loc;
	AttrNumber			attnum;			/* distribute column attrnum */
	Form_pg_attribute	discolumn;

	Assert(auxcolumn && rel);

	/* auxiliary column */
	coldef = makeColumnDef(NameStr(auxcolumn->attname),
						   auxcolumn->atttypid,
						   auxcolumn->atttypmod,
						   auxcolumn->attcollation);
	tableElts = lappend(tableElts, coldef);

	/* distribute column */
	loc = RelationGetLocInfo(rel);
	if (IsRelationDistributedByValue(loc))
	{
		attnum = loc->partAttrNum;
	} else
	if (IsRelationDistributedByUserDefined(loc))
	{
		Assert(list_length(loc->funcAttrNums) == 1);
		attnum = linitial_int(loc->funcAttrNums);
	} else
	{
		/* should not reach here */
		attnum = InvalidAttrNumber;
	}
	Assert(AttrNumberIsForUserDefinedAttr(attnum));
	if (distattnum)
		*distattnum = attnum;
	discolumn = rel->rd_att->attrs[attnum - 1];
	coldef = makeColumnDef(NameStr(discolumn->attname),
						   discolumn->atttypid,
						   discolumn->atttypmod,
						   discolumn->attcollation);
	tableElts = lappend(tableElts, coldef);

	/* additional fixed columns */
	coldef = makeColumnDef("auxnodeid",
						   INT4OID,
						   -1,
						   0);
	tableElts = lappend(tableElts, coldef);

	coldef = makeColumnDef("auxctid",
						   TIDOID,
						   -1,
						   0);
	tableElts = lappend(tableElts, coldef);

	return tableElts;
}

List *
QueryRewriteAuxStmt(Query *auxquery)
{
	CreateAuxStmt	   *auxstmt;
	CreateStmt		   *create_stmt;
	IndexStmt		   *index_stmt;
	HeapTuple			atttuple;
	Form_pg_attribute	auxattform;
	Form_pg_attribute	disattform;
	AttrNumber			distattnum;
	Relation			master_relation;
	Oid					master_relid;
	RelationLocInfo	   *master_reloc;
	StringInfoData		querystr;
	Query			   *create_query;
	List			   *raw_insert_parsetree = NIL;
	List			   *each_querytree_list = NIL;
	List			   *rewrite_tree_list = NIL;
	ListCell		   *lc = NULL,
					   *lc_query = NULL;
	Query			   *insert_query = NULL;

	if (auxquery->commandType != CMD_UTILITY ||
		!IsA(auxquery->utilityStmt, CreateAuxStmt))
		elog(ERROR, "Expect auxiliary table query rewriten");

	auxstmt = (CreateAuxStmt *) auxquery->utilityStmt;
	create_stmt = (CreateStmt *)auxstmt->create_stmt;
	index_stmt = (IndexStmt *) auxstmt->index_stmt;

	/* Sanity check */
	Assert(create_stmt && index_stmt);
	Assert(auxstmt->master_relation);
	Assert(auxstmt->aux_column);
	Assert(create_stmt->master_relation);

	/* Master relation check */
	master_relid = RangeVarGetRelidExtended(auxstmt->master_relation,
											ShareLock,
											false, false,
											RangeVarCallbackOwnsRelation,
											NULL);
	master_relation = relation_open(master_relid, NoLock);
	master_reloc = RelationGetLocInfo(master_relation);
	if (IsRelationReplicated(master_reloc))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("no need to build auxiliary table for replication table")));
	if (IsRelationDistributedByUserDefined(master_reloc) &&
		list_length(master_reloc->funcAttrNums) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("auxiliary table on master table which distribute by "
				 		"user-defined function with more than 2 arguments is "
				 		"not supported yet")));

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
		char relname[NAMEDATALEN];
		snprintf(relname, sizeof(relname), "_%s_%s_aux",
			RelationGetRelationName(master_relation), NameStr(auxattform->attname));
		create_stmt->relation = makeRangeVar(NULL, pstrdup(relname), -1);
		index_stmt->relation = makeRangeVar(NULL, pstrdup(relname), -1);
	}

	/* makeup table elements */
	create_stmt->tableElts = MakeAuxTableColumns(auxattform, master_relation, &distattnum);
	Assert(AttributeNumberIsValid(distattnum));
	create_stmt->aux_attnum = auxattform->attnum;
	disattform = master_relation->rd_att->attrs[distattnum - 1];

	create_query = copyObject(auxquery);
	create_query->commandType = CMD_UTILITY;
	create_query->utilityStmt = (Node *) create_stmt;

	initStringInfo(&querystr);
	deparse_query(create_query, &querystr, NIL, false, false);

	/* create auxiliary table first */
	ProcessUtility(create_query->utilityStmt,
				   querystr.data,
				   PROCESS_UTILITY_TOPLEVEL, NULL, NULL,
				   false,
				   NULL);

	/* Insert into auxiliary table */
	resetStringInfo(&querystr);
	appendStringInfoString(&querystr, "INSERT INTO ");
	if (create_stmt->relation->schemaname)
		appendStringInfo(&querystr, "%s.", create_stmt->relation->schemaname);
	appendStringInfo(&querystr, "%s ", create_stmt->relation->relname);
	appendStringInfo(&querystr, "SELECT %s, %s, xc_node_id, ctid FROM ",
					NameStr(auxattform->attname),
					NameStr(disattform->attname));
	if (auxstmt->master_relation->schemaname)
		appendStringInfo(&querystr, "%s.", auxstmt->master_relation->schemaname);
	appendStringInfo(&querystr, "%s;", auxstmt->master_relation->relname);

	ReleaseSysCache(atttuple);
	relation_close(master_relation, NoLock);

	raw_insert_parsetree = pg_parse_query(querystr.data);
	foreach (lc, raw_insert_parsetree)
	{
		each_querytree_list = pg_analyze_and_rewrite((Node *) lfirst(lc), querystr.data, NULL, 0);
		foreach (lc_query, each_querytree_list)
		{
			if (IsA(lfirst(lc_query), Query))
			{
				insert_query = (Query *) lfirst(lc_query);
				insert_query->canSetTag = false;
				insert_query->querySource = QSRC_PARSER;
			}
			rewrite_tree_list = lappend(rewrite_tree_list, lfirst(lc_query));
		}
	}

	/* Create index for auxiliary table */
	auxquery->utilityStmt = (Node *) index_stmt;
	auxquery->canSetTag = false;
	auxquery->querySource = QSRC_PARSER;

	return lappend(rewrite_tree_list, auxquery);
}
