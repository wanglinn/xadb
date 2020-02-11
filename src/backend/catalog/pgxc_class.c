/*-------------------------------------------------------------------------
 *
 * pgxc_class.c
 *	routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_opclass_d.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"
#include "utils/array.h"

const DistributeNameType all_distribute_name_type[] = {
	{LOCATOR_TYPE_HASH,			"hash"},
	{LOCATOR_TYPE_REPLICATED,	"replication"},
	{LOCATOR_TYPE_LIST,			"list"},
	{LOCATOR_TYPE_MODULO,		"modulo"},
	{LOCATOR_TYPE_RANDOM,		"random"},
	{LOCATOR_TYPE_RANGE,		"range"},
	{LOCATOR_TYPE_HASHMAP,		"hashmap"}
};
const uint32 cnt_distribute_name_type = lengthof(all_distribute_name_type);

static void SetDistributeByDatums(Datum *datums, bool *nulls, bool *replace, List *keys, char loc_type)
{
	int2vector *attr_array;
	oidvector  *class_array;
	oidvector  *collation_array;
	List	   *exprs;
	LocatorKeyInfo *key;
	ListCell   *lc;
	uint32		i;
	uint32		nkeys;

	switch(loc_type)
	{
	case LOCATOR_TYPE_HASH:
	case LOCATOR_TYPE_HASHMAP:
	case LOCATOR_TYPE_MODULO:
	case LOCATOR_TYPE_LIST:
	case LOCATOR_TYPE_RANGE:
		if (list_length(keys) <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid distribute by key length %d", list_length(keys))));
		nkeys = list_length(keys);
		attr_array = buildint2vector(NULL, nkeys);
		if (loc_type == LOCATOR_TYPE_MODULO)
			class_array = NULL;
		else
			class_array = buildoidvector(NULL, nkeys);
		if (loc_type == LOCATOR_TYPE_LIST ||
			loc_type == LOCATOR_TYPE_RANGE)
			collation_array = buildoidvector(NULL, nkeys);
		else
			collation_array = NULL;
		exprs = NIL;
		i = 0;
		foreach (lc, keys)
		{
			key = lfirst(lc);
			if (key->attno == InvalidAttrNumber)
			{
				Assert(key->key != NULL);
				exprs = lappend(exprs, key->key);
				attr_array->values[i] = InvalidAttrNumber;
			}else
			{
				Assert(key->attno > 0);
				attr_array->values[i] = key->attno;
			}
			if (class_array)
				class_array->values[i] = key->opclass;
			if (collation_array)
				collation_array->values[i] = key->collation;
		}

		datums[Anum_pgxc_class_pcattrs - 1] = PointerGetDatum(attr_array);
		nulls[Anum_pgxc_class_pcattrs - 1] = false;
		if (class_array)
		{
			datums[Anum_pgxc_class_pcclass - 1] = PointerGetDatum(class_array);
			nulls[Anum_pgxc_class_pcclass - 1] = false;
		}else
		{
			nulls[Anum_pgxc_class_pcclass - 1] = true;
		}
		if (exprs != NIL)
		{
			char *str = nodeToString(exprs);
			datums[Anum_pgxc_class_pcexprs - 1] = CStringGetTextDatum(str);
			nulls[Anum_pgxc_class_pcexprs - 1] = false;
		}else
		{
			nulls[Anum_pgxc_class_pcexprs - 1] = true;
		}
		if (collation_array)
		{
			datums[Anum_pgxc_class_pccollation - 1] = PointerGetDatum(collation_array);
			nulls[Anum_pgxc_class_pccollation - 1] = false;
		}else
		{
			nulls[Anum_pgxc_class_pccollation - 1] = true;
		}
		break;
	case LOCATOR_TYPE_REPLICATED:
	case LOCATOR_TYPE_RANDOM:
		/* nothing todo */
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("unknown distribute type %d", loc_type)));
	}
	if (replace)
	{
		replace[Anum_pgxc_class_pcattrs - 1] = true;
		replace[Anum_pgxc_class_pcclass - 1] = true;
		replace[Anum_pgxc_class_pcexprs - 1] = true;
		replace[Anum_pgxc_class_pccollation - 1] = true;
	}
}

static void SetDistributeToDatums(Datum *datums, bool *nulls, bool *replace, List *values, Oid *oids, int numnodes, char loc_type)
{
	oidvector  *nodes_array;

	switch(loc_type)
	{
	case LOCATOR_TYPE_LIST:
	case LOCATOR_TYPE_RANGE:
		if (list_length(values) != numnodes)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("values count not same node count")));
		datums[Anum_pgxc_class_pcvalues - 1] = CStringGetTextDatum(nodeToString(values));
		nulls[Anum_pgxc_class_pcvalues - 1] = false;
		break;
	case LOCATOR_TYPE_HASH:
		if (values != NIL)
		{
			if (list_length(values) != numnodes)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("values count not same node count")));
			datums[Anum_pgxc_class_pcvalues - 1] = CStringGetTextDatum(nodeToString(values));
			nulls[Anum_pgxc_class_pcvalues - 1] = false;
		}else
		{
			nulls[Anum_pgxc_class_pcvalues - 1] = true;
		}
		break;
	default:
		nulls[Anum_pgxc_class_pcvalues - 1] = true;
		break;
	}

	nodes_array = buildoidvector(oids, numnodes);
	datums[Anum_pgxc_class_nodeoids - 1] = PointerGetDatum(nodes_array);
	nulls[Anum_pgxc_class_nodeoids - 1] = false;
	if (replace)
	{
		replace[Anum_pgxc_class_nodeoids - 1] = true;
		replace[Anum_pgxc_class_pcvalues - 1] = true;
	}
}

static void SetKeysDepend(Oid reloid, List *keys)
{
	ListCell	   *lc;
	LocatorKeyInfo *key;
	ObjectAddress	myself;
	ObjectAddress	referenced;

	myself.classId = RelationRelationId;
	myself.objectId = reloid;
	myself.objectSubId = 0;

	foreach (lc, keys)
	{
		key = lfirst(lc);
		if (OidIsValid(key->opclass))
		{
			referenced.classId = OperatorClassRelationId;
			referenced.objectId = key->opclass;
			referenced.objectSubId = 0;
			recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
		}
		if (OidIsValid(key->collation) &&
			key->collation != DEFAULT_COLLATION_OID)
		{
			referenced.classId = CollationRelationId;
			referenced.objectId = key->collation;
			referenced.objectSubId = 0;
			recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
		}
		if (key->key != NULL)
			recordDependencyOnSingleRelExpr(&myself,
											(Node*)key->key,
											reloid,
											DEPENDENCY_NORMAL,
											DEPENDENCY_AUTO,
											true);
	}
}

/*
 * PgxcClassCreate
 *		Create a pgxc_class entry
 */
void
PgxcClassCreate(Oid pcrelid,
				char pclocatortype,
				List *keys,
				List *values,
				int numnodes,
				Oid *nodes)
{
	Relation	pgxcclassrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_class];
	Datum		datums[Natts_pgxc_class];

	if (!OidIsValid(pcrelid))
	{
		elog(ERROR, "Invalid pgxc relation.");
		return;
	}

	MemSet(nulls, true, sizeof(nulls));
	MemSet(datums, 0, sizeof(datums));

	datums[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);
	nulls[Anum_pgxc_class_pcrelid - 1] = false;
	datums[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);
	nulls[Anum_pgxc_class_pclocatortype - 1] = false;

	SetDistributeByDatums(datums, nulls, NULL, keys, pclocatortype);
	SetKeysDepend(pcrelid, keys);
	SetDistributeToDatums(datums, nulls, NULL, values, nodes, numnodes, pclocatortype);

	/* Open the relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(RelationGetDescr(pgxcclassrel), datums, nulls);

	CatalogTupleInsert(pgxcclassrel, htup);

	heap_close(pgxcclassrel, RowExclusiveLock);
}


/*
 * PgxcClassAlter
 *		Modify a pgxc_class entry with given data
 */
void
PgxcClassAlter(Oid pcrelid,
			   char pclocatortype,
			   List *keys,
			   List *values,
			   int numnodes,
			   Oid *nodes,
			   PgxcClassAlterType type)
{
	Relation	rel;
	HeapTuple	oldtup, newtup;

	Datum		new_record[Natts_pgxc_class];
	bool		new_record_nulls[Natts_pgxc_class];
	bool		new_record_repl[Natts_pgxc_class];

	Assert(OidIsValid(pcrelid));

	rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
	oldtup = SearchSysCacheCopy1(PGXCCLASSRELID,
								 ObjectIdGetDatum(pcrelid));

	if (!HeapTupleIsValid(oldtup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	/* Initialize fields */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, true, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/* Fields are updated depending on operation type */
	if (type & PGXC_CLASS_ALTER_DISTRIBUTION)
	{
		new_record[Anum_pgxc_class_pclocatortype - 1] = pclocatortype;
		new_record_nulls[Anum_pgxc_class_pclocatortype - 1] = false;
		new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
		SetDistributeByDatums(new_record, new_record_nulls, new_record_repl, keys, pclocatortype);
	}
	if (type & PGXC_CLASS_ALTER_NODES)
	{
		SetDistributeToDatums(new_record, new_record_nulls, new_record_repl, values, nodes, numnodes, pclocatortype);
	}

	/* Set up new fields */
	/* Relation Oid */
	if (new_record_repl[Anum_pgxc_class_pcrelid - 1])
		new_record[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);

	/* Locator type */
	if (new_record_repl[Anum_pgxc_class_pclocatortype - 1])
		new_record[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	/* Attribute number of distribution column */
	if (type & PGXC_CLASS_ALTER_DISTRIBUTION)
	{
		/* remove dependency on the old attribute */
		deleteDependencyRecordsForClass(PgxcClassRelationId, pcrelid,
										TypeRelationId, DEPENDENCY_NORMAL);
		deleteDependencyRecordsForClass(PgxcClassRelationId, pcrelid,
										CollationRelationId, DEPENDENCY_NORMAL);

		SetKeysDepend(pcrelid, keys);
	}

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * RemovePGXCClass():
 *		Remove extended PGXC information
 */
void
RemovePgxcClass(Oid pcrelid)
{
	Relation  relation;
	HeapTuple tup;

	/*
	 * Delete the pgxc_class tuple.
	 */
	relation = heap_open(PgxcClassRelationId, RowExclusiveLock);
	tup = SearchSysCache(PGXCCLASSRELID,
						 ObjectIdGetDatum(pcrelid),
						 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

void
CreatePgxcRelationAttrDepend(Oid relid, AttrNumber attnum)
{
	ObjectAddress myself, referenced;
	Oid typid, collid;
	int32 typmod;

	if (!OidIsValid(relid) || !AttributeNumberIsValid(attnum))
		return ;

	get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);

	myself.classId = PgxcClassRelationId;
	myself.objectId = relid;
	myself.objectSubId = 0;

	referenced.classId = TypeRelationId;
	referenced.objectId = typid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* The default collation is pinned, so don't bother recording it */
	if (OidIsValid(collid) && collid != DEFAULT_COLLATION_OID)
	{
		referenced.classId = CollationRelationId;
		referenced.objectId = collid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
}
