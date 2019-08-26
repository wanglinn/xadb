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
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"
#include "utils/array.h"


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
	oidvector  *nodes_array;
	LocatorKeyInfo *key;

	if (!OidIsValid(pcrelid))
	{
		elog(ERROR, "Invalid pgxc relation.");
		return;
	}

	MemSet(nulls, 0, sizeof(nulls));
	MemSet(datums, 0, sizeof(datums));

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(nodes, numnodes);

	datums[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
	datums[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	switch(pclocatortype)
	{
	case LOCATOR_TYPE_HASH:
	case LOCATOR_TYPE_HASHMAP:
	case LOCATOR_TYPE_MODULO:
		if (list_length(keys) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid distribute by key length %d", list_length(keys))));
		key = linitial(keys);
		if (key->attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("not support expression for distribute by yet!")));
		Assert(key->attno > 0);
		datums[Anum_pgxc_class_pcattnum - 1] = Int16GetDatum(key->attno);
		break;
	case LOCATOR_TYPE_LIST:
	case LOCATOR_TYPE_RANGE:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("list and range not support for distribute by not support yet!")));
	case LOCATOR_TYPE_REPLICATED:
	case LOCATOR_TYPE_RANDOM:
		/* nothing todo */
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("unknown distribute type %d", pclocatortype)));
	}

	/* Node information */
	datums[Anum_pgxc_class_nodeoids - 1] = PointerGetDatum(nodes_array);

	/* Open the relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(pgxcclassrel->rd_att, datums, nulls);

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
	oidvector  *nodes_array;

	Datum		new_record[Natts_pgxc_class];
	bool		new_record_nulls[Natts_pgxc_class];
	bool		new_record_repl[Natts_pgxc_class];

	Assert(OidIsValid(pcrelid));

	rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
	oldtup = SearchSysCacheCopy1(PGXCCLASSRELID,
								 ObjectIdGetDatum(pcrelid));

	if (!HeapTupleIsValid(oldtup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(nodes, numnodes);

	/* Initialize fields */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/* Fields are updated depending on operation type */
	switch (type)
	{
		case PGXC_CLASS_ALTER_DISTRIBUTION:
			new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
			new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
			break;
		case PGXC_CLASS_ALTER_NODES:
			new_record_repl[Anum_pgxc_class_nodeoids - 1] = true;
			break;
		case PGXC_CLASS_ALTER_ALL:
		default:
			new_record_repl[Anum_pgxc_class_pcrelid - 1] = true;
			new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
			new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
			new_record_repl[Anum_pgxc_class_nodeoids - 1] = true;
	}

	/* Set up new fields */
	/* Relation Oid */
	if (new_record_repl[Anum_pgxc_class_pcrelid - 1])
		new_record[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);

	/* Locator type */
	if (new_record_repl[Anum_pgxc_class_pclocatortype - 1])
		new_record[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	/* Attribute number of distribution column */
	if (new_record_repl[Anum_pgxc_class_pcattnum - 1])
	{
		LocatorKeyInfo *key;
		if (list_length(keys) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid distribute by key length %d", list_length(keys))));
		key = linitial(keys);
		if (key->attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("not support expression for distribute by yet!")));
		Assert(key->attno > 0);

		new_record[Anum_pgxc_class_pcattnum - 1] = Int16GetDatum(key->attno);

		/* remove dependency on the old attribute */
		deleteDependencyRecordsForClass(PgxcClassRelationId, pcrelid,
										TypeRelationId, DEPENDENCY_NORMAL);
		deleteDependencyRecordsForClass(PgxcClassRelationId, pcrelid,
										CollationRelationId, DEPENDENCY_NORMAL);
		/* then create new dependency */
		CreatePgxcRelationAttrDepend(pcrelid, Int16GetDatum(key->attno));
	}

	/* Node information */
	if (new_record_repl[Anum_pgxc_class_nodeoids - 1])
		new_record[Anum_pgxc_class_nodeoids - 1] = PointerGetDatum(nodes_array);

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

	simple_heap_delete(relation, &tup->t_self);

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
