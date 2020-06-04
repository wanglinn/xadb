/*-------------------------------------------------------------------------
 *
 * partdesc.c
 *		Support routines for manipulating partition descriptors
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/backend/partitioning/partdesc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "storage/bufmgr.h"
#include "storage/sinval.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/partcache.h"
#include "utils/syscache.h"
#ifdef ADB
#include "access/nbtree.h"
#include "access/hash.h"
#include "catalog/pg_opclass.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#endif /* ADB */

typedef struct PartitionDirectoryData
{
	MemoryContext pdir_mcxt;
	HTAB	   *pdir_hash;
}			PartitionDirectoryData;

typedef struct PartitionDirectoryEntry
{
	Oid			reloid;
	Relation	rel;
	PartitionDesc pd;
} PartitionDirectoryEntry;

static PartitionDesc BuildPartitionDescInternal(PartitionBoundSpec **boundspecs, Oid *oids,
												int nparts, PartitionKey key,
												MemoryContext mem_context);
/*
 * RelationBuildPartitionDesc
 *		Form rel's partition descriptor, and store in relcache entry
 *
 * Note: the descriptor won't be flushed from the cache by
 * RelationClearRelation() unless it's changed because of
 * addition or removal of a partition.  Hence, code holding a lock
 * that's sufficient to prevent that can assume that rd_partdesc
 * won't change underneath it.
 */
void
RelationBuildPartitionDesc(Relation rel)
{
	List	   *inhoids;
	PartitionBoundSpec **boundspecs = NULL;
	Oid		   *oids = NULL;
	ListCell   *cell;
	int			i,
				nparts;
	PartitionKey key = RelationGetPartitionKey(rel);

	/*
	 * Get partition oids from pg_inherits.  This uses a single snapshot to
	 * fetch the list of children, so while more children may be getting added
	 * concurrently, whatever this function returns will be accurate as of
	 * some well-defined point in time.
	 */
	inhoids = find_inheritance_children(RelationGetRelid(rel), NoLock);
	nparts = list_length(inhoids);

	/* Allocate arrays for OIDs and boundspecs. */
	if (nparts > 0)
	{
		oids = palloc(nparts * sizeof(Oid));
		boundspecs = palloc(nparts * sizeof(PartitionBoundSpec *));
	}

	/* Collect bound spec nodes for each partition. */
	i = 0;
	foreach(cell, inhoids)
	{
		Oid			inhrelid = lfirst_oid(cell);
		HeapTuple	tuple;
		PartitionBoundSpec *boundspec = NULL;

		/* Try fetching the tuple from the catcache, for speed. */
		tuple = SearchSysCache1(RELOID, inhrelid);
		if (HeapTupleIsValid(tuple))
		{
			Datum		datum;
			bool		isnull;

			datum = SysCacheGetAttr(RELOID, tuple,
									Anum_pg_class_relpartbound,
									&isnull);
			if (!isnull)
				boundspec = stringToNode(TextDatumGetCString(datum));
			ReleaseSysCache(tuple);
		}

		/*
		 * The system cache may be out of date; if so, we may find no pg_class
		 * tuple or an old one where relpartbound is NULL.  In that case, try
		 * the table directly.  We can't just AcceptInvalidationMessages() and
		 * retry the system cache lookup because it's possible that a
		 * concurrent ATTACH PARTITION operation has removed itself to the
		 * ProcArray but yet added invalidation messages to the shared queue;
		 * InvalidateSystemCaches() would work, but seems excessive.
		 *
		 * Note that this algorithm assumes that PartitionBoundSpec we manage
		 * to fetch is the right one -- so this is only good enough for
		 * concurrent ATTACH PARTITION, not concurrent DETACH PARTITION or
		 * some hypothetical operation that changes the partition bounds.
		 */
		if (boundspec == NULL)
		{
			Relation	pg_class;
			SysScanDesc scan;
			ScanKeyData key[1];
			Datum		datum;
			bool		isnull;

			pg_class = table_open(RelationRelationId, AccessShareLock);
			ScanKeyInit(&key[0],
						Anum_pg_class_oid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(inhrelid));
			scan = systable_beginscan(pg_class, ClassOidIndexId, true,
									  NULL, 1, key);
			tuple = systable_getnext(scan);
			datum = heap_getattr(tuple, Anum_pg_class_relpartbound,
								 RelationGetDescr(pg_class), &isnull);
			if (!isnull)
				boundspec = stringToNode(TextDatumGetCString(datum));
			systable_endscan(scan);
			table_close(pg_class, AccessShareLock);
		}

		/* Sanity checks. */
		if (!boundspec)
			elog(ERROR, "missing relpartbound for relation %u", inhrelid);
		if (!IsA(boundspec, PartitionBoundSpec))
			elog(ERROR, "invalid relpartbound for relation %u", inhrelid);

		/*
		 * If the PartitionBoundSpec says this is the default partition, its
		 * OID should match pg_partitioned_table.partdefid; if not, the
		 * catalog is corrupt.
		 */
		if (boundspec->is_default)
		{
			Oid			partdefid;

			partdefid = get_default_partition_oid(RelationGetRelid(rel));
			if (partdefid != inhrelid)
				elog(ERROR, "expected partdefid %u, but got %u",
					 inhrelid, partdefid);
		}

		/* Save results. */
		oids[i] = inhrelid;
		boundspecs[i] = boundspec;
		++i;
	}

	/* Assert we aren't about to leak any old data structure */
	Assert(rel->rd_pdcxt == NULL);
	Assert(rel->rd_partdesc == NULL);

	/*
	 * Now build the actual relcache partition descriptor.  Note that the
	 * order of operations here is fairly critical.  If we fail partway
	 * through this code, we won't have leaked memory because the rd_pdcxt is
	 * attached to the relcache entry immediately, so it'll be freed whenever
	 * the entry is rebuilt or destroyed.  However, we don't assign to
	 * rd_partdesc until the cached data structure is fully complete and
	 * valid, so that no other code might try to use it.
	 */
	rel->rd_pdcxt = AllocSetContextCreate(CacheMemoryContext,
										  "partition descriptor",
										  ALLOCSET_SMALL_SIZES);
	MemoryContextCopyAndSetIdentifier(rel->rd_pdcxt,
									  RelationGetRelationName(rel));

	rel->rd_partdesc = BuildPartitionDescInternal(boundspecs, oids, nparts, key, rel->rd_pdcxt);
}

static PartitionDesc
BuildPartitionDescInternal(PartitionBoundSpec **boundspecs, Oid *oids, int nparts,
						   PartitionKey key, MemoryContext mem_context)
{
	PartitionDesc		partdesc;
	PartitionBoundInfo	boundinfo;
	MemoryContext		oldcxt;
	int				   *mapping;
	int					i;

	partdesc = (PartitionDescData *)
		MemoryContextAllocZero(mem_context, sizeof(PartitionDescData));
	partdesc->nparts = nparts;
	/* If there are no partitions, the rest of the partdesc can stay zero */
	if (nparts > 0)
	{
		/* Create PartitionBoundInfo, using the caller's context. */
		boundinfo = partition_bounds_create(boundspecs, nparts, key, &mapping);

		/* Now copy all info into relcache's partdesc. */
		oldcxt = MemoryContextSwitchTo(mem_context);
		partdesc->boundinfo = partition_bounds_copy(boundinfo, key);
		partdesc->oids = (Oid *) palloc(nparts * sizeof(Oid));
		partdesc->is_leaf = (bool *) palloc(nparts * sizeof(bool));
		MemoryContextSwitchTo(oldcxt);

		/*
		 * Assign OIDs from the original array into mapped indexes of the
		 * result array.  The order of OIDs in the former is defined by the
		 * catalog scan that retrieved them, whereas that in the latter is
		 * defined by canonicalized representation of the partition bounds.
		 *
		 * Also record leaf-ness of each partition.  For this we use
		 * get_rel_relkind() which may leak memory, so be sure to run it in
		 * the caller's context.
		 */
		for (i = 0; i < nparts; i++)
		{
			int			index = mapping[i];

			partdesc->oids[index] = oids[i];
			partdesc->is_leaf[index] =
				(get_rel_relkind(oids[i]) != RELKIND_PARTITIONED_TABLE);
		}
	}

	return partdesc;
}

/*
 * CreatePartitionDirectory
 *		Create a new partition directory object.
 */
PartitionDirectory
CreatePartitionDirectory(MemoryContext mcxt)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(mcxt);
	PartitionDirectory pdir;
	HASHCTL		ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartitionDirectoryEntry);
	ctl.hcxt = mcxt;

	pdir = palloc(sizeof(PartitionDirectoryData));
	pdir->pdir_mcxt = mcxt;
	pdir->pdir_hash = hash_create("partition directory", 256, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemoryContextSwitchTo(oldcontext);
	return pdir;
}

/*
 * PartitionDirectoryLookup
 *		Look up the partition descriptor for a relation in the directory.
 *
 * The purpose of this function is to ensure that we get the same
 * PartitionDesc for each relation every time we look it up.  In the
 * face of current DDL, different PartitionDescs may be constructed with
 * different views of the catalog state, but any single particular OID
 * will always get the same PartitionDesc for as long as the same
 * PartitionDirectory is used.
 */
PartitionDesc
PartitionDirectoryLookup(PartitionDirectory pdir, Relation rel)
{
	PartitionDirectoryEntry *pde;
	Oid			relid = RelationGetRelid(rel);
	bool		found;

	pde = hash_search(pdir->pdir_hash, &relid, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * We must keep a reference count on the relation so that the
		 * PartitionDesc to which we are pointing can't get destroyed.
		 */
		RelationIncrementReferenceCount(rel);
		pde->rel = rel;
		pde->pd = RelationGetPartitionDesc(rel);
		Assert(pde->pd != NULL);
	}
	return pde->pd;
}

/*
 * DestroyPartitionDirectory
 *		Destroy a partition directory.
 *
 * Release the reference counts we're holding.
 */
void
DestroyPartitionDirectory(PartitionDirectory pdir)
{
	HASH_SEQ_STATUS status;
	PartitionDirectoryEntry *pde;

	hash_seq_init(&status, pdir->pdir_hash);
	while ((pde = hash_seq_search(&status)) != NULL)
		RelationDecrementReferenceCount(pde->rel);
}

/*
 * equalPartitionDescs
 *		Compare two partition descriptors for logical equality
 */
bool
equalPartitionDescs(PartitionKey key, PartitionDesc partdesc1,
					PartitionDesc partdesc2)
{
	int			i;

	if (partdesc1 != NULL)
	{
		if (partdesc2 == NULL)
			return false;
		if (partdesc1->nparts != partdesc2->nparts)
			return false;

		Assert(key != NULL || partdesc1->nparts == 0);

		/*
		 * Same oids? If the partitioning structure did not change, that is,
		 * no partitions were added or removed to the relation, the oids array
		 * should still match element-by-element.
		 */
		for (i = 0; i < partdesc1->nparts; i++)
		{
			if (partdesc1->oids[i] != partdesc2->oids[i])
				return false;
		}

		/*
		 * Now compare partition bound collections.  The logic to iterate over
		 * the collections is private to partition.c.
		 */
		if (partdesc1->boundinfo != NULL)
		{
			if (partdesc2->boundinfo == NULL)
				return false;

			if (!partition_bounds_equal(key->partnatts, key->parttyplen,
										key->parttypbyval,
										partdesc1->boundinfo,
										partdesc2->boundinfo))
				return false;
		}
		else if (partdesc2->boundinfo != NULL)
			return false;
	}
	else if (partdesc2 != NULL)
		return false;

	return true;
}

/*
 * get_default_oid_from_partdesc
 *
 * Given a partition descriptor, return the OID of the default partition, if
 * one exists; else, return InvalidOid.
 */
Oid
get_default_oid_from_partdesc(PartitionDesc partdesc)
{
	if (partdesc && partdesc->boundinfo &&
		partition_bound_has_default(partdesc->boundinfo))
		return partdesc->oids[partdesc->boundinfo->default_index];

	return InvalidOid;
}

#ifdef ADB
PartitionKey RelationGenerateDistributeKey(Relation rel, AttrNumber *attno,
						List *exprs, Oid *opclass, Oid *collation, char strategy, int16 natts)
{
	PartitionKey	key;
	MemoryContext	partkeycxt,
					oldcxt;
	int16			procnum;

	HeapTuple		opclasstup;
	Form_pg_opclass	opclassform;
	Oid				funcid;
	ListCell	   *lc;
	uint16			i;

	partkeycxt = AllocSetContextCreate(CurrentMemoryContext,
									   "disttribute key",
									   ALLOCSET_SMALL_SIZES);
	MemoryContextCopyAndSetIdentifier(partkeycxt,
									  RelationGetRelationName(rel));

	key = (PartitionKey) MemoryContextAllocZero(partkeycxt,
												sizeof(PartitionKeyData));

	key->strategy = strategy;
	key->partnatts = natts;

	if (exprs)
	{
		exprs = (List*)eval_const_expressions(NULL, (Node*)exprs);
		fix_opfuncids((Node*)exprs);

		oldcxt = MemoryContextSwitchTo(partkeycxt);
		key->partexprs = (List *) copyObject(exprs);
		MemoryContextSwitchTo(oldcxt);
	}

	oldcxt = MemoryContextSwitchTo(partkeycxt);
	key->partattrs = (AttrNumber *) palloc0(natts * sizeof(AttrNumber));
	key->partopfamily = (Oid *) palloc0(natts * sizeof(Oid));
	key->partopcintype = (Oid *) palloc0(natts * sizeof(Oid));
	key->partsupfunc = (FmgrInfo *) palloc0(natts * sizeof(FmgrInfo));

	key->partcollation = (Oid *) palloc0(natts * sizeof(Oid));

	/* Gather type and collation info as well */
	key->parttypid = (Oid *) palloc0(natts * sizeof(Oid));
	key->parttypmod = (int32 *) palloc0(natts * sizeof(int32));
	key->parttyplen = (int16 *) palloc0(natts * sizeof(int16));
	key->parttypbyval = (bool *) palloc0(natts * sizeof(bool));
	key->parttypalign = (char *) palloc0(natts * sizeof(char));
	key->parttypcoll = (Oid *) palloc0(natts * sizeof(Oid));
	MemoryContextSwitchTo(oldcxt);

	/* determine support function number to search for */
#warning do we need use HASHSTANDARD_PROC?
	procnum = (strategy == PARTITION_STRATEGY_HASH) ?
		HASHEXTENDED_PROC : BTORDER_PROC;

	/* Copy partattrs and fill other per-attribute info */
	memcpy(key->partattrs, attno, natts * sizeof(int16));
	lc = list_head(key->partexprs);

	for (i=0; i<natts; ++i)
	{
		/* Collect opfamily information */
		opclasstup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass[i]));
		if (!HeapTupleIsValid(opclasstup))
			elog(ERROR, "cache lookup failed for opclass %u", opclass[i]);

		opclassform = (Form_pg_opclass) GETSTRUCT(opclasstup);
		key->partopfamily[i] = opclassform->opcfamily;
		key->partopcintype[i] = opclassform->opcintype;

		/* Get a support function for the specified opfamily and datatypes */
		funcid = get_opfamily_proc(opclassform->opcfamily,
								   opclassform->opcintype,
								   opclassform->opcintype,
								   procnum);
		if (!OidIsValid(funcid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator class \"%s\" of access method %s is missing support function %d for type %s",
							NameStr(opclassform->opcname),
							(strategy == PARTITION_STRATEGY_HASH) ? "hash" : "btree",
							procnum,
							format_type_be(opclassform->opcintype))));

		fmgr_info_cxt(funcid, &key->partsupfunc[i], partkeycxt);

		/* Collation */
		if (collation)
			key->partcollation[i] = collation[i];
		else
			key->partcollation[i] = InvalidOid;

		/* Collect type information */
		if (attno[i] != 0)
		{
			Form_pg_attribute att = TupleDescAttr(rel->rd_att, attno[i] - 1);

			key->parttypid[i] = att->atttypid;
			key->parttypmod[i] = att->atttypmod;
			key->parttypcoll[i] = att->attcollation;
		}
		else
		{
			Node *node;
			if (lc == NULL)
				elog(ERROR, "wrong number of distribute key expressions");

			node = lfirst(lc);
			lc = lnext(lc);
			key->parttypid[i] = exprType(node);
			key->parttypmod[i] = exprTypmod(node);
			key->parttypcoll[i] = exprCollation(node);
		}
		get_typlenbyvalalign(key->parttypid[i],
							 &key->parttyplen[i],
							 &key->parttypbyval[i],
							 &key->parttypalign[i]);

		ReleaseSysCache(opclasstup);
	}

	return key;
}

PartitionKey RelationGenerateDistributeKeyFromLocInfo(Relation rel, struct RelationLocInfo *loc)
{
	List	   *exprs;
	ListCell   *lc;
	Oid			opclass[PARTITION_MAX_KEYS];
	Oid			collation[PARTITION_MAX_KEYS];
	AttrNumber	attr[PARTITION_MAX_KEYS];
	uint32		i;
	char		strategy;

	if (loc->locatorType == LOCATOR_TYPE_LIST)
		strategy = PARTITION_STRATEGY_LIST;
	else if (loc->locatorType == LOCATOR_TYPE_RANGE)
		strategy = PARTITION_STRATEGY_RANGE;
	else
		return NULL;

	Assert(list_length(loc->keys) > 0);
	Assert(list_length(loc->keys) <= PARTITION_MAX_KEYS);

	exprs = NIL;
	i = 0;
	foreach(lc, loc->keys)
	{
		LocatorKeyInfo *key = lfirst(lc);
		opclass[i] = key->opclass;
		collation[i] = key->collation;
		attr[i] = key->attno;
		if (key->attno == InvalidAttrNumber)
		{
			Assert(key->key != NULL);
			exprs = lappend(exprs, key->key);
		}
	}

	return RelationGenerateDistributeKey(rel,
										 attr,
										 exprs,
										 opclass,
										 collation,
										 strategy,
										 list_length(loc->keys));
}

PartitionDesc DistributeRelationGenerateDesc(PartitionKey key, List *nodeoids, List *values)
{
	ListCell			   *lc,*lc2;
	Oid					   *oids;
	PartitionBoundSpec	   *spec;
	PartitionBoundSpec	  **boundspecs;
	int						i,nparts;

	Assert(list_length(nodeoids) == list_length(values));
	nparts = list_length(nodeoids);

	boundspecs = palloc(nparts * sizeof(boundspecs[0]));
	oids = palloc(nparts * sizeof(oids[0]));
	i = 0;

	if (key->strategy == PARTITION_STRATEGY_LIST)
	{
		forboth(lc, values, lc2, nodeoids)
		{
			spec = makeNode(PartitionBoundSpec);
			spec->strategy = PARTITION_STRATEGY_LIST;
			spec->listdatums = lfirst_node(List, lc);

			boundspecs[i] = spec;
			oids[i] = lfirst_oid(lc2);
			++i;
		}
	}else if(key->strategy == PARTITION_STRATEGY_RANGE)
	{
		List *list2;
		forboth(lc, values, lc2, nodeoids)
		{
			list2 = lfirst_node(List, lc);
			Assert(list_length(list2) == 2);

			spec = makeNode(PartitionBoundSpec);
			spec->strategy = PARTITION_STRATEGY_RANGE;
			spec->lowerdatums = linitial_node(List, list2);
			spec->upperdatums = llast_node(List, list2);

			boundspecs[i] = spec;
			oids[i] = lfirst_oid(lc2);
			++i;
		}
	}else
	{
		return NULL;
	}

	return BuildPartitionDescInternal(boundspecs, oids, nparts, key, CurrentMemoryContext);
}
#endif /* ADB */
