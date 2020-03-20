/*-------------------------------------------------------------------------
 *
 * locator.c
 *		Functions that help manage table location information such as
 * partitioning and replication information.
 *
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/backend/pgxc/locator/locator.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/hash.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/sysattr.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "intercomm/inter-node.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "parser/parse_coerce.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/slot.h"
#include "postmaster/autovacuum.h"
#include "utils/fmgrprotos.h"
#include "utils/typcache.h"
#include "pgxc/slot.h"
#include "tcop/tcopprot.h"

#define PUSH_REDUCE_EXPR_BMS	1
#define PUSH_REDUCE_EXPR_LIST	2

typedef struct ExecNodeStateEvalInfo
{
	Expr xpr;
	List *list;
	ListCell *lc;
}ExecNodeStateEvalInfo;

typedef struct ReduceParam
{
	Param param;
	Index relid;
	AttrNumber attno;
}ReduceParam;

typedef struct PullReducePathExprVarAttnosContext
{
	Bitmapset  *varattnos;
	List	   *attnoList;
	Index 		relid;
	int			flags;	/* PUSH_REDUCE_EXPR_XXX */
} PullReducePathExprVarAttnosContext;

typedef struct CreateReduceExprContext
{
	List *oldAttrs;
	List *newAttrs;
	Index newRelid;
} CreateReduceExprContext;

static Expr *pgxc_find_distcol_expr(Index varno, AttrNumber attrNum, Node *quals);
static Oid adbGetSlaveNodeid(Oid masterid);

Oid		primary_data_node = InvalidOid;
int		num_preferred_data_nodes = 0;
Oid		preferred_data_node[MAX_PREFERRED_NODES];
#ifdef ADB
extern bool enable_readsql_on_slave;
#endif

/*
 * GetPreferredRepNodeIds
 * Pick any Datanode from given list, however fetch a preferred node first.
 */
List *
GetPreferredRepNodeIds(List *nodeids)
{
	ListCell *lc;
	if (list_length(nodeids) <= 0)
		elog(ERROR, "a list of nodes should have at least one node");

	foreach(lc, nodeids)
	{
		if (is_pgxc_nodepreferred(lfirst_oid(lc)))
			return list_make1_oid(lfirst_oid(lc));
	}

	return list_make1_oid(linitial_oid(nodeids));
}

/*
 * get_nodeid_from_modulo - determine node based on modulo
 *
 * compute_modulo
 */
static Oid
get_nodeid_from_modulo(int modulo, List *nodeids)
{
	if (nodeids == NIL || modulo >= list_length(nodeids) || modulo < 0)
		ereport(ERROR, (errmsg("Modulo value out of range\n")));

	return list_nth_oid(nodeids, modulo);
}


/*
 * GetRelationDistribColumn
 * Return hash column name for relation or NULL if relation is not distributed.
 * TODO: drop this function when FQS droped
 */
char *
GetRelationDistribColumn(RelationLocInfo *locInfo)
{
	LocatorKeyInfo *key;
	/* No relation, so simply leave */
	if (!locInfo)
		return NULL;

	/* No distribution column if relation is not distributed with a key */
	if (!IsRelationDistributedByValue(locInfo) ||
		list_length(locInfo->keys) != 1)
		return NULL;

	key = FirstLocKeyInfo(locInfo);
	if (key->attno == InvalidAttrNumber)
		return NULL;

	/* Return column name */
	return get_attname(locInfo->relid, key->attno, false);
}

/*
 * IsDistribOnlyOneColumn
 * Return whether column for relation is used only attNum for distribution or not.
 */
bool
IsDistribOnlyOneColumn(Oid relid, AttrNumber attNum)
{
	RelationLocInfo *locInfo = GetRelationLocInfo(relid);
	bool result;

	/* No locator info, so leave */
	if (!locInfo)
		return false;

	result = false;
	if (IsRelationDistributedByValue(locInfo) &&
		list_length(locInfo->keys) == 1)
	{
		result = (FirstLocKeyInfo(locInfo)->attno == attNum);
	}
	FreeRelationLocInfo(locInfo);

	return false;
}

bool LocatorIncludeColumn(RelationLocInfo *loc, AttrNumber attno, bool include_expr)
{
	return LocatorKeyIncludeColumn(loc->keys, attno, include_expr);
}

bool LocatorKeyIncludeColumn(List *keys, AttrNumber attno, bool include_expr)
{
	ListCell	   *lc;
	LocatorKeyInfo *key;
	bool			has_expr = false;

	foreach(lc, keys)
	{
		key = lfirst(lc);
		if (key->attno == attno)
			return true;
		if (key->key != NULL)
			has_expr = true;
	}

	if (include_expr && has_expr)
	{
		Bitmapset  *bms = NULL;
		bool		result;
		foreach(lc, keys)
		{
			key = lfirst(lc);
			if (key->key)
				pull_varattnos((Node*)key->key, 1, &bms);
		}

		result = bms_is_member(attno-FirstLowInvalidHeapAttributeNumber, bms);
		bms_free(bms);
		return result;
	}

	return false;
}

/*
 * IsTypeDistributable
 * Returns whether the data type is distributable using a column value.
 */
bool
IsTypeDistributable(Oid col_type)
{
	TypeCacheEntry *typeCache = lookup_type_cache(col_type, TYPECACHE_HASH_PROC);
	return OidIsValid(typeCache->hash_proc);
}

Oid
GetRandomRelNodeId(Oid relid)
{
	Relation	rel = table_open(relid, AccessShareLock);
	Datum		n;
	Oid			ret_node;

	Assert(rel->rd_locator_info);
	Assert(rel->rd_locator_info->locatorType == LOCATOR_TYPE_REPLICATED ||
		   rel->rd_locator_info->locatorType == LOCATOR_TYPE_RANDOM);

	n = DirectFunctionCall1(int4random_max, Int32GetDatum(list_length(rel->rd_locator_info->nodeids)));
	ret_node = list_nth_oid(rel->rd_locator_info->nodeids, DatumGetInt32(n));

	table_close(rel, AccessShareLock);

	return ret_node;
}

/*
 * IsTableDistOnPrimary
 * Does the table distribution list include the primary node?
 */
bool
IsTableDistOnPrimary(RelationLocInfo *rel_loc_info)
{
	if (!OidIsValid(primary_data_node) || !rel_loc_info)
		return false;

	return list_member_oid(rel_loc_info->nodeids, primary_data_node);
}


/*
 * IsLocatorInfoEqual
 * Check equality of given locator information
 */
bool
IsLocatorInfoEqual(const RelationLocInfo *a, const RelationLocInfo *b)
{
	ListCell *lc1,*lc2;

	if (a == b)
		return true;

	/* tested "a == b", run to here "a != b" is true */
	if (a == NULL || b == NULL)
		return false;

	if (a->relid != b->relid ||
		a->locatorType != b->locatorType ||
		list_length(a->keys) != list_length(b->keys) ||
		list_length(a->nodeids) != list_length(a->nodeids))
		return false;

	if (IsRelationDistributedByValue(a))
	{
		if (equal(a->nodeids, b->nodeids) == false)
			return false;
	}else if(list_equal_oid_without_order(a->nodeids, b->nodeids) == false)
	{
		return false;
	}

	forboth(lc1, a->keys, lc2, b->keys)
	{
		LocatorKeyInfo *l = lfirst(lc1);
		LocatorKeyInfo *r = lfirst(lc2);
		if (l->opclass != r->opclass ||
			l->opfamily != r->opfamily ||
			l->collation != r->collation)
			return false;
		if (equal(l->key, r->key) == false)
			return false;
	}

	if (equal(a->values, b->values) == false)
		return false;

	/* Everything is equal */
	return true;
}

ExecNodes *MakeExecNodesByOids(RelationLocInfo *loc_info, List *oids, RelationAccessType accesstype)
{
	ListCell *lc;
	ExecNodes *exec_nodes;
	if (oids == NIL)
		return NULL;

	exec_nodes = makeNode(ExecNodes);
	exec_nodes->accesstype = accesstype;
	exec_nodes->baselocatortype = loc_info->locatorType;
	//exec_nodes->en_relid = loc_info->relid;
	exec_nodes->nodeids = oids;
	foreach(lc, oids)
		exec_nodes->nodeids = list_append_unique_oid(exec_nodes->nodeids, lfirst_oid(lc));

	return exec_nodes;
}

/*
 * GetRelationNodes
 *
 * Get list of relation nodes
 * If the table is replicated and we are reading, we can just pick one.
 * If the table is partitioned, we apply partitioning column value, if possible.
 *
 * If the relation is partitioned, partValue will be applied if present
 * (indicating a value appears for partitioning column), otherwise it
 * is ignored.
 *
 * preferredNodes is only used when for replicated tables. If set, it will
 * use one of the nodes specified if the table is replicated on it.
 * This helps optimize for avoiding introducing additional nodes into the
 * transaction.
 *
 * The returned List is a copy, so it should be freed when finished.
 */
ExecNodes *
GetRelationNodes(RelationLocInfo *rel_loc_info,
				 int nelems,
				 Datum* dist_col_values,
				 bool* dist_col_nulls,
				 Oid* dist_col_types,
				 RelationAccessType accessType)
{
	ExecNodes  *exec_nodes;
	int			modulo;

	int			nodeIndex;
	int			slotstatus;
	List		*tmp_nodeids = NIL;
	ListCell	*lc;


	if (rel_loc_info == NULL)
		return NULL;
	
	/* Delete duplicate node information, avoid duplicate connection between poolmgr and datanode. */
	if (rel_loc_info->nodeids)
		foreach (lc, rel_loc_info->nodeids)
			tmp_nodeids  = list_append_unique_oid(tmp_nodeids, lfirst_oid(lc));
		
	exec_nodes = makeNode(ExecNodes);
	exec_nodes->baselocatortype = rel_loc_info->locatorType;
	exec_nodes->accesstype = accessType;

	switch (rel_loc_info->locatorType)
	{
		case LOCATOR_TYPE_REPLICATED:

			/*
			 * When intention is to read from replicated table, return all the
			 * nodes so that planner can choose one depending upon the rest of
			 * the JOIN tree. But while reading with update lock, we need to
			 * read from the primary node (if exists) so as to avoid the
			 * deadlock.
			 * For write access set primary node (if exists).
			 */
			exec_nodes->nodeids = tmp_nodeids;
			if (accessType == RELATION_ACCESS_UPDATE || accessType == RELATION_ACCESS_INSERT)
			{
				/* we need to write to all synchronously */
			}
			else if (accessType == RELATION_ACCESS_READ_FOR_UPDATE &&
					IsTableDistOnPrimary(rel_loc_info))
			{
				/*
				 * We should ensure row is locked on the primary node to
				 * avoid distributed deadlock if updating the same row
				 * concurrently
				 */
				exec_nodes->nodeids = list_make1_oid(primary_data_node);
			}
			break;

		case LOCATOR_TYPE_HASH:
		case LOCATOR_TYPE_MODULO:
			{
				if(dist_col_nulls[0])
				{
					if(accessType == RELATION_ACCESS_INSERT)
					{
						/* Insert NULL to first node*/
						modulo = 0;
					}else
					{
						exec_nodes->nodeids = tmp_nodeids;
						break;
					}
				}else
				{
					if(rel_loc_info->locatorType == LOCATOR_TYPE_HASH)
					{
						int32 hashVal = execHashValue(dist_col_values[0],
													  dist_col_types[0],
													  InvalidOid);
						modulo = (uint32)hashVal % (uint32)list_length(rel_loc_info->nodeids);
					}else
					{
						modulo = execModuloValue(dist_col_values[0],
												dist_col_types[0],
												list_length(rel_loc_info->nodeids));
					}
				}
				exec_nodes->nodeids = list_make1_oid(get_nodeid_from_modulo(modulo, rel_loc_info->nodeids));
			}
			break;

		case LOCATOR_TYPE_RANDOM:
			/*
			 * random, get random one in case of insert. If not insert, all
			 * node needed
			 */
			if (accessType == RELATION_ACCESS_INSERT)
				exec_nodes->nodeids = list_make1_oid(GetRandomRelNodeId(rel_loc_info->relid));
			else
				exec_nodes->nodeids = tmp_nodeids;
			break;

		case LOCATOR_TYPE_HASHMAP:
			{
				if(IS_PGXC_DATANODE)
					return NULL;

				if(dist_col_nulls[0])
				{
					if(accessType == RELATION_ACCESS_INSERT)
					{
						/* Insert NULL to first node*/
						modulo = 0;
						SlotGetInfo(0, &nodeIndex, &slotstatus);
						exec_nodes->nodeids = list_make1_oid(nodeIndex);
					}else
					{
						exec_nodes->nodeids = tmp_nodeids;
						break;
					}
				}else
				{
						int32 hashVal = execHashValue(dist_col_values[0],
													  dist_col_types[0],
													  InvalidOid);
						modulo = (uint32)hashVal % HASHMAP_SLOTSIZE;
						SlotGetInfo(modulo, &nodeIndex, &slotstatus);
						exec_nodes->nodeids = list_make1_oid(nodeIndex);
				}
			}
			break;
			/* PGXCTODO case LOCATOR_TYPE_RANGE: */
			/* PGXCTODO case LOCATOR_TYPE_CUSTOM: */
		default:
			ereport(ERROR, (errmsg("Error: no such supported locator type: %c\n",
								   rel_loc_info->locatorType)));
			break;
	}

	return exec_nodes;
}

/*
 * GetRelationNodesByQuals
 * A wrapper around GetRelationNodes to reduce the node list by looking at the
 * quals. varno is assumed to be the varno of reloid inside the quals. No check
 * is made to see if that's correct.
 */
ExecNodes *
GetRelationNodesByQuals(Oid reloid, Index varno, Node *quals,
						RelationAccessType relaccess)
{
	RelationLocInfo *rel_loc_info = GetRelationLocInfo(reloid);
	Expr			*distcol_expr = NULL;
	ExecNodes		*exec_nodes;
	Datum			distcol_value;
	bool			distcol_isnull;
	Oid				distcol_type;

	if (!rel_loc_info)
		return NULL;

	/*
	 * If the table distributed by value, check if we can reduce the Datanodes
	 * by looking at the qualifiers for this relation
	 */
	if (IsRelationDistributedByValue(rel_loc_info))
	{
		Oid				disttype;
		int32			disttypmod;
		Oid				collation;
		AttrNumber		attno;

		attno = GetFirstLocAttNumIfOnlyOne(rel_loc_info);
		get_atttypetypmodcoll(reloid, attno, &disttype, &disttypmod, &collation);
		distcol_expr = pgxc_find_distcol_expr(varno, attno, quals);

		/*
		 * If the type of expression used to find the Datanode, is not same as
		 * the distribution column type, try casting it. This is same as what
		 * will happen in case of inserting that type of expression value as the
		 * distribution column value.
		 */
		if (distcol_expr)
		{
			distcol_expr = (Expr *)coerce_to_target_type(NULL,
													(Node *)distcol_expr,
													exprType((Node *)distcol_expr),
													disttype, disttypmod,
													COERCION_ASSIGNMENT,
													COERCE_IMPLICIT_CAST, -1);
			/*
			 * PGXC_FQS_TODO: We should set the bound parameters here, but we don't have
			 * PlannerInfo struct and we don't handle them right now.
			 * Even if constant expression mutator changes the expression, it will
			 * only simplify it, keeping the semantics same
			 */
			distcol_expr = (Expr *)eval_const_expressions(NULL,
															(Node *)distcol_expr);
		}
	}

	if (distcol_expr && IsA(distcol_expr, Const))
	{
		Const *const_expr = (Const *)distcol_expr;
		distcol_value = const_expr->constvalue;
		distcol_isnull = const_expr->constisnull;
		distcol_type = const_expr->consttype;
	}
	else
	{
		distcol_value = (Datum) 0;
		distcol_isnull = true;
		distcol_type = InvalidOid;
	}

	exec_nodes = GetRelationNodes(rel_loc_info,
								  1,
								  &distcol_value,
								  &distcol_isnull,
								  &distcol_type,
								  relaccess);
	return exec_nodes;
}

/*
 * GetLocatorType
 * Returns the locator type of the table.
 */
char
GetLocatorType(Oid relid)
{
	char		ret = LOCATOR_TYPE_NONE;
	RelationLocInfo *locInfo = GetRelationLocInfo(relid);

	if (locInfo != NULL)
		ret = locInfo->locatorType;

	return ret;
}

/*
 * HasRelationLocator
 *
 * return true if the specified relation has locator, otherwise
 * return false.
 */
bool
HasRelationLocator(Oid relid)
{
	Relation		pcrel;
	ScanKeyData		skey;
	SysScanDesc		pcscan;
	bool			result;

	ScanKeyInit(&skey,
				Anum_pgxc_class_pcrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
	pcscan = systable_beginscan(pcrel,
								PgxcClassPgxcRelIdIndexId,
								true,
								NULL,
								1,
								&skey);
	result = HeapTupleIsValid(systable_getnext(pcscan));
	systable_endscan(pcscan);
	heap_close(pcrel, AccessShareLock);

	return result;
}

static void* GetPGXCClassAttr(HeapTuple htup, AttrNumber attno, TupleDesc desc, bool can_be_null, Oid relid)
{
	bool isnull;
	Datum datum;

	Assert(attno > 0 && attno <= (int) HeapTupleHeaderGetNatts(htup->t_data));
	datum = fastgetattr(htup, attno, desc, &isnull);

	if (isnull)
	{
		if (can_be_null == false)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid column(%d) data for pgxc_class(%u)", attno, relid)));
		}
		return NULL;
	}
	return DatumGetPointer(datum);
}
/*
 * RelationIdBuildLocator
 * Build locator information associated with the specified relation id.
 */
RelationLocInfo *
RelationIdBuildLocator(Oid relid)
{
	Relation		pcrel;
	ScanKeyData		skey;
	SysScanDesc		pcscan;
	HeapTuple		htup;
	Form_pgxc_class pgxc_class;
	RelationLocInfo*relationLocInfo;
	text		   *txt;
	int				j;

	ScanKeyInit(&skey,
				Anum_pgxc_class_pcrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
	pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true,
								NULL, 1, &skey);
	htup = systable_getnext(pcscan);
	if (!HeapTupleIsValid(htup))
	{
		systable_endscan(pcscan);
		heap_close(pcrel, AccessShareLock);
		return NULL;
	}

	pgxc_class = (Form_pgxc_class) GETSTRUCT(htup);
	relationLocInfo = (RelationLocInfo *) palloc0(sizeof(RelationLocInfo));
	relationLocInfo->relid = relid;
	relationLocInfo->locatorType = pgxc_class->pclocatortype;
	switch(relationLocInfo->locatorType)
	{
	case LOCATOR_TYPE_HASH:
	case LOCATOR_TYPE_HASHMAP:
	case LOCATOR_TYPE_LIST:
	case LOCATOR_TYPE_RANGE:
	case LOCATOR_TYPE_MODULO:
		{
			List		   *exprs = NIL;
			ListCell	   *lc = NULL;
			LocatorKeyInfo *key;
			int2vector	   *attr_array;
			oidvector	   *class_array;
			oidvector	   *collation_array = NULL;
			char		   *str;

			attr_array = GetPGXCClassAttr(htup, Anum_pgxc_class_pcattrs, RelationGetDescr(pcrel), false, relid);
			if (relationLocInfo->locatorType == LOCATOR_TYPE_MODULO)
			{
				class_array = NULL;
			}else
			{
				class_array = GetPGXCClassAttr(htup, Anum_pgxc_class_pcclass, RelationGetDescr(pcrel), false, relid);
				if (class_array->dim1 != attr_array->dim1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid column(%d) data for pgxc_class(%u)",
									Anum_pgxc_class_pcclass, relid)));
				}
			}

			txt = GetPGXCClassAttr(htup, Anum_pgxc_class_pcexprs, RelationGetDescr(pcrel), true, relid);
			if (txt != NULL)
			{
				str = text_to_cstring(txt);
				exprs = stringToNode(str);
				if (!IsA(exprs, List))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid column(%d) data for pgxc_class(%u)",
									Anum_pgxc_class_pcexprs, relid)));
				lc = list_head(exprs);
				pfree(str);
			}

			if (relationLocInfo->locatorType == LOCATOR_TYPE_LIST ||
				relationLocInfo->locatorType == LOCATOR_TYPE_RANGE)
			{
				txt = GetPGXCClassAttr(htup, Anum_pgxc_class_pcvalues, RelationGetDescr(pcrel), false, relid);
				str = text_to_cstring(txt);
				relationLocInfo->values = stringToNode(str);
				if (!IsA(relationLocInfo->values, List) ||
					list_length(relationLocInfo->values) != pgxc_class->nodeoids.dim1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid column(%d) data for pgxc_class(%u)",
									Anum_pgxc_class_pcvalues, relid)));
				}
				pfree(str);

				collation_array = GetPGXCClassAttr(htup, Anum_pgxc_class_pccollation, RelationGetDescr(pcrel), false, relid);
				if (class_array->dim1 != attr_array->dim1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid column(%d) data for pgxc_class(%u)",
									Anum_pgxc_class_pccollation, relid)));
				}
			}

			for (j=0;j<attr_array->dim1;++j)
			{
				key = palloc0(sizeof(*key));
				key->attno = attr_array->values[j];
				if (key->attno == InvalidAttrNumber)
				{
					if (lc == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("invalid column(%d) data for pgxc_class(%u)",
										Anum_pgxc_class_pcexprs, relid)));
					key->key = lfirst(lc);
					lc = lnext(lc);
				}
				if (relationLocInfo->locatorType == LOCATOR_TYPE_MODULO)
				{
					key->opclass =
						key->opfamily =
						key->collation = InvalidOid;
				}else
				{
					key->opclass = class_array->values[j];
					key->opfamily = get_opclass_family(key->opclass);
					if (collation_array)
						key->collation = collation_array->values[j];
					else
						key->collation = InvalidOid;
				}
				relationLocInfo->keys = lappend(relationLocInfo->keys, key);
			}

			if (relationLocInfo->locatorType == LOCATOR_TYPE_LIST ||
				relationLocInfo->locatorType == LOCATOR_TYPE_RANGE)
			{
				txt = GetPGXCClassAttr(htup,
									   Anum_pgxc_class_pcvalues,
									   RelationGetDescr(pcrel),
									   false,
									   relid);
				str = text_to_cstring(txt);
				relationLocInfo->values = stringToNode(str);
				pfree(str);
				if (!IsA(relationLocInfo->values, List))
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid column(%d) data for pgxc_class(%u)",
									Anum_pgxc_class_pcvalues, relid)));
				}
			}
		}
		break;
	case LOCATOR_TYPE_REPLICATED:
	case LOCATOR_TYPE_RANDOM:
		break;	/* nothing todo */
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("invalid distribute type %d", relationLocInfo->locatorType)));
	}
	relationLocInfo->nodeids = NIL;
	relationLocInfo->masternodeids = NIL;
	relationLocInfo->slavenodeids = NIL;

	if (pgxc_class->pclocatortype == LOCATOR_TYPE_HASH &&
		(txt = GetPGXCClassAttr(htup, Anum_pgxc_class_pcvalues, RelationGetDescr(pcrel), true, relid)) != NULL)
	{
		char *str = text_to_cstring(txt);
		List *list = stringToNode(str);
		ListCell *lc,*lc2;
		Oid *oids;
		uint32 count = 0;

		if (!IsA(list, List) ||
			list_length(list) != pgxc_class->nodeoids.dim1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid column(%d) data for pgxc_class(%u)",
							Anum_pgxc_class_pcvalues, relid)));

		foreach (lc, list)
		{
			if (!IsA(lfirst(lc), IntList))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid column(%d) data for pgxc_class(%u)",
								Anum_pgxc_class_pcvalues, relid)));
			count += list_length(lfirst(lc));
		}
		oids = palloc0(sizeof(oids[0]) * count);
		j = 0;
		foreach (lc, list)
		{
			foreach(lc2, lfirst(lc))
			{
				if (lfirst_int(lc2) < 0 ||
					lfirst_int(lc2) >= count ||
					oids[lfirst_int(lc2)] != InvalidOid)
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid column(%d) data for pgxc_class(%u)",
									Anum_pgxc_class_pcvalues, relid)));
				}
				oids[lfirst_int(lc2)] = pgxc_class->nodeoids.values[j];
			}
			++j;
		}
		for (j=0; j<count; ++j)
			relationLocInfo->nodeids = lappend_oid(relationLocInfo->nodeids, oids[j]);
		pfree(str);
		pfree(oids);
		list_free_deep(list);
	}
	else if (pgxc_class->pclocatortype != LOCATOR_TYPE_HASHMAP)
	{
		for (j = 0; j < pgxc_class->nodeoids.dim1; j++)
			relationLocInfo->nodeids = lappend_oid(relationLocInfo->nodeids,
							pgxc_class->nodeoids.values[j]);
	}
	else
	{
		relationLocInfo->nodeids = GetSlotNodeOids();
	}

	if (IsCnMaster())
	{
		relationLocInfo->masternodeids = relationLocInfo->nodeids;
		relationLocInfo->slavenodeids = adbUseDnSlaveNodeids(relationLocInfo->nodeids);

		if (enable_readsql_on_slave && sql_readonly == SQLTYPE_READ)
			relationLocInfo->nodeids = relationLocInfo->slavenodeids;
	}

	if (relationLocInfo->locatorType == LOCATOR_TYPE_LIST ||
		relationLocInfo->locatorType == LOCATOR_TYPE_RANGE)
	{
		if (list_length(relationLocInfo->values) != list_length(relationLocInfo->nodeids))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pgxc_class(%u) attribute %d and %d element count not same",
					 		relid, Anum_pgxc_class_nodeoids, Anum_pgxc_class_pcvalues)));
		}
	}

	systable_endscan(pcscan);
	heap_close(pcrel, AccessShareLock);

	return relationLocInfo;
}

/*
 * RelationBuildLocator
 * Build locator information associated with the specified relation.
 */
void
RelationBuildLocator(Relation rel)
{
	MemoryContext volatile loc_context = AllocSetContextCreate(CacheMemoryContext,
															   "locator info",
															   ALLOCSET_DEFAULT_SIZES);
	MemoryContext volatile oldContext = MemoryContextSwitchTo(loc_context);

	PG_TRY();
	{
		rel->rd_locator_info = RelationIdBuildLocator(RelationGetRelid(rel));
	}PG_CATCH();
	{
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(loc_context);
		PG_RE_THROW();
	}PG_END_TRY();
	MemoryContextSwitchTo(oldContext);
	if (rel->rd_locator_info)
	{
		Assert(GetMemoryChunkContext(rel->rd_locator_info) == loc_context);
		MemoryContextSetParent(loc_context, CacheMemoryContext);
	}else
	{
		MemoryContextDelete(loc_context);
	}
}

/*
 * GetLocatorRelationInfo
 * Returns the locator information for relation,
 * in a copy of the RelationLocatorInfo struct in relcache
 */
RelationLocInfo *
GetRelationLocInfo(Oid relid)
{
	RelationLocInfo *ret_loc_info = NULL;
	Relation	rel = relation_open(relid, AccessShareLock);

	/* Relation needs to be valid */
	Assert(rel->rd_isvalid);

	if (rel->rd_locator_info)
		ret_loc_info = CopyRelationLocInfo(rel->rd_locator_info);

	relation_close(rel, AccessShareLock);

	return ret_loc_info;
}

/*
 * CopyRelationLocInfo
 * Copy the RelationLocInfo struct
 */
RelationLocInfo *
CopyRelationLocInfo(RelationLocInfo *srcInfo)
{
	RelationLocInfo *destInfo;
	ListCell	   *lc;
	LocatorKeyInfo *key;

	Assert(srcInfo);
	destInfo = (RelationLocInfo *) palloc0(sizeof(RelationLocInfo));

	destInfo->relid = srcInfo->relid;
	destInfo->locatorType = srcInfo->locatorType;
	foreach(lc, srcInfo->keys)
	{
		key = palloc(sizeof(*key));
		memcpy(key, lfirst(lc), sizeof(*key));
		key->key = copyObject(((LocatorKeyInfo*)lfirst(lc))->key);
		destInfo->keys = lappend(destInfo->keys, key);
	}
	destInfo->values = copyObject(srcInfo->values);
	destInfo->masternodeids = list_copy(srcInfo->masternodeids);
	destInfo->slavenodeids = list_copy(srcInfo->slavenodeids);
	Assert(srcInfo->nodeids == srcInfo->masternodeids ||
		   srcInfo->nodeids == srcInfo->slavenodeids);
	if (srcInfo->nodeids == srcInfo->masternodeids)
		destInfo->nodeids = destInfo->masternodeids;
	else
		destInfo->nodeids = destInfo->slavenodeids;

	/* Note: for roundrobin, we use the relcache entry */
	return destInfo;
}

/*
 * FreeRelationLocInfo
 * Free RelationLocInfo struct
 */
void
FreeRelationLocInfo(RelationLocInfo *relationLocInfo)
{
	if (relationLocInfo)
	{
		ListCell *lc;
		if(relationLocInfo->masternodeids == NIL && relationLocInfo->slavenodeids == NIL)
		{
			list_free(relationLocInfo->nodeids);
		}
		else
		{
			Assert(relationLocInfo->nodeids == relationLocInfo->masternodeids || relationLocInfo->nodeids == relationLocInfo->slavenodeids);
			relationLocInfo->nodeids = NIL;
			list_free(relationLocInfo->masternodeids);
			list_free(relationLocInfo->slavenodeids);
		}
		foreach(lc, relationLocInfo->keys)
		{
			LocatorKeyInfo *key = lfirst(lc);
			if (key->key)
				pfree(key->key);
			pfree(key);
		}
		list_free(relationLocInfo->keys);
		list_free(relationLocInfo->values);
		pfree(relationLocInfo);
	}
}

/*
 * FreeExecNodes
 * Free the contents of the ExecNodes expression
 */
void
FreeExecNodes(ExecNodes **exec_nodes)
{
	ExecNodes *tmp_en = *exec_nodes;

	/* Nothing to do */
	if (!tmp_en)
		return;
	list_free(tmp_en->nodeids);
	pfree(tmp_en);
	*exec_nodes = NULL;
}

/*
 * pgxc_find_distcol_expr
 * Search through the quals provided and find out an expression which will give
 * us value of distribution column if exists in the quals. Say for a table
 * tab1 (val int, val2 int) distributed by hash(val), a query "SELECT * FROM
 * tab1 WHERE val = fn(x, y, z) and val2 = 3", fn(x,y,z) is the expression which
 * decides the distribution column value in the rows qualified by this query.
 * Hence return fn(x, y, z). But for a query "SELECT * FROM tab1 WHERE val =
 * fn(x, y, z) || val2 = 3", there is no expression which decides the values
 * distribution column val can take in the qualified rows. So, in such cases
 * this function returns NULL.
 */
static Expr *
pgxc_find_distcol_expr(Index varno,
					   AttrNumber attrNum,
					   Node *quals)
{
	List *lquals;
	ListCell *qual_cell;

	/* If no quals, no distribution column expression */
	if (!quals)
		return NULL;

	/* Convert the qualification into List if it's not already so */
	if (!IsA(quals, List))
		lquals = make_ands_implicit((Expr *)quals);
	else
		lquals = (List *)quals;

	/*
	 * For every ANDed expression, check if that expression is of the form
	 * <distribution_col> = <expr>. If so return expr.
	 */
	foreach(qual_cell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qual_cell);
		OpExpr *op;
		Expr *lexpr;
		Expr *rexpr;
		Var *var_expr;
		Expr *distcol_expr;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;

		lexpr = linitial(op->args);
		rexpr = lsecond(op->args);

		/*
		 * If either of the operands is a RelabelType, extract the Var in the RelabelType.
		 * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
		 * If we do not handle these then our optimization does not work in case of varchar
		 * For example if col is of type varchar and is the dist key then
		 * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
		 * should be shipped to one of the nodes only
		 */
		if (IsA(lexpr, RelabelType))
			lexpr = ((RelabelType*)lexpr)->arg;
		if (IsA(rexpr, RelabelType))
			rexpr = ((RelabelType*)rexpr)->arg;

		/*
		 * If either of the operands is a Var expression, assume the other
		 * one is distribution column expression. If none is Var check next
		 * qual.
		 */
		if (IsA(lexpr, Var))
		{
			var_expr = (Var *)lexpr;
			distcol_expr = rexpr;
		}
		else if (IsA(rexpr, Var))
		{
			var_expr = (Var *)rexpr;
			distcol_expr = lexpr;
		}
		else
			continue;
		/*
		 * If Var found is not the distribution column of required relation,
		 * check next qual
		 */
		if (var_expr->varno != varno || var_expr->varattno != attrNum)
			continue;
		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
			!op_hashjoinable(op->opno, exprType((Node *)lexpr)))
			continue;
		/* Found the distribution column expression return it */
		return distcol_expr;
	}
	/* Exhausted all quals, but no distribution column expression */
	return NULL;
}

List *
GetInvolvedNodes(RelationLocInfo *rel_loc,
				 int nelems, Datum* dist_values,
				 bool* dist_nulls, Oid* dist_types,
				 RelationAccessType accessType)
{
	List	   *node_list = NIL;
	int32		modulo;

	int			nodeIndex;
	int			slotstatus;

	if (rel_loc == NULL)
		return NIL;
#warning change code
	switch (rel_loc->locatorType)
	{
		case LOCATOR_TYPE_REPLICATED:
			{
				node_list = list_copy(rel_loc->nodeids);
				if (accessType == RELATION_ACCESS_UPDATE ||
					accessType == RELATION_ACCESS_INSERT)
				{
					node_list = list_copy(rel_loc->nodeids);
				} else
				if (accessType == RELATION_ACCESS_READ_FOR_UPDATE &&
					IsTableDistOnPrimary(rel_loc))
				{
					Assert(OidIsValid(primary_data_node));
					node_list = list_make1_oid(primary_data_node);
				}
			}
			break;

		case LOCATOR_TYPE_HASH:
		case LOCATOR_TYPE_MODULO:
			{
				int nnodes;

				Assert(rel_loc->nodeids);
				nnodes = list_length(rel_loc->nodeids);
				Assert(nnodes > 0);

				if(dist_nulls[0])
				{
					if(accessType == RELATION_ACCESS_INSERT)
						modulo = 0;	/* Insert NULL to first node*/
					else
					{
						node_list = list_copy(rel_loc->nodeids);
						break;
					}
				} else
				{
					if(rel_loc->locatorType == LOCATOR_TYPE_HASH)
					{
						int32 hashVal = execHashValue(dist_values[0], dist_types[0], InvalidOid);
						modulo = (uint32)hashVal % (uint32)nnodes;
					}else
						modulo = execModuloValue(dist_values[0], dist_types[0], nnodes);
				}
				node_list = list_make1_oid(list_nth_oid(rel_loc->nodeids, modulo));
			}
			break;

		case LOCATOR_TYPE_RANDOM:
			{
				/*
				 * random, get next one in case of insert. If not insert, all
				 * node needed
				 */
				if (accessType == RELATION_ACCESS_INSERT)
					node_list = list_make1_oid(GetRandomRelNodeId(rel_loc->relid));
				else
					node_list = list_copy(rel_loc->nodeids);
			}
			break;

			/* TODO case LOCATOR_TYPE_RANGE: */
			/* TODO case LOCATOR_TYPE_CUSTOM: */
			case LOCATOR_TYPE_HASHMAP:
			{
				int nnodes;

				Assert(rel_loc->nodeids);
				nnodes = list_length(rel_loc->nodeids);
				Assert(nnodes > 0);

				if(dist_nulls[0])
				{
					if(accessType == RELATION_ACCESS_INSERT)
					{
						modulo = 0; /* Insert NULL to first node*/
						SlotGetInfo(0, &nodeIndex, &slotstatus);
						node_list = list_make1_oid(nodeIndex);
					}
					else
					{
						node_list = list_copy(rel_loc->nodeids);
						break;
					}
				} else
				{
					int32 hashVal = execHashValue(dist_values[0], dist_types[0], InvalidOid);
					modulo = (uint32)hashVal % HASHMAP_SLOTSIZE;
					SlotGetInfo(modulo, &nodeIndex, &slotstatus);
					node_list = list_make1_oid(nodeIndex);
				}
			}
			break;

		default:
			ereport(ERROR,
					(errmsg("locator type '%c' not support yet", rel_loc->locatorType)));
			break;
	}

	return node_list;
}

List *
adbUseDnSlaveNodeids(List *nodeids)
{
	ListCell *lc;
	List *slaveNodeListids = NIL;
	Oid slaveNodeid;

	if (!nodeids)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("the list of datanode master nodeids is NIL")));

	foreach (lc, nodeids)
	{
		slaveNodeid = adbGetSlaveNodeid(lfirst_oid(lc));
		if (!OidIsValid(slaveNodeid))
		{
			/* If there is no slave, master replaces */
			slaveNodeListids = lappend_oid(slaveNodeListids, lfirst_oid(lc));
		}
		else
		{
			slaveNodeListids = lappend_oid(slaveNodeListids, slaveNodeid);
		}
	}

	return slaveNodeListids;
}

static Oid
adbGetSlaveNodeid(Oid masterid)
{
	const char *masterName;
	Relation rel;
	TableScanDesc scan;
	HeapTuple tuple;
	ScanKeyData key[1];
	Oid slaveid = InvalidOid;

	if (!OidIsValid(masterid))
		return masterid;

	masterName = GetNodeName(masterid);

	if (!masterName)
		ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
			, errmsg("cannot find the datanode master which oid is \"%u\" "
				"in pgxc_node of coordinator", masterid)));

	ScanKeyInit(&key[0]
		, Anum_pgxc_node_node_master_oid
		, BTEqualStrategyNumber
		, F_OIDEQ
		, ObjectIdGetDatum(masterid));
	rel = table_open(PgxcNodeRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 1, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		slaveid = ((Form_pgxc_node)GETSTRUCT(tuple))->oid;
		break;
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	return slaveid;
}

List *
adbGetRelationNodeids(Oid relid)
{
	List *nodeids = NIL;
	Relation pcrel;
	ScanKeyData skey;
	SysScanDesc pcscan;
	HeapTuple htup;
	Form_pgxc_class pgxc_class;
	int j = 0;

	ScanKeyInit(&skey,
				Anum_pgxc_class_pcrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	pcrel = table_open(PgxcClassRelationId, AccessShareLock);
	pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true,
								NULL, 1, &skey);
	htup = systable_getnext(pcscan);
	if (!HeapTupleIsValid(htup))
	{
		systable_endscan(pcscan);
		heap_close(pcrel, AccessShareLock);
		return NIL;
	}

	pgxc_class = (Form_pgxc_class)GETSTRUCT(htup);

	if (pgxc_class->pclocatortype != LOCATOR_TYPE_HASHMAP)
	{
		for (j = 0; j < pgxc_class->nodeoids.dim1; j++)
		{
			nodeids = lappend_oid(nodeids,
							pgxc_class->nodeoids.values[j]);
		}
	}
	else
	{
		nodeids = GetSlotNodeOids();
	}

	systable_endscan(pcscan);
	table_close(pcrel, AccessShareLock);

	return nodeids;
}

/* this is an transition function */
AttrNumber GetFirstLocAttNumIfOnlyOne(RelationLocInfo *loc)
{
	LocatorKeyInfo *key;
	if (list_length(loc->keys) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("not distribute by only one expression not support yet")));
	}
	key = FirstLocKeyInfo(loc);
	if (key->attno == InvalidAttrNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("distribute by expression not support yet!")));
	}
	return key->attno;
}
