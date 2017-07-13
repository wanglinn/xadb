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
#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
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
#include "utils/tqual.h"
#include "optimizer/clauses.h"
#include "optimizer/paths.h"
#include "parser/parse_coerce.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "postmaster/autovacuum.h"
#include "utils/typcache.h"

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
static Datum rrobinStateEvalFunc(ExprState *expression,
												ExprContext *econtext,
												bool *isNull,
												ExprDoneCond *isDone);
static Datum nthIntEvalFunc(GenericExprState *expression,
												ExprContext *econtext,
												bool *isNull,
												ExprDoneCond *isDone);

static Datum OidStateEvalFunc(GenericExprState *expression,
												ExprContext *econtext,
												bool *isNull,
												ExprDoneCond *isDone);
static oidvector *makeDatanodeOidVector(List *list);
static Expr *makeLocatorModuleExpr(RelationLocInfo *loc, Index varno, bool using_param);
static Param *makeReduceParam(Oid type, int paramid, int parammod, Oid collid, Index relid, AttrNumber varno);
static bool expr_have_param(Node *node, void *none);
static Node *reduceParam2VarMutator(Node *node, void *none);
static bool PullReducePathExprAttnosWalker(ReduceParam *rp, PullReducePathExprVarAttnosContext *context);
static Node *CreateReduceExprMutator(Node *node, CreateReduceExprContext *context);

Oid		primary_data_node = InvalidOid;
int		num_preferred_data_nodes = 0;
Oid		preferred_data_node[MAX_PREFERRED_NODES];

/*
 * GetPreferredReplicationNode
 * Pick any Datanode from given list, however fetch a preferred node first.
 */
List *
GetPreferredReplicationNode(List *relNodes)
{
	ListCell	*item;
	int			nodeid = -1;

	if (list_length(relNodes) <= 0)
		elog(ERROR, "a list of nodes should have at least one node");

	foreach(item, relNodes)
	{
		int cnt_nodes;
		for (cnt_nodes = 0;
				cnt_nodes < num_preferred_data_nodes && nodeid < 0;
				cnt_nodes++)
		{
			if (PGXCNodeGetNodeId(preferred_data_node[cnt_nodes],
								  PGXC_NODE_DATANODE) == lfirst_int(item))
				nodeid = lfirst_int(item);
		}
		if (nodeid >= 0)
			break;
	}
	if (nodeid < 0)
		return list_make1_int(linitial_int(relNodes));

	return list_make1_int(nodeid);
}

/*
 * get_node_from_modulo - determine node based on modulo
 *
 * compute_modulo
 */
static int
get_node_from_modulo(int modulo, List *nodeList)
{
	if (nodeList == NIL || modulo >= list_length(nodeList) || modulo < 0)
		ereport(ERROR, (errmsg("Modulo value out of range\n")));

	return list_nth_int(nodeList, modulo);
}


/*
 * GetRelationDistribColumn
 * Return hash column name for relation or NULL if relation is not distributed.
 */
char *
GetRelationDistribColumn(RelationLocInfo *locInfo)
{
	/* No relation, so simply leave */
	if (!locInfo)
		return NULL;

	/* No distribution column if relation is not distributed with a key */
	if (!IsRelationDistributedByValue(locInfo))
		return NULL;

	/* Return column name */
	return get_attname(locInfo->relid, locInfo->partAttrNum);
}

List *
GetRelationDistribColumnList(RelationLocInfo *locInfo)
{
	List *result = NIL;
	ListCell *lc = NULL;
	char *attname = NULL;

	if (!locInfo)
		return NIL;

	if (!IsRelationDistributedByUserDefined(locInfo))
		return NIL;

	foreach (lc, locInfo->funcAttrNums)
	{
		attname = get_attname(locInfo->relid, (AttrNumber)lfirst_int(lc));
		result = lappend(result, attname);
	}

	return result;
}

Oid
GetRelationDistribFunc(Oid relid)
{
	RelationLocInfo *locInfo = GetRelationLocInfo(relid);

	if (!locInfo)
		return InvalidOid;

	if (!IsRelationDistributedByUserDefined(locInfo))
		return InvalidOid;

	return locInfo->funcid;
}


/*
 * IsDistribColumn
 * Return whether column for relation is used for distribution or not.
 */
bool
IsDistribColumn(Oid relid, AttrNumber attNum)
{
	RelationLocInfo *locInfo = GetRelationLocInfo(relid);

	/* No locator info, so leave */
	if (!locInfo)
		return false;

	if (IsRelationDistributedByValue(locInfo))
	{
		return locInfo->partAttrNum == attNum;
	} else
	if (IsRelationDistributedByUserDefined(locInfo))
	{
		if (list_member_int(locInfo->funcAttrNums, (int)attNum))
			return true;
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


/*
 * GetRoundRobinNode
 * Update the round robin node for the relation.
 * PGXCTODO - may not want to bother with locking here, we could track
 * these in the session memory context instead...
 */
int
GetRoundRobinNode(Oid relid)
{
	int			ret_node;
	Relation	rel = relation_open(relid, AccessShareLock);

	Assert (rel->rd_locator_info->locatorType == LOCATOR_TYPE_REPLICATED ||
			rel->rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN);

	ret_node = lfirst_int(rel->rd_locator_info->roundRobinNode);

	/* Move round robin indicator to next node */
	if (rel->rd_locator_info->roundRobinNode->next != NULL)
		rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->roundRobinNode->next;
	else
		/* reset to first one */
		rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->nodeList->head;

	relation_close(rel, AccessShareLock);

	return ret_node;
}

/*
 * IsTableDistOnPrimary
 * Does the table distribution list include the primary node?
 */
bool
IsTableDistOnPrimary(RelationLocInfo *rel_loc_info)
{
	ListCell *item;

	if (!OidIsValid(primary_data_node) ||
		rel_loc_info == NULL ||
		list_length(rel_loc_info->nodeList) == 0)
		return false;

	foreach(item, rel_loc_info->nodeList)
	{
		if (PGXCNodeGetNodeId(primary_data_node, PGXC_NODE_DATANODE) == lfirst_int(item))
			return true;
	}
	return false;
}


/*
 * IsLocatorInfoEqual
 * Check equality of given locator information
 */
bool
IsLocatorInfoEqual(RelationLocInfo *locInfo1,
				   RelationLocInfo *locInfo2)
{
	List *nodeList1, *nodeList2;
	Assert(locInfo1 && locInfo2);

	nodeList1 = locInfo1->nodeList;
	nodeList2 = locInfo2->nodeList;

	/* Same relation? */
	if (locInfo1->relid != locInfo2->relid)
		return false;

	/* Same locator type? */
	if (locInfo1->locatorType != locInfo2->locatorType)
		return false;

	/* Same attribute number? */
	if (locInfo1->partAttrNum != locInfo2->partAttrNum)
		return false;

	/* Same node list? */
	if (list_difference_int(nodeList1, nodeList2) != NIL ||
		list_difference_int(nodeList2, nodeList1) != NIL)
		return false;

	if (locInfo1->funcid != locInfo2->funcid)
		return false;

	if (!equal(locInfo1->funcAttrNums, locInfo2->funcAttrNums))
		return false;

	/* Everything is equal */
	return true;
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
	ExecNodes	*exec_nodes;
	int32 modulo;
	int nodeIndex;

	if (rel_loc_info == NULL)
		return NULL;

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
			exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
			if (accessType == RELATION_ACCESS_UPDATE || accessType == RELATION_ACCESS_INSERT)
			{
				/* we need to write to all synchronously */

				/*
				 * Write to primary node first, to reduce chance of a deadlock
				 * on replicated tables. If -1, do not use primary copy.
				 */
				if (IsTableDistOnPrimary(rel_loc_info)
						&& exec_nodes->nodeList
						&& list_length(exec_nodes->nodeList) > 1) /* make sure more than 1 */
				{
					exec_nodes->primarynodelist = list_make1_int(PGXCNodeGetNodeId(primary_data_node,
																	PGXC_NODE_DATANODE));
					exec_nodes->nodeList = list_delete_int(exec_nodes->nodeList,
															PGXCNodeGetNodeId(primary_data_node,
															PGXC_NODE_DATANODE));
				}
			}
			else if (accessType == RELATION_ACCESS_READ_FOR_UPDATE &&
					IsTableDistOnPrimary(rel_loc_info))
			{
				/*
				 * We should ensure row is locked on the primary node to
				 * avoid distributed deadlock if updating the same row
				 * concurrently
				 */
				exec_nodes->nodeList = list_make1_int(PGXCNodeGetNodeId(primary_data_node, PGXC_NODE_DATANODE));
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
						exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
						break;
					}
				}else
				{
					if(rel_loc_info->locatorType == LOCATOR_TYPE_HASH)
					{
						int32 hashVal = execHashValue(dist_col_values[0],
											  dist_col_types[0],
											  InvalidOid);
						modulo = execModuloValue(Int32GetDatum(hashVal),
												 INT4OID,
												 list_length(rel_loc_info->nodeList));
					}else
					{
						modulo = execModuloValue(dist_col_values[0],
												dist_col_types[0],
												list_length(rel_loc_info->nodeList));
					}
				}
				nodeIndex = get_node_from_modulo(modulo, rel_loc_info->nodeList);
				exec_nodes->nodeList = list_make1_int(nodeIndex);
			}
			break;

		case LOCATOR_TYPE_RROBIN:
			/*
			 * round robin, get next one in case of insert. If not insert, all
			 * node needed
			 */
			if (accessType == RELATION_ACCESS_INSERT)
				exec_nodes->nodeList = list_make1_int(GetRoundRobinNode(rel_loc_info->relid));
			else
				exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
			break;

		case LOCATOR_TYPE_USER_DEFINED:
			{
				int 	i;
				Datum 	result;
				bool	allValuesNotNull = true;

				Assert(nelems >= 1);
				Assert(OidIsValid(rel_loc_info->funcid));
				Assert(rel_loc_info->funcAttrNums);

				for (i = 0; i < nelems; i++)
				{
					if(dist_col_nulls[i])
					{
						allValuesNotNull = false;
						break;
					}
				}

				exec_nodes->en_funcid = rel_loc_info->funcid;

				/*
				 * If the table is distributed by user-defined partition function,
				 * we should get all parameters' value to evaluate the value to
				 * reduce the Datanodes if possible.
				 *
				 * First, check whether values' type match function arguments or not,
				 * if not, coerce them.
				 */
				if (allValuesNotNull)
				{
					CoerceUserDefinedFuncArgs(rel_loc_info->funcid,
											  nelems,
											  dist_col_values,
											  dist_col_nulls,
											  dist_col_types);
					result = OidFunctionCallN(rel_loc_info->funcid,
											  nelems,
											  dist_col_values,
											  dist_col_nulls);
					modulo = execModuloValue(result,
											 get_func_rettype(rel_loc_info->funcid),
											 list_length(rel_loc_info->nodeList));
					nodeIndex = get_node_from_modulo(modulo, rel_loc_info->nodeList);
					exec_nodes->nodeList = list_make1_int(nodeIndex);
				} else
				{
					if (accessType == RELATION_ACCESS_INSERT)
						/* Insert NULL to first node*/
						exec_nodes->nodeList = list_make1_int(linitial_int(rel_loc_info->nodeList));
					else
						exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
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
	 * If the table distributed by user-defined partition function,
	 * we should get all qualifiers of the distributed column from the quals,
	 * then check if we can reduce the Datanodes by evaluating the value by
	 * user-defined partition function.
	 */
	if (IsRelationDistributedByUserDefined(rel_loc_info))
		return GetRelationNodesByMultQuals(rel_loc_info,
										   reloid,
										   varno,
										   quals,
										   relaccess);

	/*
	 * If the table distributed by value, check if we can reduce the Datanodes
	 * by looking at the qualifiers for this relation
	 */
	if (IsRelationDistributedByValue(rel_loc_info))
	{
		Oid		disttype = get_atttype(reloid, rel_loc_info->partAttrNum);
		int32	disttypmod = get_atttypmod(reloid, rel_loc_info->partAttrNum);
		distcol_expr = pgxc_find_distcol_expr(varno, rel_loc_info->partAttrNum,
													quals);
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

ExecNodes *
GetRelationNodesByMultQuals(RelationLocInfo *rel_loc_info,
							Oid reloid, Index varno, Node *quals,
							RelationAccessType relaccess)
{
	int			i;
	int 		nargs;		
	Oid 		disttype;
	int32 		disttypmod;
	Expr		*distcol_expr = NULL;
	ListCell	*cell = NULL;
	AttrNumber	attnum;
	Datum		*distcol_values = NULL;
	bool		*distcol_isnulls = NULL;
	Oid			*distcol_types = NULL;

	Oid			*argtypes = NULL;
	int			nelems;
	ExecNodes	*nodes = NULL;

	Assert(rel_loc_info);
	Assert(IsRelationDistributedByUserDefined(rel_loc_info));

	if (!IsRelationDistributedByUserDefined(rel_loc_info))
		return NULL;

	Assert(OidIsValid(rel_loc_info->relid));
	Assert(OidIsValid(rel_loc_info->funcid));
	Assert(rel_loc_info->funcAttrNums);

	nargs = list_length(rel_loc_info->funcAttrNums);
	distcol_values = (Datum *)palloc0(sizeof(Datum) * nargs);
	distcol_isnulls = (bool *)palloc0(sizeof(bool) * nargs);
	distcol_types = (Oid *)palloc0(sizeof(Oid) * nargs);
	(void)get_func_signature(rel_loc_info->funcid, &argtypes, &nelems);

	Assert(nelems == nargs);

	i = 0;
	foreach (cell, rel_loc_info->funcAttrNums)
	{
		attnum = lfirst_int(cell);
		disttype = argtypes[i];
		disttypmod = -1;
		distcol_expr = pgxc_find_distcol_expr(varno, attnum, quals);

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

		if (distcol_expr && IsA(distcol_expr, Const))
		{
			Const *const_expr = (Const *)distcol_expr;
			distcol_values[i] = datumCopy(const_expr->constvalue,
										  const_expr->constbyval,
										  const_expr->constlen);
			distcol_isnulls[i] = const_expr->constisnull;
			distcol_types[i] = const_expr->consttype;
		}
		else
		{
			distcol_values[i] = (Datum) 0;
			distcol_isnulls[i] = true;
			distcol_types[i] = InvalidOid;
		}

		i++;
	}

	if (argtypes)
		pfree(argtypes);

	nodes = GetRelationNodes(rel_loc_info,
							nargs,
							distcol_values,
							distcol_isnulls,
							distcol_types,
							relaccess);
	pfree(distcol_values);
	pfree(distcol_isnulls);
	pfree(distcol_types);

	return nodes;
}

void
CoerceUserDefinedFuncArgs(Oid funcid,
						  int nargs,
						  Datum *values,
						  bool *nulls,
						  Oid *types)
{
	Oid 		*func_argstype = NULL;
	int 		nelems, i;
	Oid 		targetTypInput, targettypIOParam;
	Oid 		srcTypOutput;
	bool 		srcTypIsVarlena;
	Datum 		srcValue;
	
	(void)get_func_signature(funcid, &func_argstype, &nelems);
	Assert(nargs == nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			continue;

		if (func_argstype[i] == types[i])
			continue;

		srcValue = values[i];
		getTypeOutputInfo(types[i], &srcTypOutput, &srcTypIsVarlena);
		getTypeInputInfo(func_argstype[i], &targetTypInput, &targettypIOParam);
		values[i] = OidInputFunctionCall(targetTypInput,
								   OidOutputFunctionCall(srcTypOutput, srcValue),
								   targettypIOParam,
								   -1);
	}
	pfree(func_argstype);
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
 * GetAllDataNodes
 * Return a list of all Datanodes.
 * We assume all tables use all nodes in the prototype, so just return a list
 * from first one.
 */
List *
GetAllDataNodes(void)
{
	int			i;
	List	   *nodeList = NIL;

	for (i = 0; i < NumDataNodes; i++)
		nodeList = lappend_int(nodeList, i);

	return nodeList;
}

/*
 * GetAllCoordNodes
 * Return a list of all Coordinators
 * This is used to send DDL to all nodes and to clean up pooler connections.
 * Do not put in the list the local Coordinator where this function is launched.
 */
List *
GetAllCoordNodes(void)
{
	int			i;
	List	   *nodeList = NIL;

	for (i = 0; i < NumCoords; i++)
	{
		/*
		 * Do not put in list the Coordinator we are on,
		 * it doesn't make sense to connect to the local Coordinator.
		 */

		if (i != PGXCNodeId - 1)
			nodeList = lappend_int(nodeList, i);
	}

	return nodeList;
}


/*
 * RelationBuildLocator
 * Build locator information associated with the specified relation.
 */
void
RelationBuildLocator(Relation rel)
{
	Relation	pcrel;
	ScanKeyData	skey;
	SysScanDesc	pcscan;
	HeapTuple	htup;
	MemoryContext	oldContext;
	RelationLocInfo	*relationLocInfo;
	int		j;
	Form_pgxc_class	pgxc_class;

	ScanKeyInit(&skey,
				Anum_pgxc_class_pcrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));

	pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
	pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true,
								NULL, 1, &skey);
	htup = systable_getnext(pcscan);

	if (!HeapTupleIsValid(htup))
	{
		/* Assume local relation only */
		rel->rd_locator_info = NULL;
		systable_endscan(pcscan);
		heap_close(pcrel, AccessShareLock);
		return;
	}

	pgxc_class = (Form_pgxc_class) GETSTRUCT(htup);

	oldContext = MemoryContextSwitchTo(CacheMemoryContext);

	relationLocInfo = (RelationLocInfo *) palloc(sizeof(RelationLocInfo));
	rel->rd_locator_info = relationLocInfo;

	relationLocInfo->relid = RelationGetRelid(rel);
	relationLocInfo->locatorType = pgxc_class->pclocatortype;

	relationLocInfo->partAttrNum = pgxc_class->pcattnum;
	relationLocInfo->nodeList = NIL;

	for (j = 0; j < pgxc_class->nodeoids.dim1; j++)
		relationLocInfo->nodeList = lappend_int(relationLocInfo->nodeList,
												PGXCNodeGetNodeId(pgxc_class->nodeoids.values[j],
																  PGXC_NODE_DATANODE));

	/*
	 * If the locator type is round robin, we set a node to
	 * use next time. In addition, if it is replicated,
	 * we choose a node to use for balancing reads.
	 */
	if (relationLocInfo->locatorType == LOCATOR_TYPE_RROBIN
		|| relationLocInfo->locatorType == LOCATOR_TYPE_REPLICATED)
	{
		int offset;
		/*
		 * pick a random one to start with,
		 * since each process will do this independently
		 */
		offset = abs(rand()) % list_length(relationLocInfo->nodeList);

		srand(time(NULL));
		/* fix: Access to field 'head' results in a dereference of a
		 * null pointer (loaded from field 'nodeList')
		 */
		AssertArg(relationLocInfo->nodeList);
		relationLocInfo->roundRobinNode = relationLocInfo->nodeList->head; /* initialize */
		for (j = 0; j < offset && relationLocInfo->roundRobinNode->next != NULL; j++)
			relationLocInfo->roundRobinNode = relationLocInfo->roundRobinNode->next;
	}

	relationLocInfo->funcid = InvalidOid;
	relationLocInfo->funcAttrNums = NIL;
	if (relationLocInfo->locatorType == LOCATOR_TYPE_USER_DEFINED)
	{
		Datum funcidDatum;
		Datum attrnumsDatum;
		bool isnull;
		int2vector *attrnums = NULL;

		funcidDatum = SysCacheGetAttr(PGXCCLASSRELID, htup, 
									Anum_pgxc_class_pcfuncid, &isnull);
		Assert(!isnull);
		relationLocInfo->funcid = DatumGetObjectId(funcidDatum);

		attrnumsDatum = SysCacheGetAttr(PGXCCLASSRELID, htup, 
									Anum_pgxc_class_pcfuncattnums, &isnull);
		Assert(!isnull);
		attrnums = (int2vector *)DatumGetPointer(attrnumsDatum);
		for (j = 0; j < attrnums->dim1; j++)
			relationLocInfo->funcAttrNums = lappend_int(relationLocInfo->funcAttrNums,
														attrnums->values[j]);
	}

	systable_endscan(pcscan);
	heap_close(pcrel, AccessShareLock);

	MemoryContextSwitchTo(oldContext);
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

	Assert(srcInfo);
	destInfo = (RelationLocInfo *) palloc0(sizeof(RelationLocInfo));

	destInfo->relid = srcInfo->relid;
	destInfo->locatorType = srcInfo->locatorType;
	destInfo->partAttrNum = srcInfo->partAttrNum;
	if (srcInfo->nodeList)
		destInfo->nodeList = list_copy(srcInfo->nodeList);

	destInfo->funcid = srcInfo->funcid;
	if (srcInfo->funcAttrNums)
		destInfo->funcAttrNums = list_copy(srcInfo->funcAttrNums);

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
		list_free(relationLocInfo->nodeList);
		list_free(relationLocInfo->funcAttrNums);
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
	list_free(tmp_en->primarynodelist);
	list_free(tmp_en->nodeList);
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

ExprState *ExecInitRelationExecNode(RelationLocInfo *loc,
									RelationAccessType relaccess,
									Index rel_index)
{
	Expr *expr;
	ExprState *exprState;
	AssertArg(loc);

	exprState = NULL;
	if(IsRelationReplicated(loc))
	{
		ereport(ERROR, (errmsg("not support replicate table")));
	}else if(IsLocatorNone(loc->locatorType))
	{
		ereport(ERROR, (errmsg("not support replicate none table")));
	}else if(loc->locatorType == LOCATOR_TYPE_HASH
			|| loc->locatorType == LOCATOR_TYPE_MODULO
			|| loc->locatorType == LOCATOR_TYPE_USER_DEFINED)
	{
		GenericExprState *ge;
		ListCell *lc;
		int i;

		expr = makeLocatorModuleExpr(loc, rel_index, false);

		ge = makeNode(GenericExprState);
		int *pint = palloc(sizeof(int) * list_length(loc->nodeList));
		for(i=0,lc=list_head(loc->nodeList);lc!=NULL;lc=lnext(lc),++i)
			pint[i] = lfirst_int(lc);
		ge->xprstate.expr = (Expr*)pint;
		ge->xprstate.evalfunc = (ExprStateEvalFunc)nthIntEvalFunc;
		ge->arg = ExecInitExpr(expr, NULL);
		exprState = (ExprState*)ge;
	}else if(loc->locatorType == LOCATOR_TYPE_RROBIN)
	{
		ExecNodeStateEvalInfo *info;
		Assert(list_length(loc->nodeList) > 0);
		info = palloc0(sizeof(*info));
		NodeSetTag(info, T_Expr);
		info->list = list_copy(loc->nodeList);
		info->lc = NULL;
		exprState = makeNode(ExprState);
		exprState->evalfunc = rrobinStateEvalFunc;
		exprState->expr = (Expr*)info;
	}else
	{
		ereport(ERROR, (errmsg("not support locator type '%c'", loc->locatorType)));
	}

	return exprState;
}

struct ExprState *ExecInitRelationExecNodeOid(RelationLocInfo *loc,
										   RelationAccessType relaccess,
										   Index rel_index)
{
	ListCell *lc;
	Oid *oids;
	int max_val;
	GenericExprState *exprState = makeNode(GenericExprState);
	exprState->arg = ExecInitRelationExecNode(loc, relaccess, rel_index);

	max_val = 0;
	foreach(lc, loc->nodeList)
	{
		if(lfirst_int(lc) > max_val)
			max_val = lfirst_int(lc);
	}

	oids = palloc0(sizeof(Oid)*(max_val+1));
	foreach(lc, loc->nodeList)
	{
		oids[lfirst_int(lc)] = PGXCNodeGetNodeOid(lfirst_int(lc), PGXC_NODE_DATANODE);
	}
	exprState->xprstate.expr = (Expr*)oids;
	exprState->xprstate.evalfunc = (ExprStateEvalFunc)OidStateEvalFunc;

	return (ExprState*)exprState;
}

static Datum rrobinStateEvalFunc(ExprState *expression, ExprContext *econtext,
								 bool *isNull, ExprDoneCond *isDone)
{
	ExecNodeStateEvalInfo *info = (ExecNodeStateEvalInfo*)(expression->expr);
	ListCell *lc = info->lc;

	if(lc)
		lc = lnext(lc);
	if(lc == NULL)
		lc = list_head(info->list);
	info->lc = lc;

	*isNull = false;
	if(isDone)
		*isDone = ExprSingleResult;

	return Int32GetDatum(lfirst_int(lc));
}

static Datum nthIntEvalFunc(GenericExprState *expression,
												ExprContext *econtext,
												bool *isNull,
												ExprDoneCond *isDone)
{
	int *pint = (int*)expression->xprstate.expr;
	Datum datum = ExecEvalExpr(expression->arg, econtext, isNull, isDone);
	int32 i32;
	if(*isNull)
		return (Datum)0;

	i32 = DatumGetInt32(datum);
	if(i32<0)
		i32 = -i32;
	return Int32GetDatum(pint[i32]);
}

static Datum OidStateEvalFunc(GenericExprState *expression, ExprContext *econtext,
							bool *isNull, ExprDoneCond *isDone)
{
	Oid *oid = (Oid*)expression->xprstate.expr;
	Datum datum = ExecEvalExpr(expression->arg, econtext, isNull, isDone);
	if(*isNull)
		return (Datum)0;

	return ObjectIdGetDatum(oid[DatumGetInt32(datum)]);
}

/*
 * Var(s) using Param
 */
Expr *MakeReducePathExpr(RelationLocInfo *loc,
								RelationAccessType relaccess,
								Index rel_index)
{
	Expr *expr;
	oidvector *vector;
	if(loc == NULL)
	{
		Assert(relaccess == RELATION_ACCESS_READ);
		Assert(OidIsValid(PGXCNodeOid));
		return (Expr*)makeConst(OIDOID,
								-1,
								InvalidOid,
								sizeof(Oid),
								ObjectIdGetDatum(PGXCNodeOid),
								false,
								true);
	}
	Assert(list_length(loc->nodeList) > 0);
	if(IsRelationReplicated(loc))
	{
		if(relaccess == RELATION_ACCESS_READ)
		{
			ereport(ERROR, (errmsg("not support replicate table yet")));
		}else
		{
			OidVectorLoopExpr *ovl = makeNode(OidVectorLoopExpr);
			ovl->signalRowMode = false;
			vector = makeDatanodeOidVector(loc->nodeList);
			ovl->vector = PointerGetDatum(vector);
			expr = (Expr*)ovl;
		}
	}else if(IsLocatorNone(loc->locatorType))
	{
		ereport(ERROR, (errmsg("not support replicate none table")));
	}else if(loc->locatorType == LOCATOR_TYPE_HASH
			|| loc->locatorType == LOCATOR_TYPE_MODULO
			|| loc->locatorType == LOCATOR_TYPE_USER_DEFINED)
	{
		expr = makeLocatorModuleExpr(loc, rel_index, true);
		vector = makeDatanodeOidVector(loc->nodeList);
		ArrayRef *aref = makeNode(ArrayRef);
		aref->refarraytype = OIDARRAYOID;
		aref->refelemtype = OIDOID;
		aref->reftypmod = -1;
		aref->refcollid = InvalidOid;
		aref->refupperindexpr = list_make1(expr);
		aref->reflowerindexpr = NIL;
		aref->refexpr = (Expr*)makeConst(OIDARRAYOID,
										 -1,
										 InvalidOid,
										 -1,
										 PointerGetDatum(vector),
										 false,
										 false);
		aref->refassgnexpr = NULL;
		expr = &aref->xpr;
	}else if(loc->locatorType == LOCATOR_TYPE_RROBIN)
	{
		OidVectorLoopExpr *ovl = makeNode(OidVectorLoopExpr);
		ovl->signalRowMode = true;
		vector = makeDatanodeOidVector(loc->nodeList);
		ovl->vector = PointerGetDatum(vector);
		expr = (Expr*)ovl;
	}else
	{
		ereport(ERROR, (errmsg("not support locator type '%c'", loc->locatorType)));
	}

	return expr;
}

Expr *ReducePathExpr2PlanExpr(Expr *expr)
{
	if(expr_have_param((Node*)expr, NULL) == false)
		return expr;
	return (Expr*)reduceParam2VarMutator((Node*)expr, NULL);
}

Expr *MakeReduce2CoordinatorExpr(void)
{
	Assert(OidIsValid(PGXCNodeOid));
	return (Expr*) makeConst(OIDOID,
							 -1,
							 InvalidOid,
							 sizeof(Oid),
							 ObjectIdGetDatum(PGXCNodeOid),
							 false,
							 false);
}

bool IsReduce2Coordinator(Expr *expr)
{
	Const *c;
	if(!IsA(expr, Const))
		return false;
	Assert(OidIsValid(PGXCNodeOid));
	c = (Const*)expr;

	return c->constisnull == false &&
		   c->consttype == OIDOID &&
		   DatumGetObjectId(c->constvalue) == PGXCNodeOid;
}

static int oid_qsort_cmp(const void *a, const void *b)
{
	Oid l = *(Oid*)a;
	Oid r = *(Oid*)b;

	if(l < r)
		return -1;
	else if(l == r)
		return 0;
	return 1;
}

Expr *MakeReduceReplicateExpr(List *oids)
{
	OidVectorLoopExpr *ovl = makeNode(OidVectorLoopExpr);
	oidvector *vector = makeDatanodeOidVector(oids);
	ovl->signalRowMode = false;
	pg_qsort(vector->values, vector->dim1, sizeof(Oid), oid_qsort_cmp);
	ovl->vector = PointerGetDatum(vector);
	return (Expr*)ovl;
}

bool IsReduceReplicateExpr(Expr *expr)
{
	return IsA(expr, OidVectorLoopExpr) &&
		   ((OidVectorLoopExpr*)expr)->signalRowMode == false;
}

List *ReduceReplicateExprGetList(Expr *expr)
{
	List *list;
	oidvector *oids;
	Size i;
	Assert(IsReduceReplicateExpr(expr));
	oids = (oidvector*)DatumGetPointer(((OidVectorLoopExpr*)expr)->vector);

	list = NIL;
	for(i=0;i<oids->dim1;++i)
		list = lappend_oid(list, oids->values[i]);

	return list;
}

Expr *CreateReduceValExprAs(Expr *expr, Index newRelid, List *newAttnos)
{
	ListCell *lc;
	CreateReduceExprContext context;
	Expr *newExpr;
	AssertArg(expr && IsReduceExprByValue(expr));
	AssertArg(newRelid > 0 && newAttnos != NIL);

	context.oldAttrs = GetReducePathExprAttnoList(expr, NULL);
	if(list_length(context.oldAttrs) != list_length(newAttnos))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("diffent count of varattno")));
	}

	/* check replicate attno */
	context.newAttrs = NIL;
	foreach(lc, newAttnos)
	{
		if(list_member_int(context.newAttrs, lfirst_int(lc)))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("replicate var attno %d", lfirst_int(lc))));
		}
		context.newAttrs = lappend_int(context.newAttrs, lfirst_int(lc));
	}

	context.newRelid = newRelid;
	newExpr = (Expr*)CreateReduceExprMutator((Node*)expr, &context);

	list_free(context.newAttrs);
	list_free(context.oldAttrs);
	return newExpr;
}

bool IsReduceExprByValue(Expr *expr)
{
	ArrayRef *ref;
	if(expr == NULL || !IsA(expr, ArrayRef))
		return false;

	ref = (ArrayRef*)expr;
	return ref->refarraytype == OIDARRAYOID &&
		   ref->refelemtype == OIDOID &&
		   IsA(ref->refexpr, Const);
}

Index PullReducePathExprAttnos(Expr *expr, Bitmapset **varattnos)
{
	PullReducePathExprVarAttnosContext context;

	context.relid = 0;
	context.varattnos = *varattnos;
	context.attnoList = NIL;
	context.flags = PUSH_REDUCE_EXPR_BMS;

	PullReducePathExprAttnosWalker((ReduceParam*)expr, &context);

	*varattnos = context.varattnos;
	return context.relid;
}

List *GetReducePathExprAttnoList(Expr *expr, Index *relid)
{
	PullReducePathExprVarAttnosContext context;

	context.relid = 0;
	context.varattnos = NULL;
	context.attnoList = NIL;
	context.flags = PUSH_REDUCE_EXPR_LIST;

	PullReducePathExprAttnosWalker((ReduceParam*)expr, &context);

	if(relid)
		*relid = context.relid;
	return context.attnoList;
}

static oidvector *makeDatanodeOidVector(List *list)
{
	oidvector *oids;
	ListCell *lc;
	Size i;

	Assert(list != NIL);
	oids = palloc0(offsetof(oidvector, values) + list_length(list) * sizeof(Oid));
	oids->ndim = 1;
	oids->dataoffset = 0;
	oids->elemtype = OIDOID;
	oids->dim1 = list_length(list);
	oids->lbound1 = 0;
	i = 0;
	if(IsA(list, OidList))
	{
		foreach(lc, list)
		{
			oids->values[i] = lfirst_oid(lc);
			++i;
		}
	}else
	{
		Assert(IsA(list, IntList));
		foreach(lc, list)
		{
			oids->values[i] = PGXCNodeGetNodeOid(lfirst_int(lc), PGXC_NODE_DATANODE);
			++i;
		}
	}

	return oids;
}

static Expr *makeLocatorModuleExpr(RelationLocInfo *loc, Index varno, bool using_param)
{
	ListCell *lc;
	Expr *expr;
	Oid typid;
	int32 typmod;
	Oid collid;
	int i;
	if(loc->locatorType != LOCATOR_TYPE_USER_DEFINED)
	{
		get_atttypetypmodcoll(loc->relid, loc->partAttrNum, &typid, &typmod, &collid);
		if(using_param)
		{
			expr = (Expr*) makeReduceParam(typid, 1, typmod, collid, varno, loc->partAttrNum);
		}else
		{
			expr = (Expr*) makeVar(varno, loc->partAttrNum, typid, typmod, collid, 0);
		}
		if(loc->locatorType == LOCATOR_TYPE_HASH)
			expr = makeHashExpr(expr);
		else
			expr = (Expr*)coerce_to_target_type(NULL, (Node*)expr,
												typid, INT8OID, -1,
												COERCION_EXPLICIT,
												COERCE_IMPLICIT_CAST,
												-1);
	}else
	{
		List *args = NIL;
		Oid *argTypes;
		int narg;
		get_func_signature(loc->funcid, &argTypes, &narg);
		if(narg < list_length(loc->funcAttrNums))
		{
			Relation rel = RelationIdGetRelation(loc->relid);
			ereport(ERROR,
						  (errmsg("too many argument for user hash distrbute table \"%s\"",
						   RelationGetRelationName(rel))));
		}
		i=0;
		foreach(lc, loc->funcAttrNums)
		{
			get_atttypetypmodcoll(loc->relid, lfirst_int(lc), &typid, &typmod, &collid);
			if(using_param)
			{
				expr = (Expr*)makeReduceParam(typid, i+1, typmod, collid, varno, lfirst_int(lc));
			}else
			{
				expr = (Expr*)makeVar(varno, lfirst_int(lc), typid, typmod, collid, 0);
			}
			expr = (Expr*)coerce_to_target_type(NULL,
												(Node*)expr,
												typid,
												argTypes[i],
												-1,
												COERCION_EXPLICIT,
												COERCE_IMPLICIT_CAST,
												-1);
			args = lappend(args, expr);
			++i;
		}
		for(;i<narg;++i)
			args = lappend(args, makeNullConst(argTypes[i], -1, InvalidOid));

		expr = (Expr*) makeFuncExpr(loc->funcid,
									get_func_rettype(loc->funcid),
									args,
									InvalidOid, InvalidOid,
									COERCE_EXPLICIT_CALL);
	}
	Assert(expr);
	expr = makeModuloExpr(expr, list_length(loc->nodeList));
	expr = (Expr*)coerce_to_target_type(NULL, (Node*)expr,
								exprType((Node*)expr),
								INT4OID,
								-1,
								COERCION_EXPLICIT,
								COERCE_IMPLICIT_CAST,
								-1);

	return expr;
}

static Param *makeReduceParam(Oid type, int paramid, int parammod, Oid collid, Index relid, AttrNumber varno)
{
	ReduceParam *rp = palloc0(sizeof(ReduceParam));
	Param *param = &rp->param;
	NodeSetTag(param, T_Param);
	param->location = -1;
	param->paramid = paramid;
	param->paramtype = type;
	param->paramcollid = collid;
	param->paramtypmod = parammod;
	param->paramkind = PARAM_EXTERN;
	rp->relid = relid;
	rp->attno = varno;
	return param;
}

static bool PullReducePathExprAttnosWalker(ReduceParam *rp, PullReducePathExprVarAttnosContext *context)
{
	if(rp == NULL)
		return false;
	if(IsA(rp, Param))
	{
		int attno;
		if(context->relid == 0)
			context->relid = rp->relid;
		else if(context->relid != rp->relid)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("invalid reduce expression")));

		attno = rp->attno - FirstLowInvalidHeapAttributeNumber;

		if (context->flags & PUSH_REDUCE_EXPR_BMS)
			context->varattnos = bms_add_member(context->varattnos, attno);

		if (context->flags & PUSH_REDUCE_EXPR_LIST &&
			list_member_int(context->attnoList, attno) == false)
		{
			context->attnoList = lappend_int(context->attnoList, attno);
		}
		return false;
	}
	return expression_tree_walker((Node*)rp, PullReducePathExprAttnosWalker, context);
}

static bool expr_have_param(Node *node, void *none)
{
	if(node == NULL)
		return false;
	if(IsA(node, Param))
		return true;
	return expression_tree_walker(node, expr_have_param, NULL);
}

static Node *reduceParam2VarMutator(Node *node, void *none)
{
	if(node == NULL)
		return NULL;
	if(IsA(node, Param))
	{
		ReduceParam *rp = (ReduceParam*)node;
		Assert(rp->attno > 0);
		return (Node*)makeVar(rp->relid,
							  rp->attno,
							  rp->param.paramtype,
							  rp->param.paramtypmod,
							  rp->param.paramcollid,
							  0);
	}
	return expression_tree_mutator(node, reduceParam2VarMutator, NULL);
}

static Node *CreateReduceExprMutator(Node *node, CreateReduceExprContext *context)
{
	if(node == NULL)
		return NULL;

	if(IsA(node, Param))
	{
		ReduceParam *rp = (ReduceParam*)node;
		ReduceParam *new_rp = palloc(sizeof(*new_rp));
		ListCell *lcNew;
		ListCell *lcOld;
		memcpy(new_rp, rp, sizeof(Param));
		new_rp->attno = InvalidAttrNumber;
		new_rp->relid = 0;
		forboth(lcNew, context->newAttrs, lcOld, context->oldAttrs)
		{
			if(rp->attno == lfirst_int(lcOld))
			{
				new_rp->attno = lfirst_int(lcNew);
				new_rp->relid = context->newRelid;
				break;
			}
		}
		Assert(new_rp->attno != InvalidAttrNumber);
		Assert(new_rp->relid > 0);
		return (Node*)new_rp;
	}

	return expression_tree_mutator(node, CreateReduceExprMutator, context);
}
