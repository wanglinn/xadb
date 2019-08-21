#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/var.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "optimizer/reduceinfo.h"
#include "pgxc/slot.h"

static oidvector *makeOidVector(List *list);
static Expr* makeReduceArrayRef(List *oid_list, Expr *modulo, bool try_const, bool use_coalesce);
static int CompareOid(const void *a, const void *b);
static bool reduce_info_in_one_node_can_join(ReduceInfo *outer_info,
											 ReduceInfo *inner_info,
											 List **new_reduce_list,
											 JoinType jointype,
											 bool *is_dummy);
static inline Const* MakeInt4Const(int32 value)
{
	return makeConst(INT4OID,
					 -1,
					 InvalidOid,
					 sizeof(int32),
					 Int32GetDatum(value),
					 false,
					 true);
}

static inline Const* makeOidConst(Oid oid)
{
	return makeConst(OIDOID,
					 -1,
					 InvalidOid,
					 sizeof(Oid),
					 ObjectIdGetDatum(oid),
					 false,
					 true);
}

static inline FuncExpr* makeSimpleFuncExpr(Oid funcid, Oid rettype, List *args)
{
	return makeFuncExpr(funcid,
						rettype,
						args,
						InvalidOid,
						InvalidOid,
						COERCE_EXPLICIT_CALL);
}

static inline ReduceInfo* MakeEmptyReduceInfo(uint32 nkey)
{
	ReduceInfo *rinfo = palloc0(offsetof(ReduceInfo, keys) + sizeof(ReduceKeyInfo)*nkey);
	rinfo->nkey = nkey;
	return rinfo;
}

static inline void SetReduceKeyDefaultInfo(ReduceKeyInfo *key, const Expr *expr, Oid am_oid, const char *method_name)
{
	key->key = (Expr*)copyObject(expr);
	key->opclass = ResolveOpClass(NIL,
								  exprType((Node*)expr),
								  method_name,
								  am_oid);
	key->opfamily = get_opclass_family(key->opclass);
	key->collation = InvalidOid;
}

ReduceInfo *MakeHashReduceInfo(const List *storage, const List *exclude, const Expr *key)
{
	ReduceInfo *rinfo;
	AssertArg(storage && IsA(storage, OidList) && key);
	AssertArg(exclude == NIL || IsA(exclude, OidList));

	rinfo = MakeEmptyReduceInfo(1);
	SetReduceKeyDefaultInfo(&rinfo->keys[0], key, HASH_AM_OID, "hash");
	rinfo->storage_nodes = list_copy(storage);
	rinfo->exclude_exec = list_copy(exclude);
	rinfo->relids = pull_varnos((Node*)key);
	rinfo->type = REDUCE_TYPE_HASH;

	return rinfo;
}


ReduceInfo *MakeHashmapReduceInfo(const List *storage, const List *exclude, const Expr *key)
{
	ReduceInfo *rinfo = MakeHashReduceInfo(storage, exclude, key);
	rinfo->type = REDUCE_TYPE_HASHMAP;

	return rinfo;
}

ReduceInfo *MakeModuloReduceInfo(const List *storage, const List *exclude, const Expr *key)
{
	ReduceInfo *rinfo;
	Oid typoid;
	AssertArg(storage != NIL && IsA(storage, OidList) && key);
	AssertArg(exclude == NIL || IsA(exclude, OidList));

	typoid = exprType((Node*)key);

	rinfo = MakeEmptyReduceInfo(1);
	SetReduceKeyDefaultInfo(&rinfo->keys[0], key, BTREE_AM_OID, "btree");
	rinfo->storage_nodes = list_copy(storage);
	rinfo->exclude_exec = list_copy(exclude);
	rinfo->relids = pull_varnos((Node*)key);
	rinfo->type = REDUCE_TYPE_MODULO;

	return rinfo;
}

ReduceInfo *MakeReplicateReduceInfo(const List *storage)
{
	ReduceInfo *rinfo;
	AssertArg(storage != NIL && IsA(storage, OidList));

	rinfo = MakeEmptyReduceInfo(0);
	rinfo->storage_nodes = SortOidList(list_copy(storage));
	rinfo->type = REDUCE_TYPE_REPLICATED;

	return rinfo;
}

ReduceInfo *MakeFinalReplicateReduceInfo(void)
{
	ReduceInfo *rinfo;

	rinfo = MakeEmptyReduceInfo(0);
	rinfo->storage_nodes = list_make1_oid(InvalidOid);
	rinfo->type = REDUCE_TYPE_REPLICATED;

	return rinfo;
}

ReduceInfo *MakeRandomReduceInfo(const List *storage)
{
	ReduceInfo *rinfo;
	AssertArg(storage != NIL && IsA(storage, OidList));

	rinfo = MakeEmptyReduceInfo(0);
	rinfo->storage_nodes = SortOidList(list_copy(storage));
	rinfo->type = REDUCE_TYPE_RANDOM;

	return rinfo;
}

ReduceInfo *MakeCoordinatorReduceInfo(void)
{
	ReduceInfo *rinfo;

	rinfo = MakeEmptyReduceInfo(0);
	rinfo->storage_nodes = list_make1_oid(PGXCNodeOid);
	rinfo->type = REDUCE_TYPE_COORDINATOR;

	return rinfo;
}

ReduceInfo *MakeReduceInfoFromLocInfo(const RelationLocInfo *loc_info, const List *exclude, Oid reloid, Index relid)
{
	ReduceInfo *rinfo;
	List *rnodes = loc_info->nodeids;
	if(IsRelationReplicated(loc_info))
	{
		rinfo = MakeReplicateReduceInfo(rnodes);
	}else if(loc_info->locatorType == LOCATOR_TYPE_RANDOM)
	{
		rinfo = MakeRandomReduceInfo(rnodes);
	}else
	{
		if(loc_info->locatorType == LOCATOR_TYPE_HASH)
		{
			Var *var = makeVarByRel(loc_info->partAttrNum, reloid, relid);
			rinfo = MakeHashReduceInfo(rnodes, exclude, (Expr*)var);
		}else if(loc_info->locatorType == LOCATOR_TYPE_HASHMAP)
		{
			Var *var = makeVarByRel(loc_info->partAttrNum, reloid, relid);
			rinfo = MakeHashmapReduceInfo(rnodes, exclude, (Expr*)var);

		}else if(loc_info->locatorType == LOCATOR_TYPE_MODULO)
		{
			Var *var = makeVarByRel(loc_info->partAttrNum, reloid, relid);
			rinfo = MakeModuloReduceInfo(rnodes, exclude, (Expr*)var);
		}else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("unknown locator type %d", loc_info->locatorType)));
		}
	}
	return rinfo;
}

ReduceInfo *MakeReduceInfoUsingPathTarget(const RelationLocInfo *loc_info, const List *exclude, PathTarget *target)
{
	ReduceInfo *rinfo;
	List *rnodes = loc_info->nodeids;

	if(IsRelationReplicated(loc_info))
	{
		rinfo = MakeReplicateReduceInfo(rnodes);
	}else if(loc_info->locatorType == LOCATOR_TYPE_RANDOM)
	{
		rinfo = MakeRandomReduceInfo(rnodes);
	}else if(loc_info->locatorType == LOCATOR_TYPE_HASH)
	{
		rinfo = MakeHashReduceInfo(rnodes,
								   exclude,
								   list_nth(target->exprs, loc_info->partAttrNum-1));
	}else if(loc_info->locatorType == LOCATOR_TYPE_HASHMAP)
	{
		rinfo = MakeHashmapReduceInfo(rnodes,
									  exclude,
									  list_nth(target->exprs, loc_info->partAttrNum-1));
	}else if(loc_info->locatorType == LOCATOR_TYPE_MODULO)
	{
		rinfo = MakeModuloReduceInfo(rnodes,
									 exclude,
									 list_nth(target->exprs, loc_info->partAttrNum-1));
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("unknown locator type %d", loc_info->locatorType)));
	}
	return rinfo;

}

ReduceInfo *MakeReduceInfoAs(const ReduceInfo *reduce, List *params)
{
	ReduceInfo *rinfo;
	ListCell   *lc;
	uint32		i;

	if (list_length(params) != reduce->nkey)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid number of argument for MakeReduceInfoAs")));
	}

	rinfo = CopyReduceInfoExtend(reduce, REDUCE_MARK_ALL & (~REDUCE_MARK_KEY_EXPR));
	i = 0;
	foreach(lc, params)
		rinfo->keys[i++].key = lfirst(lc);
	
	return rinfo;
}

ReduceInfo *ConvertReduceInfo(const ReduceInfo *reduce, const PathTarget *target, Index new_relid)
{
	ReduceInfo *new_reduce;

	if (IsReduceInfoByValue(reduce))
	{
		List *list;
		List *attnos = ReduceInfoFindPathTarget(reduce, target);
		if(attnos != NIL)
		{
			list = MakeVarList(attnos, new_relid, target);
			new_reduce = MakeReduceInfoAs(reduce, list);
			list_free(attnos);
		}else
		{
			list = list_difference_oid(reduce->storage_nodes, reduce->exclude_exec);
			new_reduce = MakeRandomReduceInfo(list);
		}
		list_free(list);
	}else
	{
		new_reduce = CopyReduceInfo(reduce);
	}

	return new_reduce;
}

List *ConvertReduceInfoList(const List *reduce_list, const PathTarget *target, Index new_relid)
{
	const ListCell *lc;
	ReduceInfo *new_reduce;
	List *new_reduce_list = NIL;
	List *random_reduce_list = NIL;

	foreach(lc, reduce_list)
	{
		new_reduce = ConvertReduceInfo(lfirst(lc), target, new_relid);
		if(IsReduceInfoRandom(new_reduce))
			random_reduce_list = lappend(random_reduce_list, new_reduce);
		else
			new_reduce_list = lappend(new_reduce_list, new_reduce);
	}

	if(new_reduce_list == NIL && random_reduce_list != NIL)
	{
		if(list_length(random_reduce_list) == 1)
		{
			new_reduce_list = random_reduce_list;
			random_reduce_list = NIL;
		}else
		{
			List *exec_node = ReduceInfoListGetExecuteOidList(random_reduce_list);
			new_reduce = MakeRandomReduceInfo(exec_node);
			new_reduce_list = list_make1(new_reduce);
		}
	}

	FreeReduceInfoList(random_reduce_list);

	return new_reduce_list;
}

void FreeReduceInfo(ReduceInfo *reduce)
{
	uint32 i;
	if(reduce)
	{
		list_free(reduce->storage_nodes);
		list_free(reduce->exclude_exec);
		i = reduce->nkey;
		while (i>0)
		{
			if (reduce->keys[i].key)
				pfree(reduce->keys[i].key);
		}
		bms_free(reduce->relids);
		pfree(reduce);
	}
}

void FreeReduceInfoList(List *list)
{
	ListCell *lc;
	foreach(lc, list)
		FreeReduceInfo(lfirst(lc));
	list_free(list);
}

List *SortOidList(List *list)
{
	Oid *oids;
	ListCell *lc;
	Size i,count;

	if(list == NIL)
		return NIL;

	count = list_length(list);
	if(count == 1)
		return list;

	oids = palloc(sizeof(Oid)*count);
	i=0;
	foreach(lc, list)
		oids[i++] = lfirst_oid(lc);

	pg_qsort(oids, count, sizeof(Oid), CompareOid);

	i=0;
	foreach(lc, list)
		lfirst_oid(lc) = oids[i++];

	pfree(oids);
	return list;
}


int HashmapPathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
				   List *storage, List *exclude,
				   ReducePathCallback_function func, void *context)
{
	ReduceInfo *reduce;
	int result;
	AssertArg(root && rel && path && storage && func);

	if(expr == NULL)
		return 0;

	if (IsSlotNodeOidsEqualOidList(storage) == false)
		return 0;

	if(IsA(expr, List))
	{
		ListCell *lc;
		result = 0;
		foreach(lc, (List*)expr)
		{
			if (!IsTypeDistributable(exprType(lfirst(lc))) ||
				expression_have_subplan(lfirst(lc)))
				continue;
			reduce = MakeHashmapReduceInfo(storage, exclude, lfirst(lc));
			result = ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
			if(result < 0)
				break;
		}
	}else if(IsTypeDistributable(exprType((Node*)expr)) &&
			 expression_have_subplan(expr) == false)
	{
		reduce = MakeHashmapReduceInfo(storage, exclude, expr);
		result = ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
	}else
	{
		result = 0;
	}
	return 0;
}


int HashPathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
				   List *storage, List *exclude,
				   ReducePathCallback_function func, void *context)
{
	ReduceInfo *reduce;
	int result;
	AssertArg(root && rel && path && storage && func);

	if(expr == NULL)
		return 0;

	if(IsA(expr, List))
	{
		ListCell *lc;
		result = 0;
		foreach(lc, (List*)expr)
		{
			if (!IsTypeDistributable(exprType(lfirst(lc))) ||
				expression_have_subplan(lfirst(lc)))
				continue;
			reduce = MakeHashReduceInfo(storage, exclude, lfirst(lc));
			result = ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
			if(result < 0)
				break;
		}
	}else if(IsTypeDistributable(exprType((Node*)expr)) &&
			 expression_have_subplan(expr) == false)
	{
		reduce = MakeHashReduceInfo(storage, exclude, expr);
		result = ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
	}else
	{
		result = 0;
	}
	return 0;
}

int HashPathListByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
					   List *storage, List *exclude,
					   ReducePathCallback_function func, void *context)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = HashPathByExpr(expr, root, rel, lfirst(lc), storage, exclude, func, context);
		if(result < 0)
			break;
	}
	return result;
}

int ModuloPathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
					 List *storage, List *exclude,
					 ReducePathCallback_function func, void *context)
{
	ReduceInfo *reduce;
	int result;
	if(expr == NULL)
		return 0;

	if(IsA(expr, List))
	{
		ListCell *lc;
		result = 0;
		foreach(lc, (List*)expr)
		{
			if (!CanModuloType(exprType(lfirst(lc)), true) ||
				expression_have_subplan(lfirst(lc)))
				continue;
			reduce = MakeModuloReduceInfo(storage, exclude, lfirst(lc));
			result = ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
			if(result < 0)
				break;
		}
	}else if(CanModuloType(exprType((Node*)expr), true) &&
			 expression_have_subplan(expr) == false)
	{
		reduce = MakeModuloReduceInfo(storage, exclude, expr);
		result = ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
	}else
	{
		result = 0;
	}
	return result;
}

int ModuloPathListByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
						 List *storage, List *exclude,
						 ReducePathCallback_function func, void *context)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = ModuloPathByExpr(expr, root, rel, lfirst(lc), storage, exclude, func, context);
		if(result < 0)
			break;
	}
	return result;
}

int CoordinatorPath(PlannerInfo *root, RelOptInfo *rel, Path *path,
					ReducePathCallback_function func, void *context)
{
	ReduceInfo *reduce = MakeCoordinatorReduceInfo();
	return ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
}

int CoordinatorPathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
						ReducePathCallback_function func, void *context)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = CoordinatorPath(root, rel, lfirst(lc), func, context);
		if(result < 0)
			break;
	}
	return result;
}

int ClusterGatherSubPath(PlannerInfo *root, RelOptInfo *rel, Path *path,
					  ReducePathCallback_function func, void *context)
{
	Path *new_path;
	int result;

	new_path = (Path*)create_cluster_gather_path(path, rel);
	new_path->reduce_info_list = list_make1(MakeCoordinatorReduceInfo());
	new_path->reduce_is_valid = true;

	result = (*func)(root, new_path, context);
	if(result < 0)
		return result;

	if(path->pathkeys)
	{
		new_path = (Path*)create_cluster_merge_gather_path(root, rel, path, path->pathkeys);
		new_path->reduce_info_list = list_make1(MakeCoordinatorReduceInfo());
		new_path->reduce_is_valid = true;
		result = (*func)(root, new_path, context);
	}

	return result;
}

int ClusterGatherSubPathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
						  ReducePathCallback_function func, void *context)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = ClusterGatherSubPath(root, rel, lfirst(lc), func, context);
		if(result < 0)
			break;
	}
	return result;
}

int ParallelGatherSubPath(PlannerInfo *root, RelOptInfo *rel, Path *path,
						  ReducePathCallback_function func, void *context)
{
	Path *new_path;
	double rows;
	int result;
	Assert(path->parallel_workers > 0);

	rows = path->rows * path->parallel_workers;

	new_path = (Path*)create_gather_path(root, rel, path, path->pathtarget, PATH_REQ_OUTER(path), &rows);
	new_path->reduce_info_list = CopyReduceInfoList(get_reduce_info_list(path));
	new_path->reduce_is_valid = true;

	result = (*func)(root, new_path, context);
	if(result < 0)
		return result;

	if(path->pathkeys)
	{
		new_path = (Path*)create_gather_merge_path(root, rel, path, path->pathtarget, path->pathkeys, PATH_REQ_OUTER(path), &rows);
		new_path->reduce_info_list = CopyReduceInfoList(get_reduce_info_list(path));
		new_path->reduce_is_valid = true;
		result = (*func)(root, new_path, context);
	}

	return result;
}

int ParallelGatherSubPathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
							  ReducePathCallback_function func, void *context)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = ParallelGatherSubPath(root, rel, lfirst(lc), func, context);
		if(result < 0)
			break;
	}
	return result;
}

int ReplicatePath(PlannerInfo *root, RelOptInfo *rel, Path *path, List *storage,
						 ReducePathCallback_function func, void *context)
{
	ReduceInfo *reduce = MakeReplicateReduceInfo(storage);
	return ReducePathUsingReduceInfo(root, rel, path, func, context, reduce);
}

int ReplicatePathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist, List *storage,
							  ReducePathCallback_function func, void *context)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = ReplicatePath(root, rel, lfirst(lc), storage, func, context);
		if(result < 0)
			break;
	}
	return result;
}

/* last arg must REDUCE_TYPE_NONE */
int ReducePathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
								List *storage, List *exclude,
								ReducePathCallback_function func, void *context, ...)
{
	va_list args;
	int result;

	va_start(args, context);
	result = ReducePathByExprVA(expr, root, rel, path, storage, exclude, func, context, args);
	va_end(args);

	return result;
}

int ReducePathListByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist, List *storage, List *exclude,
						 ReducePathCallback_function func, void *context, ...)
{
	va_list args;
	ListCell *lc;
	int result = 0;

	foreach(lc, pathlist)
	{
		va_start(args, context);
		result = ReducePathByExprVA(expr, root, rel, lfirst(lc), storage, exclude, func, context, args);
		va_end(args);
		if(result < 0)
			break;
	}

	return result;
}

int ReducePathByReduceInfo(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
						   ReducePathCallback_function func, void *context, ReduceInfo *reduce)
{
	return ReducePathByExpr(expr,
							root,
							rel,
							path,
							reduce->storage_nodes,
							reduce->exclude_exec,
							func,
							context,
							reduce->type,
							REDUCE_TYPE_NONE);
}

int ReducePathListByReduceInfo(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
							   ReducePathCallback_function func, void *context, ReduceInfo *reduce)
{
	ListCell *lc;
	int result = 0;

	foreach(lc, pathlist)
	{
		result = ReducePathByExpr(expr,
								  root,
								  rel,
								  lfirst(lc),
								  reduce->storage_nodes,
								  reduce->exclude_exec,
								  func,
								  context,
								  reduce->type,
								  REDUCE_TYPE_NONE);
		if(result < 0)
			break;
	}

	return result;
}

int ReducePathByReduceInfoList(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
							   ReducePathCallback_function func, void *context, List *reduce_list)
{
	ListCell *lc;
	ReduceInfo *reduce;
	int result = 0;

	foreach(lc, reduce_list)
	{
		reduce = lfirst(lc);
		result = ReducePathByExpr(expr,
								  root,
								  rel,
								  path,
								  reduce->storage_nodes,
								  reduce->exclude_exec,
								  func,
								  context,
								  reduce->type,
								  REDUCE_TYPE_NONE);
		if(result < 0)
			break;
	}

	return result;
}

int ReducePathListByReduceInfoList(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
								   ReducePathCallback_function func, void *context, List *reduce_list)
{
	ListCell *lc_path;
	ListCell *lc_reduce;
	ReduceInfo *reduce;
	int result = 0;

	foreach(lc_path, pathlist)
	{
		foreach(lc_reduce, reduce_list)
		{
			reduce = lfirst(lc_reduce);
			result = ReducePathByExpr(expr,
									  root,
									  rel,
									  lfirst(lc_path),
									  reduce->storage_nodes,
									  reduce->exclude_exec,
									  func,
									  context,
									  reduce->type,
									  REDUCE_TYPE_NONE);
			if(result < 0)
				return result;
		}
	}

	return result;
}

int ReducePathUsingReduceInfo(PlannerInfo *root, RelOptInfo *rel, Path *path,
							  ReducePathCallback_function func, void *context, ReduceInfo *reduce)
{
	ListCell *lc;
	List *old_reduce_list;
	Path *new_path;
	int result;
	if(PATH_REQ_OUTER(path))
		return 0;

	old_reduce_list = get_reduce_info_list(path);
	new_path = NULL;
	foreach(lc, old_reduce_list)
	{
		if(IsReduceInfoEqual(reduce, lfirst(lc)))
		{
			new_path = path;
			break;
		}
	}
	if(new_path == NULL)
	{
		if (IsReduceInfoListCoordinator(old_reduce_list))
		{
			/*
			 * make sure result tuple(s) only in coordinator,
			 * some path return tuple(s) in any node,
			 * e.g "SELECT 1 AS XXX"
			 */
			path = (Path*)create_filter_path(root, rel, path, path->pathtarget,
											 list_make1(CreateNodeOidEqualOid(PGXCNodeOid)));
		}
		new_path = create_cluster_reduce_path(root, path, list_make1(reduce), rel, NIL);
	}
	result = (*func)(root, new_path, context);
	if(result < 0)
		return result;

	if (new_path != path && path->pathkeys != NIL &&
		path->parallel_workers == 0) /* parallel reduce not support merge yet */
	{
		new_path = create_cluster_reduce_path(root, path, list_make1(reduce), rel, path->pathkeys);
		result = (*func)(root, new_path, context);
	}
	return result;
}

int ReducePathUsingReduceInfoList(PlannerInfo *root, RelOptInfo *rel, Path *path,
								  ReducePathCallback_function func, void *context, List *reduce_list)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, reduce_list)
	{
		result = ReducePathUsingReduceInfo(root, rel, path, func, context, lfirst(lc));
		if(result < 0)
			return result;
	}
	return result;
}

int ReducePathListUsingReduceInfo(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
								  ReducePathCallback_function func, void *context, ReduceInfo *reduce)
{
	ListCell *lc;
	int result = 0;
	foreach(lc, pathlist)
	{
		result = ReducePathUsingReduceInfo(root, rel, lfirst(lc), func, context, reduce);
		if(result < 0)
			return result;
	}
	return result;
}

int ReducePathByExprVA(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
					   List *storage, List *exclude,
					   ReducePathCallback_function func, void *context, va_list args)
{
	int result = 0;

	for(;;)
	{
		int type = va_arg(args, int);
		if(type == REDUCE_TYPE_HASH)
			result = HashPathByExpr(expr, root,rel, path, storage, exclude, func, context);
		else if(type == REDUCE_TYPE_HASHMAP)
			result = HashmapPathByExpr(expr, root,rel, path, storage, exclude, func, context);
		else if(type == REDUCE_TYPE_MODULO)
			result = ModuloPathByExpr(expr, root,rel, path, storage, exclude, func, context);
		else if(type == REDUCE_TYPE_COORDINATOR)
			result = CoordinatorPath(root, rel, path, func, context);
		else if(type == REDUCE_TYPE_REPLICATED)
			result = ReplicatePath(root, rel, path, storage, func, context);
		else if(type == REDUCE_TYPE_GATHER)
			result = ClusterGatherSubPath(root, rel, path, func, context);
		else if(type == REDUCE_TYPE_IGNORE)
			continue;
		else if(type == REDUCE_TYPE_NONE)
			break;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unknown reduce type %d", type)));
		if(result < 0)
			break;
	}

	return result;

}

int ReducePathListByExprVA(Expr *expr,PlannerInfo *root, RelOptInfo *rel, List *pathlist, List *storage, List *exclude,
								  ReducePathCallback_function func, void *context, va_list args)
{
	ListCell *lc;
	int result = 0;

	foreach(lc, pathlist)
	{
		va_list va;

		va_copy(va, args);
		result = ReducePathByExprVA(expr, root, rel, lfirst(lc), storage, exclude, func, context, va);
		va_end(va);

		if(result < 0)
			break;
	}

	return result;
}

List *GetCheapestReducePathList(RelOptInfo *rel, List *pathlist, Path **cheapest_startup, Path **cheapest_total)
{
	ListCell *lc;
	ListCell *lck;
	ListCell *lcs;
	ListCell *lct;
	List *result;
	List *all_pathkeys = NIL;
	List *startup_pathlist = NIL;
	List *total_pathlist = NIL;
	Path *startup = NULL;
	Path *total = NULL;
	Path *path;
	PathKeysComparison keyscmp;
	bool found;

	foreach(lc, pathlist)
	{
		path = lfirst(lc);
		/* not support replicate to other yet */
		if (PATH_REQ_OUTER(path) ||
			IsReduceInfoListReplicated(get_reduce_info_list(path)))
			continue;

		if (startup == NULL || compare_path_costs(path, startup, STARTUP_COST) < 0)
			startup = path;
		if (total == NULL || compare_path_costs(path, total, TOTAL_COST) < 0)
			total = path;

		if(path->pathkeys == NIL)
			continue;

		found = false;
		forthree(lck, all_pathkeys, lcs, startup_pathlist, lct, total_pathlist)
		{
			keyscmp = compare_pathkeys(lfirst(lck), path->pathkeys);
			if(keyscmp == PATHKEYS_EQUAL)
			{
				if(compare_path_costs(path, lfirst(lcs), STARTUP_COST) < 0)
					lfirst(lcs) = path;
				if(compare_path_costs(path, lfirst(lct), TOTAL_COST) < 0)
					lfirst(lct) = path;
				found = true;
				break;
			}
		}
		if(!found)
		{
			all_pathkeys = lappend(all_pathkeys, path->pathkeys);
			startup_pathlist = lappend(startup_pathlist, path);
			total_pathlist = lappend(total_pathlist, path);
		}
	}
	list_free(all_pathkeys);

	result = NIL;
	forboth(lcs, startup_pathlist, lct, total_pathlist)
	{
		result = lappend(result, lfirst(lcs));
		if(lfirst(lcs) != lfirst(lct))
			result = lappend(result, lfirst(lct));
	}
	list_free(startup_pathlist);
	list_free(total_pathlist);

	if(cheapest_startup)
		*cheapest_startup = startup;
	if(cheapest_total)
		*cheapest_total = total;
	return result;
}

int ReducePathSave2List(PlannerInfo *root, Path *path, void *pplist)
{
	Assert(pplist);

	*((List**)pplist) = lappend(*((List**)pplist), path);

	return 0;
}

bool IsReduceInfoListByValue(List *list)
{
	ReduceInfo *rinfo;
	if(list == NIL)
		return false;

	rinfo = linitial(list);
	if(IsReduceInfoByValue(rinfo))
	{
#ifdef USE_ASSERT_CHECKING
		if(list_length(list) > 1)
		{
			ListCell *lc = list_head(list);
			for_each_cell(lc, lnext(lc))
			{
				rinfo = lfirst(lc);
				Assert(IsReduceInfoByValue(rinfo));
			}
		}
#endif
		return true;
	}
	return false;
}

bool IsReduceInfoListReplicated(List *list)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	foreach(lc, list)
	{
		rinfo = lfirst(lc);
		if(IsReduceInfoReplicated(rinfo))
		{
			Assert(list_length(list) == 1);
			return true;
		}
	}
	return false;
}

bool IsReduceInfoListReplicatedOids(List *list, List *storage, List *exclude)
{
	ReduceInfo *rinfo;
	if (list_length(list) != 1)
		return false;
	rinfo = linitial(list);
	if (!IsReduceInfoReplicated(rinfo))
		return false;

	if (list_equal_oid_without_order(storage, rinfo->storage_nodes) == false ||
		list_equal_oid_without_order(exclude, rinfo->exclude_exec) == false)
		return false;

	return true;
}

bool IsReduceInfoListFinalReplicated(List *list)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	foreach(lc, list)
	{
		rinfo = lfirst(lc);
		if (IsReduceInfoFinalReplicated(rinfo))
		{
			Assert(list_length(list) == 1);
			return true;
		}
	}
	return false;
}

bool IsReduceInfoListRandom(List *list)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	foreach(lc, list)
	{
		rinfo = lfirst(lc);
		if(IsReduceInfoRandom(rinfo))
		{
			Assert(list_length(list) == 1);
			return true;
		}
	}
	return false;
}
bool IsReduceInfoListCoordinator(List *list)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	foreach(lc, list)
	{
		rinfo = lfirst(lc);
		if(IsReduceInfoCoordinator(rinfo))
		{
			Assert(list_length(list) == 1);
			return true;
		}
	}
	return false;
}

bool IsReduceInfoListInOneNode(List *list)
{
	ReduceInfo *info;
	ListCell *lc;
	foreach(lc, list)
	{
		info = lfirst(lc);
		if(IsReduceInfoInOneNode(info))
			return true;
	}
	return false;
}

bool IsReduceInfoListExclude(List *list, Oid oid)
{
	ReduceInfo *info;
	ListCell *lc;

	foreach(lc, list)
	{
		info = lfirst(lc);
		if (IsReduceInfoExclude(info, oid))
			return true;
	}

	return false;
}

bool IsPathExcludeNodeOid(Path *path, Oid oid)
{
	List *reducelist = get_reduce_info_list(path);

	return IsReduceInfoListExclude(reducelist, oid);
}

bool IsReduceInfoStorageSubset(const ReduceInfo *rinfo, List *oidlist)
{
	ListCell *lc;
	foreach(lc, rinfo->storage_nodes)
	{
		if(list_member_oid(oidlist, lfirst_oid(lc)) == false)
			return false;
	}
	return true;
}

bool IsReduceInfoExecuteSubset(const ReduceInfo *rinfo, List *oidlist)
{
	ListCell *lc;
	foreach(lc, rinfo->storage_nodes)
	{
		if (list_member_oid(rinfo->exclude_exec, lfirst_oid(lc))== false &&
			list_member_oid(oidlist, lfirst_oid(lc)) == false)
			return false;
	}
	return true;
}

bool IsReduceInfoListExecuteSubset(List *reduce_info_list, List *oidlist)
{
	ListCell *lc;
	List *execute_list;
	bool result;

	/* further change replicate */
	if(list_length(oidlist) == 1 && linitial_oid(oidlist) == InvalidOid)
		return true;

	execute_list = ReduceInfoListGetExecuteOidList(reduce_info_list);
	result = true;
	foreach(lc, execute_list)
	{
		if(list_member_oid(oidlist, lfirst_oid(lc)) == false)
		{
			result = false;
			break;
		}
	}
	list_free(execute_list);
	return result;
}

bool IsReduceInfoListExecuteSubsetReduceInfo(List *reduce_info_list, const ReduceInfo *rinfo)
{
	ListCell *lc;
	List *execute_list;
	bool result;

	/* further change replicate */
	if (IsReduceInfoFinalReplicated(rinfo))
		return true;

	execute_list = ReduceInfoListGetExecuteOidList(reduce_info_list);
	result = true;
	foreach(lc, execute_list)
	{
		if (list_member_oid(rinfo->exclude_exec, lfirst_oid(lc)) == true ||
			list_member_oid(rinfo->storage_nodes, lfirst_oid(lc)) == false)
		{
			result = false;
			break;
		}
	}
	list_free(execute_list);
	return result;
}

List *ReduceInfoListGetExecuteOidList(const List *list)
{
	List *exclude_list;
	List *storage_list;
	List *execute_list;
	Assert(list != NIL);

	ReduceInfoListGetStorageAndExcludeOidList(list, &storage_list, &exclude_list);
	execute_list = list_difference_oid(storage_list, exclude_list);
	list_free(storage_list);
	list_free(exclude_list);

	return execute_list;
}

void ReduceInfoListGetStorageAndExcludeOidList(const List *list, List **storage, List **exclude)
{
	ReduceInfo *rinfo;
	ListCell *lc;
	List *storage_list = NIL;
	List *exclude_list = NIL;

	Assert(list && (storage || exclude));
	foreach(lc, list)
	{
		rinfo = lfirst(lc);
		if(storage)
			storage_list = list_concat_unique_oid(storage_list, rinfo->storage_nodes);
		if(exclude)
			exclude_list = list_concat_unique_oid(exclude_list, rinfo->exclude_exec);
	}

	if(storage)
		*storage = storage_list;
	if(exclude)
		*exclude = exclude_list;
}

List *PathListGetReduceInfoListExecuteOn(List *pathlist)
{
	Path	   *path;
	ReduceInfo *rinfo;
	ListCell   *lc_path;
	ListCell   *lc_reduce;
	ListCell   *lc_oid;
	List	   *reducelist;
	List	   *result = NIL;

	foreach(lc_path, pathlist)
	{
		path = lfirst(lc_path);
		reducelist = get_reduce_info_list(path);
		foreach(lc_reduce, reducelist)
		{
			rinfo = lfirst(lc_reduce);
			foreach(lc_oid, rinfo->storage_nodes)
			{
				Oid oid = lfirst_oid(lc_oid);
				if (list_member_oid(rinfo->exclude_exec, oid) == false &&
					list_member_oid(result, oid) == false)
					result = lappend_oid(result, oid);
			}
		}
	}

	return result;
}

ReduceInfo *CopyReduceInfoExtend(const ReduceInfo *reduce, int mark)
{
	ReduceInfo *rinfo;
	AssertArg(mark && (mark|REDUCE_MARK_ALL) == REDUCE_MARK_ALL);

	rinfo = MakeEmptyReduceInfo(reduce->nkey);

	if(mark & REDUCE_MARK_STORAGE)
		rinfo->storage_nodes = list_copy(reduce->storage_nodes);

	if((mark & REDUCE_MARK_EXCLUDE) && reduce->exclude_exec)
		rinfo->exclude_exec = list_copy(reduce->exclude_exec);

	if(mark & REDUCE_MARK_KEY_INFO)
	{
		uint32 i = reduce->nkey;
		while (i>0)
		{
			--i;
			if (mark & REDUCE_MARK_KEY_EXPR)
				rinfo->keys[i].key = copyObject(reduce->keys[i].key);
			if (mark & REDUCE_MARK_OPCLASS)
				rinfo->keys[i].opclass = reduce->keys[i].opclass;
			if (mark & REDUCE_MARK_COLLATION)
				rinfo->keys[i].collation = reduce->keys[i].collation;
		}
	}

	if((mark & REDUCE_MARK_RELIDS) && reduce->relids)
		rinfo->relids = bms_copy(reduce->relids);

	if(mark & REDUCE_MARK_TYPE)
		rinfo->type = reduce->type;

	return rinfo;
}

List *ReduceInfoListConcatExtend(List *dest, List *src, int mark)
{
	ListCell *lc;
	foreach(lc, src)
	{
		if (ReduceInfoListMemberExtend(dest, lfirst(lc), mark) == false)
			dest = lappend(dest, CopyReduceInfoExtend(lfirst(lc), mark));
	}
	return dest;
}

bool CompReduceInfo(const ReduceInfo *left, const ReduceInfo *right, int mark)
{
	if(left == right)
		return true;
	if(left == NULL || right == NULL)
		return false;

	if ((mark & REDUCE_MARK_TYPE) &&
		left->type != right->type)
		return false;

	if ((mark & REDUCE_MARK_STORAGE) &&
		equal(left->storage_nodes, right->storage_nodes) == false)
		return false;

	if ((mark & REDUCE_MARK_EXCLUDE) &&
		equal(left->exclude_exec, right->exclude_exec) == false)
		return false;

	if ((mark & (REDUCE_MARK_KEY_INFO|REDUCE_MARK_KEY_NUM)) &&
		left->nkey != right->nkey)
		return false;

	if (mark & REDUCE_MARK_KEY_INFO)
	{
		uint32 i = left->nkey;
		while (i>0)
		{
			--i;
			if ((mark & REDUCE_MARK_KEY_EXPR) &&
				equal(left->keys[i].key, right->keys[i].key) == false)
				return false;
			if ((mark & REDUCE_MARK_OPCLASS) &&
				left->keys[i].opclass != right->keys[i].opclass)
				return false;
			if ((mark & REDUCE_MARK_COLLATION) &&
				left->keys[i].collation != right->keys[i].collation)
				return false;
		}
	}

	if ((mark & REDUCE_MARK_RELIDS) &&
		bms_equal(left->relids, right->relids) == false)
		return false;

	return true;
}

bool CompReduceInfoList(List *left, List *right, int mark)
{
	ListCell *lc_left;
	ListCell *lc_right;

	if(left == right)
		return true;
	if(list_length(left) != list_length(right))
		return false;

	foreach(lc_left, left)
	{
		foreach(lc_right, right)
		{
			if(CompReduceInfo(lfirst(lc_left), lfirst(lc_right), mark))
				break;
		}
		if(lc_right == NULL)
			return false;
	}

	return true;
}

bool ReduceInfoListMember(List *reduce_info_list, ReduceInfo *reduce_info)
{
	return ReduceInfoListMemberExtend(reduce_info_list, reduce_info, REDUCE_MARK_ALL);
}

bool ReduceInfoListMemberExtend(const List *reduce_info_list, const ReduceInfo *reduce_info, int mark)
{
	ListCell *lc;
	AssertArg(reduce_info);
	foreach(lc, reduce_info_list)
	{
		if(CompReduceInfo(lfirst(lc), reduce_info, mark))
			return true;
	}
	return false;

}

List* GetPathListReduceInfoList(List *pathlist)
{
	List *result;
	List *reduce_list;
	ListCell *lc;
	ListCell *lc2;

	result = NIL;
	foreach(lc, pathlist)
	{
		reduce_list = get_reduce_info_list(lfirst(lc));
		foreach(lc2, reduce_list)
		{
			if(ReduceInfoListMember(result, lfirst(lc2)) == false)
				result = lappend(result, lfirst(lc2));
		}
	}
	return result;
}

int ReduceInfoIncludeExpr(ReduceInfo *reduce, Expr *expr)
{
	uint32 i,nkey;
	Assert(reduce && expr);
	for(i=0,nkey=reduce->nkey;i<nkey;++i)
	{
		if (equal(reduce->keys[i].key, expr))
			return (int)i;
	}
	return -1;
}

bool ReduceInfoListIncludeExpr(List *reduceList, Expr *expr)
{
	ListCell *lc;
	foreach(lc, reduceList)
	{
		if(ReduceInfoIncludeExpr(lfirst(lc), expr) >= 0)
			return true;
	}
	return false;
}

/*
 * return found expr index(from 1) list
 */
List* ReduceInfoFindPathTarget(const ReduceInfo* reduce, const PathTarget *target)
{
	const ListCell *lc_target;
	List *result = NIL;
	const Expr *key;
	uint32 n,nkey;
	AssertArg(target && reduce);
	AssertArg(IsReduceInfoByValue(reduce));

	for(n=0,nkey=reduce->nkey;n<nkey;++n)
	{
		int i = 1;
		key = reduce->keys[n].key;
		foreach(lc_target, target->exprs)
		{
			if(equal(lfirst(lc_target), key))
			{
				result = lappend_int(result, i);
				break;
			}
			++i;
		}
		if(lc_target == NULL)
		{
			list_free(result);
			return NIL;
		}
	}

	return result;
}

List* ReduceInfoFindTargetList(const ReduceInfo* reduce, const List *targetlist, bool skip_junk)
{
	const ListCell *lc_target;
	TargetEntry *te;
	List *result = NIL;
	const Expr *key;
	uint32 i,nkey;
	AssertArg(targetlist && reduce);
	AssertArg(IsReduceInfoByValue(reduce));

	for(i=0,nkey=reduce->nkey;i<nkey;++i)
	{
		key = reduce->keys[i].key;
		foreach(lc_target, targetlist)
		{
			te = lfirst(lc_target);
			if (skip_junk && te->resjunk)
				continue;
			if(equal(te->expr, key))
			{
				result = lappend_int(result, te->resno);
				break;
			}
		}
		if(lc_target == NULL)
		{
			list_free(result);
			return NIL;
		}
	}

	return result;
}

List* MakeVarList(const List *attnos, Index relid, const PathTarget *target)
{
	const Expr *expr;
	Var *var;
	ListCell *lc;
	List *result = NIL;
	foreach(lc, attnos)
	{
		expr = list_nth(target->exprs, lfirst_int(lc)-1);
		var = makeVar(relid,
					  lfirst_int(lc),
					  exprType((Node*)expr),
					  exprTypmod((Node*)expr),
					  exprCollation((Node*)expr),
					  0);
		result = lappend(result, var);
	}
	return result;
}

List* ExtractExprList(const List *attnos, List *exprs)
{
	ListCell   *lc;
	List	   *result = NIL;
	Assert(attnos == NIL || IsA(attnos, IntList));

	foreach(lc, attnos)
	{
		Expr *expr = list_nth(exprs, lfirst_int(lc)-1);
		result = lappend(result, expr);
	}

	return result;
}

bool IsGroupingReduceExpr(PathTarget *target, ReduceInfo *info)
{
	Bitmapset *grouping;
	ListCell *lc;
	Index i;
	int nth;
	bool result;
	AssertArg(target && info);

	if(target->sortgrouprefs == NULL)
		return false;
	if(IsReduceInfoCoordinator(info))
		return true;
	if(IsReduceInfoByValue(info) == false)
		return false;

	i=0;
	grouping = NULL;
	foreach(lc, target->exprs)
	{
		if (target->sortgrouprefs[i])
		{
			Expr *expr = lfirst(lc);
			while(IsA(expr, RelabelType))
				expr = ((RelabelType *) expr)->arg;
			nth = ReduceInfoIncludeExpr(info, expr);
			if(nth >= 0)
				grouping = bms_add_member(grouping, nth);
		}
		++i;
	}

	if(info->nkey == bms_num_members(grouping))
		result = true;
	else
		result = false;
	bms_free(grouping);

	return result;
}

bool IsReduceInfoListCanInnerJoin(List *outer_reduce_list,
							List *inner_reduce_list,
							List *restrictlist)
{
	ListCell *outer_lc,*inner_lc;
	ReduceInfo *outer_reduce;
	ReduceInfo *inner_reduce;

	foreach(outer_lc, outer_reduce_list)
	{
		outer_reduce = lfirst(outer_lc);
		AssertArg(outer_reduce);
		if(IsReduceInfoReplicated(outer_reduce))
			return IsReduceInfoListExecuteSubsetReduceInfo(inner_reduce_list, outer_reduce);

		foreach(inner_lc, inner_reduce_list)
		{
			inner_reduce = lfirst(inner_lc);
			AssertArg(inner_reduce);
			if (IsReduceInfoReplicated(inner_reduce) ||
				(IsReduceInfoCoordinator(outer_reduce) && IsReduceInfoCoordinator(inner_reduce)))
				return true;

			if (!IsReduceInfoCoordinator(outer_reduce) &&
				/* !IsReduceInfoCoordinator(inner_reduce) && // don't need this line */
				IsReduceInfoSame(outer_reduce, inner_reduce) &&
				IsReduceInfoInOneNode(outer_reduce) &&
				CompReduceInfo(outer_reduce, inner_reduce, REDUCE_MARK_EXCLUDE))
				return true;
			if (IsReduceInfoSame(outer_reduce, inner_reduce) &&
				IsReduceInfoCanInnerJoin(outer_reduce, inner_reduce, restrictlist))
				return true;
		}
	}

	return false;
}

bool IsReduceInfoListCanUniqueJoin(List *reduce_list, List *restrictlist)
{
	ListCell	   *lc;
	RestrictInfo   *ri;

	if (IsReduceInfoListInOneNode(reduce_list))
		return true;
	if (IsReduceInfoListByValue(reduce_list) == false)
		return false;

	foreach (lc, restrictlist)
	{
		ri = lfirst_node(RestrictInfo, lc);

		/* only support X=X expression */
		if (!is_opclause(ri->clause) ||
			!op_is_equivalence(((OpExpr *)(ri->clause))->opno))
			continue;

		if (ReduceInfoListIncludeExpr(reduce_list, (Expr*)get_leftop(ri->clause)) ||
			ReduceInfoListIncludeExpr(reduce_list, (Expr*)get_rightop(ri->clause)))
			return true;
	}

	return false;
}

bool IsReduceInfoCanInnerJoin(ReduceInfo *outer_rinfo, ReduceInfo *inner_rinfo, List *restrictlist)
{
	Expr *left_expr;
	Expr *right_expr;
	Expr *left_param;
	Expr *right_param;
	OpExpr *opexpr;
	ListCell *lc;
	uint32 i,nkey;
	bool found;

	AssertArg(outer_rinfo && inner_rinfo);

	if (outer_rinfo->nkey != inner_rinfo->nkey )
		return false;

	if (IsReduceInfoCoordinator(outer_rinfo) &&
		IsReduceInfoCoordinator(inner_rinfo))
		return true;
	if (IsReduceInfoSame(outer_rinfo, inner_rinfo) == false ||
		!IsReduceInfoByValue(outer_rinfo) ||
		!IsReduceInfoByValue(inner_rinfo))
		return false;

	for (i=0,nkey=outer_rinfo->nkey;i<nkey;++i)
	{
		left_param = outer_rinfo->keys[i].key;
		right_param = inner_rinfo->keys[i].key;

		found = false;
		foreach(lc, restrictlist)
		{
			opexpr = (OpExpr*)lfirst_node(RestrictInfo, lc)->clause;

			/* only support X=X expression */
			if (!is_opclause(opexpr) ||
				!op_is_equivalence(opexpr->opno))
				continue;

			left_expr = (Expr*)linitial(opexpr->args);
			right_expr = (Expr*)lsecond(opexpr->args);

			if ((EqualReduceExpr(left_expr, left_param) &&
					EqualReduceExpr(right_expr, right_param))
				|| (EqualReduceExpr(left_expr, right_param) &&
					EqualReduceExpr(right_expr, left_param)))
			{
				if (nkey == 1)
				{
					/* fast return */
					return true;
				}else
				{
					found = true;
					break;
				}
			}
		}

		if (found == false)
			return false;
	}

	return true;
}

bool
IsReduceInfoListCanLeftOrRightJoin(List *outer_reduce_list,
									  List *inner_reduce_list,
									  List *restrictlist)
{
	List *outer_nodes = NIL;
	List *inner_nodes = NIL;
	List *intersection_nodes = NIL;
	bool  res = true;
	ListCell *lc;
	ReduceInfo *reduce_info;

	foreach (lc, outer_reduce_list)
	{
		List *exec_list;
		reduce_info = (ReduceInfo *) lfirst(lc);
		Assert(reduce_info);
		/* do not support left/right join if outer is replicatable */
		if (IsReduceInfoReplicated(reduce_info))
		{
			list_free(outer_nodes);
			return false;
		}
		exec_list = list_difference_oid(reduce_info->storage_nodes, reduce_info->exclude_exec);
		if (outer_nodes == NIL)
		{
			outer_nodes = exec_list;
		}else
		{
			outer_nodes = list_intersection_oid(outer_nodes, exec_list);
			list_free(exec_list);
		}
	}

	foreach (lc, inner_reduce_list)
	{
		List *exec_list;
		reduce_info = (ReduceInfo *) lfirst(lc);
		Assert(reduce_info);
		/* do not support left/right join if inner is replicatable */
		if (IsReduceInfoReplicated(reduce_info))
		{
			list_free(outer_nodes);
			list_free(inner_nodes);
			return false;
		}
		exec_list = list_difference_oid(reduce_info->storage_nodes, reduce_info->exclude_exec);
		if (inner_nodes == NIL)
		{
			inner_nodes = exec_list;
		}else
		{
			inner_nodes = list_intersection_oid(inner_nodes, exec_list);
			list_free(exec_list);
		}
	}

	intersection_nodes = list_intersection_oid(outer_nodes, inner_nodes);
	if (list_length(intersection_nodes) == 1)
		res = false;

	list_free(outer_nodes);
	list_free(inner_nodes);
	list_free(intersection_nodes);

	return res;
}

List *FindJoinEqualExprs(ReduceInfo *rinfo, List *restrictlist, RelOptInfo *inner_rel)
{
	ListCell *lc_restrict;
	List *result;
	RestrictInfo *ri;
	Expr *left_expr;
	Expr *right_expr;
	Bitmapset *bms_found;
	int nth;
	if(restrictlist == NIL)
		return NIL;

	AssertArg(IsReduceInfoByValue(rinfo));

	result = NIL;
	bms_found = NULL;
	foreach(lc_restrict, restrictlist)
	{
		ri = lfirst(lc_restrict);

		/* only support X=X expression */
		if (!is_opclause(ri->clause) ||
			expression_have_subplan(ri->clause) ||
			!op_is_equivalence(((OpExpr *)(ri->clause))->opno))
			continue;

		left_expr = (Expr*)get_leftop(ri->clause);
		right_expr = (Expr*)get_rightop(ri->clause);

		while(IsA(left_expr, RelabelType))
			left_expr = ((RelabelType *) left_expr)->arg;
		while(IsA(right_expr, RelabelType))
			right_expr = ((RelabelType *) right_expr)->arg;

		if((nth = ReduceInfoIncludeExpr(rinfo, left_expr)) >= 0)
		{
			if(bms_is_member(nth, bms_found))
				continue;
		}else if((nth = ReduceInfoIncludeExpr(rinfo, right_expr)) >= 0)
		{
			if(bms_is_member(nth, bms_found))
			{
				continue;
			}else
			{
				Expr *tmp = left_expr;
				left_expr = right_expr;
				right_expr = tmp;
			}
		}
		if(nth >= 0)
		{
			result = lappend(result, right_expr);
			bms_found = bms_add_member(bms_found, nth);
			if(bms_num_members(bms_found) == rinfo->nkey)
			{
				/* all found */
				bms_free(bms_found);
				return result;
			}
		}
	}

	bms_free(bms_found);
	list_free(result);
	return NIL;
}

/*
 * when can join return new ReduceInfo list,
 * else return NIL
 */
bool reduce_info_list_can_join(List *outer_reduce_list,
							   List *inner_reduce_list,
							   List *restrictlist,
							   JoinType jointype,
							   List **new_reduce_list)
{
	if(IsReduceInfoListCoordinator(outer_reduce_list))
	{
		/* coordinator always can join coordinator */
		if (IsReduceInfoListCoordinator(inner_reduce_list))
		{
			if (new_reduce_list)
				*new_reduce_list = list_make1(MakeCoordinatorReduceInfo());
			return true;
		}else
		{
			return false;
		}
	}else if(IsReduceInfoListReplicated(outer_reduce_list))
	{
		/* replicate can not join coordinator */
		if(IsReduceInfoListCoordinator(inner_reduce_list))
			return false;
		if(IsReduceInfoListReplicated(inner_reduce_list))
		{
			if(CompReduceInfo(linitial(outer_reduce_list),
							  linitial(inner_reduce_list),
							  REDUCE_MARK_STORAGE) == true)
			{
				/* replicate alaways can join replicate if the storage equal */
				if (new_reduce_list)
					*new_reduce_list = list_make1(CopyReduceInfo(linitial(outer_reduce_list)));
				return true;
			}else
			{
				/* replicate storage not equal, for now can not join */
				return false;
			}
		}
	}else if(IsReduceInfoListRandom(outer_reduce_list))
	{
		/* random can not join coordinator */
		if(IsReduceInfoListCoordinator(inner_reduce_list))
			return false;
	}

	switch(jointype)
	{
	case JOIN_UNIQUE_OUTER:
	case JOIN_UNIQUE_INNER:
		if (IsReduceInfoListCanUniqueJoin(jointype == JOIN_UNIQUE_OUTER ? outer_reduce_list : inner_reduce_list,
										  restrictlist) == false)
		{
			return false;
		}
		/* do not add break, need run in JOIN_INNER case */
	case JOIN_INNER:
		if(IsReduceInfoListCanInnerJoin(outer_reduce_list, inner_reduce_list, restrictlist))
		{
			if (new_reduce_list)
			{
				*new_reduce_list = NIL;
				if(!IsReduceInfoListReplicated(outer_reduce_list))
					*new_reduce_list = CopyReduceInfoList(outer_reduce_list);
				if(!IsReduceInfoListReplicated(inner_reduce_list))
					*new_reduce_list = ReduceInfoListConcat(*new_reduce_list, inner_reduce_list);
			}
			return true;
		}
		break;
	case JOIN_LEFT:
		if(IsReduceInfoListReplicated(inner_reduce_list))
		{
			ReduceInfo *rinfo = linitial(inner_reduce_list);
			Assert(!IsReduceInfoListCoordinator(outer_reduce_list));
			if (IsReduceInfoListExecuteSubset(outer_reduce_list, rinfo->storage_nodes))
			{
				if (new_reduce_list)
					*new_reduce_list = CopyReduceInfoList(outer_reduce_list);
				return true;
			}
		}
		/* TODO run on node */
		break;
	case JOIN_FULL:
		break;
	case JOIN_RIGHT:
		if(IsReduceInfoListReplicated(outer_reduce_list))
		{
			ReduceInfo *rinfo = linitial(outer_reduce_list);
			Assert(!IsReduceInfoListCoordinator(inner_reduce_list));
			if(IsReduceInfoListExecuteSubset(inner_reduce_list, rinfo->storage_nodes))
			{
				if (new_reduce_list)
					*new_reduce_list = CopyReduceInfoList(inner_reduce_list);
				return true;
			}
		}
		break;
	case JOIN_SEMI:
		if(IsReduceInfoListReplicated(inner_reduce_list))
		{
			ReduceInfo *rinfo = linitial(inner_reduce_list);
			if (IsReduceInfoListExecuteSubset(outer_reduce_list, rinfo->storage_nodes))
			{
				if (new_reduce_list)
					*new_reduce_list = CopyReduceInfoList(outer_reduce_list);
				return true;
			}
		}else if (!IsReduceInfoListReplicated(outer_reduce_list) &&
			IsReduceInfoListCanInnerJoin(outer_reduce_list, inner_reduce_list, restrictlist))
		{
			if (new_reduce_list)
				*new_reduce_list = CopyReduceInfoList(outer_reduce_list);
			return true;
		}
		break;
	case JOIN_ANTI:
		if (IsReduceInfoListReplicated(inner_reduce_list))
		{
			if (new_reduce_list)
				*new_reduce_list = CopyReduceInfoList(outer_reduce_list);
			return true;
		}
		break;
	}

	return reduce_info_list_is_one_node_can_join(outer_reduce_list,
												 inner_reduce_list,
												 new_reduce_list,
												 jointype,
												 NULL);
}

bool reduce_info_list_is_one_node_can_join(List *outer_reduce_list,
										   List *inner_reduce_list,
										   List **new_reduce_list,
										   JoinType jointype,
										   bool *is_dummy)
{
	ListCell   *olc;
	ListCell   *ilc;
	ReduceInfo *orinfo;
	ReduceInfo *irinfo;
	bool		can_join = false;

	/* test outer and inner is in one node */
	if (new_reduce_list)
		*new_reduce_list = NIL;
	foreach(olc, outer_reduce_list)
	{
		orinfo = lfirst(olc);
		if (!IsReduceInfoInOneNode(orinfo))
			continue;

		foreach(ilc, inner_reduce_list)
		{
			irinfo = lfirst(ilc);
			if (IsReduceInfoInOneNode(irinfo) &&
				reduce_info_in_one_node_can_join(orinfo, irinfo, new_reduce_list, jointype, is_dummy))
				can_join = true;
		}
	}
	return can_join;
}

static bool reduce_info_in_one_node_can_join(ReduceInfo *outer_info,
											 ReduceInfo *inner_info,
											 List **new_reduce_list,
											 JoinType jointype,
											 bool *is_dummy)
{
	ListCell *lc;
	Oid outer_at_oid = InvalidOid;
	Oid inner_at_oid = InvalidOid;

	Assert(IsReduceInfoInOneNode(outer_info) &&
		   IsReduceInfoInOneNode(inner_info));

	foreach(lc, outer_info->storage_nodes)
	{
		if (list_member_oid(outer_info->exclude_exec, lfirst_oid(lc)) == false)
		{
			outer_at_oid = lfirst_oid(lc);
			break;
		}
	}
	foreach(lc, inner_info->storage_nodes)
	{
		if (list_member_oid(inner_info->exclude_exec, lfirst_oid(lc)) == false)
		{
			inner_at_oid = lfirst_oid(lc);
			break;
		}
	}

	Assert(OidIsValid(outer_at_oid) && OidIsValid(inner_at_oid));
	if (outer_at_oid == inner_at_oid ||
		CompReduceInfo(outer_info, inner_info, REDUCE_MARK_STORAGE|REDUCE_MARK_TYPE))
	{
		/* in same node */
		if (is_dummy)
			*is_dummy = (outer_at_oid != inner_at_oid);

		if (new_reduce_list)
		{
			switch (jointype)
			{
			case JOIN_INNER:
			case JOIN_UNIQUE_OUTER:
			case JOIN_UNIQUE_INNER:
				if (IsReduceInfoByValue(outer_info))
				{
					*new_reduce_list = lappend(*new_reduce_list, outer_info);
					if (IsReduceInfoByValue(inner_info))
						*new_reduce_list = lappend(*new_reduce_list, inner_info);
				}else if(IsReduceInfoByValue(inner_info))
				{
					*new_reduce_list = lappend(*new_reduce_list, inner_info);
					if (IsReduceInfoByValue(outer_info))
						*new_reduce_list = lappend(*new_reduce_list, outer_info);
				}else
				{
					List *storage = list_make1_oid(outer_at_oid);
					if (outer_at_oid != inner_at_oid)
						storage = lappend_oid(storage, inner_at_oid);
					*new_reduce_list = lappend(*new_reduce_list, MakeRandomReduceInfo(storage));
					list_free(storage);
				}
				break;
			case JOIN_LEFT:
			case JOIN_SEMI:
			case JOIN_ANTI:
				*new_reduce_list = lappend(*new_reduce_list, outer_info);
				break;
			case JOIN_FULL:
				{
					List *storage = list_make1_oid(outer_at_oid);
					if (outer_at_oid != inner_at_oid)
						storage = lappend_oid(storage, inner_at_oid);
					*new_reduce_list = lappend(*new_reduce_list, MakeRandomReduceInfo(storage));
					list_free(storage);
				}
				break;
			case JOIN_RIGHT:
				*new_reduce_list = lappend(*new_reduce_list, inner_info);
				break;
			}
		}
		return true;
	}

	return false;
}

bool CanOnceGroupingClusterPath(PathTarget *target, Path *path)
{
	List *list;
	ListCell *lc;
	ReduceInfo *info;
	bool result = false;

	list = get_reduce_info_list(path);
	foreach(lc, list)
	{
		info = lfirst(lc);
		if (IsReduceInfoCoordinator(info)  ||
			IsReduceInfoReplicated(info) ||
			IsReduceInfoInOneNode(info)  ||
			IsGroupingReduceExpr(target, info))
		{
			result = true;
			break;
		}
	}

	return result;
}

bool CanOnceDistinctReduceInfoList(List *distinct, List *reduce_list)
{
	ListCell *lc_reduce;
	foreach(lc_reduce, reduce_list)
	{
		if(CanOnceDistinctReduceInfo(distinct, lfirst(lc_reduce)))
			return true;
	}
	return false;
}

bool CanOnceDistinctReduceInfo(List *distinct, ReduceInfo *reduce_info)
{
	ListCell   *lc;
	Expr	   *key;
	Expr	   *expr;
	uint32		i,nkey;

	if(IsReduceInfoByValue(reduce_info) == false)
		return false;

	Assert(reduce_info->nkey > 0);
	for(i=0,nkey=reduce_info->nkey;i<nkey;++i)
	{
		key = reduce_info->keys[i].key;
		foreach(lc, distinct)
		{
			expr = lfirst(lc);
			while(IsA(expr, RelabelType))
				expr = ((RelabelType *) expr)->arg;
			if(equal(key, expr))
				break;
		}
		if(lc == NULL)
			return false;
	}

	return true;
}

bool CanOnceWindowAggClusterPath(WindowClause *wclause, List *tlist, Path *path)
{
	List *reduce_list;
	ListCell *lc;
	Expr *expr;

	reduce_list = get_reduce_info_list(path);
	Assert(reduce_list != NIL);
	if (IsReduceInfoListCoordinator(reduce_list) ||
		IsReduceInfoListInOneNode(reduce_list))
		return true;
	if (IsReduceInfoListByValue(reduce_list) == false ||
		wclause->partitionClause == NIL)
		return false;

	foreach(lc, wclause->partitionClause)
	{
		expr = (Expr*)get_sortgroupclause_expr(lfirst(lc), tlist);
		while(IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;
		if(ReduceInfoListIncludeExpr(reduce_list, expr))
			return true;
	}

	return false;
}

bool HaveOnceWindowAggClusterPath(List *wclauses, List *tlist, Path *path)
{
	ListCell *lc;
	foreach(lc, wclauses)
	{
		if(CanOnceWindowAggClusterPath(lfirst(lc), tlist, path))
			return true;
	}

	return false;
}

Var *makeVarByRel(AttrNumber attno, Oid rel_oid, Index rel_index)
{
	Oid typid;
	Oid collid;
	int32 typmod;
	AssertArg(OidIsValid(rel_oid) && rel_index > 0);
	get_atttypetypmodcoll(rel_oid, attno, &typid, &typmod, &collid);
	return makeVar(rel_index, attno, typid, typmod, collid, 0);
}

Expr *CreateExprUsingReduceInfo(ReduceInfo *reduce)
{
	Expr *result;
	AssertArg(reduce && list_length(reduce->storage_nodes) > 0);

	if (reduce->type != REDUCE_TYPE_HASHMAP &&
		list_member_oid(reduce->storage_nodes, InvalidOid))
	{
		ereport(ERROR,
				(errmsg("reduce info has an invalid OID")));
	}

	switch(reduce->type)
	{
	case REDUCE_TYPE_HASH:
		if (reduce->nkey < 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid argument for create reduce expr")));
		}else
		{
			List *args;
			uint32 i,nkey;

			/* make hash_combin_mod(node_count, hash_value1, hash_value2 ...) */
			args = list_make1(MakeInt4Const(list_length(reduce->storage_nodes)));
			for(i=0,nkey=reduce->nkey;i<nkey;++i)
				args = lappend(args, makeHashExprFamily(reduce->keys[0].key, reduce->keys[0].opfamily));
			result = (Expr*)makeSimpleFuncExpr(F_HASH_COMBIN_MOD, INT4OID, args);

			result = makeReduceArrayRef(reduce->storage_nodes, result, bms_is_empty(reduce->relids), true);
		}
		break;

	case REDUCE_TYPE_HASHMAP:
		if (reduce->nkey != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("not support multi-col for hash yet")));
		result = makeHashExprFamily(reduce->keys[0].key, reduce->keys[0].opfamily);

		result = (Expr*) makeSimpleFuncExpr(F_HASH_COMBIN_MOD,
											INT4OID,
											list_make2(MakeInt4Const(HASHMAP_SLOTSIZE), result));
		result = (Expr*) makeSimpleFuncExpr(F_NODEID_FROM_HASHVALUE,
											INT4OID,
											list_make1(result));
		break;

	case REDUCE_TYPE_MODULO:
		if (reduce->nkey != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("not support multi-col for modulo")));
		}else
		{
			result = (Expr*)coerce_to_target_type(NULL,
												  (Node*)reduce->keys[0].key,
												  exprType((Node*)reduce->keys[0].key),
												  INT4OID,
												  -1,
												  COERCION_EXPLICIT,
												  COERCE_IMPLICIT_CAST,
												  -1);
			result = (Expr*)makeSimpleFuncExpr(F_HASH_COMBIN_MOD,
											   INT4OID,
											   list_make2(MakeInt4Const(list_length(reduce->storage_nodes)), result));
			result = makeReduceArrayRef(reduce->storage_nodes, result, bms_is_empty(reduce->relids), true);
		}
		break;
	case REDUCE_TYPE_REPLICATED:
		{
			oidvector *vector;
			Const *c;
			FuncExpr *func;
			if(reduce->exclude_exec != NIL)
			{
				List *list_exec = list_difference_oid(reduce->storage_nodes, reduce->exclude_exec);
				vector = makeOidVector(list_exec);
				list_free(list_exec);
			}else
			{
				vector = makeOidVector(reduce->storage_nodes);
			}
			c = makeConst(OIDARRAYOID,
						  -1,
						  InvalidOid,
						  -1,
						  PointerGetDatum(vector),
						  false,
						  false);
			func = makeSimpleFuncExpr(F_ARRAY_UNNEST,
									  OIDOID,
									  list_make1(c));
			/* unnest(array) return set */
			func->funcretset = true;
			result = (Expr*)func;
		}
		break;
	case REDUCE_TYPE_RANDOM:
		{
			List *store;
			if (reduce->exclude_exec != NIL)
				store = list_difference_oid(reduce->storage_nodes, reduce->exclude_exec);
			else
				store = reduce->storage_nodes;
			/* result = random(list_length(store)) */
			result = (Expr*)makeSimpleFuncExpr(F_INT4RANDOM_MAX,
											   INT4OID,
											   list_make1(MakeInt4Const(list_length(store))));
			/* result = store[result] */
			result = makeReduceArrayRef(store, result, false, false);

			/* clean resource */
			if (store != reduce->storage_nodes)
				list_free(store);
		}
		break;
	case REDUCE_TYPE_COORDINATOR:
		Assert(IsCnMaster());
		result = (Expr*) makeOidConst(PGXCNodeOid);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce type %d", reduce->type)));
		result = NULL;	/* keep quiet */
		break;
	}

	return result;
}

Expr *CreateReduceModuloExpr(Relation rel, const RelationLocInfo *loc, Index relid)
{
	Expr *result;

	if (loc->locatorType == LOCATOR_TYPE_HASH ||
		loc->locatorType == LOCATOR_TYPE_MODULO)
	{
		Form_pg_attribute attr;
		List *args;
		Var *var;

		/* make hash_combin_mod(node_count, hash(column)) */
		attr = TupleDescAttr(RelationGetDescr(rel), loc->partAttrNum-1);
		args = list_make1(MakeInt4Const(list_length(loc->nodeids)));
		var = makeVar(relid, loc->partAttrNum, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
		if (loc->locatorType == LOCATOR_TYPE_HASH)
		{
			result = makeHashExpr((Expr*)var);
		}else
		{
			result = (Expr*)coerce_to_target_type(NULL,
												  (Node*)var,
												  var->vartype,
												  INT4OID,
												  -1,
												  COERCION_EXPLICIT,
												  COERCE_IMPLICIT_CAST,
												  -1);
		}
		args = lappend(args, result);
		result = (Expr*)makeSimpleFuncExpr(F_HASH_COMBIN_MOD,
										   INT4OID,
										   args);
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown locator type %d", loc->locatorType)));
		return NULL;	/* never run, keep compiler quiet */
	}
	return result;
}

static Expr* CreateNodeOidOperatorOid(Oid opno, Expr *expr)
{
	OpExpr *op = makeNode(OpExpr);
	op->args = list_make2(makeFuncExpr(F_ADB_NODE_OID,
									   OIDOID,
									   NIL,
									   InvalidOid,
									   InvalidOid,
									   COERCE_EXPLICIT_CALL),
						  expr);
	op->opno = opno;
	Assert(OidIsValid(opno));
	op->opfuncid = get_opcode(opno);
	op->opresulttype = get_op_rettype(opno);
	op->opretset = false;
	op->opcollid = op->inputcollid = InvalidOid;
	op->location = -1;
	return (Expr*)op;
}

/* make (adb_node_oid() = nodeoid) expr */
Expr *CreateNodeOidEqualOid(Oid nodeoid)
{
	return CreateNodeOidOperatorOid(OidEqualOperator, (Expr*)makeOidConst(nodeoid));
}

/* make (adb_node_oid() = expr) expr */
Expr *CreateNodeOidEqualExpr(Expr *expr)
{
	return CreateNodeOidOperatorOid(OidEqualOperator, expr);
}

/* make (adb_node_oid() != nodeoid) expr */
Expr *CreateNodeOidNotEqualOid(Oid nodeoid)
{
	return CreateNodeOidOperatorOid(OidNotEqualOperator, (Expr*)makeOidConst(nodeoid));
}

bool EqualReduceExpr(Expr *left, Expr *right)
{
	Expr *r;
	if(left == right)
		return true;
	if(left == NULL || right == NULL)
		return false;

	for(;;)
	{
		r = right;
		for(;;)
		{
			if(left == r)
				return true;
			if(left == NULL || r == NULL)
				return false;
			if(equal(left, r))
				return true;
			if(IsA(r, RelabelType))
			{
				r = ((RelabelType*)r)->arg;
				continue;
			}else
			{
				break;
			}
		}
		if(IsA(left, RelabelType))
		{
			left = ((RelabelType*)left)->arg;
			continue;
		}else
		{
			break;
		}
	}

	return false;
}

static oidvector *makeOidVector(List *list)
{
	oidvector *oids;
	ListCell *lc;
	Size i;
	int length;
	int size;

	Assert(list != NIL && IsA(list, OidList));

	length = list_length(list);
	size = offsetof(oidvector, values) + (length) * sizeof(Oid);

	oids = palloc0(size);
	oids->ndim = 1;
	oids->dataoffset = 0;
	oids->elemtype = OIDOID;
	oids->dim1 = length;
	oids->lbound1 = 0;
	i = 0;
	foreach(lc, list)
	{
		oids->values[i] = lfirst_oid(lc);
		++i;
	}
	SET_VARSIZE(oids, size);

	return oids;
}

/*
 * oid_list[modulo] expr
 */
static Expr* makeReduceArrayRef(List *oid_list, Expr *modulo, bool try_const, bool use_coalesce)
{
	ArrayRef *aref;
	if(try_const)
	{
		Node *node = eval_const_expressions(NULL, (Node*)modulo);
		if (IsA(node, Const))
		{
			Const *c = (Const*)node;
			Oid node_oid;
			if (c->constisnull)
			{
				/* when is null reduce to first node */
				node_oid = linitial_oid(oid_list);
			}else
			{
				int32 n;
				Assert(c->consttype == INT4OID);
				n = DatumGetInt32(c->constvalue);
				Assert(n>=0 && n<list_length(oid_list));
				node_oid = list_nth_oid(oid_list, n);
				Assert(OidIsValid(node_oid));
			}
			return (Expr*)makeConst(OIDOID,
									-1,
									InvalidOid,
									sizeof(Oid),
									ObjectIdGetDatum(node_oid),
									false,
									true);
		}
	}

	if (use_coalesce)
	{
		CoalesceExpr *coalesce;
		coalesce = makeNode(CoalesceExpr);
		coalesce->coalescetype = INT4OID;
		coalesce->coalescecollid = InvalidOid;
		coalesce->args = list_make2(modulo,
									makeConst(INT4OID,
											-1,
											InvalidOid,
											sizeof(int32),
											Int32GetDatum(0), /* when null, reduce to first node */
											false,
											true)
									);
		coalesce->location = -1;
		modulo = (Expr*)coalesce;
	}

	/* when "modulo" return NULL, then return 0 */
	Assert(exprType((Node*)modulo) == INT4OID);

	aref = makeNode(ArrayRef);
	aref->refarraytype = OIDARRAYOID;
	aref->refelemtype = OIDOID;
	aref->reftypmod = -1;
	aref->refcollid = InvalidOid;
	aref->refupperindexpr = list_make1(modulo);
	aref->reflowerindexpr = NIL;
	aref->refexpr = (Expr*)makeConst(OIDARRAYOID,
									 -1,
									 InvalidOid,
									 -1,
									 PointerGetDatum(makeOidVector(oid_list)),
									 false,
									 false);
	aref->refassgnexpr = NULL;

	return (Expr*)aref;
}

static int CompareOid(const void *a, const void *b)
{
	Oid			oa = *((const Oid *) a);
	Oid			ob = *((const Oid *) b);

	if (oa == ob)
		return 0;
	return (oa > ob) ? 1 : -1;
}

bool CanModuloType(Oid type, bool no_error)
{
	Oid target = INT4OID;
	return can_coerce_type(1, &type, &target, COERCION_EXPLICIT);
}

typedef struct ReduceSetExprState
{
	uint32	current;
	uint32	count;
	Oid		oids[FLEXIBLE_ARRAY_MEMBER];
}ReduceSetExprState;

static Datum ExecReduceExpr(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond *isDone)
{
	Datum datum = ExecEvalExpr(state, econtext, isnull);
	*isDone = ExprSingleResult;
	return datum;
}

static Datum ExecReduceSetExpr(ReduceSetExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond *isDone)
{
	Oid oid;

re_start_:
	if (state->current < state->count)
	{
		oid = state->oids[state->current];
		++(state->current);
		*isnull = false;
		*isDone = ExprMultipleResult;
	}else if(state->current == state->count)
	{
		++(state->current);
		oid = InvalidOid;
		*isnull = true;
		*isDone = ExprEndResult;
	}else if(state->current > state->count)
	{
		/* reloop */
		state->current = 0;
		oid = InvalidOid;
		goto re_start_;
	}

	return ObjectIdGetDatum(oid);
}

ReduceExprState* ExecInitReduceExpr(Expr *expr)
{
	ReduceExprState *result;
	if (IsA(expr, FuncExpr) &&
		((FuncExpr*)expr)->funcid == F_ARRAY_UNNEST)
	{
		ReduceSetExprState *set;
		oidvector *ov;
		Const *c = linitial(castNode(FuncExpr, expr)->args);

		if (list_length(castNode(FuncExpr, expr)->args) != 1 ||
			(c = linitial(castNode(FuncExpr, expr)->args)) == NULL ||
			!IsA(c, Const) ||
			((Const*)c)->constisnull ||
			((Const*)c)->consttype != OIDARRAYOID ||
			(ov = (oidvector*)DatumGetPointer(c->constvalue))->elemtype != OIDOID ||
			ov->dim1 < 0 ||
			ov->lbound1 != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Invalid reduce expr")));
		}

		if (ov->dim1 == 1)
		{
			c = makeConst(OIDOID,
						  -1,
						  InvalidOid,
						  sizeof(Oid),
						  ObjectIdGetDatum(ov->values[0]),
						  false,
						  true);
			return ExecInitReduceExpr((Expr*)c);
		}

		set = palloc0(offsetof(ReduceSetExprState, oids)+sizeof(Oid)*ov->dim1);
		/* set->current = 0; don't need */
		set->count = ov->dim1;
		memcpy(set->oids, ov->values, sizeof(Oid)*ov->dim1);

		result = palloc(sizeof(ReduceExprState));
		result->state = set;
		result->evalfunc = (Datum(*)(void*, ExprContext*, bool*,ExprDoneCond*))ExecReduceSetExpr;
	}else
	{
		result = palloc(sizeof(ReduceExprState));
		result->state = ExecInitExpr(expr, NULL);
		result->evalfunc = (Datum(*)(void*, ExprContext*, bool*,ExprDoneCond*))ExecReduceExpr;
	}

	return result;
}
