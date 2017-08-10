#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "parser/parse_coerce.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "optimizer/reduceinfo.h"

#define MakeEmptyReduceInfo() palloc0(sizeof(ReduceInfo))
static bool GetRelidsWalker(Var *var, Relids *relids);
static Param *makeReduceParam(Oid type, int paramid, int parammod, Oid collid);
static oidvector *makeOidVector(List *list);
static ArrayRef* makeReduceArrayRef(List *oid_list, Expr *modulo);
static Node* ReduceParam2ExprMutator(Node *node, List *params);
static int CompareOid(const void *a, const void *b);

ReduceInfo *MakeHashReduceInfo(const List *storage, const List *exclude, const Expr *param)
{
	ReduceInfo *rinfo;
	TypeCacheEntry *typeCache;
	Oid typoid;
	AssertArg(storage && IsA(storage, OidList) && param);
	AssertArg(exclude == NIL || IsA(exclude, OidList));

	typoid = exprType((Node*)param);
	typeCache = lookup_type_cache(typoid, TYPECACHE_HASH_PROC);
	if(!OidIsValid(typeCache->hash_proc))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a hash function for type %s",
						format_type_be(typoid))));
	}

	rinfo = MakeEmptyReduceInfo();
	rinfo->storage_nodes = list_copy(storage);
	rinfo->exclude_exec = list_copy(exclude);
	rinfo->params = list_make1(copyObject(param));
	GetRelidsWalker((Var*)param, &rinfo->relids);
	rinfo->type = REDUCE_TYPE_HASH;

	return rinfo;
}

ReduceInfo *MakeCustomReduceInfoByRel(const List *storage, const List *exclude,
						const List *attnums, Oid funcid, Oid reloid, Index rel_index)
{
	ListCell *lc;
	List *params = NIL;

	foreach(lc, attnums)
		params = lappend(params, makeVarByRel(lfirst_int(lc), reloid, rel_index));

	return MakeCustomReduceInfo(storage, exclude, params, funcid, reloid);
}

ReduceInfo *MakeCustomReduceInfo(const List *storage, const List *exclude, List *params, Oid funcid, Oid reloid)
{
	ReduceInfo *rinfo;
	List *args;
	ListCell *lc;
	Oid *argTypes;
	int narg;
	int i;

	get_func_signature(funcid, &argTypes, &narg);
	if(narg < list_length(params))
	{
		Relation rel = RelationIdGetRelation(reloid);
		ereport(ERROR,
					  (errmsg("too many argument for user hash distrbute table \"%s\"",
					   RelationGetRelationName(rel))));
	}

	i=0;
	args = NIL;
	rinfo = MakeEmptyReduceInfo();
	rinfo->params = params;
	foreach(lc, params)
	{
		Expr *expr = lfirst(lc);
		Oid typid = exprType((Node*)expr);
		expr = (Expr*)makeReduceParam(typid,
									  i+1,
									  exprTypmod((Node*)expr),
									  exprCollation((Node*)expr));

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
	{
		rinfo->params = lappend(rinfo->params,
								makeNullConst(argTypes[i], -1, InvalidOid));
		args = lappend(args, makeReduceParam(argTypes[i], i+1, -1, InvalidOid));
	}

	rinfo->expr = (Expr*) makeFuncExpr(funcid,
								get_func_rettype(funcid),
								args,
								InvalidOid, InvalidOid,
								COERCE_EXPLICIT_CALL);

	rinfo->storage_nodes = list_copy(storage);
	rinfo->exclude_exec = list_copy(exclude);
	GetRelidsWalker((Var*)(rinfo->params), &rinfo->relids);
	rinfo->type = REDUCE_TYPE_CUSTOM;

	return rinfo;
}

ReduceInfo *MakeModuloReduceInfo(const List *storage, const List *exclude, const Expr *param)
{
	ReduceInfo *rinfo;
	AssertArg(storage != NIL && IsA(storage, OidList) && param);
	AssertArg(exclude == NIL || IsA(exclude, OidList));

	rinfo = MakeEmptyReduceInfo();
	rinfo->storage_nodes = list_copy(storage);
	rinfo->exclude_exec = list_copy(exclude);
	rinfo->params = list_make1(copyObject(param));
	GetRelidsWalker((Var*)(rinfo->params), &rinfo->relids);
	rinfo->type = REDUCE_TYPE_MODULO;

	return rinfo;
}

ReduceInfo *MakeReplicateReduceInfo(const List *storage)
{
	ReduceInfo *rinfo;
	AssertArg(storage != NIL && IsA(storage, OidList));

	rinfo = MakeEmptyReduceInfo();
	rinfo->storage_nodes = SortOidList(list_copy(storage));
	rinfo->type = REDUCE_TYPE_REPLICATED;

	return rinfo;
}

ReduceInfo *MakeRoundReduceInfo(const List *storage)
{
	ReduceInfo *rinfo;
	AssertArg(storage != NIL && IsA(storage, OidList));

	rinfo = MakeEmptyReduceInfo();
	rinfo->storage_nodes = SortOidList(list_copy(storage));
	rinfo->type = REDUCE_TYPE_ROUND;

	return rinfo;
}

ReduceInfo *MakeCoordinatorReduceInfo(void)
{
	ReduceInfo *rinfo;

	rinfo = MakeEmptyReduceInfo();
	rinfo->storage_nodes = list_make1_oid(PGXCNodeOid);
	rinfo->type = REDUCE_TYPE_COORDINATOR;

	return rinfo;
}

ReduceInfo *MakeReduceInfoAs(const ReduceInfo *reduce, List *params)
{
	ReduceInfo *rinfo = CopyReduceInfoExtend(reduce, REDUCE_MARK_ALL & ~REDUCE_MARK_PARAMS);
	rinfo->params = params;
	return rinfo;
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
bool IsReduceInfoListRound(List *list)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	foreach(lc, list)
	{
		rinfo = lfirst(lc);
		if(IsReduceInfoRound(rinfo))
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

ReduceInfo *CopyReduceInfoExtend(const ReduceInfo *reduce, int mark)
{
	ReduceInfo *rinfo;
	AssertArg(mark && (mark|REDUCE_MARK_ALL) == REDUCE_MARK_ALL);

	rinfo = MakeEmptyReduceInfo();

	if(mark & REDUCE_MARK_STORAGE)
		rinfo->storage_nodes = list_copy(reduce->storage_nodes);

	if((mark & REDUCE_MARK_EXCLUDE) && reduce->exclude_exec)
		rinfo->exclude_exec = list_copy(reduce->exclude_exec);

	if(mark & REDUCE_MARK_PARAMS)
		rinfo->params = copyObject(reduce->params);

	if((mark & REDUCE_MARK_EXPR) && reduce->expr)
		rinfo->expr = copyObject(reduce->expr);

	if((mark & REDUCE_MARK_RELIDS) && reduce->relids)
		rinfo->relids = bms_copy(reduce->relids);

	if(mark & REDUCE_MARK_TYPE)
		rinfo->type = reduce->type;

	return rinfo;
}

bool CompReduceInfo(const ReduceInfo *left, const ReduceInfo *right, int mark)
{
	if(left == right)
		return true;
	if(left == NULL || right == NULL)
		return false;

	if ((mark & REDUCE_MARK_STORAGE) &&
		equal(left->storage_nodes, right->storage_nodes) == false)
		return false;

	if ((mark & REDUCE_MARK_EXCLUDE) &&
		equal(left->exclude_exec, right->exclude_exec) == false)
		return false;

	if ((mark & REDUCE_MARK_PARAMS) &&
		equal(left->params, right->params) == false)
		return false;

	if ((mark & REDUCE_MARK_EXPR) &&
		equal(left->expr, right->expr) == false)
		return false;

	if ((mark & REDUCE_MARK_RELIDS) &&
		bms_equal(left->relids, right->relids) == false)
		return false;

	if ((mark & REDUCE_MARK_TYPE) &&
		left->type != right->type)
		return false;

	return true;
}

bool IsReduceInfoStorageEqual(const ReduceInfo *left, const ReduceInfo *right)
{
	AssertArg(left->type == right->type);
	AssertArg(left->storage_nodes != NIL && right->storage_nodes != NIL);
	AssertArg(IsA(left->storage_nodes, OidList) && IsA(right->storage_nodes, OidList));

	if(IsReduceInfoByValue(left))
	{
		return equal(left->storage_nodes, right->storage_nodes);
	}else if(list_length(left->storage_nodes) != list_length(right->storage_nodes))
	{
		return false;
	}else if(left->storage_nodes == right->storage_nodes)
	{
		return true;
	}else
	{
		ListCell *lc;
		foreach(lc, right->storage_nodes)
		{
			if(list_member_oid(left->storage_nodes, lfirst_oid(lc)) == false)
				return false;
		}
	}
	return true;
}

int ReduceInfoIncludeExpr(ReduceInfo *reduce, Expr *expr)
{
	ListCell *lc;
	int i = 0;
	Assert(reduce);
	foreach(lc, reduce->params)
	{
		if(equal(lfirst(lc), (Node*)expr))
			return i;
		++i;
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
List* ReduceInfoFindTarget(ReduceInfo* reduce, PathTarget *target)
{
	ListCell *lc_param;
	ListCell *lc_target;
	List *result = NIL;
	AssertArg(target && reduce);
	AssertArg(IsReduceInfoByValue(reduce));

	foreach(lc_param, reduce->params)
	{
		int i = 1;
		foreach(lc_target, target->exprs)
		{
			if(equal(lfirst(lc_target), lfirst(lc_param)))
			{
				result = lappend_int(result, i);
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

extern List* MakeVarList(List *attnos, Index relid, PathTarget *target)
{
	Expr *expr;
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

	if(list_length(info->params) == bms_num_members(grouping))
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
			return true;

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

bool IsReduceInfoCanInnerJoin(ReduceInfo *outer_rinfo, ReduceInfo *inner_rinfo, List *restrictlist)
{
	Expr *left_expr;
	Expr *right_expr;
	Expr *left_param;
	Expr *right_param;
	RestrictInfo *ri;
	ListCell *lc;

	AssertArg(outer_rinfo && inner_rinfo);

	/* for now support only one distribute cloumn */
	if (list_length(outer_rinfo->params) != 1 ||
		list_length(inner_rinfo->params) != 1)
		return false;

	if (IsReduceInfoCoordinator(outer_rinfo) &&
		IsReduceInfoCoordinator(inner_rinfo))
		return true;
	if (IsReduceInfoSame(outer_rinfo, inner_rinfo) == false ||
		!IsReduceInfoByValue(outer_rinfo) ||
		!IsReduceInfoByValue(inner_rinfo))
		return false;

	Assert(list_length(outer_rinfo->params) == 1);
	Assert(list_length(outer_rinfo->params) == list_length(inner_rinfo->params));
	left_param = linitial(outer_rinfo->params);
	right_param = linitial(inner_rinfo->params);

	foreach(lc, restrictlist)
	{
		ri = lfirst(lc);

		/* only support X=X expression */
		if (!is_opclause(ri->clause) ||
			!op_is_equivalence(((OpExpr *)(ri->clause))->opno))
			continue;

		left_expr = (Expr*)get_leftop(ri->clause);
		right_expr = (Expr*)get_rightop(ri->clause);

		while(IsA(left_expr, RelabelType))
			left_expr = ((RelabelType *) left_expr)->arg;
		while(IsA(right_expr, RelabelType))
			right_expr = ((RelabelType *) right_expr)->arg;

		if ((equal(left_expr, left_param) &&
				equal(right_expr, right_param))
			|| (equal(left_expr, right_param) &&
				equal(right_expr, left_param)))
		{
			return true;
		}
	}

	return false;
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

bool CanMakeSemiAntiClusterJoinPath(PlannerInfo *root, SemiAntiJoinContext *context)
{
	RestrictInfo   *ri;
	ReduceInfo	   *outer_reduce;
	ReduceInfo	   *inner_reduce;
	ListCell	   *ri_lc;
	ListCell	   *outer_lc;
	ListCell	   *inner_lc;
	Expr		   *lexpr;
	Expr		   *rexpr;
	Path		   *outer_path = NULL;
	Path		   *inner_path = NULL;
	List		   *outer_nodes = NIL;
	List		   *inner_nodes = NIL;
	List		   *outer_reduce_list = NIL;
	List		   *inner_reduce_list = NIL;

	AssertArg(context);
	outer_reduce_list = context->outer_reduce_list;
	inner_reduce_list = context->inner_reduce_list;
	outer_path = context->outer_path;
	inner_path = context->inner_path;

	foreach (outer_lc, outer_reduce_list)
	{
		outer_reduce = (ReduceInfo *) lfirst(outer_lc);
		Assert(outer_reduce);
		/* do not support semi/anti join if outer is replicatable */
		if (IsReduceInfoReplicated(outer_reduce))
			return false;
		outer_nodes = list_difference_oid(outer_reduce->storage_nodes, outer_reduce->exclude_exec);

		foreach (inner_lc, inner_reduce_list)
		{
			inner_reduce = (ReduceInfo *) lfirst(inner_lc);
			Assert(inner_reduce);
			if (IsReduceInfoReplicated(inner_reduce))
			{
				List *diff_nodes = NIL;
				inner_nodes = list_difference_oid(inner_reduce->storage_nodes, inner_reduce->exclude_exec);
				diff_nodes = list_difference_oid(outer_nodes, inner_nodes);
				list_free(inner_nodes);
				if (diff_nodes != NIL)
				{
					list_free(diff_nodes);
					return false;
				}
				return true;
			}

			if (IsReduceInfoCoordinator(outer_reduce) &&
				IsReduceInfoCoordinator(inner_reduce))
				return true;

			if (IsReduceInfoSame(outer_reduce, inner_reduce) &&
				IsReduceInfoByValue(outer_reduce))
			{
				Expr *outer_param;
				Expr *inner_param;

				/* for now only support one param only */
				if (list_length(outer_reduce->params) > 1)
					continue;

				outer_param = linitial(outer_reduce->params);
				inner_param = linitial(inner_reduce->params);
				foreach (ri_lc, context->restrict_list)
				{
					ri = lfirst(ri_lc);

					/* only support X=X expression */
					if (!is_opclause(ri->clause) ||
						!op_is_equivalence(((OpExpr *)(ri->clause))->opno))
						continue;

					lexpr = (Expr*)get_leftop(ri->clause);
					rexpr = (Expr*)get_rightop(ri->clause);

					while(IsA(lexpr, RelabelType))
						lexpr = ((RelabelType *) lexpr)->arg;
					while(IsA(rexpr, RelabelType))
						rexpr = ((RelabelType *) rexpr)->arg;

					if ((equal(lexpr, inner_param) && equal(rexpr, outer_param)) ||
						(equal(lexpr, outer_param) && equal(rexpr, inner_param)))
						return true;
				}
			}
		}
	}

	/* reduce to coordinator */
	context->outer_path = create_cluster_reduce_path(root,
													 outer_path,
													 list_make1(MakeCoordinatorReduceInfo()),
													 context->outer_rel,
													 NIL);
	context->inner_path = create_cluster_reduce_path(root,
													 inner_path,
													 list_make1(MakeCoordinatorReduceInfo()),
													 context->inner_rel,
													 NIL);

	return true;
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
			if(bms_num_members(bms_found) == list_length(rinfo->params))
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
	ListCell *lc_distinct;
	ListCell *lc_param;

	if(IsReduceInfoByValue(reduce_info) == false)
		false;

	Assert(reduce_info->params);
	foreach(lc_param, reduce_info->params)
	{
		foreach(lc_distinct, distinct)
		{
			Expr *expr = lfirst(lc_distinct);
			while(IsA(expr, RelabelType))
				expr = ((RelabelType *) expr)->arg;
			if(equal(lfirst(lc_param), expr))
				break;
		}
		if(lc_distinct == NULL)
			return false;
	}

	return true;
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

	switch(reduce->type)
	{
	case REDUCE_TYPE_HASH:
		Assert(list_length(reduce->params) == 1);
		result = makeHashExpr(linitial(reduce->params));
		result = makeModuloExpr(result, list_length(reduce->storage_nodes));
		Assert(exprType((Node*)result) == INT4OID);
		result = (Expr*) makeFuncExpr(F_INT4ABS,
									  INT4OID,
									  list_make1(result),
									  InvalidOid, InvalidOid,
									  COERCE_EXPLICIT_CALL);
		result = (Expr*) makeReduceArrayRef(reduce->storage_nodes, result);
		break;
	case REDUCE_TYPE_CUSTOM:
		Assert(list_length(reduce->params) > 0 && reduce->expr != NULL);
		result = (Expr*)ReduceParam2ExprMutator((Node*)reduce->expr, reduce->params);
		result = (Expr*)makeModuloExpr(result, list_length(reduce->storage_nodes));
		result = (Expr*)coerce_to_target_type(NULL, (Node*)result,
								exprType((Node*)result),
								INT4OID,
								-1,
								COERCION_EXPLICIT,
								COERCE_IMPLICIT_CAST,
								-1);
		result = (Expr*) makeFuncExpr(F_INT4ABS,
									  INT4OID,
									  list_make1(result),
									  InvalidOid, InvalidOid,
									  COERCE_EXPLICIT_CALL);
		result = (Expr*) makeReduceArrayRef(reduce->storage_nodes, result);
		break;
	case REDUCE_TYPE_MODULO:
		Assert(list_length(reduce->params) == 1);
		result = makeModuloExpr(linitial(reduce->params), list_length(reduce->storage_nodes));
		result = (Expr*)coerce_to_target_type(NULL, (Node*)result,
								exprType((Node*)result),
								INT4OID,
								-1,
								COERCION_EXPLICIT,
								COERCE_IMPLICIT_CAST,
								-1);
		result = (Expr*) makeFuncExpr(F_INT4ABS,
									  INT4OID,
									  list_make1(result),
									  InvalidOid, InvalidOid,
									  COERCE_EXPLICIT_CALL);
		result = (Expr*) makeReduceArrayRef(reduce->storage_nodes, result);
		break;
	case REDUCE_TYPE_REPLICATED:
	case REDUCE_TYPE_ROUND:
		{
			oidvector *vector;
			OidVectorLoopExpr *ovl = makeNode(OidVectorLoopExpr);
			if(reduce->exclude_exec != NIL)
			{
				List *list_exec = list_difference_oid(reduce->storage_nodes, reduce->exclude_exec);
				vector = makeOidVector(list_exec);
				list_free(list_exec);
			}else
			{
				vector = makeOidVector(reduce->storage_nodes);
			}
			ovl->signalRowMode = (reduce->type == REDUCE_TYPE_ROUND ? true:false);
			ovl->vector = PointerGetDatum(vector);
			result = (Expr*)ovl;
		}
		break;
	case REDUCE_TYPE_COORDINATOR:
		Assert(IS_PGXC_COORDINATOR && !IsConnFromCoord());
		result = (Expr*)makeConst(OIDOID,
								  -1,
								  InvalidOid,
								  sizeof(Oid),
								  ObjectIdGetDatum(PGXCNodeOid),
								  false,
								  true);
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

static bool GetRelidsWalker(Var *var, Relids *relids)
{
	if(var == NULL)
		return false;
	if(IsA(var, Var))
	{
		*relids = bms_add_member(*relids, var->varno);
		return false;
	}
	return expression_tree_walker((Node*)var, GetRelidsWalker, relids);
}

static Param *makeReduceParam(Oid type, int paramid, int parammod, Oid collid)
{
	Param *param = palloc(sizeof(Param));
	param->location = -1;
	param->paramid = paramid;
	param->paramtype = type;
	param->paramcollid = collid;
	param->paramtypmod = parammod;
	param->paramkind = PARAM_EXTERN;
	return param;
}

static oidvector *makeOidVector(List *list)
{
	oidvector *oids;
	ListCell *lc;
	Size i;

	Assert(list != NIL && IsA(list, OidList));
	oids = palloc0(offsetof(oidvector, values) + list_length(list) * sizeof(Oid));
	oids->ndim = 1;
	oids->dataoffset = 0;
	oids->elemtype = OIDOID;
	oids->dim1 = list_length(list);
	oids->lbound1 = 0;
	i = 0;
	foreach(lc, list)
	{
		oids->values[i] = lfirst_oid(lc);
		++i;
	}
	SET_VARSIZE(oids, sizeof(Oid)*list_length(list));

	return oids;
}

/*
 * oid_list[modulo] expr
 */
static ArrayRef* makeReduceArrayRef(List *oid_list, Expr *modulo)
{
	oidvector *vector = makeOidVector(oid_list);
	ArrayRef *aref = makeNode(ArrayRef);
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
									 PointerGetDatum(vector),
									 false,
									 false);
	aref->refassgnexpr = NULL;

	return aref;
}

static Node* ReduceParam2ExprMutator(Node *node, List *params)
{
	if(node == NULL)
		return NULL;
	if(IsA(node, Param))
	{
		Param *param = (Param*)node;
		Node *new_node;
		Assert(param->paramkind == PARAM_EXTERN);
		Assert(param->paramid <= list_length(params));
		new_node = list_nth(params, param->paramid-1);
		Assert(param->paramtype == exprType(new_node));
		Assert(param->paramtypmod == exprTypmod(new_node));
		Assert(param->paramcollid == exprCollation(new_node));
		return new_node;
	}
	return expression_tree_mutator(node, ReduceParam2ExprMutator, params);
}

static int CompareOid(const void *a, const void *b)
{
	Oid			oa = *((const Oid *) a);
	Oid			ob = *((const Oid *) b);

	if (oa == ob)
		return 0;
	return (oa > ob) ? 1 : -1;
}
