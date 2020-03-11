/*-------------------------------------------------------------------------
 *
 * joinpath.c
 *	  Routines to find all possible paths for processing a set of joins
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/joinpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"

#ifdef ADB
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/reduceinfo.h"
#include "pgxc/pgxc.h"
#include "utils/lsyscache.h"
#endif /* ADB */

/* Hook for plugins to get control in add_paths_to_joinrel() */
set_join_pathlist_hook_type set_join_pathlist_hook = NULL;

/*
 * Paths parameterized by the parent can be considered to be parameterized by
 * any of its child.
 */
#define PATH_PARAM_BY_PARENT(path, rel)	\
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path),	\
									   (rel)->top_parent_relids))
#define PATH_PARAM_BY_REL_SELF(path, rel)  \
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path), (rel)->relids))

#define PATH_PARAM_BY_REL(path, rel)	\
	(PATH_PARAM_BY_REL_SELF(path, rel) || PATH_PARAM_BY_PARENT(path, rel))

static void try_partial_mergejoin_path(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   Path *outer_path,
						   Path *inner_path,
						   List *pathkeys,
						   List *mergeclauses,
						   List *outersortkeys,
						   List *innersortkeys,
						   JoinType jointype,
						   JoinPathExtraData *extra);
static void sort_inner_and_outer(PlannerInfo *root, RelOptInfo *joinrel,
					 RelOptInfo *outerrel, RelOptInfo *innerrel,
					 JoinType jointype, JoinPathExtraData *extra);
static void match_unsorted_outer(PlannerInfo *root, RelOptInfo *joinrel,
					 RelOptInfo *outerrel, RelOptInfo *innerrel,
					 JoinType jointype, JoinPathExtraData *extra);
static void consider_parallel_nestloop(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra);
static void consider_parallel_mergejoin(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra,
							Path *inner_cheapest_total);
static void hash_inner_and_outer(PlannerInfo *root, RelOptInfo *joinrel,
					 RelOptInfo *outerrel, RelOptInfo *innerrel,
					 JoinType jointype, JoinPathExtraData *extra);
static List *select_mergejoin_clauses(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *outerrel,
						 RelOptInfo *innerrel,
						 List *restrictlist,
						 JoinType jointype,
						 bool *mergejoin_allowed);
static void generate_mergejoin_paths(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *innerrel,
						 Path *outerpath,
						 JoinType jointype,
						 JoinPathExtraData *extra,
						 bool useallclauses,
						 Path *inner_cheapest_total,
						 List *merge_pathkeys,
						 bool is_partial);
#ifdef ADB

#define CLUSTER_TRY_NESTLOOP_JOIN	1
#define CLUSTER_TRY_MERGE_JOIN		2
#define CLUSTER_TRY_HASH_JOIN		4

typedef struct ClusterJoinContext
{
	PlannerInfo		   *root;
	RelOptInfo		   *joinrel;
	RelOptInfo		   *outerrel;
	RelOptInfo		   *innerrel;
	JoinPathExtraData  *extra;

	List			   *hashclauses;

	List			   *merge_pathkeys;
	List			   *merge_clauses;
	List			   *inner_pathlist;

	JoinType			jointype;
	int					try_match;	/* CLUSTER_TRY_XXX_JOIN */
}ClusterJoinContext;

#define PATH_PARAM_OUTER_REL(path, rel)	\
	((path)->param_info && !bms_is_subset((path)->param_info->ppi_req_outer, (rel)->relids))

extern bool enable_coordinator_calculate;	/* GUC in guc.c */

static Path* get_cheapest_join_path(ClusterJoinContext *jcontext,
									Path *outer_path,
									CostSelector criterion,
									bool must_no_param,
									List **new_reduce_list);
static void set_all_join_inner_path(ClusterJoinContext *jcontext, Path *outerpath, List *inner_pathlist);
static void sort_cluster_inner_and_outer(ClusterJoinContext *jcontext, Path *outerpath, Path *innerpath,
										 List *all_pathkeys, List *reduce_list);
static void add_cluster_paths_to_joinrel(PlannerInfo *root,
										 RelOptInfo *joinrel,
										 RelOptInfo *outerrel,
										 RelOptInfo *innerrel,
										 JoinType jointype,
										 JoinPathExtraData *extra);
static bool add_cluster_paths_to_joinrel_internal(ClusterJoinContext *jcontext,
												  List *outer_pathlist,
												  List *inner_pathlist,
												  bool nestjoinOK,
												  bool first_try);
static void try_cluster_join_path(ClusterJoinContext *jcontext,
								  Path *outer_path,
								  Path *inner_path,
								  List *new_reduce_list);
static List *create_outer_reduce_info_for_join(List *inner_reduce_list, RelOptInfo *outerrel,
											   JoinType jointype, JoinPathExtraData *extra);
static List *create_inner_reduce_info_for_join(List *outer_reduce_list, RelOptInfo *innerrel,
											   JoinType jointype, JoinPathExtraData *extra);
static List *create_and_append_replicate_reduceinfo(List *list, const ReduceInfo *rinfo);
static List *reduce_paths_for_join(PlannerInfo *root, RelOptInfo *rel, List *pathlist, List *reduce_list, bool parallel);
static List *coord_paths_for_join(PlannerInfo *root, RelOptInfo *rel, bool parallel);
static bool get_cluster_join_exprs(RelOptInfo *outerrel, RelOptInfo *innerrel,
								   List **outer_exprs, List **inner_exprs,
								   List *restrictlist);
#endif /* ADB */

/*
 * add_paths_to_joinrel
 *	  Given a join relation and two component rels from which it can be made,
 *	  consider all possible paths that use the two component rels as outer
 *	  and inner rel respectively.  Add these paths to the join rel's pathlist
 *	  if they survive comparison with other paths (and remove any existing
 *	  paths that are dominated by these paths).
 *
 * Modifies the pathlist field of the joinrel node to contain the best
 * paths found so far.
 *
 * jointype is not necessarily the same as sjinfo->jointype; it might be
 * "flipped around" if we are considering joining the rels in the opposite
 * direction from what's indicated in sjinfo.
 *
 * Also, this routine and others in this module accept the special JoinTypes
 * JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER to indicate that we should
 * unique-ify the outer or inner relation and then apply a regular inner
 * join.  These values are not allowed to propagate outside this module,
 * however.  Path cost estimation code may need to recognize that it's
 * dealing with such a case --- the combination of nominal jointype INNER
 * with sjinfo->jointype == JOIN_SEMI indicates that.
 */
void
add_paths_to_joinrel(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 SpecialJoinInfo *sjinfo,
					 List *restrictlist)
{
	JoinPathExtraData extra;
	bool		mergejoin_allowed = true;
	ListCell   *lc;
	Relids		joinrelids;

	/*
	 * PlannerInfo doesn't contain the SpecialJoinInfos created for joins
	 * between child relations, even if there is a SpecialJoinInfo node for
	 * the join between the topmost parents. So, while calculating Relids set
	 * representing the restriction, consider relids of topmost parent of
	 * partitions.
	 */
	if (joinrel->reloptkind == RELOPT_OTHER_JOINREL)
		joinrelids = joinrel->top_parent_relids;
	else
		joinrelids = joinrel->relids;

	extra.restrictlist = restrictlist;
	extra.mergeclause_list = NIL;
	extra.sjinfo = sjinfo;
	extra.param_source_rels = NULL;

	/*
	 * See if the inner relation is provably unique for this outer rel.
	 *
	 * We have some special cases: for JOIN_SEMI and JOIN_ANTI, it doesn't
	 * matter since the executor can make the equivalent optimization anyway;
	 * we need not expend planner cycles on proofs.  For JOIN_UNIQUE_INNER, we
	 * must be considering a semijoin whose inner side is not provably unique
	 * (else reduce_unique_semijoins would've simplified it), so there's no
	 * point in calling innerrel_is_unique.  However, if the LHS covers all of
	 * the semijoin's min_lefthand, then it's appropriate to set inner_unique
	 * because the path produced by create_unique_path will be unique relative
	 * to the LHS.  (If we have an LHS that's only part of the min_lefthand,
	 * that is *not* true.)  For JOIN_UNIQUE_OUTER, pass JOIN_INNER to avoid
	 * letting that value escape this module.
	 */
	switch (jointype)
	{
		case JOIN_SEMI:
		case JOIN_ANTI:
			extra.inner_unique = false; /* well, unproven */
			break;
		case JOIN_UNIQUE_INNER:
			extra.inner_unique = bms_is_subset(sjinfo->min_lefthand,
											   outerrel->relids);
			break;
		case JOIN_UNIQUE_OUTER:
			extra.inner_unique = innerrel_is_unique(root,
													joinrel->relids,
													outerrel->relids,
													innerrel,
													JOIN_INNER,
													restrictlist,
													false);
			break;
		default:
			extra.inner_unique = innerrel_is_unique(root,
													joinrel->relids,
													outerrel->relids,
													innerrel,
													jointype,
													restrictlist,
													false);
			break;
	}

	/*
	 * Find potential mergejoin clauses.  We can skip this if we are not
	 * interested in doing a mergejoin.  However, mergejoin may be our only
	 * way of implementing a full outer join, so override enable_mergejoin if
	 * it's a full join.
	 */
	if (enable_mergejoin || jointype == JOIN_FULL)
		extra.mergeclause_list = select_mergejoin_clauses(root,
														  joinrel,
														  outerrel,
														  innerrel,
														  restrictlist,
														  jointype,
														  &mergejoin_allowed);

	/*
	 * If it's SEMI, ANTI, or inner_unique join, compute correction factors
	 * for cost estimation.  These will be the same for all paths.
	 */
	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique)
		compute_semi_anti_join_factors(root, joinrel, outerrel, innerrel,
									   jointype, sjinfo, restrictlist,
									   &extra.semifactors);

	/*
	 * Decide whether it's sensible to generate parameterized paths for this
	 * joinrel, and if so, which relations such paths should require.  There
	 * is usually no need to create a parameterized result path unless there
	 * is a join order restriction that prevents joining one of our input rels
	 * directly to the parameter source rel instead of joining to the other
	 * input rel.  (But see allow_star_schema_join().)	This restriction
	 * reduces the number of parameterized paths we have to deal with at
	 * higher join levels, without compromising the quality of the resulting
	 * plan.  We express the restriction as a Relids set that must overlap the
	 * parameterization of any proposed join path.
	 */
	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo2 = (SpecialJoinInfo *) lfirst(lc);

		/*
		 * SJ is relevant to this join if we have some part of its RHS
		 * (possibly not all of it), and haven't yet joined to its LHS.  (This
		 * test is pretty simplistic, but should be sufficient considering the
		 * join has already been proven legal.)  If the SJ is relevant, it
		 * presents constraints for joining to anything not in its RHS.
		 */
		if (bms_overlap(joinrelids, sjinfo2->min_righthand) &&
			!bms_overlap(joinrelids, sjinfo2->min_lefthand))
			extra.param_source_rels = bms_join(extra.param_source_rels,
											   bms_difference(root->all_baserels,
															  sjinfo2->min_righthand));

		/* full joins constrain both sides symmetrically */
		if (sjinfo2->jointype == JOIN_FULL &&
			bms_overlap(joinrelids, sjinfo2->min_lefthand) &&
			!bms_overlap(joinrelids, sjinfo2->min_righthand))
			extra.param_source_rels = bms_join(extra.param_source_rels,
											   bms_difference(root->all_baserels,
															  sjinfo2->min_lefthand));
	}

	/*
	 * However, when a LATERAL subquery is involved, there will simply not be
	 * any paths for the joinrel that aren't parameterized by whatever the
	 * subquery is parameterized by, unless its parameterization is resolved
	 * within the joinrel.  So we might as well allow additional dependencies
	 * on whatever residual lateral dependencies the joinrel will have.
	 */
	extra.param_source_rels = bms_add_members(extra.param_source_rels,
											  joinrel->lateral_relids);

	/*
	 * 1. Consider mergejoin paths where both relations must be explicitly
	 * sorted.  Skip this if we can't mergejoin.
	 */
	if (mergejoin_allowed)
		sort_inner_and_outer(root, joinrel, outerrel, innerrel,
							 jointype, &extra);

	/*
	 * 2. Consider paths where the outer relation need not be explicitly
	 * sorted. This includes both nestloops and mergejoins where the outer
	 * path is already ordered.  Again, skip this if we can't mergejoin.
	 * (That's okay because we know that nestloop can't handle right/full
	 * joins at all, so it wouldn't work in the prohibited cases either.)
	 */
	if (mergejoin_allowed)
		match_unsorted_outer(root, joinrel, outerrel, innerrel,
							 jointype, &extra);
#ifdef NOT_USED

	/*
	 * 3. Consider paths where the inner relation need not be explicitly
	 * sorted.  This includes mergejoins only (nestloops were already built in
	 * match_unsorted_outer).
	 *
	 * Diked out as redundant 2/13/2000 -- tgl.  There isn't any really
	 * significant difference between the inner and outer side of a mergejoin,
	 * so match_unsorted_inner creates no paths that aren't equivalent to
	 * those made by match_unsorted_outer when add_paths_to_joinrel() is
	 * invoked with the two rels given in the other order.
	 */
	if (mergejoin_allowed)
		match_unsorted_inner(root, joinrel, outerrel, innerrel,
							 jointype, &extra);
#endif

	/*
	 * 4. Consider paths where both outer and inner relations must be hashed
	 * before being joined.  As above, disregard enable_hashjoin for full
	 * joins, because there may be no other alternative.
	 */
	if (enable_hashjoin || jointype == JOIN_FULL)
		hash_inner_and_outer(root, joinrel, outerrel, innerrel,
							 jointype, &extra);

#ifdef ADB
	add_cluster_paths_to_joinrel(root, joinrel, outerrel, innerrel, jointype, &extra);
#endif /* ADB */

	/*
	 * 5. If inner and outer relations are foreign tables (or joins) belonging
	 * to the same server and assigned to the same user to check access
	 * permissions as, give the FDW a chance to push down joins.
	 */
	if (joinrel->fdwroutine &&
		joinrel->fdwroutine->GetForeignJoinPaths)
		joinrel->fdwroutine->GetForeignJoinPaths(root, joinrel,
												 outerrel, innerrel,
												 jointype, &extra);

	/*
	 * 6. Finally, give extensions a chance to manipulate the path list.
	 */
	if (set_join_pathlist_hook)
		set_join_pathlist_hook(root, joinrel, outerrel, innerrel,
							   jointype, &extra);

#ifdef ADB
	/*
	 * If the inner and outer relations have RemoteQuery paths, check if this
	 * JOIN can be pushed to the data-nodes. If so, create a RemoteQuery path
	 * corresponding to the this JOIN.
	 */
	create_joinrel_rqpath(root, joinrel, outerrel, innerrel, restrictlist,
								jointype, sjinfo, extra.param_source_rels);
#endif /* ADB */
}

/*
 * We override the param_source_rels heuristic to accept nestloop paths in
 * which the outer rel satisfies some but not all of the inner path's
 * parameterization.  This is necessary to get good plans for star-schema
 * scenarios, in which a parameterized path for a large table may require
 * parameters from multiple small tables that will not get joined directly to
 * each other.  We can handle that by stacking nestloops that have the small
 * tables on the outside; but this breaks the rule the param_source_rels
 * heuristic is based on, namely that parameters should not be passed down
 * across joins unless there's a join-order-constraint-based reason to do so.
 * So we ignore the param_source_rels restriction when this case applies.
 *
 * allow_star_schema_join() returns true if the param_source_rels restriction
 * should be overridden, ie, it's okay to perform this join.
 */
static inline bool
allow_star_schema_join(PlannerInfo *root,
					   Relids outerrelids,
					   Relids inner_paramrels)
{
	/*
	 * It's a star-schema case if the outer rel provides some but not all of
	 * the inner rel's parameterization.
	 */
	return (bms_overlap(inner_paramrels, outerrelids) &&
			bms_nonempty_difference(inner_paramrels, outerrelids));
}

/*
 * try_nestloop_path
 *	  Consider a nestloop join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void
try_nestloop_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  Path *outer_path,
				  Path *inner_path,
				  List *pathkeys,
				  JoinType jointype,
				  ADB_ONLY_ARG_COMMA(List *reduce_info_list)
				  JoinPathExtraData *extra)
{
	Relids		required_outer;
	JoinCostWorkspace workspace;
	RelOptInfo *innerrel = inner_path->parent;
	RelOptInfo *outerrel = outer_path->parent;
	Relids		innerrelids;
	Relids		outerrelids;
	Relids		inner_paramrels = PATH_REQ_OUTER(inner_path);
	Relids		outer_paramrels = PATH_REQ_OUTER(outer_path);
#ifdef ADB
	Path 		*path;
#endif /* ADB */

	/*
	 * Paths are parameterized by top-level parents, so run parameterization
	 * tests on the parent relids.
	 */
	if (innerrel->top_parent_relids)
		innerrelids = innerrel->top_parent_relids;
	else
		innerrelids = innerrel->relids;

	if (outerrel->top_parent_relids)
		outerrelids = outerrel->top_parent_relids;
	else
		outerrelids = outerrel->relids;

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible --- unless allow_star_schema_join
	 * says to allow it anyway.  Also, we must reject if have_dangerous_phv
	 * doesn't like the look of it, which could only happen if the nestloop is
	 * still parameterized.
	 */
	required_outer = calc_nestloop_required_outer(outerrelids, outer_paramrels,
												  innerrelids, inner_paramrels);
	if (required_outer &&
		((!bms_overlap(required_outer, extra->param_source_rels) &&
		  !allow_star_schema_join(root, outerrelids, inner_paramrels)) ||
		 have_dangerous_phv(root, outerrelids, inner_paramrels)))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/*
	 * Do a precheck to quickly eliminate obviously-inferior paths.  We
	 * calculate a cheap lower bound on the path's cost and then use
	 * add_path_precheck() to see if the path is clearly going to be dominated
	 * by some existing path for the joinrel.  If not, do the full pushup with
	 * creating a fully valid path structure and submitting it to add_path().
	 * The latter two steps are expensive enough to make this two-phase
	 * methodology worthwhile.
	 */
#ifdef ADB
	workspace.is_cluster = (reduce_info_list ? true:false);
#endif /* ADB */
	initial_cost_nestloop(root, &workspace, jointype,
						  outer_path, inner_path, extra);

	if (add_path_precheck(joinrel,
						  workspace.startup_cost, workspace.total_cost,
						  pathkeys, required_outer))
	{
		/*
		 * If the inner path is parameterized, it is parameterized by the
		 * topmost parent of the outer rel, not the outer rel itself.  Fix
		 * that.
		 */
		if (PATH_PARAM_BY_PARENT(inner_path, outer_path->parent))
		{
			inner_path = reparameterize_path_by_child(root, inner_path,
													  outer_path->parent);

			/*
			 * If we could not translate the path, we can't create nest loop
			 * path.
			 */
			if (!inner_path)
			{
				bms_free(required_outer);
				return;
			}
		}

#ifdef ADB
		path = ((Path*)
#else
		add_path(joinrel, (Path *)
#endif /* ADB */
				 create_nestloop_path(root,
									  joinrel,
									  jointype,
									  &workspace,
									  extra,
									  outer_path,
									  inner_path,
									  extra->restrictlist,
									  pathkeys,
									  ADB_ONLY_ARG_COMMA(reduce_info_list)
									  required_outer));
#ifdef ADB
		if(reduce_info_list)
		{
			path->reduce_info_list = reduce_info_list;
			path->reduce_is_valid = true;
			add_cluster_path(joinrel, path);
		}else
		{
			add_path(joinrel, path);
		}
#endif /* ADB */
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_partial_nestloop_path
 *	  Consider a partial nestloop join path; if it appears useful, push it into
 *	  the joinrel's partial_pathlist via add_partial_path().
 */
static void
try_partial_nestloop_path(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  List *pathkeys,
						  JoinType jointype,
						  JoinPathExtraData *extra)
{
	JoinCostWorkspace workspace;

	/*
	 * If the inner path is parameterized, the parameterization must be fully
	 * satisfied by the proposed outer path.  Parameterized partial paths are
	 * not supported.  The caller should already have verified that no
	 * extra_lateral_rels are required here.
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	if (inner_path->param_info != NULL)
	{
		Relids		inner_paramrels = inner_path->param_info->ppi_req_outer;
		RelOptInfo *outerrel = outer_path->parent;
		Relids		outerrelids;

		/*
		 * The inner and outer paths are parameterized, if at all, by the top
		 * level parents, not the child relations, so we must use those relids
		 * for our parameterization tests.
		 */
		if (outerrel->top_parent_relids)
			outerrelids = outerrel->top_parent_relids;
		else
			outerrelids = outerrel->relids;

		if (!bms_is_subset(inner_paramrels, outerrelids))
			return;
	}

	/*
	 * Before creating a path, get a quick lower bound on what it is likely to
	 * cost.  Bail out right away if it looks terrible.
	 */
#ifdef ADB
	workspace.is_cluster = false;
#endif /* ADB */
	initial_cost_nestloop(root, &workspace, jointype,
						  outer_path, inner_path, extra);
	if (!add_partial_path_precheck(joinrel, workspace.total_cost, pathkeys))
		return;

	/*
	 * If the inner path is parameterized, it is parameterized by the topmost
	 * parent of the outer rel, not the outer rel itself.  Fix that.
	 */
	if (PATH_PARAM_BY_PARENT(inner_path, outer_path->parent))
	{
		inner_path = reparameterize_path_by_child(root, inner_path,
												  outer_path->parent);

		/*
		 * If we could not translate the path, we can't create nest loop path.
		 */
		if (!inner_path)
			return;
	}

	/* Might be good enough to be worth trying, so let's try it. */
	add_partial_path(joinrel, (Path *)
					 create_nestloop_path(root,
										  joinrel,
										  jointype,
										  &workspace,
										  extra,
										  outer_path,
										  inner_path,
										  extra->restrictlist,
										  pathkeys,
										  ADB_ONLY_ARG_COMMA(NIL)
										  NULL));
}

#ifdef ADB
static void
try_cluster_partial_nestloop_path(PlannerInfo *root,
								  RelOptInfo *joinrel,
								  Path *outer_path,
								  Path *inner_path,
								  List *pathkeys,
								  JoinType jointype,
								  JoinPathExtraData *extra,
								  List *reduce_info_list)
{
	JoinCostWorkspace workspace;
	NestPath *nestloop;

	/*
	 * If the inner path is parameterized, the parameterization must be fully
	 * satisfied by the proposed outer path.  Parameterized partial paths are
	 * not supported.  The caller should already have verified that no
	 * extra_lateral_rels are required here.
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	AssertArg(reduce_info_list != NIL);
	if (inner_path->param_info != NULL)
	{
		Relids		inner_paramrels = inner_path->param_info->ppi_req_outer;

		if (!bms_is_subset(inner_paramrels, outer_path->parent->relids))
			return;
	}

	/*
	 * Before creating a path, get a quick lower bound on what it is likely to
	 * cost.  Bail out right away if it looks terrible.
	 */
	workspace.is_cluster = true;
	initial_cost_nestloop(root, &workspace, jointype,
						  outer_path, inner_path,
						  extra);
	if (!add_cluster_partial_path_precheck(joinrel, reduce_info_list, workspace.total_cost, pathkeys))
		return;

	/* Might be good enough to be worth trying, so let's try it. */
	nestloop = create_nestloop_path(root,
									joinrel,
									jointype,
									&workspace,
									extra,
									outer_path,
									inner_path,
									extra->restrictlist,
									pathkeys,
									reduce_info_list,
									NULL);
	nestloop->path.reduce_info_list = reduce_info_list;
	nestloop->path.reduce_is_valid = true;
	add_cluster_partial_path(joinrel, (Path*)nestloop);
}
#endif /* ADB */

/*
 * try_mergejoin_path
 *	  Consider a merge join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void
try_mergejoin_path(PlannerInfo *root,
				   RelOptInfo *joinrel,
				   Path *outer_path,
				   Path *inner_path,
				   List *pathkeys,
				   List *mergeclauses,
				   List *outersortkeys,
				   List *innersortkeys,
				   JoinType jointype,
				   ADB_ONLY_ARG_COMMA(List *reduce_info_list)
				   JoinPathExtraData *extra,
				   bool is_partial)
{
	Relids		required_outer;
	JoinCostWorkspace workspace;

	if (is_partial)
	{
		try_partial_mergejoin_path(root,
								   joinrel,
								   outer_path,
								   inner_path,
								   pathkeys,
								   mergeclauses,
								   outersortkeys,
								   innersortkeys,
								   jointype,
								   extra);
		return;
	}

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible.
	 */
	required_outer = calc_non_nestloop_required_outer(outer_path,
													  inner_path);
	if (required_outer &&
		!bms_overlap(required_outer, extra->param_source_rels))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/*
	 * If the given paths are already well enough ordered, we can skip doing
	 * an explicit sort.
	 */
	if (outersortkeys &&
		pathkeys_contained_in(outersortkeys, outer_path->pathkeys))
		outersortkeys = NIL;
	if (innersortkeys &&
		pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
		innersortkeys = NIL;

	/*
	 * See comments in try_nestloop_path().
	 */
#ifdef ADB
	workspace.is_cluster = (reduce_info_list ? true:false);
#endif /* ADB */
	initial_cost_mergejoin(root, &workspace, jointype, mergeclauses,
						   outer_path, inner_path,
						   outersortkeys, innersortkeys,
						   extra);

	if (add_path_precheck(joinrel,
						  workspace.startup_cost, workspace.total_cost,
						  pathkeys, required_outer))
	{
#ifdef ADB
		Path *path = ((Path*)
#else
		add_path(joinrel, (Path *)
#endif /* ADB */
				 create_mergejoin_path(root,
									   joinrel,
									   jointype,
									   &workspace,
									   extra,
									   outer_path,
									   inner_path,
									   extra->restrictlist,
									   pathkeys,
									   required_outer,
									   mergeclauses,
									   ADB_ONLY_ARG_COMMA(reduce_info_list)
									   outersortkeys,
									   innersortkeys));
#ifdef ADB
		if(reduce_info_list)
		{
			path->reduce_info_list = reduce_info_list;
			path->reduce_is_valid = true;
			add_cluster_path(joinrel, path);
		}else
		{
			add_path(joinrel, path);
		}
#endif /* ADB */
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_partial_mergejoin_path
 *	  Consider a partial merge join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_partial_path().
 */
static void
try_partial_mergejoin_path(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   Path *outer_path,
						   Path *inner_path,
						   List *pathkeys,
						   List *mergeclauses,
						   List *outersortkeys,
						   List *innersortkeys,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	JoinCostWorkspace workspace;

	/*
	 * See comments in try_partial_hashjoin_path().
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	if (inner_path->param_info != NULL)
	{
		Relids		inner_paramrels = inner_path->param_info->ppi_req_outer;

		if (!bms_is_empty(inner_paramrels))
			return;
	}

	/*
	 * If the given paths are already well enough ordered, we can skip doing
	 * an explicit sort.
	 */
	if (outersortkeys &&
		pathkeys_contained_in(outersortkeys, outer_path->pathkeys))
		outersortkeys = NIL;
	if (innersortkeys &&
		pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
		innersortkeys = NIL;

	/*
	 * See comments in try_partial_nestloop_path().
	 */
	initial_cost_mergejoin(root, &workspace, jointype, mergeclauses,
						   outer_path, inner_path,
						   outersortkeys, innersortkeys,
						   extra);

	if (!add_partial_path_precheck(joinrel, workspace.total_cost, pathkeys))
		return;

	/* Might be good enough to be worth trying, so let's try it. */
	add_partial_path(joinrel, (Path *)
					 create_mergejoin_path(root,
										   joinrel,
										   jointype,
										   &workspace,
										   extra,
										   outer_path,
										   inner_path,
										   extra->restrictlist,
										   pathkeys,
										   NULL,
										   mergeclauses, ADB_ONLY_ARG_COMMA(NIL)
										   outersortkeys,
										   innersortkeys));
}

/*
 * try_hashjoin_path
 *	  Consider a hash join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void
try_hashjoin_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  Path *outer_path,
				  Path *inner_path,
				  List *hashclauses,
				  JoinType jointype,
				  ADB_ONLY_ARG_COMMA(List *reduce_info_list)
				  JoinPathExtraData *extra)
{
	Relids		required_outer;
	JoinCostWorkspace workspace;

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible.
	 */
	required_outer = calc_non_nestloop_required_outer(outer_path,
													  inner_path);
	if (required_outer &&
		!bms_overlap(required_outer, extra->param_source_rels))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/*
	 * See comments in try_nestloop_path().  Also note that hashjoin paths
	 * never have any output pathkeys, per comments in create_hashjoin_path.
	 */
#ifdef ADB
	workspace.is_cluster = (reduce_info_list ? true:false);
#endif /* ADB */
	initial_cost_hashjoin(root, &workspace, jointype, hashclauses,
						  outer_path, inner_path, extra, false);

	if (add_path_precheck(joinrel,
						  workspace.startup_cost, workspace.total_cost,
						  NIL, required_outer))
	{
#ifdef ADB
		Path *path = ((Path*)
#else
		add_path(joinrel, (Path *)
#endif /* ADB */
				 create_hashjoin_path(root,
									  joinrel,
									  jointype,
									  &workspace,
									  extra,
									  outer_path,
									  inner_path,
									  false,	/* parallel_hash */
									  extra->restrictlist,
									  required_outer,
									  ADB_ONLY_ARG_COMMA(reduce_info_list)
									  hashclauses));
#ifdef ADB
		if(reduce_info_list)
		{
			path->reduce_info_list = reduce_info_list;
			path->reduce_is_valid = true;
			add_cluster_path(joinrel, path);
		}else
		{
			add_path(joinrel, path);
		}
#endif /* ADB */
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_partial_hashjoin_path
 *	  Consider a partial hashjoin join path; if it appears useful, push it into
 *	  the joinrel's partial_pathlist via add_partial_path().
 *	  The outer side is partial.  If parallel_hash is true, then the inner path
 *	  must be partial and will be run in parallel to create one or more shared
 *	  hash tables; otherwise the inner path must be complete and a copy of it
 *	  is run in every process to create separate identical private hash tables.
 */
static void
try_partial_hashjoin_path(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  List *hashclauses,
						  JoinType jointype,
						  ADB_ONLY_ARG_COMMA(List *reduce_info_list)
						  JoinPathExtraData *extra,
						  bool parallel_hash)
{
	JoinCostWorkspace workspace;
	Path *path;

	/*
	 * If the inner path is parameterized, the parameterization must be fully
	 * satisfied by the proposed outer path.  Parameterized partial paths are
	 * not supported.  The caller should already have verified that no
	 * extra_lateral_rels are required here.
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	if (inner_path->param_info != NULL)
	{
		Relids		inner_paramrels = inner_path->param_info->ppi_req_outer;

		if (!bms_is_empty(inner_paramrels))
			return;
	}

	/*
	 * Before creating a path, get a quick lower bound on what it is likely to
	 * cost.  Bail out right away if it looks terrible.
	 */
#ifdef ADB
	workspace.is_cluster = (reduce_info_list ? true:false);
#endif /* ADB */
	initial_cost_hashjoin(root, &workspace, jointype, hashclauses,
						  outer_path, inner_path, extra, parallel_hash);
#ifdef ADB
	if (reduce_info_list)
	{
		if (!add_cluster_partial_path_precheck(joinrel, reduce_info_list, workspace.total_cost, NIL))
			return;
	}else
#endif /* ADB */
	if (!add_partial_path_precheck(joinrel, workspace.total_cost, NIL))
		return;

	/* Might be good enough to be worth trying, so let's try it. */
	path = (Path *)
					 create_hashjoin_path(root,
										  joinrel,
										  jointype,
										  &workspace,
										  extra,
										  outer_path,
										  inner_path,
										  parallel_hash,
										  extra->restrictlist,
										  NULL,
										  ADB_ONLY_ARG_COMMA(reduce_info_list)
										  hashclauses);
#ifdef ADB
	if (reduce_info_list)
		add_cluster_partial_path(joinrel, path);
	else
#endif /* ADB */
	add_partial_path(joinrel, path);
}

/*
 * clause_sides_match_join
 *	  Determine whether a join clause is of the right form to use in this join.
 *
 * We already know that the clause is a binary opclause referencing only the
 * rels in the current join.  The point here is to check whether it has the
 * form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
 * rather than mixing outer and inner vars on either side.  If it matches,
 * we set the transient flag outer_is_left to identify which side is which.
 */
static inline bool
clause_sides_match_join(RestrictInfo *rinfo, RelOptInfo *outerrel,
						RelOptInfo *innerrel)
{
	if (bms_is_subset(rinfo->left_relids, outerrel->relids) &&
		bms_is_subset(rinfo->right_relids, innerrel->relids))
	{
		/* lefthand side is outer */
		rinfo->outer_is_left = true;
		return true;
	}
	else if (bms_is_subset(rinfo->left_relids, innerrel->relids) &&
			 bms_is_subset(rinfo->right_relids, outerrel->relids))
	{
		/* righthand side is outer */
		rinfo->outer_is_left = false;
		return true;
	}
	return false;				/* no good for these input relations */
}

/*
 * sort_inner_and_outer
 *	  Create mergejoin join paths by explicitly sorting both the outer and
 *	  inner join relations on each available merge ordering.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
sort_inner_and_outer(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 JoinPathExtraData *extra)
{
	JoinType	save_jointype = jointype;
	Path	   *outer_path;
	Path	   *inner_path;
	Path	   *cheapest_partial_outer = NULL;
	Path	   *cheapest_safe_inner = NULL;
	List	   *all_pathkeys;
	ListCell   *l;


	/*
	 * We only consider the cheapest-total-cost input paths, since we are
	 * assuming here that a sort is required.  We will consider
	 * cheapest-startup-cost input paths later, and only if they don't need a
	 * sort.
	 *
	 * This function intentionally does not consider parameterized input
	 * paths, except when the cheapest-total is parameterized.  If we did so,
	 * we'd have a combinatorial explosion of mergejoin paths of dubious
	 * value.  This interacts with decisions elsewhere that also discriminate
	 * against mergejoins with parameterized inputs; see comments in
	 * src/backend/optimizer/README.
	 */
	outer_path = outerrel->cheapest_total_path;
	inner_path = innerrel->cheapest_total_path;

	/*
	 * If either cheapest-total path is parameterized by the other rel, we
	 * can't use a mergejoin.  (There's no use looking for alternative input
	 * paths, since these should already be the least-parameterized available
	 * paths.)
	 */
	if (PATH_PARAM_BY_REL(outer_path, innerrel) ||
		PATH_PARAM_BY_REL(inner_path, outerrel))
		return;

	/*
	 * If unique-ification is requested, do it and then handle as a plain
	 * inner join.
	 */
	if (jointype == JOIN_UNIQUE_OUTER)
	{
		outer_path = (Path *) create_unique_path(root, outerrel,
												 outer_path, extra->sjinfo);
		Assert(outer_path);
		jointype = JOIN_INNER;
	}
	else if (jointype == JOIN_UNIQUE_INNER)
	{
		inner_path = (Path *) create_unique_path(root, innerrel,
												 inner_path, extra->sjinfo);
		Assert(inner_path);
		jointype = JOIN_INNER;
	}

	/*
	 * If the joinrel is parallel-safe, we may be able to consider a partial
	 * merge join.  However, we can't handle JOIN_UNIQUE_OUTER, because the
	 * outer path will be partial, and therefore we won't be able to properly
	 * guarantee uniqueness.  Similarly, we can't handle JOIN_FULL and
	 * JOIN_RIGHT, because they can produce false null extended rows.  Also,
	 * the resulting path must not be parameterized.
	 */
	if (joinrel->consider_parallel &&
		save_jointype != JOIN_UNIQUE_OUTER &&
		save_jointype != JOIN_FULL &&
		save_jointype != JOIN_RIGHT &&
		outerrel->partial_pathlist != NIL &&
		bms_is_empty(joinrel->lateral_relids))
	{
		cheapest_partial_outer = (Path *) linitial(outerrel->partial_pathlist);

		if (inner_path->parallel_safe)
			cheapest_safe_inner = inner_path;
		else if (save_jointype != JOIN_UNIQUE_INNER)
			cheapest_safe_inner =
				get_cheapest_parallel_safe_total_inner(innerrel->pathlist);
	}

	/*
	 * Each possible ordering of the available mergejoin clauses will generate
	 * a differently-sorted result path at essentially the same cost.  We have
	 * no basis for choosing one over another at this level of joining, but
	 * some sort orders may be more useful than others for higher-level
	 * mergejoins, so it's worth considering multiple orderings.
	 *
	 * Actually, it's not quite true that every mergeclause ordering will
	 * generate a different path order, because some of the clauses may be
	 * partially redundant (refer to the same EquivalenceClasses).  Therefore,
	 * what we do is convert the mergeclause list to a list of canonical
	 * pathkeys, and then consider different orderings of the pathkeys.
	 *
	 * Generating a path for *every* permutation of the pathkeys doesn't seem
	 * like a winning strategy; the cost in planning time is too high. For
	 * now, we generate one path for each pathkey, listing that pathkey first
	 * and the rest in random order.  This should allow at least a one-clause
	 * mergejoin without re-sorting against any other possible mergejoin
	 * partner path.  But if we've not guessed the right ordering of secondary
	 * keys, we may end up evaluating clauses as qpquals when they could have
	 * been done as mergeclauses.  (In practice, it's rare that there's more
	 * than two or three mergeclauses, so expending a huge amount of thought
	 * on that is probably not worth it.)
	 *
	 * The pathkey order returned by select_outer_pathkeys_for_merge() has
	 * some heuristics behind it (see that function), so be sure to try it
	 * exactly as-is as well as making variants.
	 */
	all_pathkeys = select_outer_pathkeys_for_merge(root,
												   extra->mergeclause_list,
												   joinrel);

	foreach(l, all_pathkeys)
	{
		List	   *front_pathkey = (List *) lfirst(l);
		List	   *cur_mergeclauses;
		List	   *outerkeys;
		List	   *innerkeys;
		List	   *merge_pathkeys;

		/* Make a pathkey list with this guy first */
		if (l != list_head(all_pathkeys))
			outerkeys = lcons(front_pathkey,
							  list_delete_ptr(list_copy(all_pathkeys),
											  front_pathkey));
		else
			outerkeys = all_pathkeys;	/* no work at first one... */

		/* Sort the mergeclauses into the corresponding ordering */
		cur_mergeclauses =
			find_mergeclauses_for_outer_pathkeys(root,
												 outerkeys,
												 extra->mergeclause_list);

		/* Should have used them all... */
		Assert(list_length(cur_mergeclauses) == list_length(extra->mergeclause_list));

		/* Build sort pathkeys for the inner side */
		innerkeys = make_inner_pathkeys_for_merge(root,
												  cur_mergeclauses,
												  outerkeys);

		/* Build pathkeys representing output sort order */
		merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
											 outerkeys);

		/*
		 * And now we can make the path.
		 *
		 * Note: it's possible that the cheapest paths will already be sorted
		 * properly.  try_mergejoin_path will detect that case and suppress an
		 * explicit sort step, so we needn't do so here.
		 */
		try_mergejoin_path(root,
						   joinrel,
						   outer_path,
						   inner_path,
						   merge_pathkeys,
						   cur_mergeclauses,
						   outerkeys,
						   innerkeys,
						   jointype,
						   ADB_ONLY_ARG_COMMA(NIL)
						   extra,
						   false);

		/*
		 * If we have partial outer and parallel safe inner path then try
		 * partial mergejoin path.
		 */
		if (cheapest_partial_outer && cheapest_safe_inner)
			try_partial_mergejoin_path(root,
									   joinrel,
									   cheapest_partial_outer,
									   cheapest_safe_inner,
									   merge_pathkeys,
									   cur_mergeclauses,
									   outerkeys,
									   innerkeys,
									   jointype,
									   extra);
	}
#ifdef ADB
	if(outerrel->cluster_pathlist && innerrel->cluster_pathlist)
	{
		List	   *new_reduce_list;
		ClusterJoinContext jcontext;
		MemSet(&jcontext, 0, sizeof(jcontext));
		jcontext.root = root;
		jcontext.joinrel = joinrel;
		jcontext.outerrel = outerrel;
		jcontext.innerrel = innerrel;
		jcontext.extra = extra;
		jcontext.jointype = save_jointype;
		jcontext.try_match = CLUSTER_TRY_MERGE_JOIN;
		set_all_join_inner_path(&jcontext, outerrel->cheapest_cluster_total_path, innerrel->cluster_pathlist);
		inner_path = get_cheapest_join_path(&jcontext,
											outerrel->cheapest_cluster_total_path,
											TOTAL_COST,
											false,
											&new_reduce_list);
		if(inner_path)
		{
			outer_path = outerrel->cheapest_cluster_total_path;
			sort_cluster_inner_and_outer(&jcontext, outer_path, inner_path, all_pathkeys, new_reduce_list);
			if(PATH_REQ_OUTER(outer_path))
			{
				outer_path = get_cheapest_path_for_pathkeys(outerrel->cluster_pathlist, NIL, NULL, TOTAL_COST, false);
				if(outer_path)
				{
					set_all_join_inner_path(&jcontext, outer_path, innerrel->cluster_pathlist);
					inner_path = get_cheapest_join_path(&jcontext, outer_path, TOTAL_COST, false, &new_reduce_list);
					if(inner_path)
						sort_cluster_inner_and_outer(&jcontext, outer_path, inner_path, all_pathkeys, new_reduce_list);
				}
			}
			if(outer_path && inner_path && PATH_REQ_OUTER(inner_path))
			{
				inner_path = get_cheapest_join_path(&jcontext,
													outer_path,
													TOTAL_COST,
													true,
													&new_reduce_list);
				if(inner_path)
					sort_cluster_inner_and_outer(&jcontext, outer_path, inner_path, all_pathkeys, new_reduce_list);
			}
		}
	}
#endif /* ADB */
}

/*
 * generate_mergejoin_paths
 *	Creates possible mergejoin paths for input outerpath.
 *
 * We generate mergejoins if mergejoin clauses are available.  We have
 * two ways to generate the inner path for a mergejoin: sort the cheapest
 * inner path, or use an inner path that is already suitably ordered for the
 * merge.  If we have several mergeclauses, it could be that there is no inner
 * path (or only a very expensive one) for the full list of mergeclauses, but
 * better paths exist if we truncate the mergeclause list (thereby discarding
 * some sort key requirements).  So, we consider truncations of the
 * mergeclause list as well as the full list.  (Ideally we'd consider all
 * subsets of the mergeclause list, but that seems way too expensive.)
 */
static void
generate_mergejoin_paths(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *innerrel,
						 Path *outerpath,
						 JoinType jointype,
						 JoinPathExtraData *extra,
						 bool useallclauses,
						 Path *inner_cheapest_total,
						 List *merge_pathkeys,
						 bool is_partial)
{
	List	   *mergeclauses;
	List	   *innersortkeys;
	List	   *trialsortkeys;
	Path	   *cheapest_startup_inner;
	Path	   *cheapest_total_inner;
	JoinType	save_jointype = jointype;
	int			num_sortkeys;
	int			sortkeycnt;

	if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
		jointype = JOIN_INNER;

	/* Look for useful mergeclauses (if any) */
	mergeclauses =
		find_mergeclauses_for_outer_pathkeys(root,
											 outerpath->pathkeys,
											 extra->mergeclause_list);

	/*
	 * Done with this outer path if no chance for a mergejoin.
	 *
	 * Special corner case: for "x FULL JOIN y ON true", there will be no join
	 * clauses at all.  Ordinarily we'd generate a clauseless nestloop path,
	 * but since mergejoin is our only join type that supports FULL JOIN
	 * without any join clauses, it's necessary to generate a clauseless
	 * mergejoin path instead.
	 */
	if (mergeclauses == NIL)
	{
		if (jointype == JOIN_FULL)
			 /* okay to try for mergejoin */ ;
		else
			return;
	}
	if (useallclauses &&
		list_length(mergeclauses) != list_length(extra->mergeclause_list))
		return;

	/* Compute the required ordering of the inner path */
	innersortkeys = make_inner_pathkeys_for_merge(root,
												  mergeclauses,
												  outerpath->pathkeys);

	/*
	 * Generate a mergejoin on the basis of sorting the cheapest inner. Since
	 * a sort will be needed, only cheapest total cost matters. (But
	 * try_mergejoin_path will do the right thing if inner_cheapest_total is
	 * already correctly sorted.)
	 */
	try_mergejoin_path(root,
					   joinrel,
					   outerpath,
					   inner_cheapest_total,
					   merge_pathkeys,
					   mergeclauses,
					   NIL,
					   innersortkeys,
					   jointype, ADB_ONLY_ARG_COMMA(NIL)
					   extra,
					   is_partial);

	/* Can't do anything else if inner path needs to be unique'd */
	if (save_jointype == JOIN_UNIQUE_INNER)
		return;

	/*
	 * Look for presorted inner paths that satisfy the innersortkey list ---
	 * or any truncation thereof, if we are allowed to build a mergejoin using
	 * a subset of the merge clauses.  Here, we consider both cheap startup
	 * cost and cheap total cost.
	 *
	 * Currently we do not consider parameterized inner paths here. This
	 * interacts with decisions elsewhere that also discriminate against
	 * mergejoins with parameterized inputs; see comments in
	 * src/backend/optimizer/README.
	 *
	 * As we shorten the sortkey list, we should consider only paths that are
	 * strictly cheaper than (in particular, not the same as) any path found
	 * in an earlier iteration.  Otherwise we'd be intentionally using fewer
	 * merge keys than a given path allows (treating the rest as plain
	 * joinquals), which is unlikely to be a good idea.  Also, eliminating
	 * paths here on the basis of compare_path_costs is a lot cheaper than
	 * building the mergejoin path only to throw it away.
	 *
	 * If inner_cheapest_total is well enough sorted to have not required a
	 * sort in the path made above, we shouldn't make a duplicate path with
	 * it, either.  We handle that case with the same logic that handles the
	 * previous consideration, by initializing the variables that track
	 * cheapest-so-far properly.  Note that we do NOT reject
	 * inner_cheapest_total if we find it matches some shorter set of
	 * pathkeys.  That case corresponds to using fewer mergekeys to avoid
	 * sorting inner_cheapest_total, whereas we did sort it above, so the
	 * plans being considered are different.
	 */
	if (pathkeys_contained_in(innersortkeys,
							  inner_cheapest_total->pathkeys))
	{
		/* inner_cheapest_total didn't require a sort */
		cheapest_startup_inner = inner_cheapest_total;
		cheapest_total_inner = inner_cheapest_total;
	}
	else
	{
		/* it did require a sort, at least for the full set of keys */
		cheapest_startup_inner = NULL;
		cheapest_total_inner = NULL;
	}
	num_sortkeys = list_length(innersortkeys);
	if (num_sortkeys > 1 && !useallclauses)
		trialsortkeys = list_copy(innersortkeys);	/* need modifiable copy */
	else
		trialsortkeys = innersortkeys;	/* won't really truncate */

	for (sortkeycnt = num_sortkeys; sortkeycnt > 0; sortkeycnt--)
	{
		Path	   *innerpath;
		List	   *newclauses = NIL;

		/*
		 * Look for an inner path ordered well enough for the first
		 * 'sortkeycnt' innersortkeys.  NB: trialsortkeys list is modified
		 * destructively, which is why we made a copy...
		 */
		trialsortkeys = list_truncate(trialsortkeys, sortkeycnt);
		innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist,
												   trialsortkeys,
												   NULL,
												   TOTAL_COST,
												   is_partial);
		if (innerpath != NULL &&
			(cheapest_total_inner == NULL ||
			 compare_path_costs(innerpath, cheapest_total_inner,
								TOTAL_COST) < 0))
		{
			/* Found a cheap (or even-cheaper) sorted path */
			/* Select the right mergeclauses, if we didn't already */
			if (sortkeycnt < num_sortkeys)
			{
				newclauses =
					trim_mergeclauses_for_inner_pathkeys(root,
														 mergeclauses,
														 trialsortkeys);
				Assert(newclauses != NIL);
			}
			else
				newclauses = mergeclauses;
			try_mergejoin_path(root,
							   joinrel,
							   outerpath,
							   innerpath,
							   merge_pathkeys,
							   newclauses,
							   NIL,
							   NIL,
							   jointype, ADB_ONLY_ARG_COMMA(NIL)
							   extra,
							   is_partial);
			cheapest_total_inner = innerpath;
		}
		/* Same on the basis of cheapest startup cost ... */
		innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist,
												   trialsortkeys,
												   NULL,
												   STARTUP_COST,
												   is_partial);
		if (innerpath != NULL &&
			(cheapest_startup_inner == NULL ||
			 compare_path_costs(innerpath, cheapest_startup_inner,
								STARTUP_COST) < 0))
		{
			/* Found a cheap (or even-cheaper) sorted path */
			if (innerpath != cheapest_total_inner)
			{
				/*
				 * Avoid rebuilding clause list if we already made one; saves
				 * memory in big join trees...
				 */
				if (newclauses == NIL)
				{
					if (sortkeycnt < num_sortkeys)
					{
						newclauses =
							trim_mergeclauses_for_inner_pathkeys(root,
																 mergeclauses,
																 trialsortkeys);
						Assert(newclauses != NIL);
					}
					else
						newclauses = mergeclauses;
				}
				try_mergejoin_path(root,
								   joinrel,
								   outerpath,
								   innerpath,
								   merge_pathkeys,
								   newclauses,
								   NIL,
								   NIL,
								   jointype, ADB_ONLY_ARG_COMMA(NIL)
								   extra,
								   is_partial);
			}
			cheapest_startup_inner = innerpath;
		}

		/*
		 * Don't consider truncated sortkeys if we need all clauses.
		 */
		if (useallclauses)
			break;
	}
}

/*
 * match_unsorted_outer
 *	  Creates possible join paths for processing a single join relation
 *	  'joinrel' by employing either iterative substitution or
 *	  mergejoining on each of its possible outer paths (considering
 *	  only outer paths that are already ordered well enough for merging).
 *
 * We always generate a nestloop path for each available outer path.
 * In fact we may generate as many as five: one on the cheapest-total-cost
 * inner path, one on the same with materialization, one on the
 * cheapest-startup-cost inner path (if different), one on the
 * cheapest-total inner-indexscan path (if any), and one on the
 * cheapest-startup inner-indexscan path (if different).
 *
 * We also consider mergejoins if mergejoin clauses are available.  See
 * detailed comments in generate_mergejoin_paths.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
match_unsorted_outer(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 JoinPathExtraData *extra)
{
	JoinType	save_jointype = jointype;
	bool		nestjoinOK;
	bool		useallclauses;
	Path	   *inner_cheapest_total = innerrel->cheapest_total_path;
	Path	   *matpath = NULL;
	ListCell   *lc1;

	/*
	 * Nestloop only supports inner, left, semi, and anti joins.  Also, if we
	 * are doing a right or full mergejoin, we must use *all* the mergeclauses
	 * as join clauses, else we will not have a valid plan.  (Although these
	 * two flags are currently inverses, keep them separate for clarity and
	 * possible future changes.)
	 */
	switch (jointype)
	{
		case JOIN_INNER:
		case JOIN_LEFT:
		case JOIN_SEMI:
		case JOIN_ANTI:
			nestjoinOK = true;
			useallclauses = false;
			break;
		case JOIN_RIGHT:
		case JOIN_FULL:
			nestjoinOK = false;
			useallclauses = true;
			break;
		case JOIN_UNIQUE_OUTER:
		case JOIN_UNIQUE_INNER:
			jointype = JOIN_INNER;
			nestjoinOK = true;
			useallclauses = false;
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) jointype);
			nestjoinOK = false; /* keep compiler quiet */
			useallclauses = false;
			break;
	}

	/*
	 * If inner_cheapest_total is parameterized by the outer rel, ignore it;
	 * we will consider it below as a member of cheapest_parameterized_paths,
	 * but the other possibilities considered in this routine aren't usable.
	 */
	if (PATH_PARAM_BY_REL(inner_cheapest_total, outerrel))
		inner_cheapest_total = NULL;

	/*
	 * If we need to unique-ify the inner path, we will consider only the
	 * cheapest-total inner.
	 */
	if (save_jointype == JOIN_UNIQUE_INNER)
	{
		/* No way to do this with an inner path parameterized by outer rel */
		if (inner_cheapest_total == NULL)
			return;
		inner_cheapest_total = (Path *)
			create_unique_path(root, innerrel, inner_cheapest_total, extra->sjinfo);
		Assert(inner_cheapest_total);
	}
	else if (nestjoinOK)
	{
		/*
		 * Consider materializing the cheapest inner path, unless
		 * enable_material is off or the path in question materializes its
		 * output anyway.
		 */
		if (enable_material && inner_cheapest_total != NULL &&
			!ExecMaterializesOutput(inner_cheapest_total->pathtype))
			matpath = (Path *)
				create_material_path(innerrel, inner_cheapest_total);
	}

	foreach(lc1, outerrel->pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *merge_pathkeys;

		/*
		 * We cannot use an outer path that is parameterized by the inner rel.
		 */
		if (PATH_PARAM_BY_REL(outerpath, innerrel))
			continue;

		/*
		 * If we need to unique-ify the outer path, it's pointless to consider
		 * any but the cheapest outer.  (XXX we don't consider parameterized
		 * outers, nor inners, for unique-ified cases.  Should we?)
		 */
		if (save_jointype == JOIN_UNIQUE_OUTER)
		{
			if (outerpath != outerrel->cheapest_total_path)
				continue;
			outerpath = (Path *) create_unique_path(root, outerrel,
													outerpath, extra->sjinfo);
			Assert(outerpath);
		}

		/*
		 * The result will have this sort order (even if it is implemented as
		 * a nestloop, and even if some of the mergeclauses are implemented by
		 * qpquals rather than as true mergeclauses):
		 */
		merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
											 outerpath->pathkeys);

		if (save_jointype == JOIN_UNIQUE_INNER)
		{
			/*
			 * Consider nestloop join, but only with the unique-ified cheapest
			 * inner path
			 */
			try_nestloop_path(root,
							  joinrel,
							  outerpath,
							  inner_cheapest_total,
							  merge_pathkeys,
							  jointype,
							  ADB_ONLY_ARG_COMMA(NIL)
							  extra);
		}
		else if (nestjoinOK)
		{
			/*
			 * Consider nestloop joins using this outer path and various
			 * available paths for the inner relation.  We consider the
			 * cheapest-total paths for each available parameterization of the
			 * inner relation, including the unparameterized case.
			 */
			ListCell   *lc2;

			foreach(lc2, innerrel->cheapest_parameterized_paths)
			{
				Path	   *innerpath = (Path *) lfirst(lc2);

				try_nestloop_path(root,
								  joinrel,
								  outerpath,
								  innerpath,
								  merge_pathkeys,
								  jointype,
								  ADB_ONLY_ARG_COMMA(NIL)
								  extra);
			}

			/* Also consider materialized form of the cheapest inner path */
			if (matpath != NULL)
				try_nestloop_path(root,
								  joinrel,
								  outerpath,
								  matpath,
								  merge_pathkeys,
								  jointype,
								  ADB_ONLY_ARG_COMMA(NIL)
								  extra);
		}

		/* Can't do anything else if outer path needs to be unique'd */
		if (save_jointype == JOIN_UNIQUE_OUTER)
			continue;

		/* Can't do anything else if inner rel is parameterized by outer */
		if (inner_cheapest_total == NULL)
			continue;

		/* Generate merge join paths */
		generate_mergejoin_paths(root, joinrel, innerrel, outerpath,
								 save_jointype, extra, useallclauses,
								 inner_cheapest_total, merge_pathkeys,
								 false);
	}

	/*
	 * Consider partial nestloop and mergejoin plan if outerrel has any
	 * partial path and the joinrel is parallel-safe.  However, we can't
	 * handle JOIN_UNIQUE_OUTER, because the outer path will be partial, and
	 * therefore we won't be able to properly guarantee uniqueness.  Nor can
	 * we handle extra_lateral_rels, since partial paths must not be
	 * parameterized. Similarly, we can't handle JOIN_FULL and JOIN_RIGHT,
	 * because they can produce false null extended rows.
	 */
	if (joinrel->consider_parallel &&
		save_jointype != JOIN_UNIQUE_OUTER &&
		save_jointype != JOIN_FULL &&
		save_jointype != JOIN_RIGHT &&
#ifdef ADB
		(outerrel->partial_pathlist != NIL ||
		 outerrel->cluster_partial_pathlist != NIL) &&
#else
		outerrel->partial_pathlist != NIL &&
#endif /* ADB */
		bms_is_empty(joinrel->lateral_relids))
	{
		if (nestjoinOK)
			consider_parallel_nestloop(root, joinrel, outerrel, innerrel,
									   save_jointype, extra);

		/*
		 * If inner_cheapest_total is NULL or non parallel-safe then find the
		 * cheapest total parallel safe path.  If doing JOIN_UNIQUE_INNER, we
		 * can't use any alternative inner path.
		 */
		if (inner_cheapest_total == NULL ||
			!inner_cheapest_total->parallel_safe)
		{
			if (save_jointype == JOIN_UNIQUE_INNER)
				return;

			inner_cheapest_total = get_cheapest_parallel_safe_total_inner(
																		  innerrel->pathlist);
		}

		if (inner_cheapest_total)
			consider_parallel_mergejoin(root, joinrel, outerrel, innerrel,
										save_jointype, extra,
										inner_cheapest_total);
	}
}

/*
 * consider_parallel_mergejoin
 *	  Try to build partial paths for a joinrel by joining a partial path
 *	  for the outer relation to a complete path for the inner relation.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 * 'inner_cheapest_total' cheapest total path for innerrel
 */
static void
consider_parallel_mergejoin(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra,
							Path *inner_cheapest_total)
{
	ListCell   *lc1;

	/* generate merge join path for each partial outer path */
	foreach(lc1, outerrel->partial_pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *merge_pathkeys;

		/*
		 * Figure out what useful ordering any paths we create will have.
		 */
		merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
											 outerpath->pathkeys);

		generate_mergejoin_paths(root, joinrel, innerrel, outerpath, jointype,
								 extra, false, inner_cheapest_total,
								 merge_pathkeys, true);
	}
}

/*
 * consider_parallel_nestloop
 *	  Try to build partial paths for a joinrel by joining a partial path for the
 *	  outer relation to a complete path for the inner relation.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
consider_parallel_nestloop(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	JoinType	save_jointype = jointype;
	ListCell   *lc1;
#ifdef ADB
	bool		tried;
#endif /* ADB */

	if (jointype == JOIN_UNIQUE_INNER)
		jointype = JOIN_INNER;

	foreach(lc1, outerrel->partial_pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *pathkeys;
		ListCell   *lc2;

		/* Figure out what useful ordering any paths we create will have. */
		pathkeys = build_join_pathkeys(root, joinrel, jointype,
									   outerpath->pathkeys);

		/*
		 * Try the cheapest parameterized paths; only those which will produce
		 * an unparameterized path when joined to this outerrel will survive
		 * try_partial_nestloop_path.  The cheapest unparameterized path is
		 * also in this list.
		 */
		foreach(lc2, innerrel->cheapest_parameterized_paths)
		{
			Path	   *innerpath = (Path *) lfirst(lc2);

			/* Can't join to an inner path that is not parallel-safe */
			if (!innerpath->parallel_safe)
				continue;

			/*
			 * If we're doing JOIN_UNIQUE_INNER, we can only use the inner's
			 * cheapest_total_path, and we have to unique-ify it.  (We might
			 * be able to relax this to allow other safe, unparameterized
			 * inner paths, but right now create_unique_path is not on board
			 * with that.)
			 */
			if (save_jointype == JOIN_UNIQUE_INNER)
			{
				if (innerpath != innerrel->cheapest_total_path)
					continue;
				innerpath = (Path *) create_unique_path(root, innerrel,
														innerpath,
														extra->sjinfo);
				Assert(innerpath);
			}

			try_partial_nestloop_path(root, joinrel, outerpath, innerpath,
									  pathkeys, jointype, extra);
		}
	}
#ifdef ADB
	tried = false;
	foreach(lc1, outerrel->cluster_partial_pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *outer_reduce_list = get_reduce_info_list(outerpath);
		List	   *new_reduce_list;
		List	   *pathkeys;
		ListCell   *lc2;

		if (joinrel->have_upper_reference &&
			IsReduceInfoListReplicated(outer_reduce_list) == false)
			continue;

		/* Figure out what useful ordering any paths we create will have. */
		pathkeys = build_join_pathkeys(root, joinrel, jointype,
									   outerpath->pathkeys);

		/*
		 * Try the cheapest parameterized paths; only those which will produce
		 * an unparameterized path when joined to this outerrel will survive
		 * try_partial_nestloop_path.  The cheapest unparameterized path is
		 * also in this list.
		 */
		foreach(lc2, innerrel->cheapest_cluster_parameterized_paths)
		{
			Path	   *innerpath = (Path *) lfirst(lc2);

			if (!innerpath->parallel_safe ||
				reduce_info_list_can_join(outer_reduce_list,
										  get_reduce_info_list(innerpath),
										  extra->restrictlist,
										  jointype,
										  &new_reduce_list) == false)
				continue;
			if (joinrel->have_upper_reference &&
				!IsReduceInfoListReplicated(new_reduce_list) == false)
				continue;

			/*
			 * If we're doing JOIN_UNIQUE_INNER, we can only use the inner's
			 * cheapest_total_path, and we have to unique-ify it.  (We might
			 * be able to relax this to allow other safe, unparameterized
			 * inner paths, but right now create_unique_path is not on board
			 * with that.)
			 */
			if (save_jointype == JOIN_UNIQUE_INNER)
			{
				innerpath = (Path *) create_cluster_unique_path(root, innerrel,
																innerpath,
																extra->sjinfo);
				Assert(innerpath);
			}

			try_cluster_partial_nestloop_path(root, joinrel, outerpath, innerpath,
											  pathkeys, jointype, extra, new_reduce_list);
			if (enable_material &&
				!ExecMaterializesOutput(innerpath->pathtype))
			{
				innerpath = (Path *) create_material_path(innerrel, innerpath);
				try_cluster_partial_nestloop_path(root, joinrel, outerpath, innerpath,
												  pathkeys, jointype, extra, new_reduce_list);
			}
			tried = true;
		}
	}
	if(tried == false && outerrel->cluster_partial_pathlist)
	{
		/* reduce outer paths */
		ClusterJoinContext jcontext;
		Path *outerpath;
		Path *innerpath;
		List *pathkeys;
		List *inner_reduce_list;
		List *need_reduce_list;
		List *outer_pathlist;
		//List *outer_reduce_list;
		List *new_reduce_list;
		List *inner_pathlist = NIL;
		foreach(lc1, innerrel->cluster_pathlist)
		{
			innerpath = lfirst(lc1);
			if(innerpath->parallel_safe)
				inner_pathlist = lappend(inner_pathlist, innerpath);
		}
		if (inner_pathlist == NIL)
			goto end_reduce_outer_;

		MemSet(&jcontext, 0, sizeof(jcontext));
		jcontext.root = root;
		jcontext.joinrel = joinrel;
		jcontext.outerrel = outerrel;
		jcontext.innerrel = innerrel;
		jcontext.extra = extra;
		jcontext.jointype = save_jointype;
		jcontext.try_match = CLUSTER_TRY_NESTLOOP_JOIN;

		inner_reduce_list = GetPathListReduceInfoList(inner_pathlist);
		need_reduce_list = create_outer_reduce_info_for_join(inner_reduce_list, outerrel, jointype, extra);
		outer_pathlist = reduce_paths_for_join(root, outerrel, outerrel->cluster_partial_pathlist, need_reduce_list, true);
		foreach(lc1, outer_pathlist)
		{
			outerpath = lfirst(lc1);
			//outer_reduce_list = get_reduce_info_list(outerpath);

			set_all_join_inner_path(&jcontext, outerpath, inner_pathlist);
			innerpath = get_cheapest_join_path(&jcontext, outerpath, TOTAL_COST, false, &new_reduce_list);
			if (innerpath == NULL)
				continue;

			pathkeys = build_join_pathkeys(root, joinrel, jointype,
										   outerpath->pathkeys);
retry_partial_nestloop_:
			try_cluster_partial_nestloop_path(root, joinrel, outerpath, innerpath,
											  pathkeys, jointype, extra, new_reduce_list);
			if (save_jointype == JOIN_UNIQUE_INNER)
			{
				innerpath = (Path *) create_cluster_unique_path(root, innerrel,
																innerpath,
																extra->sjinfo);
				Assert(innerpath);
			}
			if (enable_material &&
				!ExecMaterializesOutput(innerpath->pathtype))
			{
				innerpath = (Path *) create_material_path(innerrel, innerpath);
				try_cluster_partial_nestloop_path(root, joinrel, outerpath, innerpath,
												  pathkeys, jointype, extra, new_reduce_list);
			}

			/* try no param inner path */
			if (!PATH_PARAM_OUTER_REL(innerpath, outerrel))
				continue;
			innerpath = get_cheapest_join_path(&jcontext, outerpath, TOTAL_COST, true, &new_reduce_list);
			if (innerpath != NULL)
			{
				Assert(!PATH_PARAM_OUTER_REL(innerpath, outerrel));
				goto retry_partial_nestloop_;
			}
		}
	}
end_reduce_outer_:
	(void)0;
#endif /* ADB */
}

/*
 * hash_inner_and_outer
 *	  Create hashjoin join paths by explicitly hashing both the outer and
 *	  inner keys of each available hash clause.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
hash_inner_and_outer(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 JoinPathExtraData *extra)
{
	JoinType	save_jointype = jointype;
	bool		isouterjoin = IS_OUTER_JOIN(jointype);
	List	   *hashclauses;
	ListCell   *l;

	/*
	 * We need to build only one hashclauses list for any given pair of outer
	 * and inner relations; all of the hashable clauses will be used as keys.
	 *
	 * Scan the join's restrictinfo list to find hashjoinable clauses that are
	 * usable with this pair of sub-relations.
	 */
	hashclauses = NIL;
	foreach(l, extra->restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		/*
		 * If processing an outer join, only use its own join clauses for
		 * hashing.  For inner joins we need not be so picky.
		 */
		if (isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		if (!restrictinfo->can_join ||
			restrictinfo->hashjoinoperator == InvalidOid)
			continue;			/* not hashjoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer".
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel, innerrel))
			continue;			/* no good for these input relations */

		hashclauses = lappend(hashclauses, restrictinfo);
	}

	/* If we found any usable hashclauses, make paths */
	if (hashclauses)
	{
		/*
		 * We consider both the cheapest-total-cost and cheapest-startup-cost
		 * outer paths.  There's no need to consider any but the
		 * cheapest-total-cost inner path, however.
		 */
		Path	   *cheapest_startup_outer = outerrel->cheapest_startup_path;
		Path	   *cheapest_total_outer = outerrel->cheapest_total_path;
		Path	   *cheapest_total_inner = innerrel->cheapest_total_path;

		/*
		 * If either cheapest-total path is parameterized by the other rel, we
		 * can't use a hashjoin.  (There's no use looking for alternative
		 * input paths, since these should already be the least-parameterized
		 * available paths.)
		 */
		if (PATH_PARAM_BY_REL(cheapest_total_outer, innerrel) ||
			PATH_PARAM_BY_REL(cheapest_total_inner, outerrel))
			return;

		/* Unique-ify if need be; we ignore parameterized possibilities */
		if (jointype == JOIN_UNIQUE_OUTER)
		{
			cheapest_total_outer = (Path *)
				create_unique_path(root, outerrel,
								   cheapest_total_outer, extra->sjinfo);
			Assert(cheapest_total_outer);
			jointype = JOIN_INNER;
			try_hashjoin_path(root,
							  joinrel,
							  cheapest_total_outer,
							  cheapest_total_inner,
							  hashclauses,
							  jointype,
							  ADB_ONLY_ARG_COMMA(NIL)
							  extra);
			/* no possibility of cheap startup here */
		}
		else if (jointype == JOIN_UNIQUE_INNER)
		{
			cheapest_total_inner = (Path *)
				create_unique_path(root, innerrel,
								   cheapest_total_inner, extra->sjinfo);
			Assert(cheapest_total_inner);
			jointype = JOIN_INNER;
			try_hashjoin_path(root,
							  joinrel,
							  cheapest_total_outer,
							  cheapest_total_inner,
							  hashclauses,
							  jointype,
							  ADB_ONLY_ARG_COMMA(NIL)
							  extra);
			if (cheapest_startup_outer != NULL &&
				cheapest_startup_outer != cheapest_total_outer)
				try_hashjoin_path(root,
								  joinrel,
								  cheapest_startup_outer,
								  cheapest_total_inner,
								  hashclauses,
								  jointype,
								  ADB_ONLY_ARG_COMMA(NIL)
								  extra);
		}
		else
		{
			/*
			 * For other jointypes, we consider the cheapest startup outer
			 * together with the cheapest total inner, and then consider
			 * pairings of cheapest-total paths including parameterized ones.
			 * There is no use in generating parameterized paths on the basis
			 * of possibly cheap startup cost, so this is sufficient.
			 */
			ListCell   *lc1;
			ListCell   *lc2;

			if (cheapest_startup_outer != NULL)
				try_hashjoin_path(root,
								  joinrel,
								  cheapest_startup_outer,
								  cheapest_total_inner,
								  hashclauses,
								  jointype,
								  ADB_ONLY_ARG_COMMA(NIL)
								  extra);

			foreach(lc1, outerrel->cheapest_parameterized_paths)
			{
				Path	   *outerpath = (Path *) lfirst(lc1);

				/*
				 * We cannot use an outer path that is parameterized by the
				 * inner rel.
				 */
				if (PATH_PARAM_BY_REL(outerpath, innerrel))
					continue;

				foreach(lc2, innerrel->cheapest_parameterized_paths)
				{
					Path	   *innerpath = (Path *) lfirst(lc2);

					/*
					 * We cannot use an inner path that is parameterized by
					 * the outer rel, either.
					 */
					if (PATH_PARAM_BY_REL(innerpath, outerrel))
						continue;

					if (outerpath == cheapest_startup_outer &&
						innerpath == cheapest_total_inner)
						continue;	/* already tried it */

					try_hashjoin_path(root,
									  joinrel,
									  outerpath,
									  innerpath,
									  hashclauses,
									  jointype,
									  ADB_ONLY_ARG_COMMA(NIL)
									  extra);
				}
			}
		}

		/*
		 * If the joinrel is parallel-safe, we may be able to consider a
		 * partial hash join.  However, we can't handle JOIN_UNIQUE_OUTER,
		 * because the outer path will be partial, and therefore we won't be
		 * able to properly guarantee uniqueness.  Similarly, we can't handle
		 * JOIN_FULL and JOIN_RIGHT, because they can produce false null
		 * extended rows.  Also, the resulting path must not be parameterized.
		 * We would be able to support JOIN_FULL and JOIN_RIGHT for Parallel
		 * Hash, since in that case we're back to a single hash table with a
		 * single set of match bits for each batch, but that will require
		 * figuring out a deadlock-free way to wait for the probe to finish.
		 */
		if (joinrel->consider_parallel &&
			save_jointype != JOIN_UNIQUE_OUTER &&
			save_jointype != JOIN_FULL &&
			save_jointype != JOIN_RIGHT &&
			outerrel->partial_pathlist != NIL &&
			bms_is_empty(joinrel->lateral_relids))
		{
			Path	   *cheapest_partial_outer;
			Path	   *cheapest_partial_inner = NULL;
			Path	   *cheapest_safe_inner = NULL;

			cheapest_partial_outer =
				(Path *) linitial(outerrel->partial_pathlist);

			/*
			 * Can we use a partial inner plan too, so that we can build a
			 * shared hash table in parallel?  We can't handle
			 * JOIN_UNIQUE_INNER because we can't guarantee uniqueness.
			 */
			if (innerrel->partial_pathlist != NIL &&
				save_jointype != JOIN_UNIQUE_INNER &&
				enable_parallel_hash)
			{
				cheapest_partial_inner =
					(Path *) linitial(innerrel->partial_pathlist);
				try_partial_hashjoin_path(root, joinrel,
										  cheapest_partial_outer,
										  cheapest_partial_inner,
										  hashclauses, jointype, ADB_ONLY_ARG_COMMA(NIL) extra,
										  true /* parallel_hash */ );
			}

			/*
			 * Normally, given that the joinrel is parallel-safe, the cheapest
			 * total inner path will also be parallel-safe, but if not, we'll
			 * have to search for the cheapest safe, unparameterized inner
			 * path.  If doing JOIN_UNIQUE_INNER, we can't use any alternative
			 * inner path.
			 */
			if (cheapest_total_inner->parallel_safe)
				cheapest_safe_inner = cheapest_total_inner;
			else if (save_jointype != JOIN_UNIQUE_INNER)
				cheapest_safe_inner =
					get_cheapest_parallel_safe_total_inner(innerrel->pathlist);

			if (cheapest_safe_inner != NULL)
				try_partial_hashjoin_path(root, joinrel,
										  cheapest_partial_outer,
										  cheapest_safe_inner,
										  hashclauses, jointype, ADB_ONLY_ARG_COMMA(NIL) extra,
										  false /* parallel_hash */ );
		}
	}
}

/*
 * select_mergejoin_clauses
 *	  Select mergejoin clauses that are usable for a particular join.
 *	  Returns a list of RestrictInfo nodes for those clauses.
 *
 * *mergejoin_allowed is normally set to true, but it is set to false if
 * this is a right/full join and there are nonmergejoinable join clauses.
 * The executor's mergejoin machinery cannot handle such cases, so we have
 * to avoid generating a mergejoin plan.  (Note that this flag does NOT
 * consider whether there are actually any mergejoinable clauses.  This is
 * correct because in some cases we need to build a clauseless mergejoin.
 * Simply returning NIL is therefore not enough to distinguish safe from
 * unsafe cases.)
 *
 * We also mark each selected RestrictInfo to show which side is currently
 * being considered as outer.  These are transient markings that are only
 * good for the duration of the current add_paths_to_joinrel() call!
 *
 * We examine each restrictinfo clause known for the join to see
 * if it is mergejoinable and involves vars from the two sub-relations
 * currently of interest.
 */
static List *
select_mergejoin_clauses(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *outerrel,
						 RelOptInfo *innerrel,
						 List *restrictlist,
						 JoinType jointype,
						 bool *mergejoin_allowed)
{
	List	   *result_list = NIL;
	bool		isouterjoin = IS_OUTER_JOIN(jointype);
	bool		have_nonmergeable_joinclause = false;
	ListCell   *l;

	foreach(l, restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		/*
		 * If processing an outer join, only use its own join clauses in the
		 * merge.  For inner joins we can use pushed-down clauses too. (Note:
		 * we don't set have_nonmergeable_joinclause here because pushed-down
		 * clauses will become otherquals not joinquals.)
		 */
		if (isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		/* Check that clause is a mergeable operator clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
		{
			/*
			 * The executor can handle extra joinquals that are constants, but
			 * not anything else, when doing right/full merge join.  (The
			 * reason to support constants is so we can do FULL JOIN ON
			 * FALSE.)
			 */
			if (!restrictinfo->clause || !IsA(restrictinfo->clause, Const))
				have_nonmergeable_joinclause = true;
			continue;			/* not mergejoinable */
		}

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer".
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel, innerrel))
		{
			have_nonmergeable_joinclause = true;
			continue;			/* no good for these input relations */
		}

		/*
		 * Insist that each side have a non-redundant eclass.  This
		 * restriction is needed because various bits of the planner expect
		 * that each clause in a merge be associable with some pathkey in a
		 * canonical pathkey list, but redundant eclasses can't appear in
		 * canonical sort orderings.  (XXX it might be worth relaxing this,
		 * but not enough time to address it for 8.3.)
		 *
		 * Note: it would be bad if this condition failed for an otherwise
		 * mergejoinable FULL JOIN clause, since that would result in
		 * undesirable planner failure.  I believe that is not possible
		 * however; a variable involved in a full join could only appear in
		 * below_outer_join eclasses, which aren't considered redundant.
		 *
		 * This case *can* happen for left/right join clauses: the outer-side
		 * variable could be equated to a constant.  Because we will propagate
		 * that constant across the join clause, the loss of ability to do a
		 * mergejoin is not really all that big a deal, and so it's not clear
		 * that improving this is important.
		 */
		update_mergeclause_eclasses(root, restrictinfo);

		if (EC_MUST_BE_REDUNDANT(restrictinfo->left_ec) ||
			EC_MUST_BE_REDUNDANT(restrictinfo->right_ec))
		{
			have_nonmergeable_joinclause = true;
			continue;			/* can't handle redundant eclasses */
		}

		result_list = lappend(result_list, restrictinfo);
	}

	/*
	 * Report whether mergejoin is allowed (see comment at top of function).
	 */
	switch (jointype)
	{
		case JOIN_RIGHT:
		case JOIN_FULL:
			*mergejoin_allowed = !have_nonmergeable_joinclause;
			break;
		default:
			*mergejoin_allowed = true;
			break;
	}

	return result_list;
}

#ifdef ADB
static Path* get_cheapest_join_path(ClusterJoinContext *jcontext,
									Path *outer_path,
									CostSelector criterion,
									bool must_no_param,
									List **new_reduce_list)
{
	List *outer_reduce_list;
	ListCell *lc;
	Path *cheapest_inner_path;
	Path *path;
	Assert(jcontext->try_match == CLUSTER_TRY_NESTLOOP_JOIN ||
		   jcontext->try_match == CLUSTER_TRY_HASH_JOIN ||
		   jcontext->try_match == CLUSTER_TRY_MERGE_JOIN);

	if(PATH_PARAM_BY_REL(outer_path, jcontext->innerrel))
		return NULL;

	outer_reduce_list = get_reduce_info_list(outer_path);
	cheapest_inner_path = NULL;
	foreach(lc, jcontext->inner_pathlist)
	{
		path = lfirst(lc);
		if ((must_no_param && PATH_PARAM_OUTER_REL(path, jcontext->outerrel)) ||
			(jcontext->try_match  != CLUSTER_TRY_NESTLOOP_JOIN && PATH_PARAM_BY_REL(path, jcontext->outerrel)))
			continue;

		if (cheapest_inner_path != NULL &&
			compare_path_costs(cheapest_inner_path, path, criterion) <= 0)
			continue;

		cheapest_inner_path = path;
	}

	if(cheapest_inner_path)
	{
		bool can_join PG_USED_FOR_ASSERTS_ONLY;
		can_join = reduce_info_list_can_join(outer_reduce_list,
											 get_reduce_info_list(cheapest_inner_path),
											 jcontext->extra->restrictlist,
											 jcontext->jointype,
											 new_reduce_list);
		Assert(can_join);
	}
	return cheapest_inner_path;
}

static void set_all_join_inner_path(ClusterJoinContext *jcontext, Path *outer_path, List *inner_pathlist)
{
	List *outer_reduce_list = get_reduce_info_list(outer_path);
	List *list = NIL;
	List *restrictlist = jcontext->extra->restrictlist;
	ListCell *lc;
	JoinType jointype = jcontext->jointype;
	bool have_upper_reference = jcontext->joinrel->have_upper_reference;

	if(jcontext->inner_pathlist != NIL)
	{
		list_free(jcontext->inner_pathlist);
		jcontext->inner_pathlist = NIL;
	}

	if (have_upper_reference &&
		IsReduceInfoListReplicated(outer_reduce_list) == false)
		return;

	foreach(lc, inner_pathlist)
	{
		Path *inner_path = lfirst(lc);
		List *inner_reduce_list;
		if (inner_path->reduce_is_valid == false &&
			(inner_pathlist == jcontext->innerrel->pathlist ||
			 list_member_ptr(jcontext->innerrel->pathlist, inner_path)))
		{
			inner_path->reduce_info_list = inner_reduce_list = list_make1(MakeCoordinatorReduceInfo());
			inner_path->reduce_is_valid = true;
		}else
		{
			inner_reduce_list = get_reduce_info_list(inner_path);
		}

		if (have_upper_reference &&
			IsReduceInfoListReplicated(inner_reduce_list) == false)
			continue;

		if(reduce_info_list_can_join(outer_reduce_list,
									 inner_reduce_list,
									 restrictlist,
									 jointype,
									 NULL))
		{
			list = lappend(list, lfirst(lc));
		}
	}

	jcontext->inner_pathlist = list;
}

static void add_cluster_paths_to_joinrel(PlannerInfo *root,
										 RelOptInfo *joinrel,
										 RelOptInfo *outerrel,
										 RelOptInfo *innerrel,
										 JoinType jointype,
										 JoinPathExtraData *extra)
{
	ClusterJoinContext jcontext;
	List	   *all_outer_reduce;
	List	   *all_inner_reduce;
	List	   *base_inner_pathlist;
	List	   *base_outer_pathlist;
	ListCell   *lc1;
	PlannerGlobal *glob = root->glob;
	int			resultRelation;
	bool		nestjoinOK;
	bool		useallclauses pg_attribute_unused(); /* when create merge join path we need this, for now just set attribute to unused */
	bool		tried_join;

	if (outerrel->cluster_pathlist == NIL &&
		innerrel->cluster_pathlist == NIL)
		return;

	if (outerrel->cluster_pathlist == NIL)
	{
		if (have_remote_query_path((Path*)outerrel->pathlist))
			return;
		base_outer_pathlist = outerrel->pathlist;
	}else
	{
		base_outer_pathlist = outerrel->cluster_pathlist;
	}

	if (innerrel->cluster_pathlist == NIL)
	{
		if (have_remote_query_path((Path*)innerrel->pathlist))
			return;
		base_inner_pathlist = innerrel->pathlist;
	}else
	{
		base_inner_pathlist = innerrel->cluster_pathlist;
	}

	MemSet(&jcontext, 0, sizeof(jcontext));
	jcontext.root = root;
	jcontext.joinrel = joinrel;
	jcontext.outerrel = outerrel;
	jcontext.innerrel = innerrel;
	jcontext.extra = extra;
	jcontext.jointype = jointype;

	/*
	 * Nestloop only supports inner, left, semi, and anti joins.  Also, if we
	 * are doing a right or full mergejoin, we must use *all* the mergeclauses
	 * as join clauses, else we will not have a valid plan.  (Although these
	 * two flags are currently inverses, keep them separate for clarity and
	 * possible future changes.)
	 */
	switch (jointype)
	{
		case JOIN_INNER:
		case JOIN_LEFT:
		case JOIN_SEMI:
		case JOIN_ANTI:
			nestjoinOK = true;
			useallclauses = false;
			break;
		case JOIN_RIGHT:
		case JOIN_FULL:
			nestjoinOK = false;
			useallclauses = true;
			break;
		case JOIN_UNIQUE_OUTER:
		case JOIN_UNIQUE_INNER:
			jointype = JOIN_INNER;
			nestjoinOK = true;
			useallclauses = false;
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) jointype);
			nestjoinOK = false; /* keep compiler quiet */
			useallclauses = false;
			break;
	}

	/*jcontext.hashclauses = NIL;*/
	if(jointype != JOIN_FULL)
	{
		bool isouterjoin = IS_OUTER_JOIN(jointype);
		foreach(lc1, extra->restrictlist)
		{
			RestrictInfo *restrictinfo = lfirst(lc1);
			/*
			 * If processing an outer join, only use its own join clauses for
			 * hashing.  For inner joins we need not be so picky.
			 */
			if (isouterjoin && restrictinfo->is_pushed_down)
				continue;

			if (!restrictinfo->can_join ||
				restrictinfo->hashjoinoperator == InvalidOid)
				continue;			/* not hashjoinable */

			/*
			 * Check if clause has the form "outer op inner" or "inner op outer".
			 */
			if (!clause_sides_match_join(restrictinfo, outerrel, innerrel))
				continue;			/* no good for these input relations */

			jcontext.hashclauses = lappend(jcontext.hashclauses, restrictinfo);
		}
	}

	tried_join = add_cluster_paths_to_joinrel_internal(&jcontext,
													   base_outer_pathlist,
													   base_inner_pathlist,
													   nestjoinOK,
													   true);

	resultRelation = root->parse->resultRelation;
	all_outer_reduce = all_inner_reduce = NIL;
	if (tried_join == false)
	{
		List *need_reduce_list;
		List *inner_pathlist;
		List *outer_pathlist;

		/* reduce inner paths when inner is not modify target */
		if (resultRelation == 0 ||
			bms_is_member(resultRelation, innerrel->relids) == false)
		{
			if(outerrel->cluster_pathlist == NIL)
				all_outer_reduce = list_make1(MakeCoordinatorReduceInfo());
			else
				all_outer_reduce = GetPathListReduceInfoList(outerrel->cluster_pathlist);
			need_reduce_list = create_inner_reduce_info_for_join(all_outer_reduce, innerrel, jointype, extra);
			inner_pathlist = reduce_paths_for_join(root,
												   innerrel,
												   base_inner_pathlist,
												   need_reduce_list,
												   false);
			tried_join = add_cluster_paths_to_joinrel_internal(&jcontext,
															   base_outer_pathlist,
															   inner_pathlist,
															   nestjoinOK,
															   false);
			list_free(need_reduce_list);
			list_free(inner_pathlist);
		}

		/* reduce outer paths when inner is not modify target */
		if (resultRelation == 0 ||
			bms_is_member(resultRelation, outerrel->relids) == false)
		{
			if(innerrel->cluster_pathlist == NIL)
				all_inner_reduce = list_make1(MakeCoordinatorReduceInfo());
			else
				all_inner_reduce = GetPathListReduceInfoList(innerrel->cluster_pathlist);
			need_reduce_list = create_outer_reduce_info_for_join(all_inner_reduce, outerrel, jointype, extra);
			outer_pathlist = reduce_paths_for_join(root,
												   outerrel,
												   base_outer_pathlist,
												   need_reduce_list,
												   false);
			tried_join |= add_cluster_paths_to_joinrel_internal(&jcontext,
																outer_pathlist,
																base_inner_pathlist,
																nestjoinOK,
																false);
			list_free(need_reduce_list);
			list_free(outer_pathlist);
		}
	}

	/* reduce inner and outer both when modify(if) target not in inner and outer rel */
	if (tried_join == false &&
		all_outer_reduce != NIL &&
		all_inner_reduce != NIL &&
		(jointype == JOIN_INNER || jointype == JOIN_SEMI) &&
		(resultRelation == 0 || bms_is_member(resultRelation, joinrel->relids) == false))
	{
		List *outer_exprs;
		List *inner_exprs;
		get_cluster_join_exprs(outerrel, innerrel, &outer_exprs, &inner_exprs, extra->restrictlist);
		Assert(list_length(outer_exprs) == list_length(inner_exprs));
		if(outer_exprs && inner_exprs)
		{
			List *outer_pathlist = NIL;
			List *inner_pathlist = NIL;
			List *outer_exec = ReduceInfoListGetExecuteOidList(all_outer_reduce);
			List *inner_exec = ReduceInfoListGetExecuteOidList(all_inner_reduce);
			List *storage = list_union_oid(outer_exec, inner_exec);
			bool no_coord_oid = !list_member_oid(storage, PGXCNodeOid);
			storage = SortOidList(storage);
			if(storage)
			{
				List *reduce_outer_pathlist;
				List *reduce_inner_pathlist;
				Path *path;
				reduce_outer_pathlist = GetCheapestReducePathList(outerrel,
																  base_outer_pathlist,
																  NULL,
																  &path);
				if(path && list_member_ptr(reduce_outer_pathlist, path) == false)
					reduce_outer_pathlist = lappend(reduce_outer_pathlist, path);
				reduce_inner_pathlist = GetCheapestReducePathList(innerrel,
																  base_inner_pathlist,
																  NULL,
																  &path);
				if(path && list_member_ptr(reduce_inner_pathlist, path) == false)
					reduce_inner_pathlist = lappend(reduce_inner_pathlist, path);
re_reduce_join_:
				ReducePathListByExpr((Expr*)outer_exprs,
									 root,
									 outerrel,
									 reduce_outer_pathlist,
									 storage,
									 NIL,
									 ReducePathSave2List,
									 (void*)&outer_pathlist,
									 REDUCE_TYPE_HASH,
									 (glob->has_hashmap_rel && no_coord_oid) ? REDUCE_TYPE_HASHMAP:REDUCE_TYPE_IGNORE,
									 glob->has_modulo_rel ? REDUCE_TYPE_MODULO:REDUCE_TYPE_IGNORE,
									 REDUCE_TYPE_NONE);
				ReducePathListByExpr((Expr*)inner_exprs,
									 root,
									 innerrel,
									 reduce_inner_pathlist,
									 storage,
									 NIL,
									 ReducePathSave2List,
									 (void*)&inner_pathlist,
									 REDUCE_TYPE_HASH,
									 (glob->has_hashmap_rel && no_coord_oid) ? REDUCE_TYPE_HASHMAP:REDUCE_TYPE_IGNORE,
									 glob->has_modulo_rel ? REDUCE_TYPE_MODULO:REDUCE_TYPE_IGNORE,
									 REDUCE_TYPE_NONE);
			}
			tried_join |= add_cluster_paths_to_joinrel_internal(&jcontext, outer_pathlist, inner_pathlist, nestjoinOK, false);
			list_free(outer_pathlist);
			list_free(inner_pathlist);
			if(storage && no_coord_oid && enable_coordinator_calculate)
			{
				outer_pathlist = inner_pathlist = NIL;
				storage = SortOidList(lappend_oid(storage, PGXCNodeOid));
				no_coord_oid = false;
				goto re_reduce_join_;
			}
			list_free(outer_exec);
			list_free(inner_exec);
			list_free(storage);
			list_free(outer_exprs);
			list_free(inner_exprs);
		}
	}

	/* reduce inner and outer both to coordinator */
	if (tried_join == false &&
		list_length(joinrel->cluster_pathlist) == 0 &&
		(resultRelation == 0 || bms_is_member(resultRelation, joinrel->relids) == false))
	{
		List *outer_pathlist = outerrel->cluster_pathlist == NIL ? outerrel->pathlist:coord_paths_for_join(root, outerrel, false);
		List *inner_pathlist = innerrel->cluster_pathlist == NIL ? innerrel->pathlist:coord_paths_for_join(root, innerrel, false);

		add_cluster_paths_to_joinrel_internal(&jcontext, outer_pathlist, inner_pathlist, nestjoinOK, false);

		if(outer_pathlist != outerrel->pathlist)
			list_free(outer_pathlist);
		if(inner_pathlist != innerrel->pathlist)
			list_free(inner_pathlist);
	}

	/* try partial hash join */
	if (joinrel->consider_parallel &&
		jcontext.hashclauses != NIL &&
		jcontext.jointype != JOIN_UNIQUE_OUTER &&
		jcontext.jointype != JOIN_UNIQUE_INNER && /* for now not support */
		jcontext.jointype != JOIN_FULL &&
		jcontext.jointype != JOIN_RIGHT &&
		outerrel->cluster_partial_pathlist != NIL &&
		bms_is_empty(joinrel->lateral_relids))
	{
		Path *outer_path;
		Path *inner_path;
		List *reduce_list;
		List *outer_pathlist;
		List *need_reduce_list;
		List *inner_pathlist = NIL;

		/* find all parallel safe paths */
		foreach(lc1, innerrel->cluster_pathlist)
		{
			inner_path = lfirst(lc1);
			if (inner_path->parallel_safe)
				inner_pathlist = lappend(inner_pathlist, inner_path);
		}

		if (inner_pathlist != NIL)
		{
			tried_join = false;
			foreach(lc1, outerrel->cluster_partial_pathlist)
			{
				outer_path = lfirst(lc1);
				if (PATH_PARAM_BY_REL(outer_path, innerrel))
					continue;

				set_all_join_inner_path(&jcontext, outer_path, inner_pathlist);
				inner_path = get_cheapest_join_path(&jcontext, outer_path, TOTAL_COST, true, &reduce_list);
				if (inner_path)
				{
					try_partial_hashjoin_path(root, joinrel,
											  outer_path, inner_path,
											  jcontext.hashclauses, jointype,
											  reduce_list, extra,
											  false);
					tried_join = true;
				}
			}

			/* try parallel reduce outer */
			if (tried_join == false)
			{
				all_inner_reduce = GetPathListReduceInfoList(inner_pathlist);
				need_reduce_list = create_outer_reduce_info_for_join(all_inner_reduce,
																	 outerrel,
																	 jcontext.jointype,
																	 extra);
				outer_pathlist = reduce_paths_for_join(root,
													   outerrel,
													   outerrel->cluster_partial_pathlist,
													   need_reduce_list,
													   true);
				foreach(lc1, outer_pathlist)
				{
					outer_path = lfirst(lc1);
					if (PATH_PARAM_BY_REL(outer_path, innerrel))
						continue;

					set_all_join_inner_path(&jcontext, outer_path, inner_pathlist);
					inner_path = get_cheapest_join_path(&jcontext, outer_path, TOTAL_COST, true, &reduce_list);
					if (inner_path)
						try_partial_hashjoin_path(root, joinrel,
												  outer_path, inner_path,
												  jcontext.hashclauses, jointype,
												  reduce_list, extra,
												  false);
				}
				list_free(need_reduce_list);
				list_free(outer_pathlist);
			}

			list_free(inner_pathlist);
		}

		if (enable_parallel_hash &&
			innerrel->cluster_partial_pathlist != NIL)
		{
			tried_join = false;
			foreach(lc1, outerrel->cluster_partial_pathlist)
			{
				outer_path = lfirst(lc1);
				if (PATH_PARAM_BY_REL(outer_path, innerrel))
					continue;

				set_all_join_inner_path(&jcontext, outer_path, innerrel->cluster_partial_pathlist);
				inner_path = get_cheapest_join_path(&jcontext, outer_path, TOTAL_COST, true, &reduce_list);
				if (inner_path)
				{
					try_partial_hashjoin_path(root, joinrel,
											  outer_path, inner_path,
											  jcontext.hashclauses, jointype,
											  reduce_list, extra,
											  true);
					tried_join = true;
				}
			}

			/* try parallel reduce outer */
			if (tried_join == false)
			{
				all_inner_reduce = GetPathListReduceInfoList(innerrel->cluster_partial_pathlist);
				need_reduce_list = create_outer_reduce_info_for_join(all_inner_reduce,
																	 outerrel,
																	 jcontext.jointype,
																	 extra);
				outer_pathlist = reduce_paths_for_join(root,
													   outerrel,
													   outerrel->cluster_partial_pathlist,
													   need_reduce_list,
													   true);
				foreach(lc1, outer_pathlist)
				{
					outer_path = lfirst(lc1);
					if (PATH_PARAM_BY_REL(outer_path, innerrel))
						continue;

					set_all_join_inner_path(&jcontext, outer_path, innerrel->cluster_partial_pathlist);
					inner_path = get_cheapest_join_path(&jcontext, outer_path, TOTAL_COST, true, &reduce_list);
					if (inner_path)
						try_partial_hashjoin_path(root, joinrel,
												  outer_path, inner_path,
												  jcontext.hashclauses, jointype,
												  reduce_list, extra,
												  true);
				}
				list_free(need_reduce_list);
				list_free(outer_pathlist);
			}
		}
	}
}

static bool add_cluster_paths_to_joinrel_internal(ClusterJoinContext *jcontext,
												  List *outer_pathlist,
												  List *inner_pathlist,
												  bool nestjoinOK,
												  bool first_try)
{
	ListCell *lc1;
	Path *outer_path;
	Path *inner_path;
	List *new_reduce_list;
	bool tried;
	if(outer_pathlist == NIL || inner_pathlist == NIL)
		return false;

	tried = false;
	foreach(lc1, outer_pathlist)
	{
		outer_path = (Path *) lfirst(lc1);

		/*
		 * We cannot use an outer path that is parameterized by the inner rel.
		 */
		if (PATH_PARAM_BY_REL(outer_path, jcontext->innerrel))
			continue;

		if (outer_path->reduce_is_valid == false &&
			outer_pathlist == jcontext->outerrel->pathlist)
		{
			outer_path->reduce_info_list = list_make1(MakeCoordinatorReduceInfo());
			outer_path->reduce_is_valid = true;
		}

		set_all_join_inner_path(jcontext, outer_path, inner_pathlist);
		jcontext->merge_pathkeys = NIL;
		if (jcontext->innerrel->cluster_pathlist == inner_pathlist ||
			jcontext->innerrel->pathlist == inner_pathlist)
		{
			jcontext->try_match = CLUSTER_TRY_NESTLOOP_JOIN;
			inner_path = get_cheapest_join_path(jcontext,
												outer_path,
												TOTAL_COST,
												false,
												&new_reduce_list);
			if(inner_path == NULL)
				continue;
			tried = true;

			/*
			 * The result will have this sort order (even if it is implemented as
			 * a nestloop, and even if some of the mergeclauses are implemented by
			 * qpquals rather than as true mergeclauses):
			 */
			jcontext->merge_pathkeys = build_join_pathkeys(jcontext->root,
														   jcontext->joinrel,
														   jcontext->jointype,
														   outer_path->pathkeys);

			if (jcontext->jointype == JOIN_UNIQUE_INNER)
			{
				/*
				 * Consider nestloop join, but only with the unique-ified cheapest
				 * inner path
				 */
				try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
				if (PATH_REQ_OUTER(outer_path) == NULL &&
					PATH_PARAM_OUTER_REL(inner_path, jcontext->outerrel) &&
					(inner_path = get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, true, &new_reduce_list)) != NULL)
				{
					try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
				}
			}
			else if (nestjoinOK)
			{
				Path *unique_outer_path = NULL;
				JoinType jointype = jcontext->jointype;
				if(jointype == JOIN_UNIQUE_OUTER)
				{
					unique_outer_path = (Path*)create_cluster_unique_path(jcontext->root,
																		  jcontext->outerrel,
																		  outer_path,
																		  jcontext->extra->sjinfo);
					jointype = JOIN_INNER;
				}
				if(first_try)
				{
					ListCell   *lc2;
					List *outer_reduce_list = get_reduce_info_list(outer_path);

					foreach(lc2, jcontext->innerrel->cheapest_cluster_parameterized_paths)
					{
						List	   *tmp_reduce_list = NIL;
						Path	   *innerpath = (Path *) lfirst(lc2);
						if (reduce_info_list_can_join(outer_reduce_list,
													 get_reduce_info_list(innerpath),
													 jcontext->extra->restrictlist,
													 jcontext->jointype,
													 &tmp_reduce_list) &&
							(jcontext->joinrel->have_upper_reference == false ||
							 IsReduceInfoListReplicated(tmp_reduce_list)))
						{
							Assert(tmp_reduce_list != NIL);
							try_nestloop_path(jcontext->root,
											  jcontext->joinrel,
											  unique_outer_path ? unique_outer_path:outer_path,
											  innerpath,
											  jcontext->merge_pathkeys,
											  jointype,
											  ADB_ONLY_ARG_COMMA(tmp_reduce_list)
											  jcontext->extra);
						}
					}
				}

				/* Also consider materialized form of the cheapest inner path */
				if (enable_material &&
					!ExecMaterializesOutput(inner_path->pathtype))
				{
					MaterialPath *matpath = create_material_path(jcontext->innerrel, inner_path);
					try_nestloop_path(jcontext->root,
									  jcontext->joinrel,
									  unique_outer_path ? unique_outer_path:outer_path,
									  (Path*)matpath,
									  jcontext->merge_pathkeys,
									  jointype,
									  ADB_ONLY_ARG_COMMA(new_reduce_list)
									  jcontext->extra);
				}

				if(have_cluster_reduce_path(inner_path) == false)
				{
					try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
					if (PATH_REQ_OUTER(outer_path) == NULL &&
						PATH_PARAM_OUTER_REL(inner_path, jcontext->outerrel) &&
						(inner_path=get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, true, &new_reduce_list)) != NULL)
					{
						try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
					}
				}
			}
		}

		jcontext->try_match = CLUSTER_TRY_HASH_JOIN;
		if (jcontext->hashclauses != NIL &&
			(inner_path=get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, false, &new_reduce_list)) != NULL)
		{
			try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
			if (PATH_REQ_OUTER(outer_path) == NULL &&
				PATH_REQ_OUTER(inner_path) != NULL &&
				(inner_path=get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, true, &new_reduce_list)) != NULL)
			{
				try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
			}
			tried = true;
		}

		if(jcontext->jointype == JOIN_UNIQUE_OUTER)
			continue;

		jcontext->try_match = CLUSTER_TRY_MERGE_JOIN;
		jcontext->merge_clauses = NULL;
		if((inner_path=get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, false, &new_reduce_list)) != NULL)
		{
			if(jcontext->merge_pathkeys == NIL)
				jcontext->merge_pathkeys = build_join_pathkeys(jcontext->root,
															   jcontext->joinrel,
															   jcontext->jointype,
															   outer_path->pathkeys);

			jcontext->merge_clauses = find_mergeclauses_for_outer_pathkeys(jcontext->root,
																		   outer_path->pathkeys,
																		   jcontext->extra->mergeclause_list);
			try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
		}

		if (nestjoinOK &&
			jcontext->hashclauses == NIL &&
			jcontext->merge_clauses == NIL)
		{
			jcontext->try_match = CLUSTER_TRY_NESTLOOP_JOIN;
			inner_path=get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, false, &new_reduce_list);
			if (inner_path != NULL)
			{
				try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
				if (PATH_REQ_OUTER(outer_path) == NULL &&
					PATH_REQ_OUTER(inner_path) != NULL &&
					(inner_path=get_cheapest_join_path(jcontext, outer_path, TOTAL_COST, true, &new_reduce_list)) != NULL)
				{
					try_cluster_join_path(jcontext, outer_path, inner_path, new_reduce_list);
				}
				tried = true;
			}
		}
	}
	return tried;
}

static void try_cluster_join_path(ClusterJoinContext *jcontext, Path *outer_path, Path *inner_path, List *new_reduce_list)
{
	JoinType jointype;
	AssertArg(jcontext && new_reduce_list);

	if (jcontext->joinrel->have_upper_reference &&
		!IsReduceInfoListReplicated(new_reduce_list))
		return;

	if(jcontext->jointype == JOIN_UNIQUE_OUTER)
	{
		outer_path = (Path*)create_cluster_unique_path(jcontext->root,
													   jcontext->outerrel,
													   outer_path,
													   jcontext->extra->sjinfo);
		jointype = JOIN_INNER;
	}else if(jcontext->jointype == JOIN_UNIQUE_INNER)
	{
		inner_path = (Path*)create_cluster_unique_path(jcontext->root,
													   jcontext->innerrel,
													   inner_path,
													   jcontext->extra->sjinfo);
		jointype = JOIN_INNER;
	}else
	{
		jointype = jcontext->jointype;
	}

	if (jcontext->try_match & CLUSTER_TRY_NESTLOOP_JOIN)
	{
		try_nestloop_path(jcontext->root,
						  jcontext->joinrel,
						  outer_path,
						  inner_path,
						  jcontext->merge_pathkeys,
						  jointype,
						  new_reduce_list,
						  jcontext->extra);
	}

	if ((jcontext->try_match & CLUSTER_TRY_HASH_JOIN) &&
		jcontext->hashclauses != NIL &&
		!PATH_PARAM_BY_REL(inner_path, jcontext->outerrel))
	{
		AssertArg(jcontext->hashclauses != NIL);
		try_hashjoin_path(jcontext->root,
						  jcontext->joinrel,
						  outer_path,
						  inner_path,
						  jcontext->hashclauses,
						  jointype,
						  new_reduce_list,
						  jcontext->extra);
	}

	if ((jcontext->try_match & CLUSTER_TRY_MERGE_JOIN) &&
		(jcontext->inner_pathlist || !PATH_PARAM_BY_REL(inner_path, jcontext->outerrel)))
	{
		List *innersortkeys;
		bool useallclauses;
		if (jcontext->merge_clauses == NIL)
		{
			if(jointype == JOIN_FULL)
				 /* okay to try for mergejoin */ ;
			else
				return;
		}

		useallclauses = (jointype == JOIN_RIGHT || jointype == JOIN_FULL);
		if (useallclauses &&
			list_length(jcontext->merge_clauses) != list_length(jcontext->extra->mergeclause_list))
			return;

		/* Compute the required ordering of the inner path */
		innersortkeys = make_inner_pathkeys_for_merge(jcontext->root,
													  jcontext->merge_clauses,
													  outer_path->pathkeys);
		if(!PATH_PARAM_BY_REL(inner_path, jcontext->outerrel))
		{
			try_mergejoin_path(jcontext->root,
							   jcontext->joinrel,
							   outer_path,
							   inner_path,
							   jcontext->merge_pathkeys,
							   jcontext->merge_clauses,
							   NIL,
							   innersortkeys,
							   jointype,
							   new_reduce_list,
							   jcontext->extra,
							   false);
		}
		if(jcontext->jointype == JOIN_UNIQUE_INNER)
			return;

		if(jcontext->inner_pathlist != NIL)
		{
			List *trialsortkeys;
			List *reduce_info_list;
			List *outer_reduce_list;
			Path *cheapest_startup_inner;
			Path *cheapest_total_inner;
			int num_sortkeys;

			if (!PATH_PARAM_BY_REL(inner_path, jcontext->outerrel) &&
				pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
				cheapest_startup_inner = cheapest_total_inner = inner_path;
			else
				cheapest_startup_inner = cheapest_total_inner = NULL;

			num_sortkeys = list_length(innersortkeys);
			if(num_sortkeys > 1 && !useallclauses)
				trialsortkeys = list_copy(innersortkeys);	/* need modifiable copy */
			else
				trialsortkeys = innersortkeys;		/* won't really truncate */

			outer_reduce_list = get_reduce_info_list(outer_path);
			for(;num_sortkeys > 0; num_sortkeys--)
			{
				List *newclauses = NIL;

				trialsortkeys = list_truncate(trialsortkeys, num_sortkeys);
				inner_path = get_cheapest_path_for_pathkeys(jcontext->inner_pathlist,
															trialsortkeys,
															NULL,
															TOTAL_COST,
															false);
				if (inner_path != NULL &&
					(cheapest_total_inner == NULL ||
					 compare_path_costs(inner_path, cheapest_total_inner, TOTAL_COST) < 0))
				{
					bool can_join;
					if (num_sortkeys < list_length(innersortkeys))
					{
						newclauses =
							trim_mergeclauses_for_inner_pathkeys(jcontext->root,
																 jcontext->merge_clauses,
																 trialsortkeys);
					}
					else
					{
						newclauses = jcontext->merge_clauses;
					}

					if (newclauses == NIL)
						can_join = false;
					else
						can_join = reduce_info_list_can_join(outer_reduce_list,
															get_reduce_info_list(inner_path),
															jcontext->extra->restrictlist,
															jointype,
															&reduce_info_list);
					if (can_join)
					{
						try_mergejoin_path(jcontext->root,
										   jcontext->joinrel,
										   outer_path,
										   inner_path,
										   jcontext->merge_pathkeys,
										   newclauses,
										   NIL,
										   NIL,
										   jointype,
										   reduce_info_list,
										   jcontext->extra,
										   false);
						cheapest_total_inner = inner_path;
					}
				}

				inner_path = get_cheapest_path_for_pathkeys(jcontext->inner_pathlist,
															trialsortkeys,
															NULL,
															STARTUP_COST,
															false);
				if (inner_path != NULL &&
					(cheapest_startup_inner == NULL ||
					 compare_path_costs(inner_path, cheapest_startup_inner, STARTUP_COST) < 0))
				{
					if(inner_path != cheapest_total_inner)
					{
						bool can_join;
						if(newclauses == NULL)
						{
							if(num_sortkeys < list_length(innersortkeys))
							{
								newclauses = trim_mergeclauses_for_inner_pathkeys(jcontext->root,
																				  jcontext->merge_clauses,
																				  trialsortkeys);
							}else
							{
								newclauses = jcontext->merge_clauses;
							}
						}

						if (newclauses == NIL)
							can_join = false;
						else
							can_join = reduce_info_list_can_join(outer_reduce_list,
																 get_reduce_info_list(inner_path),
																 jcontext->extra->restrictlist,
																 jointype,
																 &reduce_info_list);
						if(can_join)
						{
							try_mergejoin_path(jcontext->root,
											   jcontext->joinrel,
											   outer_path,
											   inner_path,
											   jcontext->merge_pathkeys,
											   newclauses,
											   NIL,
											   NIL,
											   jointype,
											   reduce_info_list,
											   jcontext->extra,
											   false);
							cheapest_startup_inner = inner_path;
						}
					}
				}
				if(useallclauses)
					break;
			}
		}
	}
}

static void sort_cluster_inner_and_outer(ClusterJoinContext *jcontext, Path *outerpath, Path *innerpath,
										 List *all_pathkeys, List *reduce_list)
{
	ListCell *lc;
	JoinPathExtraData *extra;
	JoinType jointype;
	AssertArg(reduce_list != NIL);
	if(all_pathkeys == NIL)
		return;

	if (jcontext->jointype == JOIN_UNIQUE_OUTER)
	{
		outerpath = (Path*)create_cluster_unique_path(jcontext->root,
													  jcontext->outerrel,
													  outerpath,
													  jcontext->extra->sjinfo);
		jointype = JOIN_INNER;
	}else if(jcontext->jointype == JOIN_UNIQUE_INNER)
	{
		innerpath = (Path*)create_cluster_unique_path(jcontext->root,
													  jcontext->innerrel,
													  innerpath,
													  jcontext->extra->sjinfo);
		jointype = JOIN_INNER;
	}else
	{
		jointype = jcontext->jointype;
	}

	extra = jcontext->extra;

	foreach(lc, all_pathkeys)
	{
		List	   *front_pathkey = (List *) lfirst(lc);
		List	   *cur_mergeclauses;
		List	   *outerkeys;
		List	   *innerkeys;
		List	   *merge_pathkeys;

		/* Make a pathkey list with this guy first */
		if (lc != list_head(all_pathkeys))
			outerkeys = lcons(front_pathkey,
							  list_delete_ptr(list_copy(all_pathkeys),
											  front_pathkey));
		else
			outerkeys = all_pathkeys;	/* no work at first one... */

		/* Sort the mergeclauses into the corresponding ordering */
		cur_mergeclauses = find_mergeclauses_for_outer_pathkeys(jcontext->root,
																outerkeys,
																extra->mergeclause_list);

		/* Should have used them all... */
		Assert(list_length(cur_mergeclauses) == list_length(extra->mergeclause_list));

		/* Build sort pathkeys for the inner side */
		innerkeys = make_inner_pathkeys_for_merge(jcontext->root,
												  cur_mergeclauses,
												  outerkeys);

		/* Build pathkeys representing output sort order */
		merge_pathkeys = build_join_pathkeys(jcontext->root, jcontext->joinrel, jointype, outerkeys);

		/*
		 * And now we can make the path.
		 *
		 * Note: it's possible that the cheapest paths will already be sorted
		 * properly.  try_mergejoin_path will detect that case and suppress an
		 * explicit sort step, so we needn't do so here.
		 */
		try_mergejoin_path(jcontext->root,
						   jcontext->joinrel,
						   outerpath,
						   innerpath,
						   merge_pathkeys,
						   cur_mergeclauses,
						   outerkeys,
						   innerkeys,
						   jointype,
						   ADB_ONLY_ARG_COMMA(reduce_list)
						   extra,
						   false);
	}
}

static List *create_inner_reduce_info_for_join(List *outer_reduce_list, RelOptInfo *innerrel,
											   JoinType jointype, JoinPathExtraData *extra)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	List *exprList;
	List *need_reduce_list = NIL;

	foreach(lc, outer_reduce_list)
	{
		rinfo = lfirst(lc);
		if (IsReduceInfoCoordinator(rinfo) ||
			IsReduceInfoReplicated(rinfo))
		{
			if(ReduceInfoListMember(need_reduce_list, rinfo) == false)
				need_reduce_list = lappend(need_reduce_list, rinfo);
			continue;
		}

		if (IsReduceInfoInOneNode(rinfo))
		{
			ReduceInfo *new_rinfo = MakeRandomReduceInfo(list_difference_oid(rinfo->storage_nodes, rinfo->exclude_exec));
			Assert(IsReduceInfoInOneNode(new_rinfo));
			if (ReduceInfoListMember(need_reduce_list, new_rinfo) == false)
				need_reduce_list = lappend(need_reduce_list, new_rinfo);
			else
				FreeReduceInfo(new_rinfo);
		}
/* ADBQ TODO add unique inner path */
		switch(jointype)
		{
		case JOIN_INNER:
		case JOIN_UNIQUE_INNER:
		case JOIN_UNIQUE_OUTER:
		case JOIN_SEMI:
			need_reduce_list = create_and_append_replicate_reduceinfo(need_reduce_list, rinfo);
			if (!IsReduceInfoByValue(rinfo) ||
				(exprList = FindJoinEqualExprs(rinfo, extra->restrictlist, innerrel)) == NIL)
				continue;
			rinfo = MakeReduceInfoAs(rinfo, exprList);
			list_free(exprList);
			if(ReduceInfoListMember(need_reduce_list, rinfo))
				FreeReduceInfo(rinfo);
			else
				need_reduce_list = lappend(need_reduce_list, rinfo);
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			need_reduce_list = create_and_append_replicate_reduceinfo(need_reduce_list, rinfo);
			break;
		case JOIN_FULL:
		case JOIN_RIGHT:
		default:
			break;
		}
	}

	return need_reduce_list;
}

static List *create_outer_reduce_info_for_join(List *inner_reduce_list, RelOptInfo *outerrel,
											   JoinType jointype, JoinPathExtraData *extra)
{
	ListCell *lc;
	ReduceInfo *rinfo;
	List *exprList;
	List *need_reduce_list = NIL;

	foreach(lc, inner_reduce_list)
	{
		rinfo = lfirst(lc);
		if (IsReduceInfoCoordinator(rinfo) ||
			IsReduceInfoReplicated(rinfo))
		{
			if(ReduceInfoListMember(need_reduce_list, rinfo) == false)
				need_reduce_list = lappend(need_reduce_list, rinfo);
			continue;
		}

		if (IsReduceInfoInOneNode(rinfo))
		{
			ReduceInfo *new_rinfo = MakeRandomReduceInfo(list_difference_oid(rinfo->storage_nodes, rinfo->exclude_exec));
			Assert(IsReduceInfoInOneNode(new_rinfo));
			if (ReduceInfoListMember(need_reduce_list, new_rinfo) == false)
				need_reduce_list = lappend(need_reduce_list, new_rinfo);
			else
				FreeReduceInfo(new_rinfo);
		}
/* ADBQ TODO add unique outer path */
		switch(jointype)
		{
		case JOIN_INNER:
		case JOIN_UNIQUE_INNER:
		case JOIN_UNIQUE_OUTER:
		case JOIN_SEMI:
			need_reduce_list = create_and_append_replicate_reduceinfo(need_reduce_list, rinfo);
			if (!IsReduceInfoByValue(rinfo) ||
				(exprList = FindJoinEqualExprs(rinfo, extra->restrictlist, outerrel)) == NIL)
				continue;
			rinfo = MakeReduceInfoAs(rinfo, exprList);
			list_free(exprList);
			if(ReduceInfoListMember(need_reduce_list, rinfo))
				FreeReduceInfo(rinfo);
			else
				need_reduce_list = lappend(need_reduce_list, rinfo);
			break;
		case JOIN_RIGHT:
			need_reduce_list = create_and_append_replicate_reduceinfo(need_reduce_list, rinfo);
			break;
		default:
			break;
		}
	}

	return need_reduce_list;
}

static List *create_and_append_replicate_reduceinfo(List *list, const ReduceInfo *rinfo)
{
	List *exec_list = list_difference_oid(rinfo->storage_nodes, rinfo->exclude_exec);
	ReduceInfo *new_rinfo = MakeReplicateReduceInfo(exec_list);
	if (ReduceInfoListMember(list, new_rinfo))
		FreeReduceInfo(new_rinfo);
	else
		list = lappend(list, new_rinfo);
	return list;
}

static List *reduce_paths_for_join(PlannerInfo *root, RelOptInfo *rel, List *pathlist, List *reduce_list, bool parallel)
{
	ListCell *lc1;
	ListCell *lc2;
	Path *cheapest_total_path;
	Path *cheapest_startup_path;
	Path *path;
	List *result = NIL;

	pathlist = GetCheapestReducePathList(rel, pathlist, &cheapest_startup_path, &cheapest_total_path);
	if(cheapest_total_path == NULL)
		return NIL;

	foreach(lc1, reduce_list)
	{
		if (cheapest_startup_path)
			ReducePathUsingReduceInfo(root, rel, cheapest_startup_path, ReducePathSave2List, (void*)&result, lfirst(lc1));
		if (cheapest_total_path && cheapest_total_path != cheapest_startup_path)
			ReducePathUsingReduceInfo(root, rel, cheapest_total_path, ReducePathSave2List, (void*)&result, lfirst(lc1));
	}
	foreach(lc1, pathlist)
	{
		path = lfirst(lc1);
		if(path == cheapest_total_path || path == cheapest_startup_path)
			continue;
		foreach(lc2, reduce_list)
		{
			if (parallel == false &&
				path->pathkeys != NIL)	/* cluster merge reduce not support parallel */
				result = lappend(result,
								 create_cluster_reduce_path(root, path, list_make1(lfirst(lc2)), rel, path->pathkeys));
			result = lappend(result,
							 create_cluster_reduce_path(root, path, list_make1(lfirst(lc2)), rel, NIL));
		}
	}
	return result;
}

static List *coord_paths_for_join(PlannerInfo *root, RelOptInfo *rel, bool parallel)
{
	List *reduce_list = list_make1(MakeCoordinatorReduceInfo());
	List *result = reduce_paths_for_join(root, rel, rel->cluster_pathlist, reduce_list, parallel);
	list_free(reduce_list);
	return result;
}

static bool get_cluster_join_exprs(RelOptInfo *outerrel, RelOptInfo *innerrel,
								   List **outer_exprs, List **inner_exprs,
								   List *restrictlist)
{
	ListCell *lc;
	List *outer = NIL;
	List *inner = NIL;
	foreach(lc, restrictlist)
	{
		RestrictInfo *ri = lfirst(lc);

		/* only support X=X expression */
		if (!is_opclause(ri->clause) ||
			ri->left_relids == NULL ||
			ri->right_relids == NULL ||
			expression_have_subplan(ri->clause) ||
			!op_is_equivalence(((OpExpr *)(ri->clause))->opno))
			continue;

		if(bms_is_subset(ri->left_relids, outerrel->relids) && bms_is_subset(ri->right_relids, innerrel->relids))
		{
			outer = lappend(outer, get_leftop(ri->clause));
			inner = lappend(inner, get_rightop(ri->clause));
		}else if(bms_is_subset(ri->left_relids, innerrel->relids) && bms_is_subset(ri->right_relids, outerrel->relids))
		{
			outer = lappend(outer, get_rightop(ri->clause));
			inner = lappend(inner, get_leftop(ri->clause));
		}
	}
	*outer_exprs = outer;
	*inner_exprs = inner;
	return outer != NIL;
}
#endif /* ADB */
