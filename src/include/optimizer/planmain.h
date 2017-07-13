/*-------------------------------------------------------------------------
 *
 * planmain.h
 *	  prototypes for various files in optimizer/plan
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planmain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANMAIN_H
#define PLANMAIN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

/* possible values for force_parallel_mode */
typedef enum
{
	FORCE_PARALLEL_OFF,
	FORCE_PARALLEL_ON,
	FORCE_PARALLEL_REGRESS
}	ForceParallelMode;

/* GUC parameters */
#define DEFAULT_CURSOR_TUPLE_FRACTION 0.1
extern double cursor_tuple_fraction;
extern int	force_parallel_mode;

/* query_planner callback to compute query_pathkeys */
typedef void (*query_pathkeys_callback) (PlannerInfo *root, void *extra);

/*
 * prototypes for plan/planmain.c
 */
extern RelOptInfo *query_planner(PlannerInfo *root, List *tlist,
			  query_pathkeys_callback qp_callback, void *qp_extra);

/*
 * prototypes for plan/planagg.c
 */
extern void preprocess_minmax_aggregates(PlannerInfo *root, List *tlist);

/*
 * prototypes for plan/createplan.c
 */
extern Plan *create_plan(PlannerInfo *root, Path *best_path);
extern ForeignScan *make_foreignscan(List *qptlist, List *qpqual,
				 Index scanrelid, List *fdw_exprs, List *fdw_private,
				 List *fdw_scan_tlist, List *fdw_recheck_quals,
				 Plan *outer_plan);
extern Plan *materialize_finished_plan(Plan *subplan);
extern bool is_projection_capable_path(Path *path);
extern bool is_projection_capable_plan(Plan *plan);

/* External use of these functions is deprecated: */
extern Sort *make_sort_from_sortclauses(List *sortcls, Plan *lefttree);
extern Agg *make_agg(List *tlist, List *qual,
		 AggStrategy aggstrategy, AggSplit aggsplit,
		 int numGroupCols, AttrNumber *grpColIdx, Oid *grpOperators,
		 List *groupingSets, List *chain,
		 double dNumGroups, Plan *lefttree);
extern Limit *make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount);

/*
 * prototypes for plan/initsplan.c
 */
extern int	from_collapse_limit;
extern int	join_collapse_limit;

extern void add_base_rels_to_query(PlannerInfo *root, Node *jtnode);
extern void build_base_rel_tlists(PlannerInfo *root, List *final_tlist);
extern void add_vars_to_targetlist(PlannerInfo *root, List *vars,
					   Relids where_needed, bool create_new_ph);
extern void find_lateral_references(PlannerInfo *root);
extern void create_lateral_join_info(PlannerInfo *root);
extern List *deconstruct_jointree(PlannerInfo *root);
extern void distribute_restrictinfo_to_rels(PlannerInfo *root,
								RestrictInfo *restrictinfo);
extern void process_implied_equality(PlannerInfo *root,
						 Oid opno,
						 Oid collation,
						 Expr *item1,
						 Expr *item2,
						 Relids qualscope,
						 Relids nullable_relids,
						 bool below_outer_join,
						 bool both_const);
extern RestrictInfo *build_implied_join_equality(Oid opno,
							Oid collation,
							Expr *item1,
							Expr *item2,
							Relids qualscope,
							Relids nullable_relids);
extern void match_foreign_keys_to_quals(PlannerInfo *root);

/*
 * prototypes for plan/analyzejoins.c
 */
extern List *remove_useless_joins(PlannerInfo *root, List *joinlist);
extern bool query_supports_distinctness(Query *query);
extern bool query_is_distinct_for(Query *query, List *colnos, List *opids);

/*
 * prototypes for plan/setrefs.c
 */
extern Plan *set_plan_references(PlannerInfo *root, Plan *plan);
extern void record_plan_function_dependency(PlannerInfo *root, Oid funcid);
extern void extract_query_dependencies(Node *query,
						   List **relationOids,
						   List **invalItems,
						   bool *hasRowSecurity);
#ifdef ADB
/*
 * prototypes for plan/pgxcplan.c
 */
typedef struct ReduceExprInfo
{
	Expr	   *expr;		/* reduce expr for path */
	Bitmapset  *varattnos;	/* var attno(s) */
	List	   *attnoList;	/* var attno(s), order by useing by reduce expr */
	Index		relid;
}ReduceExprInfo;

extern Plan *create_remotedml_plan(PlannerInfo *root, Plan *topplan,
									CmdType cmdtyp);
extern Plan *create_remotegrouping_plan(PlannerInfo *root, Plan *local_plan);
extern Sort *make_sort_from_groupcols(List *groupcls, AttrNumber *grpColIdx, Plan *lefttree);
extern Plan *create_remotequery_plan(PlannerInfo *root, RemoteQueryPath *best_path);
extern Plan *create_remotesort_plan(PlannerInfo *root, Plan *local_plan);
extern Plan *create_remotelimit_plan(PlannerInfo *root, Plan *local_plan);
extern List *pgxc_order_qual_clauses(PlannerInfo *root, List *clauses);
extern List *pgxc_build_path_tlist(PlannerInfo *root, Path *path);
extern void pgxc_copy_path_costsize(Plan *dest, Path *src);
extern Plan *pgxc_create_gating_plan(PlannerInfo *root, Path *path, Plan *plan, List *quals);
extern Node *pgxc_replace_nestloop_params(PlannerInfo *root, Node *expr);
extern List* get_remote_nodes(Plan *top_plan);
extern List* get_reduce_info_list(Path *path);
extern void free_reduce_info_list(List *list);
extern void free_reduce_info(ReduceExprInfo *info);
extern bool is_grouping_reduce_expr(PathTarget *target, ReduceExprInfo *info);
extern bool is_reduce_list_can_join(List *outer_reduce_list, List *inner_reduce_list, List *restrictlist);
#endif

#endif   /* PLANMAIN_H */
