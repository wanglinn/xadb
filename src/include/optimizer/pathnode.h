/*-------------------------------------------------------------------------
 *
 * pathnode.h
 *	  prototypes for pathnode.c, relnode.c.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/pathnode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PATHNODE_H
#define PATHNODE_H

#include "nodes/bitmapset.h"
#include "nodes/pathnodes.h"


/*
 * prototypes for pathnode.c
 */
extern int	compare_path_costs(Path *path1, Path *path2,
							   CostSelector criterion);
extern int	compare_fractional_path_costs(Path *path1, Path *path2,
										  double fraction);
extern void set_cheapest(RelOptInfo *parent_rel);
extern void add_path(RelOptInfo *parent_rel, Path *new_path);
extern bool add_path_precheck(RelOptInfo *parent_rel,
							  Cost startup_cost, Cost total_cost,
							  List *pathkeys, Relids required_outer);
extern void add_partial_path(RelOptInfo *parent_rel, Path *new_path);
extern bool add_partial_path_precheck(RelOptInfo *parent_rel,
									  Cost total_cost, List *pathkeys);
#ifdef ADB
extern void add_cluster_path(RelOptInfo *parent_rel, Path *new_path);
extern bool add_cluster_path_precheck(RelOptInfo *parent_rel, List *reducelist,
									  Cost startup_cost, Cost total_cost,
									  List *pathkeys, Relids required_outer);
extern void add_cluster_path_list(RelOptInfo *parent_rel, List *pathlist, bool free_list);
extern void add_cluster_partial_path(RelOptInfo *parent_rel, Path *new_path);
extern bool add_cluster_partial_path_precheck(RelOptInfo *parent_rel, List *reducelist,
											  Cost total_cost, List *pathkeys);
#endif /* ADB */

extern Path *create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
								 Relids required_outer, int parallel_workers);
extern Path *create_samplescan_path(PlannerInfo *root, RelOptInfo *rel,
									Relids required_outer);
extern IndexPath *create_index_path(PlannerInfo *root,
									IndexOptInfo *index,
									List *indexclauses,
									List *indexorderbys,
									List *indexorderbycols,
									List *pathkeys,
									ScanDirection indexscandir,
									bool indexonly,
									Relids required_outer,
									double loop_count,
									bool partial_path);
extern BitmapHeapPath *create_bitmap_heap_path(PlannerInfo *root,
											   RelOptInfo *rel,
											   Path *bitmapqual,
											   Relids required_outer,
											   double loop_count,
											   int parallel_degree);
extern BitmapAndPath *create_bitmap_and_path(PlannerInfo *root,
											 RelOptInfo *rel,
											 List *bitmapquals);
extern BitmapOrPath *create_bitmap_or_path(PlannerInfo *root,
										   RelOptInfo *rel,
										   List *bitmapquals);
extern TidPath *create_tidscan_path(PlannerInfo *root, RelOptInfo *rel,
									List *tidquals, Relids required_outer);
extern AppendPath *create_append_path(PlannerInfo *root, RelOptInfo *rel,
									  List *subpaths, List *partial_subpaths,
									  List *pathkeys, Relids required_outer,
									  int parallel_workers, bool parallel_aware,
									  List *partitioned_rels, double rows);
extern MergeAppendPath *create_merge_append_path(PlannerInfo *root,
												 RelOptInfo *rel,
												 List *subpaths,
												 List *pathkeys,
												 Relids required_outer,
												 List *partitioned_rels);
extern GroupResultPath *create_group_result_path(PlannerInfo *root,
												 RelOptInfo *rel,
												 PathTarget *target,
												 List *havingqual);
#ifdef ADB
extern FilterPath *create_filter_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
									  PathTarget *target, List *quals);
extern Path *replicate_to_one_node(PlannerInfo *root, RelOptInfo *rel, Path *path, List *target_oids);
#endif /* ADB */
extern MaterialPath *create_material_path(RelOptInfo *rel, Path *subpath);
extern UniquePath *create_unique_path(PlannerInfo *root, RelOptInfo *rel,
									  Path *subpath, SpecialJoinInfo *sjinfo);
#ifdef ADB
extern UniquePath *create_cluster_unique_path(PlannerInfo *root, RelOptInfo *rel,
											  Path *subpath, SpecialJoinInfo *sjinfo);
#endif /* ADB */
extern GatherPath *create_gather_path(PlannerInfo *root,
									  RelOptInfo *rel, Path *subpath, PathTarget *target,
									  Relids required_outer, double *rows);
extern GatherMergePath *create_gather_merge_path(PlannerInfo *root,
												 RelOptInfo *rel,
												 Path *subpath,
												 PathTarget *target,
												 List *pathkeys,
												 Relids required_outer,
												 double *rows);
extern SubqueryScanPath *create_subqueryscan_path(PlannerInfo *root,
												  RelOptInfo *rel, Path *subpath,
												  List *pathkeys, Relids required_outer);
extern Path *create_functionscan_path(PlannerInfo *root, RelOptInfo *rel,
									  List *pathkeys, Relids required_outer);
extern Path *create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel,
									Relids required_outer);
extern Path *create_tablefuncscan_path(PlannerInfo *root, RelOptInfo *rel,
									   Relids required_outer);
extern Path *create_ctescan_path(PlannerInfo *root, RelOptInfo *rel,
								 Relids required_outer);
extern Path *create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
											 Relids required_outer);
#ifdef ADB
extern Path *create_paramtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
								Relids required_outer);
#endif /* ADB */
extern Path *create_resultscan_path(PlannerInfo *root, RelOptInfo *rel,
									Relids required_outer);
extern Path *create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel,
									   Relids required_outer);
extern ForeignPath *create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
											PathTarget *target,
											double rows, Cost startup_cost, Cost total_cost,
											List *pathkeys,
											Relids required_outer,
											Path *fdw_outerpath,
											List *fdw_private);
extern ForeignPath *create_foreign_join_path(PlannerInfo *root, RelOptInfo *rel,
											 PathTarget *target,
											 double rows, Cost startup_cost, Cost total_cost,
											 List *pathkeys,
											 Relids required_outer,
											 Path *fdw_outerpath,
											 List *fdw_private);
extern ForeignPath *create_foreign_upper_path(PlannerInfo *root, RelOptInfo *rel,
											  PathTarget *target,
											  double rows, Cost startup_cost, Cost total_cost,
											  List *pathkeys,
											  Path *fdw_outerpath,
											  List *fdw_private);

extern Relids calc_nestloop_required_outer(Relids outerrelids,
										   Relids outer_paramrels,
										   Relids innerrelids,
										   Relids inner_paramrels);
extern Relids calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path);

extern NestPath *create_nestloop_path(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  JoinType jointype,
									  JoinCostWorkspace *workspace,
									  JoinPathExtraData *extra,
									  Path *outer_path,
									  Path *inner_path,
									  List *restrict_clauses,
									  List *pathkeys,
									  ADB_ONLY_ARG_COMMA(List *reduce_info_list)
									  Relids required_outer);

extern MergePath *create_mergejoin_path(PlannerInfo *root,
										RelOptInfo *joinrel,
										JoinType jointype,
										JoinCostWorkspace *workspace,
										JoinPathExtraData *extra,
										Path *outer_path,
										Path *inner_path,
										List *restrict_clauses,
										List *pathkeys,
										Relids required_outer,
										List *mergeclauses,
										ADB_ONLY_ARG_COMMA(List *reduce_info_list)
										List *outersortkeys,
										List *innersortkeys);

extern HashPath *create_hashjoin_path(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  JoinType jointype,
									  JoinCostWorkspace *workspace,
									  JoinPathExtraData *extra,
									  Path *outer_path,
									  Path *inner_path,
									  bool parallel_hash,
									  List *restrict_clauses,
									  Relids required_outer,
									  ADB_ONLY_ARG_COMMA(List *reduce_info_list)
									  List *hashclauses);

extern ProjectionPath *create_projection_path(PlannerInfo *root,
											  RelOptInfo *rel,
											  Path *subpath,
											  PathTarget *target);
extern Path *apply_projection_to_path(PlannerInfo *root,
									  RelOptInfo *rel,
									  Path *path,
									  PathTarget *target);
extern ProjectSetPath *create_set_projection_path(PlannerInfo *root,
												  RelOptInfo *rel,
												  Path *subpath,
												  PathTarget *target);
extern SortPath *create_sort_path(PlannerInfo *root,
								  RelOptInfo *rel,
								  Path *subpath,
								  List *pathkeys,
								  double limit_tuples);
#ifdef ADB_EXT
extern BatchSortPath *create_batchsort_path(PlannerInfo *root,
											RelOptInfo *rel,
											Path *subpath,
											List *pathkeys,
											List *groupClause,
											uint32 numBatches,
											bool parallel_sort);
#endif /* ADB_EXT */
extern GroupPath *create_group_path(PlannerInfo *root,
									RelOptInfo *rel,
									Path *subpath,
									List *groupClause,
									List *qual,
									double numGroups);
extern UpperUniquePath *create_upper_unique_path(PlannerInfo *root,
												 RelOptInfo *rel,
												 Path *subpath,
												 int numCols,
												 double numGroups);
extern AggPath *create_agg_path(PlannerInfo *root,
								RelOptInfo *rel,
								Path *subpath,
								PathTarget *target,
								AggStrategy aggstrategy,
								AggSplit aggsplit,
								List *groupClause,
								List *qual,
								const AggClauseCosts *aggcosts,
								double numGroups);
extern GroupingSetsPath *create_groupingsets_path(PlannerInfo *root,
												  RelOptInfo *rel,
												  Path *subpath,
												  List *having_qual,
												  AggStrategy aggstrategy,
												  List *rollups,
												  const AggClauseCosts *agg_costs,
												  double numGroups);
extern MinMaxAggPath *create_minmaxagg_path(PlannerInfo *root,
											RelOptInfo *rel,
											PathTarget *target,
											List *mmaggregates,
											List *quals);
extern WindowAggPath *create_windowagg_path(PlannerInfo *root,
											RelOptInfo *rel,
											Path *subpath,
											PathTarget *target,
											List *windowFuncs,
											WindowClause *winclause);
extern SetOpPath *create_setop_path(PlannerInfo *root,
									RelOptInfo *rel,
									Path *subpath,
									SetOpCmd cmd,
									SetOpStrategy strategy,
									List *distinctList,
									AttrNumber flagColIdx,
									int firstFlag,
									double numGroups,
									double outputRows);
extern RecursiveUnionPath *create_recursiveunion_path(PlannerInfo *root,
													  RelOptInfo *rel,
													  Path *leftpath,
													  Path *rightpath,
													  PathTarget *target,
													  List *distinctList,
													  int wtParam,
													  double numGroups);
extern LockRowsPath *create_lockrows_path(PlannerInfo *root, RelOptInfo *rel,
										  Path *subpath, List *rowMarks, int epqParam);
extern ModifyTablePath *create_modifytable_path(PlannerInfo *root,
												RelOptInfo *rel,
												CmdType operation, bool canSetTag,
												Index nominalRelation, Index rootRelation,
												bool partColsUpdated,
												List *resultRelations, List *subpaths,
												List *subroots,
												List *withCheckOptionLists, List *returningLists,
												List *rowMarks, OnConflictExpr *onconflict,
												int epqParam);
extern LimitPath *create_limit_path(PlannerInfo *root, RelOptInfo *rel,
									Path *subpath,
									Node *limitOffset, Node *limitCount,
									int64 offset_est, int64 count_est);
extern void adjust_limit_rows_costs(double *rows,
									Cost *startup_cost, Cost *total_cost,
									int64 offset_est, int64 count_est);

extern Path *reparameterize_path(PlannerInfo *root, Path *path,
								 Relids required_outer,
								 double loop_count);
extern Path *reparameterize_path_by_child(PlannerInfo *root, Path *path,
										  RelOptInfo *child_rel);

#ifdef ADB

#define REMOTE_EXECUTE_ON_ANY			1		/* any datanode */
#define REMOTE_EXECUTE_ON_DATANODE		(1<<1)	/* have datanode */
#define REMOTE_EXECUTE_ON_COORD			(1<<2)	/* have coordinator */
#define REMOTE_EXECUTE_ON_LOCAL			(1<<3)	/* have local coordinator */
#define REMOTE_EXECUTE_ON_MUST_LOCAL	(1<<4)	/* must run at local coordinator */

/* get_path_execute_on flags */
#define GPEO_IGNORE_SUBQUERY		1
#define GPEO_IGNORE_ANY_OTHER		(1<<1)

struct ReduceExprInfo;
struct HTAB;
typedef struct ExecNodeInfo
{
	Oid nodeOid;
	uint32 rep_count;	/* replicate table count */
	double size;		/* rows*width with replicate table */
	uint32 part_count;	/* partial table count */
	uint32 update_count;	/* update and delete table count */
	uint32 insert_count;	/* insert table count */
}ExecNodeInfo;

bool have_special_path_args(Path *path, NodeTag tag, ...);
#define have_cluster_gather_path(path) have_special_path_args(path, T_ClusterGatherPath, T_ClusterMergeGatherPath, T_Invalid)
#define have_cluster_reduce_path(path) have_special_path_args(path, T_ClusterReducePath, T_Invalid)
#define have_cluster_path(path) have_special_path_args(path, T_ClusterReducePath, T_ClusterGatherPath, T_ClusterMergeGatherPath, T_Invalid)
#define have_remote_query_path(path) have_special_path_args(path, T_RemoteQueryPath, T_Invalid)

extern ClusterMergeGatherPath *create_cluster_merge_gather_path(PlannerInfo *root
			, RelOptInfo *rel, Path *sub_path, List *pathkeys);
extern ClusterGatherPath *create_cluster_gather_path(Path *sub_path, RelOptInfo *rel);
extern Path *create_cluster_reduce_path(PlannerInfo *root,
			Path *sub_path,
			List *rinfo_list,
			RelOptInfo *rel,
			List *pathkeys);
extern ReduceScanPath *create_reducescan_path(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
											  Path *subpath, List *reduce_list, List *pathkeys,
											  List *clauses);
extern struct HTAB* get_path_execute_on(Path *path, struct HTAB *htab, PlannerInfo *root);
extern bool path_tree_have_upper_reference(Path *path, PlannerInfo *root);
extern bool expression_have_reduce_plan(Expr *expr, PlannerGlobal *glob);
extern bool expr_have_node(Expr *expr, ...);
extern bool expr_have_upper_reference(Expr *expr, PlannerInfo *root);
extern bool restrict_list_have_upper_reference(List *list, PlannerInfo *root);
#define expression_have_subplan(expr) expr_have_node(expr, T_SubPlan, T_Invalid)

#endif /* ADB */

/*
 * prototypes for relnode.c
 */
extern void setup_simple_rel_arrays(PlannerInfo *root);
extern void setup_append_rel_array(PlannerInfo *root);
extern void expand_planner_arrays(PlannerInfo *root, int add_size);
extern RelOptInfo *build_simple_rel(PlannerInfo *root, int relid,
									RelOptInfo *parent);
extern RelOptInfo *find_base_rel(PlannerInfo *root, int relid);
extern RelOptInfo *find_join_rel(PlannerInfo *root, Relids relids);
extern RelOptInfo *build_join_rel(PlannerInfo *root,
								  Relids joinrelids,
								  RelOptInfo *outer_rel,
								  RelOptInfo *inner_rel,
								  SpecialJoinInfo *sjinfo,
								  List **restrictlist_ptr);
extern Relids min_join_parameterization(PlannerInfo *root,
										Relids joinrelids,
										RelOptInfo *outer_rel,
										RelOptInfo *inner_rel);
extern RelOptInfo *fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind,
								   Relids relids);
extern Relids find_childrel_parents(PlannerInfo *root, RelOptInfo *rel);
extern ParamPathInfo *get_baserel_parampathinfo(PlannerInfo *root,
												RelOptInfo *baserel,
												Relids required_outer);
extern ParamPathInfo *get_joinrel_parampathinfo(PlannerInfo *root,
												RelOptInfo *joinrel,
												Path *outer_path,
												Path *inner_path,
												SpecialJoinInfo *sjinfo,
												Relids required_outer,
												List **restrict_clauses);
extern ParamPathInfo *get_appendrel_parampathinfo(RelOptInfo *appendrel,
												  Relids required_outer);
extern ParamPathInfo *find_param_path_info(RelOptInfo *rel,
										   Relids required_outer);
extern RelOptInfo *build_child_join_rel(PlannerInfo *root,
										RelOptInfo *outer_rel, RelOptInfo *inner_rel,
										RelOptInfo *parent_joinrel, List *restrictlist,
										SpecialJoinInfo *sjinfo, JoinType jointype);

#endif							/* PATHNODE_H */
