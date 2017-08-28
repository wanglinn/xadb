#ifndef REDUCEINFO_H
#define REDUCEINFO_H

#define REDUCE_TYPE_NONE		'\0'
#define REDUCE_TYPE_HASH		'H'
#define REDUCE_TYPE_CUSTOM		'C'
#define REDUCE_TYPE_MODULO		'M'
#define REDUCE_TYPE_REPLICATED	'R'
#define REDUCE_TYPE_ROUND		'L'
#define REDUCE_TYPE_COORDINATOR	'O'

#define REDUCE_MARK_STORAGE		0x0001
#define REDUCE_MARK_EXCLUDE		0x0002
#define REDUCE_MARK_PARAMS		0x0004
#define REDUCE_MARK_EXPR		0x0008
#define REDUCE_MARK_RELIDS		0x0010
#define REDUCE_MARK_TYPE		0x0020
#define REDUCE_MARK_NO_EXCLUDE		  \
			(REDUCE_MARK_STORAGE	| \
			 REDUCE_MARK_PARAMS		| \
			 REDUCE_MARK_EXPR		| \
			 REDUCE_MARK_RELIDS		| \
			 REDUCE_MARK_TYPE)
#define REDUCE_MARK_ALL	(REDUCE_MARK_NO_EXCLUDE|REDUCE_MARK_EXCLUDE)

typedef struct ReduceInfo
{
	List	   *storage_nodes;			/* when not reduce by value, it's sorted */
	List	   *exclude_exec;
	List	   *params;
	Expr	   *expr;					/* for custom only */
	Relids		relids;					/* params include */
	char		type;					/* REDUCE_TYPE_XXX */
}ReduceInfo;

typedef int(*ReducePathCallback_function)(PlannerInfo *root, Path *path, void *context);

extern ReduceInfo *MakeHashReduceInfo(const List *storage, const List *exclude, const Expr *param);
extern ReduceInfo *MakeCustomReduceInfoByRel(const List *storage, const List *exclude,
						const List *attnums, Oid funcid, Oid reloid, Index rel_index);
extern ReduceInfo *MakeCustomReduceInfo(const List *storage, const List *exclude, List *params, Oid funcid, Oid reloid);
extern ReduceInfo *MakeModuloReduceInfo(const List *storage, const List *exclude, const Expr *param);
extern ReduceInfo *MakeReplicateReduceInfo(const List *storage);
extern ReduceInfo *MakeRoundReduceInfo(const List *storage);
extern ReduceInfo *MakeCoordinatorReduceInfo(void);
extern ReduceInfo *MakeReduceInfoAs(const ReduceInfo *reduce, List *params);
extern ReduceInfo *ConvertReduceInfo(const ReduceInfo *reduce, const PathTarget *target, Index new_relid);
extern List *ConvertReduceInfoList(const List *reduce_list, const PathTarget *target, Index new_relid);
extern void FreeReduceInfo(ReduceInfo *reduce);
extern void FreeReduceInfoList(List *list);
extern List *SortOidList(List *list);
extern bool CanModuloType(Oid type, bool no_error);


extern int HashPathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
						  List *storage, List *exclude,
						  ReducePathCallback_function func, void *context);
extern int HashPathListByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
							  List *storage, List *exclude,
							  ReducePathCallback_function func, void *context);

extern int ModuloPathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
							List *storage, List *exclude,
							ReducePathCallback_function func, void *context);
extern int ModuloPathListByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
								List *storage, List *exclude,
								ReducePathCallback_function func, void *context);

extern int CoordinatorPath(PlannerInfo *root, RelOptInfo *rel, Path *path,
						   ReducePathCallback_function func, void *context);
extern int CoordinatorPathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
							   ReducePathCallback_function func, void *context);

extern int ReplicatePath(PlannerInfo *root, RelOptInfo *rel, Path *path, List *storage,
						 ReducePathCallback_function func, void *context);
extern int ReplicatePathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist, List *storage,
							  ReducePathCallback_function func, void *context);

extern int ReducePathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
							List *storage, List *exclude,
							ReducePathCallback_function func, void *context, ...);
extern int ReducePathListByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
								List *storage, List *exclude,
								ReducePathCallback_function func, void *context, ...);

extern int ReducePathByReduceInfo(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
								  ReducePathCallback_function func, void *context, ReduceInfo *reduce);
extern int ReducePathListByReduceInfo(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
									  ReducePathCallback_function func, void *context, ReduceInfo *reduce);
extern int ReducePathByReduceInfoList(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
								  ReducePathCallback_function func, void *context, List *reduce_list);
extern int ReducePathListByReduceInfoList(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
									  ReducePathCallback_function func, void *context, List *reduce_list);

extern int ReducePathByExprVA(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
							  List *storage, List *exclude,
							  ReducePathCallback_function func, void *context, va_list args);
extern int ReducePathListByExprVA(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
								  List *storage, List *exclude,
								  ReducePathCallback_function func, void *context, va_list args);


#define IsReduceInfoByValue(r) ((r)->type == REDUCE_TYPE_HASH || \
								(r)->type == REDUCE_TYPE_CUSTOM || \
								(r)->type == REDUCE_TYPE_MODULO)
extern bool IsReduceInfoListByValue(List *list);
#define IsReduceInfoReplicated(r)	((r)->type == REDUCE_TYPE_REPLICATED)
extern bool IsReduceInfoListReplicated(List *list);
#define IsReduceInfoRound(r)		((r)->type == REDUCE_TYPE_ROUND)
extern bool IsReduceInfoListRound(List *list);
#define IsReduceInfoCoordinator(r)	((r)->type == REDUCE_TYPE_COORDINATOR)
extern bool IsReduceInfoListCoordinator(List *list);

#define IsReduceInfoInOneNode(r) (list_length(r->storage_nodes) - list_length(r->exclude_exec) == 1)
extern bool IsReduceInfoListInOneNode(List *list);

extern bool IsReduceInfoStorageSubset(const ReduceInfo *rinfo, List *oidlist);
extern bool IsReduceInfoExecuteSubset(const ReduceInfo *rinfo, List *oidlist);
extern bool IsReduceInfoListExecuteSubset(List *reduce_info_list, List *oidlist);
extern List *ReduceInfoListGetExecuteOidList(const List *list);
extern void ReduceInfoListGetStorageAndExcludeOidList(const List *list, List **storage, List **exclude);

/* copy reduce info */
#define CopyReduceInfo(r) CopyReduceInfoExtend(r, REDUCE_MARK_ALL)
#define CopyReduceInfoList(l) CopyReduceInfoListExtend(l, REDUCE_MARK_ALL)
#define CopyReduceInfoListExtend(l, mark) ReduceInfoListConcatExtend(NIL, l, mark)
#define ReduceInfoListConcat(dest, src) ReduceInfoListConcatExtend(dest, src, REDUCE_MARK_ALL)
extern ReduceInfo *CopyReduceInfoExtend(const ReduceInfo *reduce, int mark);
extern List *ReduceInfoListConcatExtend(List *dest, List *src, int mark);

/* compare reduce info */
extern bool CompReduceInfo(const ReduceInfo *left, const ReduceInfo *right, int mark);
#define IsReduceInfoSame(l,r) CompReduceInfo(l, r, REDUCE_MARK_STORAGE|REDUCE_MARK_TYPE|REDUCE_MARK_EXPR)
#define IsReduceInfoEqual(l,r) CompReduceInfo(l, r, REDUCE_MARK_ALL)

extern int ReduceInfoIncludeExpr(ReduceInfo *reduce, Expr *expr);
extern bool ReduceInfoListIncludeExpr(List *reduceList, Expr *expr);

extern List* ReduceInfoFindTarget(const ReduceInfo* reduce, const PathTarget *target);
extern List* MakeVarList(const List *attnos, Index relid, const PathTarget *target);
extern bool IsGroupingReduceExpr(PathTarget *target, ReduceInfo *info);
extern bool IsReduceInfoListCanInnerJoin(List *outer_reduce_list,
									List *inner_reduce_list,
									List *restrictlist);
extern bool IsReduceInfoCanInnerJoin(ReduceInfo *outer_rinfo,
									 ReduceInfo *inner_rinfo,
									 List *restrictlist);
extern bool IsReduceInfoListCanLeftOrRightJoin(List *outer_reduce_list,
											   List *inner_reduce_list,
											   List *restrictlist);
extern List *FindJoinEqualExprs(ReduceInfo *rinfo, List *restrictlist, RelOptInfo *inner_rel);
extern bool reduce_info_list_can_join(List *outer_reduce_list,
									  List *inner_reduce_list,
									  List *restrictlist,
									  JoinType jointype,
									  List **new_reduce_list);

extern bool CanOnceGroupingClusterPath(PathTarget *target, Path *path);
extern bool CanOnceDistinctReduceInfoList(List *distinct, List *reduce_list);
extern bool CanOnceDistinctReduceInfo(List *distinct, ReduceInfo *reduce_info);

extern Var *makeVarByRel(AttrNumber attno, Oid rel_oid, Index rel_index);
extern Expr *CreateExprUsingReduceInfo(ReduceInfo *reduce);

#endif /* REDUCEINFO_H */
