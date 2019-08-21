#ifndef REDUCEINFO_H
#define REDUCEINFO_H

#define REDUCE_TYPE_NONE		'\0'
#define REDUCE_TYPE_HASHMAP		'B'
#define REDUCE_TYPE_HASH		'H'
#define REDUCE_TYPE_MODULO		'M'
#define REDUCE_TYPE_LIST		'l'
#define REDUCE_TYPE_RANGE		'r'
#define REDUCE_TYPE_REPLICATED	'R'
#define REDUCE_TYPE_RANDOM		'L'
#define REDUCE_TYPE_COORDINATOR	'O'
/* only using in ReducePathXXX functions */
#define REDUCE_TYPE_IGNORE		'I'
#define REDUCE_TYPE_GATHER		'G'
#define IsReduceTypeByValue(t_)			\
			((t_) == REDUCE_TYPE_HASH		|| \
			 (t_) == REDUCE_TYPE_HASHMAP	|| \
			 (t_) == REDUCE_TYPE_MODULO		|| \
			 (t_) == REDUCE_TYPE_LIST		|| \
			 (t_) == REDUCE_TYPE_RANDOM)
#define IsReduceTypeNotByValue(t_)		\
			((t_) == REDUCE_TYPE_REPLICATED	|| \
			 (t_) == REDUCE_TYPE_RANDOM		|| \
			 (t_) == REDUCE_TYPE_COORDINATOR)

#define REDUCE_MARK_STORAGE		0x0001
#define REDUCE_MARK_EXCLUDE		0x0002
#define REDUCE_MARK_KEY_NUM		0x0004
#define REDUCE_MARK_KEY_EXPR	0x0008
#define REDUCE_MARK_OPCLASS		0x0010
#define REDUCE_MARK_COLLATION	0x0011
#define REDUCE_MARK_VALUES		0x0012
#define REDUCE_MARK_RELIDS		0x0014
#define REDUCE_MARK_TYPE		0x0018
#define REDUCE_MARK_KEY_INFO		  \
			(REDUCE_MARK_KEY_EXPR	| \
			 REDUCE_MARK_OPCLASS	| \
			 REDUCE_MARK_COLLATION)
#define REDUCE_MARK_SAME			  \
			(REDUCE_MARK_STORAGE	| \
			 REDUCE_MARK_TYPE		| \
			 REDUCE_MARK_VALUES		| \
			 REDUCE_MARK_KEY_NUM	| \
			 REDUCE_MARK_OPCLASS	| \
			 REDUCE_MARK_COLLATION)
#define REDUCE_MARK_NO_EXCLUDE		  \
			(REDUCE_MARK_SAME		| \
			 REDUCE_MARK_KEY_INFO	| \
			 REDUCE_MARK_RELIDS)
#define REDUCE_MARK_ALL	(REDUCE_MARK_NO_EXCLUDE|REDUCE_MARK_EXCLUDE)

typedef struct ReduceKeyInfo
{
	Expr		   *key;					/* distribute key */
	Oid				opclass;				/* operator class to compare */
	Oid				opfamily;				/* operator family from opclass */
	Oid				collation;				/* user-specified collation */
}ReduceKeyInfo;

typedef struct ReduceInfo
{
	List		   *storage_nodes;			/* when not reduce by value, it's sorted */
	List		   *exclude_exec;			/* not have any row nodes */
	List		   *values;					/* each nodes value(s) for distribute by list and range*/
	Relids			relids;					/* params include */
	char			type;					/* REDUCE_TYPE_XXX */
	uint32			nkey;
	ReduceKeyInfo	keys[FLEXIBLE_ARRAY_MEMBER];
}ReduceInfo;
struct RelationLocInfo;

typedef int(*ReducePathCallback_function)(PlannerInfo *root, Path *path, void *context);

extern ReduceInfo *MakeHashReduceInfo(const List *storage, const List *exclude, const Expr *key);
extern ReduceInfo *MakeHashmapReduceInfo(const List *storage, const List *exclude, const Expr *key);
extern ReduceInfo *MakeModuloReduceInfo(const List *storage, const List *exclude, const Expr *key);
extern ReduceInfo *MakeReplicateReduceInfo(const List *storage);
extern ReduceInfo *MakeFinalReplicateReduceInfo(void);
extern ReduceInfo *MakeRandomReduceInfo(const List *storage);
extern ReduceInfo *MakeCoordinatorReduceInfo(void);
extern ReduceInfo *MakeReduceInfoFromLocInfo(const RelationLocInfo *loc_info, const List *exclude, Oid reloid, Index relid);
extern ReduceInfo *MakeReduceInfoUsingPathTarget(const RelationLocInfo *loc_info, const List *exclude, PathTarget *target);
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
extern int HashmapPathByExpr(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
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

extern int ClusterGatherSubPath(PlannerInfo *root, RelOptInfo *rel, Path *path,
								ReducePathCallback_function func, void *context);
extern int ClusterGatherSubPathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
									ReducePathCallback_function func, void *context);

extern int ParallelGatherSubPath(PlannerInfo *root, RelOptInfo *rel, Path *path,
								ReducePathCallback_function func, void *context);
extern int ParallelGatherSubPathList(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
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

extern int ReducePathUsingReduceInfo(PlannerInfo *root, RelOptInfo *rel, Path *path,
									 ReducePathCallback_function func, void *context, ReduceInfo *reduce);
extern int ReducePathUsingReduceInfoList(PlannerInfo *root, RelOptInfo *rel, Path *path,
										 ReducePathCallback_function func, void *context, List *reduce_list);
extern int ReducePathListUsingReduceInfo(PlannerInfo *root, RelOptInfo *rel, List *pathlist,
										 ReducePathCallback_function func, void *context, ReduceInfo *reduce);

extern int ReducePathByExprVA(Expr *expr, PlannerInfo *root, RelOptInfo *rel, Path *path,
							  List *storage, List *exclude,
							  ReducePathCallback_function func, void *context, va_list args);
extern int ReducePathListByExprVA(Expr *expr, PlannerInfo *root, RelOptInfo *rel, List *pathlist,
								  List *storage, List *exclude,
								  ReducePathCallback_function func, void *context, va_list args);

extern List *GetCheapestReducePathList(RelOptInfo *rel, List *pathlist, Path **cheapest_startup, Path **cheapest_total);


extern int ReducePathSave2List(PlannerInfo *root, Path *path, void *pplist);

#define IsReduceInfoByValue(r) IsReduceTypeByValue((r)->type)
extern bool IsReduceInfoListByValue(List *list);
#define IsReduceInfoReplicated(r)	((r)->type == REDUCE_TYPE_REPLICATED)
#define IsReduceInfoFinalReplicated(r) (IsReduceInfoReplicated(r) &&			\
										list_length(r->storage_nodes) == 1 &&	\
										linitial_oid(r->storage_nodes) == InvalidOid)
extern bool IsReduceInfoListReplicated(List *list);
extern bool IsReduceInfoListReplicatedOids(List *list, List *storage, List *exclude);
extern bool IsReduceInfoListFinalReplicated(List *list);
#define IsReduceInfoRandom(r)		((r)->type == REDUCE_TYPE_RANDOM)
extern bool IsReduceInfoListRandom(List *list);
#define IsReduceInfoCoordinator(r)	((r)->type == REDUCE_TYPE_COORDINATOR)
extern bool IsReduceInfoListCoordinator(List *list);

#define IsReduceInfoInOneNode(r) (list_length(r->storage_nodes) - list_length(r->exclude_exec) == 1 && \
								  !IsReduceInfoFinalReplicated(r))
extern bool IsReduceInfoListInOneNode(List *list);

#define IsReduceInfoExclude(r, oid) list_member_oid(r->exclude_exec, oid)
extern bool IsReduceInfoListExclude(List *list, Oid oid);
extern bool IsPathExcludeNodeOid(Path *path, Oid oid);

extern bool IsReduceInfoStorageSubset(const ReduceInfo *rinfo, List *oidlist);
extern bool IsReduceInfoExecuteSubset(const ReduceInfo *rinfo, List *oidlist);
extern bool IsReduceInfoListExecuteSubset(List *reduce_info_list, List *oidlist);
extern bool IsReduceInfoListExecuteSubsetReduceInfo(List *reduce_info_list, const ReduceInfo *rinfo);
extern List *ReduceInfoListGetExecuteOidList(const List *list);
extern void ReduceInfoListGetStorageAndExcludeOidList(const List *list, List **storage, List **exclude);
extern List *PathListGetReduceInfoListExecuteOn(List *pathlist);

/* copy reduce info */
#define CopyReduceInfo(r) CopyReduceInfoExtend(r, REDUCE_MARK_ALL)
#define CopyReduceInfoList(l) CopyReduceInfoListExtend(l, REDUCE_MARK_ALL)
#define CopyReduceInfoListExtend(l, mark) ReduceInfoListConcatExtend(NIL, l, mark)
#define ReduceInfoListConcat(dest, src) ReduceInfoListConcatExtend(dest, src, REDUCE_MARK_ALL)
extern ReduceInfo *CopyReduceInfoExtend(const ReduceInfo *reduce, int mark);
extern List *ReduceInfoListConcatExtend(List *dest, List *src, int mark);

/* compare reduce info */
extern bool CompReduceInfo(const ReduceInfo *left, const ReduceInfo *right, int mark);
extern bool CompReduceInfoList(List *left, List *right, int mark);
#define IsReduceInfoSame(l,r) CompReduceInfo(l, r, REDUCE_MARK_SAME)
#define IsReduceInfoListSame(l,r) CompReduceInfoList(l, r, REDUCE_MARK_SAME)
#define IsReduceInfoEqual(l,r) CompReduceInfo(l, r, REDUCE_MARK_ALL)
#define IsReduceInfoListEqual(l,r) CompReduceInfoList(l, r, REDUCE_MARK_ALL)

extern bool ReduceInfoListMember(List *reduce_info_list, ReduceInfo *reduce_info);
extern bool ReduceInfoListMemberExtend(const List *reduce_info_list, const ReduceInfo *reduce_info, int mark);
extern List* GetPathListReduceInfoList(List *pathlist);

extern int ReduceInfoIncludeExpr(ReduceInfo *reduce, Expr *expr);
extern bool ReduceInfoListIncludeExpr(List *reduceList, Expr *expr);

extern List* ReduceInfoFindPathTarget(const ReduceInfo* reduce, const PathTarget *target);
extern List* ReduceInfoFindTargetList(const ReduceInfo* reduce, const List *targetlist, bool skip_junk);
extern List* MakeVarList(const List *attnos, Index relid, const PathTarget *target);
extern List* ExtractExprList(const List *attnos, List *exprs);
extern bool IsGroupingReduceExpr(PathTarget *target, ReduceInfo *info);
extern bool IsReduceInfoListCanInnerJoin(List *outer_reduce_list,
									List *inner_reduce_list,
									List *restrictlist);
extern bool IsReduceInfoListCanUniqueJoin(List *reduce_list, List *restrictlist);
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
extern bool reduce_info_list_is_one_node_can_join(List *outer_reduce_list,
												  List *inner_reduce_list,
												  List **new_reduce_list,
												  JoinType jointype,
												  bool *is_dummy);
extern bool CanOnceGroupingClusterPath(PathTarget *target, Path *path);
extern bool CanOnceDistinctReduceInfoList(List *distinct, List *reduce_list);
extern bool CanOnceDistinctReduceInfo(List *distinct, ReduceInfo *reduce_info);
extern bool CanOnceWindowAggClusterPath(WindowClause *wclause, List *tlist, Path *path);
extern bool HaveOnceWindowAggClusterPath(List *wclauses, List *tlist, Path *path);

extern Var *makeVarByRel(AttrNumber attno, Oid rel_oid, Index rel_index);
extern Expr *CreateExprUsingReduceInfo(ReduceInfo *reduce);
extern Expr *CreateReduceModuloExpr(Relation rel, const RelationLocInfo *loc, Index relid);
extern Expr *CreateNodeOidEqualOid(Oid nodeoid);
extern Expr *CreateNodeOidEqualExpr(Expr *expr);
extern Expr *CreateNodeOidNotEqualOid(Oid nodeoid);
extern bool EqualReduceExpr(Expr *left, Expr *right);

/* in outobject.c */
extern char *printReduceInfo(ReduceInfo *rinfo);
extern char *printReduceInfoList(List *list);

#endif /* REDUCEINFO_H */
