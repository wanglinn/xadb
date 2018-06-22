#ifndef EXEC_CLUSTER_H
#define EXEC_CLUSTER_H

#define EXEC_CLUSTER_FLAG_NEED_REDUCE		(1<<0)
#define EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE	(1<<1)

struct Plan;
struct EState;
struct CopyStmt;

extern void exec_cluster_plan(const void *splan, int length);
extern PlanState* ExecStartClusterPlan(Plan *plan, EState *estate
								, int eflags, List *rnodes);
extern List* ExecStartClusterCopy(List *rnodes, struct CopyStmt *stmt, StringInfo mem_toc, uint32 flag);
#endif /* EXEC_CLUSTER_H */
