#ifndef EXEC_CLUSTER_H
#define EXEC_CLUSTER_H

struct Plan;
struct EState;
struct CopyStmt;

extern void exec_cluster_plan(const void *splan, int length);
extern PlanState* ExecStartClusterPlan(Plan *plan, EState *estate
								, int eflags, List *rnodes);
extern List* ExecStartClusterCopy(List *rnodes, struct CopyStmt *stmt);
#endif /* EXEC_CLUSTER_H */
