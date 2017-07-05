#ifndef EXEC_CLUSTER_H
#define EXEC_CLUSTER_H

struct Plan;
struct EState;

extern void exec_cluster_plan(const void *splan, int length);
extern PlanState* ExecStartClusterPlan(Plan *plan, EState *estate
								, int eflags, List *rnodes);

#endif /* EXEC_CLUSTER_H */
