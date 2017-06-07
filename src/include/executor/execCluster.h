#ifndef EXEC_CLUSTER_H
#define EXEC_CLUSTER_H

struct Plan;
struct EState;

extern void exec_cluster_plan(const void *splan, int length);
extern PlanState* ExecStartClusterPlan(Plan *plan, EState *estate
								, int eflags, List *rnodes);
extern Oid get_cluster_node_oid(void);

#endif /* EXEC_CLUSTER_H */
