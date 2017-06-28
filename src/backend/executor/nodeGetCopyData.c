
#include "postgres.h"

#include "nodes/execnodes.h"
#include "executor/clusterReceiver.h"
#include "executor/nodeGetCopyData.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/plannodes.h"

ClusterGetCopyDataState* ExecInitClusterGetCopyData(ClusterGetCopyData *node, EState *estate, int flags)
{
	ClusterGetCopyDataState *ps;

	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	ps = makeNode(ClusterGetCopyDataState);
	ps->ps.plan = (Plan*)node;
	ps->ps.state = estate;

	ExecAssignExprContext(estate, &ps->ps);

	ps->ps.targetlist = (List*)ExecInitExpr((Expr*)node->targetlist, &ps->ps);
	Assert(node->qual == NULL);

	ExecInitResultTupleSlot(estate, &ps->ps);

	ExecAssignResultTypeFromTL(&ps->ps);

	if(flags & EXEC_FLAG_EXPLAIN_ONLY)
		ps->buf.data = NULL;
	else
		initStringInfo(&ps->buf);

	return ps;
}

TupleTableSlot *ExecClusterGetCopyData(ClusterGetCopyDataState *node)
{
	int mtype;
	pq_startmsgread();
	mtype = pq_getbyte();
	if (mtype == EOF)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	resetStringInfo(&node->buf);
	if (pq_getmessage(&node->buf, 0))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));

	if(mtype == 'd')
	{
		clusterRecvTuple(node->ps.ps_ResultTupleSlot, node->buf.data, node->buf.len, &node->ps, NULL);
	}else if(mtype == 'c'
		|| mtype == 'X')
	{
		ExecClearTuple(node->ps.ps_ResultTupleSlot);
	}else if(mtype == 'f')
	{
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED),
				errmsg("error message from client:%s",
				   pq_getmsgstring(&node->buf))));
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message type \"%d\"", mtype)));
	}
	return node->ps.ps_ResultTupleSlot;
}

void ExecEndClusterGetCopyData(ClusterGetCopyDataState *node)
{
	if(node->buf.data)
		pfree(node->buf.data);
}
