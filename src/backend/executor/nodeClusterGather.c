
/*-------------------------------------------------------------------------
 *
 * src/backend/executor/nodeClusterGather.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/clusterReceiver.h"
#include "executor/executor.h"
#include "executor/execCluster.h"
#include "executor/nodeClusterGather.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-node.h"
#include "miscadmin.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#elif defined(HAVE_SYS_POLL_H)
#include <sys/poll.h>
#endif

ClusterGatherState *ExecInitClusterGather(ClusterGather *node, EState *estate, int flags)
{
	ClusterGatherState *gatherstate;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);
	Assert((flags & (EXEC_FLAG_MARK|EXEC_FLAG_BACKWARD|EXEC_FLAG_REWIND)) == 0);

	gatherstate = makeNode(ClusterGatherState);
	gatherstate->ps.plan = (Plan*)node;
	gatherstate->ps.state = estate;

	/*ExecAssignExprContext(estate, &gatherstate->ps);*/

	ExecInitResultTupleSlot(estate, &gatherstate->ps);

	ExecAssignResultTypeFromTL(&gatherstate->ps);

	outerPlanState(gatherstate) = ExecStartClusterPlan(outerPlan(node)
		, estate, flags, node->rnodes);
	if((flags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
	{
		gatherstate->remotes = PQNGetConnUseOidList(node->rnodes);
		gatherstate->pfds = palloc(sizeof(struct pollfd) * list_length(node->rnodes));
	}

	return gatherstate;
}

TupleTableSlot *ExecClusterGather(ClusterGatherState *node)
{
	PGconn *conn;
	const char *buf;
	PGresult *res;
	int n;
	ExecStatusType status;
#if 0
	while(node->remotes)
	{
		CHECK_FOR_INTERRUPTS();
		n = PQNWaitResult(node->remotes, &conn, false);
		if(n < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if(errno == EINTR)
				continue;
			ereport(ERROR, (errmsg("wait remote result error:%m")));
		}
		Assert(n > 0);
		if(PQisCopyOutState(conn) == false)
		{
			/* test has error */
			res = PQgetResult(conn);
			if(res == NULL)
			{
				node->remotes = list_delete_ptr(node->remotes, conn);
				continue;
			}
			status = PQresultStatus(res);
			if(status == PGRES_FATAL_ERROR)
			{
				PQNReportResultError(res, conn, ERROR, true);
			}else if(status == PGRES_COMMAND_OK)
			{
				node->remotes = list_delete_ptr(node->remotes, conn);
				continue;
			}else if(status == PGRES_COPY_IN)
			{
				PQputCopyEnd(conn, NULL);
				continue;
			}else if(status == PGRES_COPY_OUT)
			{
				continue;
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("not support execute status type \"%d\"", status)));
			}
		}
		n = PQgetCopyDataBuffer(conn, &buf, true);
		Assert(n != 0);
		if(n >= 0)
		{
			if(clusterRecvTuple(node->ps.ps_ResultTupleSlot, buf, n))
				return node->ps.ps_ResultTupleSlot;
			else
				continue;
		}
		/* end copy or error
		 * continue;
		 */
	}
#else
	ListCell *lc;
re_loop_:
	if(node->remotes == NIL)
		return ExecClearTuple(node->ps.ps_ResultTupleSlot);

	foreach(lc, node->remotes)
	{
		conn = lfirst(lc);
re_get_:
		if(PQstatus(conn) == CONNECTION_BAD)
		{
			res = PQgetResult(conn);
			PQNReportResultError(res, conn, ERROR, true);
			node->remotes = list_delete_ptr(node->remotes, conn);
			goto re_loop_;
		}
		if(PQisCopyOutState(conn))
		{
			n = PQgetCopyDataBuffer(conn, &buf, true);
			if(n > 0)
			{
				if(clusterRecvTuple(node->ps.ps_ResultTupleSlot, buf, n))
					return node->ps.ps_ResultTupleSlot;
				goto re_get_;
			}else if(n < 0)
			{
				goto re_get_;
			}
		}else if(PQisCopyInState(conn))
		{
			PQputCopyEnd(conn, NULL);
		}else if(PQisBusy(conn) == false)
		{
			res = PQgetResult(conn);
			if(res == NULL)
			{
				node->remotes = list_delete_ptr(node->remotes, conn);
				goto re_loop_;
			}
			status = PQresultStatus(res);
			if(status == PGRES_FATAL_ERROR)
			{
				PQNReportResultError(res, conn, ERROR, true);
			}else if(status == PGRES_COMMAND_OK)
			{
				node->remotes = list_delete_ptr(node->remotes, conn);
				continue;
			}else if(status == PGRES_COPY_IN)
			{
				PQputCopyEnd(conn, NULL);
				continue;
			}else if(status == PGRES_COPY_OUT)
			{
				continue;
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("not support execute status type \"%d\"", status)));
			}
		}
	}
	n=0;
	foreach(lc, node->remotes)
	{
		conn = lfirst(lc);
		node->pfds[n].fd = PQsocket(conn);
		node->pfds[n].events = POLLIN;
		++n;
	}

re_poll_:
	n = poll(node->pfds, list_length(node->remotes), -1);
	CHECK_FOR_INTERRUPTS();
	if(n < 0)
	{
		CHECK_FOR_INTERRUPTS();
		if(errno == EINTR)
			goto re_poll_;
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("poll error:%m")));
	}

	for(n=0,lc=list_head(node->remotes);lc!=NULL;lc=lnext(lc),++n)
	{
		if(node->pfds[n].revents == 0)
			continue;
		PQconsumeInput(conn);
	}
	goto re_loop_;

#endif
	return ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

void ExecEndClusterGather(ClusterGatherState *node)
{
	/*ExecFreeExprContext(&node->ps);*/
	ExecEndNode(outerPlanState(node));
}

void ExecReScanClusterGather(ClusterGatherState *node)
{
}
