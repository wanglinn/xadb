/*-------------------------------------------------------------------------
 *
 * rdc_plan.c
 *	  interface for PlanPort.
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/bin/adb_reduce/rdc_plan.c
 *
 * NOTES:
 *	  PlanPort is for a plan node. its work_port is a linked-list of
 *	  RdcPort which is for each worker of a plan node. its work_num
 *	  is initialized to zero and increase one by one while a new
 *	  RdcPort with the same RdcPortId is accepted and decrease one by
 *	  one if a CLOSE message is received from a worker of a plan node.
 *	  it will assigned -1 if its value is equal to 0 and it means the
 *	  PlanPort is invalid.
 *-------------------------------------------------------------------------
 */
#include <time.h>

#include "rdc_globals.h"
#include "rdc_plan.h"

/*
 * plan_newport
 *
 * create a new PlanPort with plan id.
 */
PlanPort *
plan_newport(RdcPortId pln_id)
{
	PlanPort   *pln_port = NULL;
	int			rdc_num = MyRdcOpts->rdc_num;
	int			work_mem = MyRdcOpts->work_mem;
	int			i;

	pln_port = (PlanPort *) palloc0(sizeof(*pln_port) + rdc_num * sizeof(RdcPortId));
	pln_port->work_port = NULL;
	pln_port->work_num = 0;
	pln_port->pln_id = pln_id;
	pln_port->create_time = time(NULL);
	pln_port->recv_from_pln = 0;
	pln_port->dscd_from_rdc = 0;
	pln_port->recv_from_rdc = 0;
	pln_port->send_to_pln = 0;
	pln_port->rdcstore = rdcstore_begin(work_mem, "PLAN", pln_id,
										MyProcPid, MyBossPid, MyStartTime);
	pln_port->rdc_num = rdc_num;
	pln_port->eof_num = 0;
	for (i = 0; i < rdc_num; i++)
		pln_port->rdc_eofs[i] = InvalidPortId;
	initStringInfo(PlanMsgBuf(pln_port));

	return pln_port;
}

/*
 * plan_freeport
 *
 * free a PlanPort
 */
void
plan_freeport(PlanPort *pln_port)
{
	if (pln_port)
	{
		elog(LOG,
			 "free port of" PLAN_PORT_PRINT_FORMAT,
			 PlanID(pln_port));
		PlanPortStats(pln_port);
		rdc_freeport(pln_port->work_port);
		rdcstore_end(pln_port->rdcstore);
		pfree(pln_port->msg_buf.data);
		pln_port->msg_buf.data = NULL;
		safe_pfree(pln_port);
	}
}

void
FreeInvalidPlanPort(PlanPort *pln_port)
{
	if (pln_port && !PlanPortIsValid(pln_port))
	{
		PlanPortStats(pln_port);
		rdc_freeport(pln_port->work_port);
		pln_port->work_port = NULL;
		rdcstore_end(pln_port->rdcstore);
		pln_port->rdcstore = NULL;
	}
}

/*
 * PlanPortStats
 *		Print statistics about the PlanPort.
 */
void
PlanPortStats(PlanPort *pln_port)
{
	if (pln_port)
	{
		elog(LOG,
			 PLAN_PORT_PRINT_FORMAT " statistics: "
			 "time to live " INT64_FORMAT
			 " seconds, recv from PLAN " UINT64_FORMAT
			 ", dscd from REDUDE " UINT64_FORMAT
			 ", recv from REDUCE " UINT64_FORMAT
			 ", send to PLAN " UINT64_FORMAT,
			 PlanID(pln_port),
			 time(NULL) - pln_port->create_time,
			 pln_port->recv_from_pln,
			 pln_port->dscd_from_rdc,
			 pln_port->recv_from_rdc,
			 pln_port->send_to_pln);
	}
}

/*
 * LookUpPlanPort
 *
 * find a PlanPort with the plan id.
 *
 * returns NULL if not found
 */
PlanPort *
LookUpPlanPort(List *pln_nodes, RdcPortId pln_id)
{
	ListCell	   *cell;
	PlanPort	   *pln_port;

	foreach (cell, pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(cell);
		Assert(pln_port);

		if (PlanID(pln_port) == pln_id)
			return pln_port;
	}

	return NULL;
}

/*
 * AddNewPlanPort
 *
 * add a new RdcPort in the PlanPort list
 */
void
AddNewPlanPort(List **pln_nodes, RdcPort *new_port)
{
	PlanPort	   *pln_port = NULL;

	AssertArg(pln_nodes && new_port);
	Assert(PlanTypeIDIsValid(new_port));

	pln_port = LookUpPlanPort(*pln_nodes, RdcPeerID(new_port));
	if (pln_port == NULL)
	{
		pln_port = plan_newport(RdcPeerID(new_port));
		pln_port->work_port = new_port;
		pln_port->work_num++;
		*pln_nodes = lappend(*pln_nodes, pln_port);
	} else
	{
		RdcPort		   *work_port = pln_port->work_port;
		/*
		 * It happens when get data from other Reduce and current
		 * Reduce has not accepted a connection from the PlanPort.
		 */
		if (work_port == NULL)
		{
			pln_port->work_port = new_port;
		} else
		{
			while (work_port && RdcNext(work_port))
				work_port = RdcNext(work_port);
			RdcNext(work_port) = new_port;
		}
		pln_port->work_num++;
	}
}
