#include "rdc_globals.h"
#include "rdc_plan.h"

/*
 * plan_newport - create a new PlanPort with plan id.
 */
PlanPort *
plan_newport(RdcPortId pln_id)
{
	PlanPort   *pln_port = NULL;
	int			rdc_num = MyRdcOpts->rdc_num;
	int			work_mem = MyRdcOpts->work_mem;
	int			i;

	pln_port = (PlanPort *) palloc0(sizeof(*pln_port) + rdc_num * sizeof(RdcPortId));
	pln_port->work_num = 0;
	pln_port->pln_id = pln_id;
	pln_port->rdcstore = rdcstore_begin(work_mem, "PLAN", pln_id);
	pln_port->rdc_num = rdc_num;
	pln_port->eof_num = 0;
	for (i = 0; i < rdc_num; i++)
		pln_port->rdc_eofs[i] = InvalidPortId;

	return pln_port;
}

/*
 * plan_freeport - free a PlanPort
 */
void
plan_freeport(PlanPort *pln_port)
{
	if (pln_port)
	{
#ifdef DEBUG_ADB
		elog(LOG, "free plan port %ld", pln_port->pln_id);
#endif
		rdc_freeport(pln_port->port);
		rdcstore_end(pln_port->rdcstore);
		safe_pfree(pln_port);
	}
}

/*
 * find_plan_port - find a PlanPort with the plan id
 *
 * returns NULL if not found
 */
PlanPort *
find_plan_port(List *pln_nodes, RdcPortId pln_id)
{
	ListCell	   *cell = NULL;
	PlanPort	   *port = NULL;

	foreach (cell, pln_nodes)
	{
		port = (PlanPort *) lfirst(cell);
		Assert(port);

		if (port->pln_id == pln_id)
			break;
	}

	return port;
}

/*
 * add_new_plan_port - add a new RdcPort in the PlanPort list
 */
void
add_new_plan_port(List **pln_nodes, RdcPort *new_port)
{
	PlanPort	   *plan_port = NULL;

	AssertArg(pln_nodes && new_port);
	Assert(PortIdIsValid(new_port));

	plan_port = find_plan_port(*pln_nodes, RdcPeerID(new_port));
	if (plan_port == NULL)
	{
		plan_port = plan_newport(RdcPeerID(new_port));
		plan_port->port = new_port;
		plan_port->work_num++;
		*pln_nodes = lappend(*pln_nodes, plan_port);
	} else
	{
		RdcPort		   *port = plan_port->port;
		/*
		 * It happens when get data from other Reduce and current
		 * Reduce has not accepted a connection from the PlanPort.
		 */
		if (port == NULL)
		{
			plan_port->port = new_port;
			plan_port->work_num++;
		} else
		{
			while (port && RdcNext(port))
				port = RdcNext(port);
			port->next = new_port;
			plan_port->work_num++;
		}
	}
}

/*
 * get_plan_port_num - RdcPort number in the PlanPort list
 */
int
get_plan_port_num(List *pln_nodes)
{
	ListCell   *lc = NULL;
	int			num = 0;
	PlanPort   *pln_port;

	foreach (lc, pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(lc);
		num += pln_port->work_num;
	}

	return num;
}
