#include "rdc_globals.h"
#include "rdc_plan.h"

/*
 * plan_newport - create a new PlanPort with plan id.
 */
PlanPort *
plan_newport(RdcPortId pln_id)
{
	PlanPort	   *pln_port = NULL;
	int				rdc_num = MyReduceOpts->rdc_num;
	int				work_mem = MyReduceOpts->work_mem;

	pln_port = (PlanPort *) palloc0(sizeof(*pln_port) + rdc_num * sizeof(bool));
	pln_port->work_num = 0;
	pln_port->pln_id = pln_id;
	pln_port->rdcstore = rdcstore_begin(work_mem, "PLAN", pln_id);
	pln_port->rdc_num = rdc_num;

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
find_plan_port(List *pln_list, RdcPortId pln_id)
{
	ListCell	   *cell = NULL;
	PlanPort	   *port = NULL;

	foreach (cell, pln_list)
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
add_new_plan_port(List **pln_list, RdcPort *new_port)
{
	PlanPort	   *plan_port = NULL;

	AssertArg(pln_list && new_port);
	Assert(PortIdIsValid(RdcID(new_port)));

	plan_port = find_plan_port(*pln_list, RdcID(new_port));
	if (plan_port == NULL)
	{
		plan_port = plan_newport(RdcID(new_port));
		plan_port->port = new_port;
		plan_port->work_num++;
		*pln_list = lappend(*pln_list, plan_port);
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
get_plan_port_num(List *pln_list)
{
	ListCell   *lc = NULL;
	int			num = 0;
	PlanPort   *pln_port;

	foreach (lc, pln_list)
	{
		pln_port = (PlanPort *) lfirst(lc);
		num += pln_port->work_num;
	}

	return num;
}
