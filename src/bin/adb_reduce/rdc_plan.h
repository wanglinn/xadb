#ifndef RDC_PLAN_H
#define RDC_PLAN_H

#include "rdc_tupstore.h"
#include "reduce/rdc_comm.h"

struct PlanPort
{
	struct RdcPort	   *port;
	RdcPortId			pln_id;
	int					work_num;
	RSstate			   *rdcstore;
	int					rdc_num;
	int					eof_num;
	RdcPortId			rdc_eofs[1];
};

extern PlanPort *plan_newport(RdcPortId pln_id);
extern void plan_freeport(PlanPort *pln_port);
extern PlanPort *find_plan_port(List *pln_list, RdcPortId pln_id);
extern void add_new_plan_port(List **pln_list, RdcPort *new_port);
extern int get_plan_port_num(List *pln_list);

#endif	/* RDC_PLAN_H */
