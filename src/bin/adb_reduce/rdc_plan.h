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
	pg_time_t			create_time;
	uint64				recv_from_pln;
	uint64				dscd_from_rdc;
	uint64				recv_from_rdc;
	uint64				send_to_pln;
	int					rdc_num;
	int					eof_num;
	RdcPortId			rdc_eofs[1];
};

#define PlanID(pln_port)			(((PlanPort *) (pln_port))->pln_id)
#define PlanWorkNum(pln_port)		(((PlanPort *) (pln_port))->work_num)
#define PlanPortIsValid(pln_port)	(PlanWorkNum(pln_port) >= 0)

#define PlanPortAddEvents(pln_port, events)			\
	do {											\
		if (pln_port) {								\
			RdcPort *port = pln_port->port;			\
			while (port) {							\
				RdcWaitEvents(port) |= (events);	\
				port = RdcNext(port);				\
			}										\
		}											\
	} while (0)

#define PlanPortRmvEvents(pln_port, events)			\
	do {											\
		if (pln_port) { 							\
			RdcPort *port = pln_port->port; 		\
			while (port) {							\
				RdcWaitEvents(port) &= ~(events);	\
				port = RdcNext(port);				\
			}										\
		}											\
	} while (0)

extern PlanPort *plan_newport(RdcPortId pln_id);
extern void plan_freeport(PlanPort *pln_port);
extern void FreeInvalidPlanPort(PlanPort *pln_port);
extern void PlanPortStats(PlanPort *pln_port);
extern PlanPort *LookUpPlanPort(List *pln_nodes, RdcPortId pln_id);
extern void AddNewPlanPort(List **pln_nodes, RdcPort *new_port);

#endif	/* RDC_PLAN_H */
