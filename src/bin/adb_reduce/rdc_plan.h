#ifndef RDC_PLAN_H
#define RDC_PLAN_H

#include "rdc_tupstore.h"
#include "reduce/rdc_comm.h"

struct PlanPort
{
	struct RdcPort	   *work_port;		/* linked-list for parallel worker of the same plan node */
	int					work_num;		/* number of parallel worker */
	RdcPortId			pln_id;			/* plan node ID */
	RSstate			   *rdcstore;		/* storage for slot received/sent */
	StringInfoData		msg_buf;		/* used for make up message, to avoid malloc and free
										   memory multiple times, call resetStringInfo before
										   use it and never free until destory PlanPort. */
	pg_time_t			create_time;	/* time when the PlanPort is created */
	uint64				recv_from_pln;	/* number of slot received from plan node */
	uint64				dscd_from_rdc;	/* number of slot discarded from other reduce */
	uint64				recv_from_rdc;	/* number of slot received from other reduce */
	uint64				send_to_pln;	/* number of slot sent to plan node */
	int					rdc_num;		/* number of reduce group */
	int					eof_num;		/* number of EOF message got from other reduce */
	RdcPortId			rdc_eofs[1];	/* array of RdcPortId which already send EOF message */
};

#define PlanID(pln_port)			(((PlanPort *) (pln_port))->pln_id)
#define PlanWorkNum(pln_port)		(((PlanPort *) (pln_port))->work_num)
#define PlanMsgBuf(pln_port)		&(((PlanPort *) (pln_port))->msg_buf)
#define PlanPortIsValid(pln_port)	(PlanWorkNum(pln_port) >= 0)


#define PlanPortAddEvents(pln_port, events)			\
	do {											\
		if (pln_port) {								\
			RdcPort *work_port = pln_port->work_port;			\
			while (work_port) {							\
				RdcWaitEvents(work_port) |= (events);	\
				work_port = RdcNext(work_port);				\
			}										\
		}											\
	} while (0)

#define PlanPortRmvEvents(pln_port, events)			\
	do {											\
		if (pln_port) { 							\
			RdcPort *work_port = pln_port->work_port; 		\
			while (work_port) {							\
				RdcWaitEvents(work_port) &= ~(events);	\
				work_port = RdcNext(work_port);				\
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
