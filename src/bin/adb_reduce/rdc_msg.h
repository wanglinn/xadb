/*-------------------------------------------------------------------------
 *
 * rdc_msg.h
 *	  interface for message
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/bin/adb_reduce/rdc_msg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RDC_MSG_H
#define RDC_MSG_H

#include "rdc_comm.h"

/* -----------Reduce message------------- */
#define RDC_ERROR_MSG		'E'
#define RDC_EOF_MSG			'e'
#define RDC_START_RQT		'S'
#define RDC_START_RSP		's'
#define RDC_GROUP_RQT		'G'
#define RDC_GROUP_RSP		'g'
#define RDC_P2R_CLOSE		'C'
#define RDC_P2R_DATA		'P'
#define RDC_R2P_DATA		'p'
#define RDC_R2R_DATA		'R'

extern int rdc_send_startup_rqt(RdcPort *port, RdcPortType type, RdcPortId id);
extern int rdc_send_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id);
extern int rdc_recv_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id);

extern int rdc_send_group_rqt(RdcPort *port, int num, const char *hosts[], int ports[]);
extern int rdc_send_group_rsp(RdcPort *port);
extern int rdc_recv_group_rsp(RdcPort *port);

extern void rdc_handle_plannode(RdcPort **rdc_nodes, int rdc_num, List *pln_list);
extern void rdc_handle_reduce(RdcPort **rdc_nodes, int rdc_num, List **pln_list);

#endif	/* RDC_MSG_H */
