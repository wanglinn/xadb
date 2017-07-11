/*-------------------------------------------------------------------------
 *
 * rdc_msg.h
 *	  interface for message
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/include/reduce/rdc_msg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RDC_MSG_H
#define RDC_MSG_H

#include "reduce/rdc_comm.h"

#define RDC_VERSION_NUM		PG_VERSION_NUM

extern RdcPortId		MyReduceId;
extern int				MyReduceIdx;

struct RdcListenMask
{
	RdcPortId	rdc_rpid;
	int			rdc_port;
	char	   *rdc_host;
};

extern void rdc_freemasks(RdcListenMask *masksm, int num);
extern int rdc_portidx(RdcListenMask *rdc_masks, int num, RdcPortId roid);

/* -----------Reduce message------------- */
#define RDC_LISTEN_PORT		'L'
#define RDC_ERROR_MSG		'E'
#define RDC_CLOSE_MSG		'C'
#define RDC_EOF_MSG			'e'
#define RDC_START_RQT		'S'
#define RDC_START_RSP		's'
#define RDC_GROUP_RQT		'G'
#define RDC_GROUP_RSP		'g'
#define RDC_P2R_DATA		'P'
#define RDC_R2P_DATA		'p'
#define RDC_R2R_DATA		'R'

extern int rdc_send_startup_rqt(RdcPort *port, RdcPortType type, RdcPortId id);
extern int rdc_send_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id);
extern int rdc_recv_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id);

extern int rdc_send_group_rqt(RdcPort *port, RdcListenMask *rdc_masks, int num);
extern int rdc_send_group_rsp(RdcPort *port);
extern int rdc_recv_group_rsp(RdcPort *port);

extern int rdc_send_close_rqt(RdcPort *port);

#endif	/* RDC_MSG_H */
