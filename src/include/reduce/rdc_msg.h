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

#define RdcIdIsSelfID(rpid)		(((RdcPortId) (rpid)) == MyReduceId)

/* -----------Reduce message------------- */
#define MSG_LISTEN_PORT		'L'
#define MSG_ERROR			'E'
#define MSG_EOF				'e'
#define MSG_PLAN_CLOSE		'C'
#define MSG_RDC_CLOSE		'c'
#define MSG_START_RQT		'S'
#define MSG_START_RSP		's'
#define MSG_GROUP_RQT		'G'
#define MSG_GROUP_RSP		'g'
#define MSG_P2R_DATA		'P'
#define MSG_R2P_DATA		'p'
#define MSG_R2R_DATA		'R'

extern int rdc_send_startup_rqt(RdcPort *port, RdcPortType type, RdcPortId id, RdcExtra extra);
extern int rdc_send_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id);
extern int rdc_recv_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id);

extern int rdc_send_group_rqt(RdcPort *port, RdcMask *rdc_masks, int num);
extern int rdc_send_group_rsp(RdcPort *port);
extern int rdc_recv_group_rsp(RdcPort *port);

#endif	/* RDC_MSG_H */
