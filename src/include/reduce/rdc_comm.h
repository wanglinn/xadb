/*-------------------------------------------------------------------------
 *
 * rdc_comm.h
 *	  interface for communication
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/include/reduce/rdc_comm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RDC_COMM_H
#define RDC_COMM_H

#if defined(RDC_FRONTEND)
#include "rdc_globals.h"
#else
#include "postgres.h"
#include "nodes/pg_list.h"
#endif
#include "getaddrinfo.h"
#include "lib/stringinfo.h"
#include "reduce/wait_event.h"

#define IS_AF_INET(fam) ((fam) == AF_INET)

extern pgsocket MyBossSock;

#if !defined(RDC_FRONTEND)
typedef struct RdcPort RdcPort;
typedef struct RdcMask RdcMask;
typedef struct RdcNode RdcNode;
#endif

typedef enum
{
	RDC_CONNECTION_OK,
	RDC_CONNECTION_BAD,
	/* Non-blocking mode only below here */

	/*
	 * The existence of these should never be relied upon - they should only
	 * be used for user feedback or similar purposes.
	 */
	RDC_CONNECTION_STARTED,					/* Waiting for connection to be made.  */
	RDC_CONNECTION_MADE,					/* Connect OK; waiting to send startup request */
	RDC_CONNECTION_AWAITING_RESPONSE,		/* Send startup request OK; Waiting for a response from the server */
	RDC_CONNECTION_ACCEPT,					/* Accept OK; waiting for a startup request from client */
	RDC_CONNECTION_SENDING_RESPONSE,		/* Receive startup request OK; waiting to send response */
	RDC_CONNECTION_AUTH_OK,					/* No use here. */
	RDC_CONNECTION_ACCEPT_NEED,				/* Internal state: accpet() needed */
	RDC_CONNECTION_NEEDED					/* Internal state: connect() needed */
} RdcConnStatusType;

typedef enum
{
	RDC_FLAG_NONE	=	(1 << 0),
	RDC_FLAG_VALID	=	(1 << 1),
	RDC_FLAG_CLOSED	=	(1 << 2),
	RDC_FLAG_RESET	=	(1 << 3),
} RdcFlagType;

typedef enum
{
	RDC_POLLING_FAILED = 0,
	RDC_POLLING_READING,				/* These two indicate that one may	  */
	RDC_POLLING_WRITING,				/* use select before polling again.   */
	RDC_POLLING_OK,
} RdcPollingStatusType;

typedef int64 RdcPortId;

#define PORTID_FORMAT			INT64_FORMAT
#define InvalidPortId			((RdcPortId) -1)

typedef enum
{
	TYPE_UNDEFINE	=	(1 << 0),	/* used for accept */
	TYPE_LOCAL		=	(1 << 1),	/* used for listen */
	TYPE_BACKEND	=	(1 << 2),	/* used for interprocess communication */
	TYPE_PLAN		=	(1 << 3),	/* used for plan node from backend */
	TYPE_REDUCE		=	(1 << 4),	/* used for connect to or connected from other reduce */
} RdcPortType;

#define InvalidPortType			TYPE_UNDEFINE
#define PortTypeIsValid(typ)	((typ) == TYPE_PLAN || \
								 (typ) == TYPE_REDUCE)

struct RdcMask
{
	RdcPortId			rdc_rpid;
	int 				rdc_port;
	char			   *rdc_host;
};

struct RdcNode
{
	RdcMask				mask;
	RdcPort			   *port;
};
#define RdcNodeID(node)				(((RdcNode *) (node))->mask.rdc_rpid)

typedef void (*RdcConnHook)(void *arg);

struct RdcPort
{
	RdcPort			   *next;			/* RdcPort next for Plan node port with the same plan id */
	pgsocket			sock;			/* file descriptors for one plan node id */
	bool				noblock;		/* is the socket in non-blocking mode? */
	bool				positive;		/* true means connect, false means be connected */
	bool				send_eof;		/* true if send EOF message */
	RdcPortType			peer_type;		/* the identity type of the peer side */
	RdcPortId			peer_id;		/* the identity id of the peer side */
	RdcPortType			self_type;		/* local identity type */
	RdcPortId			self_id;		/* local identity id */
	int					version;		/* version num */
#ifdef DEBUG_ADB
	char			   *peer_host;		/* remote host string */
	char			   *peer_port;		/* remote port string */
	char			   *self_host;		/* local host string */
	char			   *self_port;		/* local port string */
#endif

	struct sockaddr		laddr;			/* local address */
	struct sockaddr		raddr;			/* remote address */

	struct addrinfo	   *addrs;			/* used for connect */
	struct addrinfo	   *addr_cur;		/* used for connect */
	RdcConnStatusType	status;			/* used to connect with other Reduce */
	RdcConnHook			hook;			/* callback function when connect done */

	RdcFlagType			flags;			/* used for running flags */
	EventType			wait_events;	/* used for select/poll */
	StringInfoData		in_buf;			/* for normal message */
	StringInfoData		out_buf;		/* for normal message */
	StringInfoData		err_buf;		/* error message should be sent prior if have. */
};

#ifdef DEBUG_ADB
#define RdcPeerHost(port)			(((RdcPort *) (port))->peer_host)
#define RdcPeerPort(port)			(((RdcPort *) (port))->peer_port)
#define RdcSelfHost(port)			(((RdcPort *) (port))->self_host)
#define RdcSelfPort(port)			(((RdcPort *) (port))->self_port)
#define RDC_PORT_PRINT_FORMAT		" [%s %ld] {%s:%s}"
#define RDC_PORT_PRINT_VALUE(port)	RdcPeerTypeStr(port), RdcPeerID(port), RdcPeerHost(port), RdcPeerPort(port)
#else
#define RDC_PORT_PRINT_FORMAT		" [%s %ld]"
#define RDC_PORT_PRINT_VALUE(port)	RdcPeerTypeStr(port), RdcPeerID(port)
#endif
#define RdcNext(port)				(((RdcPort *) (port))->next)
#define RdcVersion(port)			(((RdcPort *) (port))->version)
#define RdcSocket(port)				(((RdcPort *) (port))->sock)
#define RdcPeerType(port)			(((RdcPort *) (port))->peer_type)
#define RdcPeerID(port)				(((RdcPort *) (port))->peer_id)
#define RdcSelfType(port)			(((RdcPort *) (port))->self_type)
#define RdcSelfID(port)				(((RdcPort *) (port))->self_id)
#define RdcStatus(port)				(((RdcPort *) (port))->status)
#define RdcFlags(port)				(((RdcPort *) (port))->flags)
#define RdcPositive(port)			(((RdcPort *) (port))->positive)
#define RdcHook(port)				(((RdcPort *) (port))->hook)
#define RdcSendEOF(port)			(((RdcPort *) (port))->send_eof)
#define RdcWaitEvents(port)			(((RdcPort *) (port))->wait_events)
#define RdcWaitRead(port)			((((RdcPort *) (port))->wait_events) & WT_SOCK_READABLE)
#define RdcWaitWrite(port)			((((RdcPort *) (port))->wait_events) & WT_SOCK_WRITEABLE)
#define RdcError(port)				rdc_geterror(port)
#define RdcPeerTypeStr(port)		rdc_type2string(RdcPeerType(port))
#define RdcSelfTypeStr(port)		rdc_type2string(RdcSelfType(port))
#define RdcInBuf(port)				&(((RdcPort *) (port))->in_buf)
#define RdcOutBuf(port)				&(((RdcPort *) (port))->out_buf)
#define RdcErrBuf(port)				&(((RdcPort *) (port))->err_buf)

#define RdcSockIsValid(port)		(RdcSocket(port) != PGINVALID_SOCKET)
#define IsRdcPortError(port)		(RdcStatus(port) == RDC_CONNECTION_BAD || \
									 ((RdcPort *) (port))->err_buf.len > 0)

#define PortIsValid(port)			((port) != NULL && RdcFlags(port) == RDC_FLAG_VALID)
#define PortMustClosed(port)		((port) != NULL && RdcFlags(port) == RDC_FLAG_CLOSED)

#define PortForBackend(port)		(RdcPeerType(port) == TYPE_BACKEND)
#define PortForPlan(port)			(RdcPeerType(port) == TYPE_PLAN)
#define PortForReduce(port)			(RdcPeerType(port) == TYPE_REDUCE)

#define PortAddEvents(port, events)	do {if (port) {RdcWaitEvents(port) |= (events);}} while(0)
#define PortRmvEvents(port, events)	do {if (port) {RdcWaitEvents(port) &= ~(events);}} while(0)

#define PlanTypeIDIsValid(port)		((port) != NULL && \
									 PortForPlan(port) && \
									 RdcPeerID(port) > InvalidPortId)

#define ReduceTypeIDIsValid(port)	((port) != NULL && \
									 PortForReduce(port) && \
									 RdcPeerID(port) > InvalidPortId &&\
									 !RdcIdIsSelfID(RdcPeerID(port)))

#define PortTypeIDIsValid(port)		(PlanTypeIDIsValid(port) || ReduceTypeIDIsValid(port))

extern const char *rdc_type2string(RdcPortType type);
extern bool BossIsLeave(void);
extern RdcPort *rdc_newport(pgsocket sock,
							RdcPortType peer_type, RdcPortId peer_id,
							RdcPortType self_type, RdcPortId self_id);
extern void rdc_freeport(RdcPort *port);
extern void rdc_resetport(RdcPort *port);
extern RdcPort *rdc_connect(const char *host, uint32 port,
							RdcPortType peer_type, RdcPortId peer_id,
							RdcPortType self_type, RdcPortId self_id);
extern RdcPort *rdc_accept(pgsocket sock);
extern RdcNode *rdc_parse_group(RdcPort *port, int *rdc_num, RdcConnHook hook);
extern RdcPollingStatusType rdc_connect_poll(RdcPort *port);
extern int rdc_puterror(RdcPort *port, const char *fmt, ...) pg_attribute_printf(2, 3);
extern int rdc_puterror_binary(RdcPort *port, const char *s, size_t len);
extern int rdc_putmessage(RdcPort *port, const char *s, size_t len);
extern int rdc_putmessage_extend(RdcPort *port, const char *s, size_t len, bool enlarge);
extern int rdc_flush(RdcPort *port);
extern int rdc_try_flush(RdcPort *port);
extern int rdc_recv(RdcPort *port);
extern int rdc_getbyte(RdcPort *port);
extern int rdc_getbytes(RdcPort *port, size_t len);
extern int rdc_discardbytes(RdcPort *port, size_t len);
extern int rdc_getmessage(RdcPort *port, size_t maxlen);
extern int rdc_set_block(RdcPort *port);
extern int rdc_set_noblock(RdcPort *port);
extern const char *rdc_geterror(RdcPort *port);

/* -----------Reduce format functions---------------- */
extern void rdc_beginmessage(StringInfo buf, char msgtype);
extern void rdc_sendbyte(StringInfo buf, int byt);
extern void rdc_sendbytes(StringInfo buf, const char *data, int datalen);
extern void rdc_sendstring(StringInfo buf, const char *str);
extern void rdc_sendint(StringInfo buf, int i, int b);
extern void rdc_sendint64(StringInfo buf, int64 i);
extern void rdc_sendRdcPortID(StringInfo buf, RdcPortId id);
extern void rdc_sendfloat4(StringInfo buf, float4 f);
extern void rdc_sendfloat8(StringInfo buf, float8 f);
extern void rdc_sendlength(StringInfo buf);
extern void rdc_endmessage(RdcPort *port, StringInfo buf);
extern void rdc_enderror(RdcPort *port, StringInfo buf);

extern int	rdc_getmsgbyte(StringInfo msg);
extern unsigned int rdc_getmsgint(StringInfo msg, int b);
extern int64 rdc_getmsgint64(StringInfo msg);
extern RdcPortId rdc_getmsgRdcPortID(StringInfo msg);
extern float4 rdc_getmsgfloat4(StringInfo msg);
extern float8 rdc_getmsgfloat8(StringInfo msg);
extern const char *rdc_getmsgbytes(StringInfo msg, int datalen);
extern void rdc_copymsgbytes(StringInfo msg, char *buf, int datalen);
extern const char *rdc_getmsgstring(StringInfo msg);
extern void rdc_getmsgend(StringInfo msg);

#endif	/* RDC_COMM_H */
