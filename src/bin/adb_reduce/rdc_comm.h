/*-------------------------------------------------------------------------
 *
 * rdc_comm.h
 *	  interface for communication
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/bin/adb_reduce/rdc_comm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RDC_COMM_H
#define RDC_COMM_H

#include "getaddrinfo.h"
#include "rdc_tupstore.h"
#include "lib/stringinfo.h"

typedef enum
{
	CONNECTION_OK,
	CONNECTION_BAD,
	/* Non-blocking mode only below here */

	/*
	 * The existence of these should never be relied upon - they should only
	 * be used for user feedback or similar purposes.
	 */
	CONNECTION_STARTED,					/* Waiting for connection to be made.  */
	CONNECTION_MADE,					/* Connect OK; waiting to send startup request */
	CONNECTION_AWAITING_RESPONSE,		/* Send startup request OK; Waiting for a response from the server */
	CONNECTION_ACCEPT,					/* Accept OK; waiting for a startup request from client */
	CONNECTION_SENDING_RESPONSE,		/* Receive startup request OK; waiting to send response */
	CONNECTION_AUTH_OK,					/* No use here. */
	CONNECTION_ACCEPT_NEED,				/* Internal state: accpet() needed */
	CONNECTION_NEEDED					/* Internal state: connect() needed */
} ConnStatusType;

typedef enum
{
	RDC_POLLING_FAILED = 0,
	RDC_POLLING_READING,				/* These two indicate that one may	  */
	RDC_POLLING_WRITING,				/* use select before polling again.   */
	RDC_POLLING_OK,
} RdcPollingStatusType;

typedef int RdcPortId;

#define InvalidPortId			-1
#define PortIdIsValid(id)		((id) > InvalidPortId)
#define PortIdIsEven(id)		((id) % 2 == 0)		/* even number */
#define PortIdIsOdd(id)			((id) % 2 == 1)		/* odd nnumber */

typedef int RdcPortType;

#define TYPE_UNDEFINE			(1 << 0)	/* used for accept */
#define TYPE_LOCAL				(1 << 1)	/* used for listen */
#define TYPE_BACKEND			(1 << 2)	/* used for interprocess communication */
#define TYPE_PLAN				(1 << 3)	/* used for plan node from backend */
#define TYPE_REDUCE				(1 << 4)	/* used for connect to or connected from other reduce */

#define InvalidPortType			TYPE_UNDEFINE
#define PortTypeIsValid(typ)	((typ) == TYPE_PLAN || \
								 (typ) == TYPE_REDUCE)

#define WE_NONE					(0)
#define WE_SOCKET_READABLE		(1 << 0)
#define WE_SOCKET_WRITEABLE		(1 << 1)

struct RdcPort
{
	RdcPort			   *next;			/* RdcPort next for Plan node port with the same plan id */
	pgsocket			sock;			/* File descriptors for one plan node id */
	bool				noblock;		/* is the socket in non-blocking mode? */
	RdcPortType			type;			/* port type, see above */
	RdcPortId			from_to;		/* accept from or connect to */
	int					version;		/* version num */
#ifdef DEBUG_ADB
	char			   *hoststr;
	char			   *portstr;
#endif

	struct sockaddr		laddr;			/* local addr */
	struct sockaddr		raddr;			/* remote addr */

	struct addrinfo	   *addrs;			/* used for connect */
	struct addrinfo	   *addr_cur;		/* used for connect */
	ConnStatusType		status;			/* used to connect other Reduce */

	uint32				wait_events;	/* used for select/poll */
	StringInfoData		in_buf;			/* for normal message */
	StringInfoData		out_buf;		/* for normal message */
	StringInfoData		err_buf;		/* error message should be sent prior if have. */
};

struct PlanPort
{
	RdcPort			   *port;
	RdcPortId			pln_id;
	int					work_num;
	RSstate			   *rdcstore;
	int					rdc_num;
	bool				rdc_eofs[1];
};

#ifdef DEBUG_ADB
#define RdcHostStr(port)			(((RdcPort *) (port))->hoststr)
#define RdcPortStr(port)			(((RdcPort *) (port))->portstr)
#else
#define RdcHostStr(port)			"null"
#define RdcPortStr(port)			"null"
#endif
#define RdcNext(port)				(((RdcPort *) (port))->next)
#define RdcVersion(port)			(((RdcPort *) (port))->version)
#define RdcSocket(port)				(((RdcPort *) (port))->sock)
#define RdcType(port)				(((RdcPort *) (port))->type)
#define RdcID(port)					(((RdcPort *) (port))->from_to)
#define RdcStatus(port)				(((RdcPort *) (port))->status)
#define RdcWaitEvent(port)			(((RdcPort *) (port))->wait_events)
#define RdcWaitRead(port)			((((RdcPort *) (port))->wait_events) & WE_SOCKET_READABLE)
#define RdcWaitWrite(port)			((((RdcPort *) (port))->wait_events) & WE_SOCKET_WRITEABLE)
#define RdcError(port)				rdc_geterror(port)
#define RdcTypeStr(port)			rdc_type2string(RdcType(port))
#define RdcInBuf(port)				&(((RdcPort *) (port))->in_buf)
#define RdcOutBuf(port)				&(((RdcPort *) (port))->out_buf)
#define RdcErrBuf(port)				&(((RdcPort *) (port))->err_buf)

#define RdcSockIsValid(port)		(RdcSocket(port) != PGINVALID_SOCKET)
#define IsRdcPortError(port)		(RdcStatus(port) == CONNECTION_BAD || \
									 ((RdcPort *) (port))->err_buf.len > 0)

#define IsPortForBackend(port)		(RdcType(port) == TYPE_BACKEND)
#define IsPortForPlan(port)			(RdcType(port) == TYPE_PLAN)
#define IsPortForReduce(port)		(RdcType(port) == TYPE_REDUCE)

typedef int PlanNodeId;
#define InvalidPlanNodeId			-1
#define PlanPortIsValid(port)		(IsPortForPlan(port) && \
									 PortIdIsValid(RdcID(port)))

typedef int ReduceNodeId;
#define InvalidReduceId				-1
#define ReducePortIsValid(port)		(IsPortForReduce(port) && \
									 PortIdIsValid(RdcID(port)))

extern const char *rdc_type2string(RdcPortType type);
extern RdcPort *rdc_newport(pgsocket sock);
extern void rdc_freeport(RdcPort *port);
extern void rdc_resetport(RdcPort *port);
extern RdcPort *rdc_connect(const char *host, uint32 port, RdcPortType type, RdcPortId id);
extern RdcPort *rdc_accept(pgsocket sock);
extern int rdc_parse_group(RdcPort *port,			/* IN */
						   int *rdc_num,			/* OUT */
#ifdef DEBUG_ADB
						   ReduceInfo **nodeinfos,	/* OUT */
#endif
						   List **connect_list);	/* OUT */
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

extern PlanPort *plan_newport(RdcPortId pln_id);
extern void plan_freeport(PlanPort *pln_port);
extern PlanPort *find_plan_port(List *pln_list, RdcPortId pln_id);
extern void add_new_plan_port(List **pln_list, RdcPort *new_port);
extern int get_plan_port_num(List *pln_list);
/* -----------Reduce format functions---------------- */
extern void rdc_beginmessage(StringInfo buf, char msgtype);
extern void rdc_sendbyte(StringInfo buf, int byt);
extern void rdc_sendbytes(StringInfo buf, const char *data, int datalen);
extern void rdc_sendstring(StringInfo buf, const char *str);
extern void rdc_sendint(StringInfo buf, int i, int b);
extern void rdc_sendint64(StringInfo buf, int64 i);
extern void rdc_sendfloat4(StringInfo buf, float4 f);
extern void rdc_sendfloat8(StringInfo buf, float8 f);
extern void rdc_sendlength(StringInfo buf);
extern void rdc_endmessage(RdcPort *port, StringInfo buf);
extern void rdc_enderror(RdcPort *port, StringInfo buf);

extern int	rdc_getmsgbyte(StringInfo msg);
extern unsigned int rdc_getmsgint(StringInfo msg, int b);
extern int64 rdc_getmsgint64(StringInfo msg);
extern float4 rdc_getmsgfloat4(StringInfo msg);
extern float8 rdc_getmsgfloat8(StringInfo msg);
extern const char *rdc_getmsgbytes(StringInfo msg, int datalen);
extern void rdc_copymsgbytes(StringInfo msg, char *buf, int datalen);
extern const char *rdc_getmsgstring(StringInfo msg);
extern void rdc_getmsgend(StringInfo msg);

#endif	/* RDC_COMM_H */
