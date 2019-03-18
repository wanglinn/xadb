
#include "agtm/agtm_transaction.h"
#include "libpq/pqnone.h"
#include "libpq/pqnode.h"
#include "storage/latch.h"

#define AGTM_WAIT_EVENT_ENLARGE_STEP	64
/* listen socket and postmaster death */
#define AGTM_WAIT_EVENT_EXIT_SIZE		2

/*
 * don't use FeBeWaitSet, function secure_read call WaitEventSetWait
 * pass in only one WaitEvent parameter, when pq_comm_node have or listen
 * has event, it does not have a correspoding processing flow
 */
static WaitEventSet *agtm_wait_event_set = NULL;
static WaitEvent *agtm_wait_event = NULL;
static Size agtm_event_max_count = 0;
static Size agtm_event_cur_count = 0;
static pgsocket agtm_listen_socket = PGINVALID_SOCKET;
int agtm_listen_port = 0;
static StringInfoData agtm_pq_buf = {NULL, 0, 0, 0};

/* events */
typedef struct AGTMWaitEventData AGTMWaitEventData;
typedef int (*OnAGTMEventFunction)(WaitEvent *event, StringInfo buf);
typedef void (*OnAGTMPreWaitEventFunction)(AGTMWaitEventData *data, int pos);
struct AGTMWaitEventData
{
	OnAGTMEventFunction event_func;
	OnAGTMPreWaitEventFunction pre_wait;
};

typedef struct AGTMMainWaitEventData
{
	AGTMWaitEventData	base;
	int					cur_event;
	bool				closing;
}AGTMMainWaitEventData;

typedef struct AGTMNodeWaitEventData
{
	AGTMWaitEventData	base;

	pq_comm_node	   *node;
	int					cur_event;
	bool				closing;
}AGTMNodeWaitEventData;

static int on_agtm_postmaster_death(WaitEvent *event, StringInfo buf);
static int on_agtm_listen_event(WaitEvent *event, StringInfo buf);
static void on_agtm_main_pre_event(AGTMWaitEventData* data, int pos);
static int on_agtm_main_client_event(WaitEvent *event, StringInfo buf);
static void on_agtm_node_pre_event(AGTMWaitEventData* data, int pos);
static int on_agtm_node_client_event(WaitEvent *event, StringInfo buf);

static const AGTMWaitEventData agtm_event_postmaster_death_data = {on_agtm_postmaster_death, NULL};
static const AGTMWaitEventData agtm_event_listen_data = {on_agtm_listen_event, NULL};

static void end_agtm_multi_client_service(int code, Datum arg);
static int agtm_try_port_msg(StringInfo s);

static void begin_agtm_multi_client_service(void)
{
	struct sockaddr_in listen_addr;
	socklen_t slen;
	pgsocket sock;
	AGTMMainWaitEventData *wed;
	Assert(agtm_listen_socket == PGINVALID_SOCKET);

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if(sock == PGINVALID_SOCKET)
	{
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("Can not create %s socket:%m", _("IPv4"))));
	}
	on_proc_exit(end_agtm_multi_client_service, 0);

	memset(&listen_addr, 0, sizeof(listen_addr));
	listen_addr.sin_family = AF_INET;
	/*listen_addr.sin_port = 0;*/
	listen_addr.sin_addr.s_addr = INADDR_ANY;
	if(bind(sock, (struct sockaddr *)&listen_addr,sizeof(listen_addr)) < 0
		|| listen(sock, PG_SOMAXCONN) < 0)
	{
		closesocket(sock);
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("could not listen on %s socket: %m", _("IPv4"))));
	}

	/* get listen port on */
	memset(&listen_addr, 0, sizeof(listen_addr));
	slen = sizeof(listen_addr);
	if(getsockname(sock, (struct sockaddr*)&listen_addr, &slen) < 0)
	{
		closesocket(sock);
		ereport(ERROR, (errcode_for_socket_access(),
			errmsg("could not get listen port on socket:%m")));
	}
	agtm_listen_socket = sock;
	agtm_listen_port = htons(listen_addr.sin_port);

	/* create WaitEventSet */
	agtm_wait_event_set = CreateWaitEventSet(TopMemoryContext,
											 AGTM_WAIT_EVENT_ENLARGE_STEP);
	agtm_wait_event = MemoryContextAlloc(TopMemoryContext,
										 sizeof(agtm_wait_event[0]) * AGTM_WAIT_EVENT_ENLARGE_STEP);
	agtm_event_max_count = AGTM_WAIT_EVENT_ENLARGE_STEP;
	agtm_event_cur_count = 0;

	AddWaitEventToSet(agtm_wait_event_set,
					 WL_SOCKET_READABLE,
					 agtm_listen_socket,
					 NULL,
					 (void*)&agtm_event_listen_data);
	++agtm_event_cur_count;

	AddWaitEventToSet(agtm_wait_event_set,
					  WL_POSTMASTER_DEATH,
					  PGINVALID_SOCKET,
					  NULL,
					  (void*)&agtm_event_postmaster_death_data);
	++agtm_event_cur_count;

	/* all base events add end, if you wang add other event(s), must change AGTM_WAIT_EVENT_EXIT_SIZE */
	Assert(agtm_event_cur_count == AGTM_WAIT_EVENT_EXIT_SIZE);
	wed = MemoryContextAllocZero(TopMemoryContext, sizeof(*wed));
	wed->base.event_func = on_agtm_main_client_event;
	wed->base.pre_wait = on_agtm_main_pre_event;
	wed->cur_event =  WL_SOCKET_READABLE;
	wed->closing = false;
	AddWaitEventToSet(agtm_wait_event_set,
					  WL_SOCKET_READABLE,
					  MyProcPort->sock,
					  NULL,
					  wed);
	++agtm_event_cur_count;

	/* init stringInfo if not inited */
	if(agtm_pq_buf.data == NULL)
	{
		MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
		initStringInfo(&agtm_pq_buf);
		MemoryContextSwitchTo(old_ctx);
	}
}

static void end_agtm_multi_client_service(int code, Datum arg)
{
	if(agtm_listen_socket != PGINVALID_SOCKET)
	{
		closesocket(agtm_listen_socket);
		agtm_listen_socket = PGINVALID_SOCKET;
	}
	if (agtm_wait_event_set != NULL)
	{
		FreeWaitEventSet(agtm_wait_event_set);
		pfree(agtm_wait_event);
	}
}

static int agtm_ReadCommand(StringInfo inBuf)
{
	AGTMWaitEventData  *evd;
	int					nevent;
	int					firstChar;

	HOLD_CANCEL_INTERRUPTS();

	do
	{
		/* here we have no client to send message */
		pq_switch_to_none();

		for(nevent=agtm_event_cur_count;nevent>0;)
		{
			--nevent;
			evd = GetWaitEventData(agtm_wait_event_set, nevent);
			if (evd->pre_wait)
				(*evd->pre_wait)(evd, nevent);
		}
		/* pre_wait maybe remove itself */
		if (agtm_event_cur_count <= AGTM_WAIT_EVENT_EXIT_SIZE)
			return EOF;

		nevent = WaitEventSetWait(agtm_wait_event_set,
								  -1,
								  agtm_wait_event,
								  agtm_event_cur_count,
								  WAIT_EVENT_CLIENT_READ);

		while (nevent > 0)
		{
			--nevent;
			pq_switch_to_none();
			evd = agtm_wait_event[nevent].user_data;
			firstChar = (*evd->event_func)(&agtm_wait_event[nevent], inBuf);
			Assert(firstChar >= 0 && firstChar != 'X');
			if (firstChar != 0)
				goto end_try_node_;
		}
	}while(true);

end_try_node_:
	/* wait client */
	RESUME_CANCEL_INTERRUPTS();
	return firstChar;
}

static int on_agtm_postmaster_death(WaitEvent *event, StringInfo buf)
{
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating connection due to unexpected postmaster exit")));
}

static int on_agtm_listen_event(WaitEvent *event, StringInfo buf)
{
	MemoryContext					oldcontext;
	pq_comm_node * volatile			node = NULL;
	AGTMNodeWaitEventData *volatile wevd = NULL;
	volatile pgsocket 				sock = PGINVALID_SOCKET;
	Assert(event->fd == agtm_listen_socket);

	PG_TRY();
	{
		sock = accept(agtm_listen_socket, NULL, 0);
		if(sock == PGINVALID_SOCKET)
		{
			ereport(COMMERROR,
					(errcode_for_socket_access(),
					errmsg("backend can not accept new client:%m")));
		}else
		{
			oldcontext = MemoryContextSwitchTo(TopMemoryContext);
			node = pq_node_new(sock, true);
			wevd = palloc0(sizeof(*wevd));
			if (agtm_event_cur_count == agtm_event_max_count)
			{
				Size new_size = agtm_event_cur_count + AGTM_WAIT_EVENT_ENLARGE_STEP;
				Assert(new_size > agtm_event_max_count);
				agtm_wait_event_set = EnlargeWaitEventSet(agtm_wait_event_set, (int)new_size);
				agtm_wait_event = repalloc(agtm_wait_event,
										   sizeof(*agtm_wait_event) * new_size);
				agtm_event_max_count = new_size;
			}
			Assert(agtm_event_cur_count < agtm_event_max_count);
			wevd->base.event_func = on_agtm_node_client_event;
			wevd->base.pre_wait = on_agtm_node_pre_event;
			wevd->node = node;
			wevd->closing = false;
			MemoryContextSwitchTo(oldcontext);

			/* must at end of PG_TRY() */
			wevd->cur_event = WL_SOCKET_READABLE;
			AddWaitEventToSet(agtm_wait_event_set,
							  WL_SOCKET_READABLE,
							  sock,
							  NULL,
							  wevd);
			++agtm_event_cur_count;
		}
	}PG_CATCH();
	{
		if (wevd)
			pfree(wevd);
		if (node)
			pq_node_close(node);
		else if (sock != PGINVALID_SOCKET)
			closesocket(sock);
		PG_RE_THROW();
	}PG_END_TRY();

	return 0;
}

static void on_agtm_main_pre_event(AGTMWaitEventData* data, int pos)
{
	AGTMMainWaitEventData *wed = (AGTMMainWaitEventData*)data;
	int new_events;

	Assert(GetWaitEventData(agtm_wait_event_set, pos) == data);
	Assert(wed->base.pre_wait == on_agtm_main_pre_event);

	if (MyProcPort->sock == PGINVALID_SOCKET ||
		wed->closing)
	{
		RemoveWaitEvent(agtm_wait_event_set, pos);
		--agtm_event_cur_count;
		pfree(wed);
		return;
	}

	new_events = socket_is_send_pending() ? WL_SOCKET_WRITEABLE:WL_SOCKET_READABLE;
	if (new_events != wed->cur_event)
	{
		ModifyWaitEvent(agtm_wait_event_set,
						pos,
						new_events,
						NULL);
		wed->cur_event = new_events;
	}
}

static int on_agtm_main_client_event(WaitEvent *event, StringInfo buf)
{
	AGTMMainWaitEventData *wed;
	int msg_type = 0;

	if (event->events & WL_SOCKET_READABLE)
	{
		if (pq_recvbuf() != 0)
			goto main_client_free_;

		msg_type = agtm_try_port_msg(buf);
		if(msg_type == 'X')
			goto main_client_free_;

		if(msg_type != 0)
		{
			pq_switch_to_socket();
			return msg_type;
		}
	}else if (event->events & WL_SOCKET_WRITEABLE)
	{
		if (socket_flush() != 0)
			goto main_client_free_;
	}

	return msg_type;

main_client_free_:
	wed = (AGTMMainWaitEventData*)event->user_data;
	wed->closing = true;
	return 0;
}

static void on_agtm_node_pre_event(AGTMWaitEventData* data, int pos)
{
	AGTMNodeWaitEventData *wed = (AGTMNodeWaitEventData*)data;
	pq_comm_node *node = wed->node;
	int new_event;
	bool write_only = pq_node_is_write_only(node);
	bool send_pending = pq_node_send_pending(node);

	if (wed->closing || (write_only && !send_pending))
	{
		RemoveWaitEvent(agtm_wait_event_set, pos);
		--agtm_event_cur_count;
		pq_node_close(node);
		pfree(wed);
		return;
	}

	new_event = send_pending ? WL_SOCKET_WRITEABLE:WL_SOCKET_READABLE;
	if (new_event != wed->cur_event)
	{
		ModifyWaitEvent(agtm_wait_event_set,
						pos,
						new_event,
						NULL);
		wed->cur_event = new_event;
	}
}

static int on_agtm_node_client_event(WaitEvent *event, StringInfo buf)
{
	AGTMNodeWaitEventData *wed = (AGTMNodeWaitEventData*)event->user_data;
	pq_comm_node *node = wed->node;
	int msg_type = 0;

	Assert(socket_pq_node(node) == event->fd);
	if (event->events & WL_SOCKET_WRITEABLE)
	{
		if(pq_node_flush_sock(node) != 0)
			goto node_client_free_;
	}

	if (event->events & WL_SOCKET_READABLE)
	{
		if (pq_node_recvbuf(node) != 0)
			goto node_client_free_;
		msg_type = pq_node_get_msg(buf, node);
		if (msg_type == 'X')
			goto node_client_free_;
		if (msg_type != 0)
			pq_node_switch_to(node);
	}

	return msg_type;

node_client_free_:
	wed->closing = true;
	return 0;
}

static int agtm_try_port_msg(StringInfo s)
{
	int32 msg_len;
	int msg_type;
	AssertArg(s);
	Assert(MyProcPort->sock != PGINVALID_SOCKET);

	/* get message type and length */
	if(agtm_pq_buf.len < 5)
		pq_getmessage_noblock(&agtm_pq_buf, 5-agtm_pq_buf.len);
	if(agtm_pq_buf.len >= 5)
	{
		memcpy(&msg_len, agtm_pq_buf.data + 1, sizeof(msg_len));
		msg_len = htonl(msg_len);
		msg_len++;	/* add length of msg type */
		if(msg_len > 5 && agtm_pq_buf.len < msg_len)
			pq_getmessage_noblock(&agtm_pq_buf, msg_len-agtm_pq_buf.len);
		if(agtm_pq_buf.len >= msg_len)
		{
			msg_type = agtm_pq_buf.data[0];
			appendBinaryStringInfo(s, agtm_pq_buf.data+5, msg_len-5);
			if(agtm_pq_buf.len > msg_len)
			{
				agtm_pq_buf.len -= msg_len;
				memmove(agtm_pq_buf.data, agtm_pq_buf.data + msg_len, agtm_pq_buf.len);
			}else
			{
				agtm_pq_buf.len = 0;
			}
			return msg_type;
		}
	}
	return 0;
}
