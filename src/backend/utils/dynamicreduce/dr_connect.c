#include "postgres.h"

#include "common/ip.h"
#include "libpq/pqformat.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

#ifdef DR_USING_EPOLL
#include "nodes/pg_list.h"
#endif
#include <unistd.h>

/*
 * sizeof(msg type)
 * sizeof(node oid)
 * sizeof(node pid)
 */
#define CONNECT_MSG_LENGTH	9
#define TRY_CONNECT_COUNT	3

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
static DRListenEventData *dr_listen_event = NULL;
static List *dr_connecting_node_list = NIL;
#define INSERT_CONNECTING_NODE(n_)	do{										\
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);	\
		dr_connecting_node_list = lappend(dr_connecting_node_list, n_);		\
		MemoryContextSwitchTo(oldcontext);									\
	}while(0)
#define DELETE_CONNECTING_NODE(n_)	dr_connecting_node_list = list_delete_ptr(dr_connecting_node_list, n_)
#else
static int dr_listen_pos = INVALID_EVENT_SET_POS;
#define INSERT_CONNECTING_NODE(n_)	((void)true)
#define DELETE_CONNECTING_NODE(n_)	((void)true)
#endif /* DR_USING_EPOLL || WITH_REDUCE_RDMA */
static pgsocket ConnectToAddress(const struct addrinfo *addr);

static void OnNodeConnectFromPreWait(DROnPreWaitArgs)
{
	DRNodeEventData *ned = (DRNodeEventData*)base;
	if (ned->status == DRN_WAIT_CLOSE)
	{
		FreeNodeEventInfo(ned);
	}else if (DRGotNodeInfo())
	{
		base->OnPreWait = NULL;
#ifdef WITH_REDUCE_RDMA
		RDRCtlWaitEvent(base->fd, POLLIN, base, RPOLL_EVENT_MOD);
		ned->waiting_events = POLLIN;
#elif defined DR_USING_EPOLL
		DRCtlWaitEvent(base->fd, EPOLLIN, base, EPOLL_CTL_MOD);
		ned->waiting_events = EPOLLIN;
#else
		ModifyWaitEvent(dr_wait_event_set,
						pos,
						WL_SOCKET_READABLE,
						NULL);
#endif
		DR_CONNECT_DEBUG((errmsg("node %p set wait read events", base)));
	}
}

static void OnNodeEventConnectFrom(DROnEventArgs)
{
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
	DRNodeEventData	   *ned = (DRNodeEventData*)base;
	pgsocket			fd = ned->base.fd;
#else
	DRNodeEventData    *ned = ev->user_data;
	pgsocket			fd = ev->fd;
#endif /* DR_USING_EPOLL */
	uint32				index;
	bool				found;
	Assert(ned->status == DRN_ACCEPTED);
	Assert(ned->nodeoid == InvalidOid);

	if (RecvMessageFromNode(ned, fd) <= 0 ||
		ned->recvBuf.len - ned->recvBuf.cursor < CONNECT_MSG_LENGTH)
		return;
	if (ned->recvBuf.data[ned->recvBuf.cursor] != ADB_DR_MSG_NODEOID)
	{
		ned->status = DRN_WAIT_CLOSE; /* on PreWait will destory it */
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 DRKeepError(),
				 errmsg("invalid message type %d from remote",
						ned->recvBuf.data[ned->recvBuf.cursor])));
		return;
	}

	memcpy(&ned->nodeoid,
		   ned->recvBuf.data + ned->recvBuf.cursor + 1,
		   sizeof(ned->nodeoid));
	memcpy(&ned->owner_pid,
		   ned->recvBuf.data + ned->recvBuf.cursor + (1+sizeof(ned->nodeoid)),
		   sizeof(ned->owner_pid));
	ned->recvBuf.cursor += CONNECT_MSG_LENGTH;
	if (DRSetNodeInfo(ned) == false)
	{
		ned->status = DRN_WAIT_CLOSE; /* on PreWait will destory it */
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 DRKeepError(),
				 errmsg("invalid node oid %u from remote dr status %d", ned->nodeoid, dr_status)));
	}

	if (oidBufferMember(&dr_latch_data->work_oid_buf, ned->nodeoid, &index) &&
		dr_latch_data->work_pid_buf.oids[index] != ned->owner_pid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 DRKeepError(),
				 errmsg("invalid pid %d for node %u", ned->owner_pid, ned->nodeoid)));
		FreeNodeEventInfo(ned);
		return;
	}else
	{
		DRNodeEventData *newned;

		newned = DRSearchNodeEventData(ned->nodeoid, HASH_ENTER, &found);
		if (found)
		{
			ned->status = DRN_WAIT_CLOSE;
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 DRKeepError(),
					 errmsg("replicate node oid %u from remote", ned->nodeoid)));
		}
		DR_CONNECT_DEBUG((errmsg("node %u from remote accept successed", ned->nodeoid)));
#ifdef WITH_REDUCE_RDMA
		RDRCtlWaitEvent(fd, ned->waiting_events, newned, RPOLL_EVENT_MOD);
		DELETE_CONNECTING_NODE(ned);
#elif defined DR_USING_EPOLL
		DRCtlWaitEvent(fd, ned->waiting_events, newned, EPOLL_CTL_MOD);
		DELETE_CONNECTING_NODE(ned);
#else
		ModifyWaitEventData(dr_wait_event_set, ev->pos, newned);
#endif
		memcpy(newned, ned, sizeof(*newned));
		pfree(ned);
		ned = newned;
	}

	DROnNodeConectSuccess(ned);
}

static void OnNodeEventConnectTo(DROnEventArgs)
{
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
	DRNodeEventData	   *ned = (DRNodeEventData*)base;
	pgsocket			event_fd = ned->base.fd;
#else
	DRNodeEventData    *ned = ev->user_data;
	pgsocket			event_fd = ev->fd;
#endif /* DR_USING_EPOLL */
	ssize_t size;

	Assert(DRSearchNodeEventData(ned->nodeoid, HASH_FIND, NULL) != NULL);

resend_:
#ifdef WITH_REDUCE_RDMA
	size = adb_rsend(event_fd,
				ned->sendBuf.data + ned->sendBuf.cursor,
				ned->sendBuf.len - ned->sendBuf.cursor,
				0);
#else
	size = send(event_fd,
				ned->sendBuf.data + ned->sendBuf.cursor,
				ned->sendBuf.len - ned->sendBuf.cursor,
				0);
#endif
	if (size < 0)
	{
		pgsocket new_fd;
		if (errno == EINTR)
			goto resend_;

		new_fd = PGINVALID_SOCKET;
		ned->addr_cur = ned->addr_cur->ai_next;
re_connect_:
		while (ned->addr_cur != NULL)
		{
			new_fd = ConnectToAddress(ned->addr_cur);
			if (new_fd != PGINVALID_SOCKET)
				break;
			else
				ned->addr_cur = ned->addr_cur->ai_next;
		}
		if (new_fd == PGINVALID_SOCKET)
		{
			if (ned->try_count < TRY_CONNECT_COUNT)
			{
				++(ned->try_count);
				ned->addr_cur = ned->addrlist;
				goto re_connect_;
			}
			ned->status = DRN_WAIT_CLOSE;
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not connect to any addres for remote node %u", ned->nodeoid)));
		}
#ifdef WITH_REDUCE_RDMA
		RDRCtlWaitEvent(event_fd, 0, NULL, RPOLL_EVENT_DEL);
		--dr_wait_count;
		adb_rclose(event_fd);
		base->fd = new_fd;
		RDRCtlWaitEvent(new_fd, POLLOUT, base, RPOLL_EVENT_ADD);
		++dr_wait_count;
#elif defined DR_USING_EPOLL
		DRCtlWaitEvent(event_fd, 0, NULL, EPOLL_CTL_DEL);
		--dr_wait_count;
		closesocket(event_fd);
		base->fd = new_fd;
		DRCtlWaitEvent(new_fd, EPOLLOUT, base, EPOLL_CTL_ADD);
		++dr_wait_count;
#else /* DR_USING_EPOLL */
		/* modify wait event */
		RemoveWaitEvent(dr_wait_event_set, ev->pos);
		--dr_wait_count;
		closesocket(ev->fd);
		ev->fd = new_fd;
		ev->pos = AddWaitEventToSet(dr_wait_event_set,
									WL_SOCKET_CONNECTED,
									new_fd,
									NULL,
									ned);
		++dr_wait_count;
#endif /* DR_USING_EPOLL */
	}else
	{
		ned->sendBuf.cursor += size;
		if (ned->sendBuf.cursor == ned->sendBuf.len)
		{
			DR_CONNECT_DEBUG((errmsg("connect to node %u success", ned->nodeoid)));
			DELETE_CONNECTING_NODE(ned);
			/* update status */
			resetStringInfo(&ned->sendBuf);
			DROnNodeConectSuccess(ned);
		}
	}
}

static void OnConnectError(DROnErrorArgs)
{
	DRNodeEventData *ned = (DRNodeEventData*)base;
	if (ned->status == DRN_WAIT_CLOSE)
		FreeNodeEventInfo(ned);
}

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
void CallConnectingOnError(void)
{
	ListCell	   *lc;
	DREventData	   *base;
	for (lc=list_head(dr_connecting_node_list);lc != NULL;)
	{
		base = lfirst(lc);
		Assert(base->type == DR_EVENT_DATA_NODE);
		lc = lnext(lc);
		((DRNodeEventData*)base)->status = DRN_WAIT_CLOSE;
		if (base->OnError)
			(*base->OnError)(base);
	}
}
void CallConnectiongPreWait(void)
{
	ListCell	   *lc;
	DREventData	   *base;
	for (lc=list_head(dr_connecting_node_list);lc != NULL;)
	{
		base = lfirst(lc);
		Assert(base->type == DR_EVENT_DATA_NODE);
		lc = lnext(lc);
		if (base->OnPreWait)
			(*base->OnPreWait)(base);
	}
}
bool HaveConnectingNode(void)
{
	return dr_connecting_node_list != NIL;
}
#endif /* DR_USING_EPOLL | WITH_REDUCE_RDMA */

static void ConnectToOneNode(const DynamicReduceNodeInfo *info, const struct addrinfo *hintp, DRNodeEventData **newed)
{
	DRNodeEventData	   *ned;
	MemoryContext		oldcontext;
	int					ret;
	pgsocket			fd;
	char				buf[32];
	bool				found;

	*newed = NULL;
	DR_CONNECT_DEBUG((errmsg("dynamic reduce begin connect to node %u(%s:%u)",
							 info->node_oid, NameStr(info->host), info->port)));
	ned = DRSearchNodeEventData(info->node_oid, HASH_ENTER, &found);
	Assert(ned->nodeoid == info->node_oid);
	if (found)
	{
		Assert(ned->owner_pid == info->pid);
		DR_CONNECT_DEBUG((errmsg("node %u already exist", info->node_oid)));
		return;
	}

	sprintf(buf, "%u", info->port);
	*newed = ned;
	MemSet(ned, 0, sizeof(*ned));
	ned->nodeoid = info->node_oid;
	ned->owner_pid = info->pid;
	ned->remote_port = info->port;
	ned->base.type = DR_EVENT_DATA_NODE;
	ned->base.OnEvent = OnNodeEventConnectTo;
	ned->base.OnError = OnConnectError;
	ret = pg_getaddrinfo_all(NameStr(info->host),
							 buf,
							 hintp,
							 &ned->addrlist);
	if (ret != 0 ||
		ned->addrlist == NULL)
	{
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not translate host name \"%s\" to address: %s\n",
						NameStr(info->host), gai_strerror(ret))));
	}
	ned->status = DRN_CONNECTING;
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	initStringInfoExtend(&ned->sendBuf, DR_SOCKET_BUF_SIZE_START);
	initStringInfoExtend(&ned->recvBuf, DR_SOCKET_BUF_SIZE_START);
	MemoryContextSwitchTo(oldcontext);

	appendStringInfoChar(&ned->sendBuf, ADB_DR_MSG_NODEOID);
	appendBinaryStringInfoNT(&ned->sendBuf, (char*)&PGXCNodeOid, sizeof(PGXCNodeOid));
	StaticAssertExpr(sizeof(MyBgworkerEntry->bgw_notify_pid) == sizeof(info->pid), "");
	appendBinaryStringInfoNT(&ned->sendBuf, (char*)&MyBgworkerEntry->bgw_notify_pid, sizeof(info->pid));
	ned->waiting_plan_id = INVALID_PLAN_ID;

	fd = PGINVALID_SOCKET;
	ned->addr_cur = ned->addrlist;
	while (ned->addr_cur != NULL)
	{
		fd = ConnectToAddress(ned->addr_cur);
		if (fd != PGINVALID_SOCKET)
			break;
		ned->addr_cur = ned->addr_cur->ai_next;
	}
	if (fd == PGINVALID_SOCKET)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to any addres for remote node %u(%s:%d)",
						info->node_oid, NameStr(info->host), info->port)));
	}
	INSERT_CONNECTING_NODE(ned);
#ifdef WITH_REDUCE_RDMA
	ned->base.fd = fd;
	RDRCtlWaitEvent(fd, POLLOUT, ned, RPOLL_EVENT_ADD);
	ned->waiting_events = POLLOUT;
#elif defined DR_USING_EPOLL
	ned->base.fd = fd;
	DRCtlWaitEvent(fd, EPOLLOUT, ned, EPOLL_CTL_ADD);
	ned->waiting_events = EPOLLOUT;
#else
	DREnlargeWaitEventSet();
	ret = AddWaitEventToSet(dr_wait_event_set,
							WL_SOCKET_CONNECTED,
							fd,
							NULL,
							ned);
	Assert(ret != INVALID_EVENT_SET_POS);
#endif
	dr_wait_count++;
}

void ConnectToAllNode(const DynamicReduceNodeInfo *info, uint32 count)
{
	DRNodeEventData * volatile newdata;
	struct addrinfo hint;
	uint32 i, my_index;

	if (count < 2)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("too few node for reduce connect:%u", count)));

	resetOidBuffer(&dr_latch_data->work_oid_buf);
	resetOidBuffer(&dr_latch_data->work_pid_buf);
	my_index = count;
	for (i=0;i<count;++i)
	{
		if (info[i].node_oid == PGXCNodeOid)
		{
			my_index = i;
		}else
		{
			appendOidBufferUniqueOid(&dr_latch_data->work_oid_buf, info[i].node_oid);
			appendOidBufferOid(&dr_latch_data->work_pid_buf, info[i].pid);
		}
	}
	if (my_index == count)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("can not found our node info in dynamic reduce info")));
	if (dr_latch_data->work_oid_buf.len != count -1)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("replicate node oid for reduce connect")));

	/* check is last owner pid is same this time? */
	for (i=0;i<count;++i)
	{
		newdata = DRSearchNodeEventData(info[i].node_oid, HASH_FIND, NULL);
		if (newdata != NULL &&
			(newdata->owner_pid != info[i].pid ||
			 newdata->remote_port != info[i].port))
		{
			DR_CONNECT_DEBUG((errmsg("node %u owner pid %d or port %d is not equal last, close it",
									 newdata->nodeoid, newdata->owner_pid, newdata->remote_port)));
			FreeNodeEventInfo(newdata);
		}
	}

	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_INET; //AF_UNSPEC;

	DR_CONNECT_DEBUG((errmsg("dynamic reduce %u begin connect other node", PGXCNodeOid)));
	newdata = NULL;
	PG_TRY();
	{
		if ((my_index % 2) == 0)
		{
			/* connect to even numbers smaller than myself */
			for (i=0;i<my_index;i+=2)
				ConnectToOneNode(&info[i], &hint, (DRNodeEventData **)&newdata);
			/* and odd numbers larger than myself */
			for (i=my_index+1;i<count;i+=2)
				ConnectToOneNode(&info[i], &hint, (DRNodeEventData **)&newdata);
		}else
		{
			/* connect to odd numbers smaller than myself */
			for (i=1;i<my_index;i+=2)
				ConnectToOneNode(&info[i], &hint, (DRNodeEventData **)&newdata);
			/* and even number larger then myself */
			for (i=my_index+1;i<count;i+=2)
				ConnectToOneNode(&info[i], &hint, (DRNodeEventData **)&newdata);
		}
	}PG_CATCH();
	{
		if (newdata)
			FreeNodeEventInfo(newdata);
		PG_RE_THROW();
	}PG_END_TRY();
}

static void OnListenEvent(DROnEventArgs)
{
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
	DRListenEventData *led = (DRListenEventData*)base;
	pgsocket fd = base->fd;
#else
	DRListenEventData *led = ev->user_data;
	pgsocket fd = GetWaitEventSocket(dr_wait_event_set, ev->pos);
#endif
	MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(led));
	volatile pgsocket newfd = PGINVALID_SOCKET;
	DRNodeEventData * volatile newdata = NULL;
	Assert(led->base.type == DR_EVENT_DATA_LISTEN);
	Assert(fd != PGINVALID_SOCKET);

	PG_TRY();
	{
		MemoryContext oldcontext;
		do
		{
#ifdef WITH_REDUCE_RDMA
			newfd = adb_raccept(fd, NULL, NULL);
#else
			newfd = accept(fd, NULL, NULL);
#endif
			if (newfd == PGINVALID_SOCKET)
			{
				if (errno == EAGAIN)
					break;
				ereport(ERROR,
						(errcode_for_socket_access(),
						 dr_status == DRS_WORKING ? DRKeepError():0,
						 errmsg("can not accept new socket:%m")));
			}
			if (dr_status != DRS_WORKING)
			{
#ifdef WITH_REDUCE_RDMA
				adb_rclose(newfd);
#else
				closesocket(newfd);
#endif
				ereport(LOG_SERVER_ONLY,
						(errmsg("closed an accept socket, because dynamic reduce status is %d", dr_status)));
				continue;
			}
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
			DREnlargeWaitEventSet();
#endif

			oldcontext = MemoryContextSwitchTo(TopMemoryContext);
			newdata = palloc0(sizeof(*newdata));
			newdata->base.type = DR_EVENT_DATA_NODE;
			newdata->base.OnEvent = OnNodeEventConnectFrom;
			newdata->base.OnError = OnConnectError;
			newdata->waiting_plan_id = INVALID_PLAN_ID;
			initStringInfoExtend(&newdata->sendBuf, DR_SOCKET_BUF_SIZE_START);
			initStringInfoExtend(&newdata->recvBuf, DR_SOCKET_BUF_SIZE_START);
			INSERT_CONNECTING_NODE(newdata);
			MemoryContextSwitchTo(oldcontext);

			newdata->nodeoid = InvalidOid;
			newdata->status = DRN_ACCEPTED;
			if (DRGotNodeInfo() == false)
			{
				/*
				 * when we not got others node info, don't try receive message
				 * from remote, because OnNodeEventConnectFrom function call
				 * function DRSetNodeInfo() will failed
				 */
				newdata->base.OnPreWait = OnNodeConnectFromPreWait;
				DR_CONNECT_DEBUG((errmsg("ned %p set prewait OnNodeConnectFromPreWait", newdata)));
			}
#ifdef WITH_REDUCE_RDMA
			newdata->base.fd = newfd;
			newdata->waiting_events = DRGotNodeInfo() ? POLLIN : 0;
			RDRCtlWaitEvent(newfd, newdata->waiting_events, newdata, RPOLL_EVENT_ADD);
			if (!pg_set_rnoblock(newfd))
				ereport(LOG_SERVER_ONLY,(errmsg("could not set rsocket noblocking %m")));
#elif defined DR_USING_EPOLL
			newdata->base.fd = newfd;
			newdata->waiting_events = DRGotNodeInfo() ? EPOLLIN : 0;
			DRCtlWaitEvent(newfd, newdata->waiting_events, newdata, EPOLL_CTL_ADD);
			if (!pg_set_noblock(newfd))
				ereport(LOG_SERVER_ONLY,(errmsg("could not set socket noblocking %m")));
#else
			if (!pg_set_noblock(newfd))
				ereport(LOG_SERVER_ONLY,(errmsg("could not set socket noblocking %m")));
			AddWaitEventToSet(dr_wait_event_set,
							  DRGotNodeInfo() ? WL_SOCKET_READABLE:0,
							  newfd,
							  NULL,
							  (void*)newdata);
#endif
			++dr_wait_count;
			newdata = NULL;
		}while(led->noblock);
	}PG_CATCH();
	{
		if (newfd != PGINVALID_SOCKET)
#ifdef WITH_REDUCE_RDMA
			adb_rclose(newfd);
#else
			closesocket(newfd);
#endif
		if (newdata)
		{
			DELETE_CONNECTING_NODE(newdata);
			if (newdata->recvBuf.data)
				pfree(newdata->recvBuf.data);
			if (newdata->sendBuf.data)
				pfree(newdata->sendBuf.data);
			pfree(newdata);
		}
		PG_RE_THROW();
	}PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

DRListenEventData* GetListenEventData(void)
{
	DRListenEventData  *led;
	struct sockaddr_in	addr_inet;
	socklen_t			addrlen;
	pgsocket			fd;
#ifndef WIN32
	int					one = 1;
#endif

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
	if (dr_listen_event != NULL)
		return dr_listen_event;
#else
	if (dr_listen_pos != INVALID_EVENT_SET_POS)
	{
		led = GetWaitEventData(dr_wait_event_set, dr_listen_pos);
		Assert(led->base.type == DR_EVENT_DATA_LISTEN);
		return led;
	}
	DREnlargeWaitEventSet();
#endif

	led = MemoryContextAllocZero(TopMemoryContext, sizeof(*led));
	led->base.type = DR_EVENT_DATA_LISTEN;
	led->base.OnEvent = OnListenEvent;
#ifdef WITH_REDUCE_RDMA
	fd = adb_rsocket(AF_INET, SOCK_STREAM, 0);
#else
	fd = socket(AF_INET, SOCK_STREAM, 0);
#endif
	if (fd == PGINVALID_SOCKET)
	{
		pfree(led);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create socket: %m")));
	}
#ifdef WITH_REDUCE_RDMA
	led->noblock = pg_set_rnoblock(fd);
#else
	led->noblock = pg_set_noblock(fd);
#endif
	
#ifndef WIN32
#ifdef WITH_REDUCE_RDMA
	adb_rsetsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof(one));
#else
	/* ignore result */
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof(one));
#endif
#endif
	MemSet(&addr_inet, 0, sizeof(addr_inet));
	addr_inet.sin_family = AF_INET;
	/* addr_inet.sin_port = 0; */
	addr_inet.sin_addr.s_addr = htonl(INADDR_ANY);

#ifdef WITH_REDUCE_RDMA
	if (adb_rbind(fd, (struct sockaddr *)&addr_inet, sizeof(addr_inet)) < 0)
#else
	if (bind(fd, (struct sockaddr *)&addr_inet, sizeof(addr_inet)) < 0)
#endif
	{
#ifdef WITH_REDUCE_RDMA
		adb_rclose(fd);
#else
		closesocket(fd);
#endif
		pfree(led);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not bind IPv4 socket: %m")));
	}

#ifdef WITH_REDUCE_RDMA
	if (adb_rlisten(fd, PG_SOMAXCONN) < 0)
#else
	if (listen(fd, PG_SOMAXCONN) < 0)
#endif
	{
#ifdef WITH_REDUCE_RDMA
		adb_rclose(fd);
#else
		closesocket(fd);
#endif
		pfree(led);
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not listen on IPv4 socket: %m")));
	}

	/* get random listen port */
	MemSet(&addr_inet, 0, sizeof(addr_inet));
	addrlen = sizeof(addr_inet);
#ifdef WITH_REDUCE_RDMA
	if (adb_rgetsockname(fd, (struct sockaddr *)&addr_inet, &addrlen) < 0)
#else
	if (getsockname(fd, &addr_inet, &addrlen) < 0)
#endif
	{
#ifdef WITH_REDUCE_RDMA
		adb_rclose(fd);
#else
		closesocket(fd);
#endif
		pfree(led);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("getsockname(2) failed: %m")));
	}
	led->port = htons(addr_inet.sin_port);

#ifdef WITH_REDUCE_RDMA
	led->base.fd = fd;
	RDRCtlWaitEvent(fd, POLLIN, led, RPOLL_EVENT_ADD);
	dr_listen_event = led;
#elif defined DR_USING_EPOLL
	led->base.fd = fd;
	DRCtlWaitEvent(fd, EPOLLIN, led, EPOLL_CTL_ADD);
	dr_listen_event = led;
#else
	dr_listen_pos = AddWaitEventToSet(dr_wait_event_set,
									  WL_SOCKET_READABLE,
									  fd, NULL, led);
	Assert(dr_listen_pos != INVALID_EVENT_SET_POS);
	Assert(GetWaitEventData(dr_wait_event_set, dr_listen_pos) == led);
#endif
	++dr_wait_count;

	return led;
}

static pgsocket ConnectToAddress(const struct addrinfo *addr)
{
#ifdef WITH_REDUCE_RDMA
	volatile pgsocket fd = adb_rsocket(addr->ai_family, SOCK_STREAM, 0);
#else
	volatile pgsocket fd = socket(addr->ai_family, SOCK_STREAM, 0);
#endif

	if (fd == PGINVALID_SOCKET)
	{
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create socket: %m, addr->ai_family is %d", addr->ai_family)));
	}
#ifdef WITH_REDUCE_RDMA
	if (!pg_set_rnoblock(fd))
#else
	if (!pg_set_noblock(fd))
#endif
	{
#ifdef WITH_REDUCE_RDMA
		adb_rclose(fd);
#else
		closesocket(fd);
#endif
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not set socket to nonblocking mode: %m")));
	}
#ifdef WITH_REDUCE_RDMA
	if (adb_rconnect(fd, addr->ai_addr, addr->ai_addrlen) < 0)
#else
	if (connect(fd, addr->ai_addr, addr->ai_addrlen) < 0)
#endif
	{
		if (errno != EINPROGRESS &&
#if (EINTR != EWOULDBLOCK)
			errno != EWOULDBLOCK &&
#endif
			errno != EINTR)
		{
#ifdef WITH_REDUCE_RDMA
			adb_rclose(fd);
#else
			closesocket(fd);
#endif
			ereport(WARNING,
					(errmsg("could not connect to remote node")));
			return PGINVALID_SOCKET;
		}
	}

	return fd;
}

void FreeNodeEventInfo(DRNodeEventData *ned)
{
	DR_CONNECT_DEBUG((errmsg("node %u(%p) free", ned->nodeoid, ned)));
#ifdef WITH_REDUCE_RDMA
	if (ned->base.fd != PGINVALID_SOCKET)
	{
		RDRCtlWaitEvent(ned->base.fd, 0, ned, RPOLL_EVENT_DEL);
		--dr_wait_count;
		adb_rclose(ned->base.fd);
		ned->base.fd = PGINVALID_SOCKET;
	}
#elif defined DR_USING_EPOLL
	if (ned->base.fd != PGINVALID_SOCKET)
	{
		DRCtlWaitEvent(ned->base.fd, 0, ned, EPOLL_CTL_DEL);
		--dr_wait_count;
		closesocket(ned->base.fd);
		ned->base.fd = PGINVALID_SOCKET;
	}
#else
	WaitEvent we;
	if (FindWaitEventInfoWithData(dr_wait_event_set, 0, ned, &we) != NULL)
	{
		RemoveWaitEvent(dr_wait_event_set, we.pos);
		--dr_wait_count;
		if (we.fd != PGINVALID_SOCKET)
			closesocket(we.fd);
	}
#endif
	DELETE_CONNECTING_NODE(ned);
	if (ned->addrlist)
		pg_freeaddrinfo_all(AF_UNSPEC, ned->addrlist);
	if (ned->recvBuf.data)
		pfree(ned->recvBuf.data);
	if (ned->sendBuf.data)
		pfree(ned->sendBuf.data);

	if (ned->cached_data)
		DropNodeAllPlanCacheData(ned, true);

	if (OidIsValid(ned->nodeoid))
		DRSearchNodeEventData(ned->nodeoid, HASH_REMOVE, NULL);
	else
		pfree(ned);
}

#ifdef DR_USING_EPOLL
void DRCtlWaitEvent(pgsocket fd, uint32_t events, void *ptr, int ctl)
{
	struct epoll_event event;

	event.events = events;
	event.data.ptr = ptr;

	if (epoll_ctl(dr_epoll_fd, ctl, fd, &event) < 0 &&
		dr_status != DRS_FAILED)
	{
		ereport(ERROR,
				(errcode_for_socket_access(),
				 DRKeepError(),
				 errmsg("dynamic reduce epoll_ctl() failed: %m")));
	}
}
#endif /* DR_USING_EPOLL */

#ifdef WITH_REDUCE_RDMA
void RDRCtlWaitEvent(pgsocket fd, uint32_t events, void *ptr, RPOLL_EVENTS ctl)
{
	bool found;
	int i;

	if (ctl == RPOLL_EVENT_ADD)
	{
		if (poll_count == poll_max)
		{
			poll_fd = repalloc(poll_fd, (poll_max+STEP_POLL_ALLOC)*sizeof(*poll_fd));
			poll_max += STEP_POLL_ALLOC;
			dr_create_rhandle_list();
		}
		Assert(poll_count < poll_max);
		poll_fd[poll_count].fd = fd;
		poll_fd[poll_count].events = events;
		poll_count++;

		dr_rhandle_add(fd, ptr);
		((DRNodeEventData*)ptr)->base.fd = fd;
	}

	if (ctl == RPOLL_EVENT_MOD)
	{
		found = false;
		for (i = 0; i < poll_count; i++)
		{
			if (poll_fd[i].fd == fd)
			{
				found = true;
				poll_fd[i].events = events;
				break;
			}
		}
		Assert (found);
		dr_rhandle_mod(fd, ptr);
		((DRNodeEventData*)ptr)->base.fd = fd;
	}

	if (ctl == RPOLL_EVENT_DEL)
	{
		found = false;
		for (i = 0; i < poll_count-1; i++)
		{
			if (poll_fd[i].fd == fd)
			{
				found = true;
				memmove(&poll_fd[i],
						&poll_fd[i+1],
						(poll_count-i-1) * sizeof(struct pollfd));
				break;
			}
		}
		poll_count--;
		dr_rhandle_del(fd);
	}

	return;
}
#endif /* WITH_REDUCE_RDMA */
