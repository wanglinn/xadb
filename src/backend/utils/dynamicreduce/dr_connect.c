#include "postgres.h"

#include "common/ip.h"
#include "libpq/pqformat.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

#include <unistd.h>

/*
 * sizeof(msg type)
 * sizeof(node oid)
 * sizeof(node pid)
 */
#define CONNECT_MSG_LENGTH	9

static int dr_listen_pos = INVALID_EVENT_SET_POS;
static pgsocket ConnectToAddress(const struct addrinfo *addr);

static void OnNodeEventConnectFrom(WaitEvent *ev)
{
	DRNodeEventData    *ned = ev->user_data;
	uint32				index;
	bool				found;
	Assert(ned->status == DRN_ACCEPTED);
	Assert(ned->nodeoid == InvalidOid);

	if (RecvMessageFromNode(ned, ev) <= 0 ||
		ned->recvBuf.len - ned->recvBuf.cursor < CONNECT_MSG_LENGTH)
		return;
	if (ned->recvBuf.data[ned->recvBuf.cursor] != ADB_DR_MSG_NODEOID)
	{
		ned->status = DRN_WAIT_CLOSE; /* on PreWait will destory it */
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
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
	if (ned->nodeoid == InvalidOid ||
		ned->nodeoid == PGXCNodeOid)
	{
		ned->status = DRN_WAIT_CLOSE; /* on PreWait will destory it */
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid node oid %u from remote", ned->nodeoid)));
	}

	if (oidBufferMember(&dr_latch_data->work_oid_buf, ned->nodeoid, &index) &&
		dr_latch_data->work_pid_buf.oids[index] != ned->owner_pid)
	{
		ereport(LOG_SERVER_ONLY,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
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
			//dr_clear_network = true;
			ereport(ERROR,
					(errmsg("replicate node oid %u from remote", ned->nodeoid)));
		}
		DR_CONNECT_DEBUG((errmsg("node %u from remote accept successed", ned->nodeoid)));
		ModifyWaitEventData(dr_wait_event_set, ev->pos, newned);
		memcpy(newned, ned, sizeof(*newned));
		pfree(ned);
		ned = newned;
	}

	DROnNodeConectSuccess(ned, ev);
}

static void OnNodeEventConnectTo(WaitEvent *ev)
{
	DRNodeEventData *ned = ev->user_data;
	ssize_t size;

	Assert(DRSearchNodeEventData(ned->nodeoid, HASH_FIND, NULL) != NULL);

resend_:
	size = send(ev->fd,
				ned->sendBuf.data + ned->sendBuf.cursor,
				ned->sendBuf.len - ned->sendBuf.cursor,
				0);
	if (size < 0)
	{
		pgsocket fd;
		if (errno == EINTR)
			goto resend_;

		fd = PGINVALID_SOCKET;
		ned->addr_cur = ned->addr_cur->ai_next;
		while (ned->addr_cur != NULL)
		{
			fd = ConnectToAddress(ned->addr_cur);
			if (fd != PGINVALID_SOCKET)
				break;
		}
		if (fd == PGINVALID_SOCKET)
		{
			ned->status = DRN_WAIT_CLOSE;
			ereport(ERROR,
					(errmsg("could not connect to any addres for remote node %u", ned->nodeoid)));
		}

		/* modify wait event */
		RemoveWaitEvent(dr_wait_event_set, ev->pos);
		--dr_wait_count;
		closesocket(ev->fd);
		ev->fd = fd;
		ev->pos = AddWaitEventToSet(dr_wait_event_set,
									WL_SOCKET_CONNECTED,
									fd,
									NULL,
									ned);
		++dr_wait_count;
	}else
	{
		ned->sendBuf.cursor += size;
		if (ned->sendBuf.cursor == ned->sendBuf.len)
		{
			DR_CONNECT_DEBUG((errmsg("connect to node %u success", ned->nodeoid)));
			/* update status */
			resetStringInfo(&ned->sendBuf);
			DROnNodeConectSuccess(ned, ev);
		}
	}
}

static void OnConnectError(struct DREventData *base, int pos)
{
	DRNodeEventData *ned = (DRNodeEventData*)base;
	if (ned->status == DRN_WAIT_CLOSE)
		FreeNodeEventInfo(ned);
}

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
	ned->owner_pid = info->pid;
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
				(errmsg("could not translate host name \"%s\" to address: %s\n",
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
				(errmsg("could not connect to any addres for remote node %u(%s:%d)",
						info->node_oid, NameStr(info->host), info->port)));
	}
	DREnlargeWaitEventSet();
	ret = AddWaitEventToSet(dr_wait_event_set,
							WL_SOCKET_CONNECTED,
							fd,
							NULL,
							ned);
	Assert(ret != INVALID_EVENT_SET_POS);
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
				 errmsg("too few node fro reduce connect:%u", count)));

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
			newdata->owner_pid != info[i].pid)
		{
			DR_CONNECT_DEBUG((errmsg("node %u owner pid %d is not equal last, close it",
									 newdata->nodeoid, newdata->owner_pid)));
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
		FreeNodeEventInfo(newdata);
		PG_RE_THROW();
	}PG_END_TRY();
}

static void OnListenEvent(WaitEvent *ev)
{
	DRListenEventData *led = ev->user_data;
	MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(led));
	volatile pgsocket newfd = PGINVALID_SOCKET;
	pgsocket fd = GetWaitEventSocket(dr_wait_event_set, ev->pos);
	DRNodeEventData * volatile newdata = NULL;
	Assert(led->base.type == DR_EVENT_DATA_LISTEN);
	Assert(fd != PGINVALID_SOCKET);

	PG_TRY();
	{
		MemoryContext oldcontext;
		do
		{
			newfd = accept(fd, NULL, NULL);
			if (newfd == PGINVALID_SOCKET)
			{
				if (errno == EAGAIN)
					break;
				ereport(ERROR,
						(errcode_for_socket_access(),
						 errmsg("can not accept new socket:%m")));
			}

			DREnlargeWaitEventSet();

			oldcontext = MemoryContextSwitchTo(TopMemoryContext);
			newdata = palloc0(sizeof(*newdata));
			newdata->base.type = DR_EVENT_DATA_NODE;
			newdata->base.OnEvent = OnNodeEventConnectFrom;
			newdata->base.OnError = OnConnectError;
			initStringInfoExtend(&newdata->sendBuf, DR_SOCKET_BUF_SIZE_START);
			initStringInfoExtend(&newdata->recvBuf, DR_SOCKET_BUF_SIZE_START);
			MemoryContextSwitchTo(oldcontext);

			newdata->nodeoid = InvalidOid;
			newdata->status = DRN_ACCEPTED;
			AddWaitEventToSet(dr_wait_event_set,
							  WL_SOCKET_READABLE,
							  newfd,
							  NULL,
							  (void*)newdata);
			++dr_wait_count;

			newdata = NULL;
		}while(led->noblock);
	}PG_CATCH();
	{
		if (newfd != PGINVALID_SOCKET)
			closesocket(newfd);
		if (newdata)
		{
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

	if (dr_listen_pos != INVALID_EVENT_SET_POS)
	{
		led = GetWaitEventData(dr_wait_event_set, dr_listen_pos);
		Assert(led->base.type == DR_EVENT_DATA_LISTEN);
		return led;
	}

	DREnlargeWaitEventSet();

	led = MemoryContextAllocZero(TopMemoryContext, sizeof(*led));
	led->base.type = DR_EVENT_DATA_LISTEN;
	led->base.OnEvent = OnListenEvent;
	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == PGINVALID_SOCKET)
	{
		pfree(led);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create socket: %m")));
	}
	led->noblock = pg_set_noblock(fd);
#ifndef WIN32
	/* ignore result */
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof(one));
#endif
	MemSet(&addr_inet, 0, sizeof(addr_inet));
	addr_inet.sin_family = AF_INET;
	/* addr_inet.sin_port = 0; */
	addr_inet.sin_addr.s_addr = htonl(INADDR_ANY);

	if (bind(fd, (struct sockaddr *)&addr_inet, sizeof(addr_inet)) < 0)
	{
		closesocket(fd);
		pfree(led);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not bind IPv4 socket: %m")));
	}
	if (listen(fd, PG_SOMAXCONN) < 0)
	{
		closesocket(fd);
		pfree(led);
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not listen on IPv4 socket: %m")));
	}

	/* get random listen port */
	MemSet(&addr_inet, 0, sizeof(addr_inet));
	addrlen = sizeof(addr_inet);
	if (getsockname(fd, &addr_inet, &addrlen) < 0)
	{
		closesocket(fd);
		pfree(led);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("getsockname(2) failed: %m")));
	}
	led->port = htons(addr_inet.sin_port);

	dr_listen_pos = AddWaitEventToSet(dr_wait_event_set,
									  WL_SOCKET_READABLE,
									  fd, NULL, led);
	Assert(dr_listen_pos != INVALID_EVENT_SET_POS);
	Assert(GetWaitEventData(dr_wait_event_set, dr_listen_pos) == led);
	++dr_wait_count;

	return led;
}

static pgsocket ConnectToAddress(const struct addrinfo *addr)
{
	volatile pgsocket fd = socket(addr->ai_family, SOCK_STREAM, 0);
	if (fd == PGINVALID_SOCKET)
	{
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not create socket: %m")));
	}
	if (!pg_set_noblock(fd))
	{
		closesocket(fd);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not set socket to nonblocking mode: %m")));
	}
	if (connect(fd, addr->ai_addr, addr->ai_addrlen) < 0)
	{
		if (errno != EINPROGRESS &&
#if (EINTR != EWOULDBLOCK)
			errno != EWOULDBLOCK &&
#endif
			errno != EINTR)
		{
			closesocket(fd);
			ereport(WARNING,
					(errmsg("could not connect to remote node")));
			return PGINVALID_SOCKET;
		}
	}

	return fd;
}

void FreeNodeEventInfo(DRNodeEventData *ned)
{
	WaitEvent we;
	if (FindWaitEventInfoWithData(dr_wait_event_set, 0, ned, &we) != NULL)
	{
		RemoveWaitEvent(dr_wait_event_set, we.pos);
		--dr_wait_count;
		if (we.fd != PGINVALID_SOCKET)
			closesocket(we.fd);
	}
	if (ned->addrlist)
		pg_freeaddrinfo_all(AF_UNSPEC, ned->addrlist);
	if (ned->recvBuf.data)
		pfree(ned->recvBuf.data);
	if (ned->sendBuf.data)
		pfree(ned->sendBuf.data);

	if (OidIsValid(ned->nodeoid))
		DRSearchNodeEventData(ned->nodeoid, HASH_REMOVE, NULL);
	else
		pfree(ned);
}
