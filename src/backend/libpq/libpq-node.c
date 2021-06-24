#include "postgres.h"

#include "catalog/pgxc_node.h"
#include "common/hashfn.h"
#include "executor/clusterReceiver.h"
#include "intercomm/inter-node.h"
#include "libpq-fe.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "utils/dynamicreduce.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "utils/hsearch.h"
#include "access/xact.h"
#include "access/transam.h"


#if defined (WITH_RDMA) || defined(WITH_REDUCE_RDMA)
#include "rdma/adb_rsocket.h"
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#elif defined(HAVE_SYS_POLL_H)
#include <sys/poll.h>
#endif

#include "libpq/libpq-node.h"

typedef struct OidPGconn
{
	Oid oid;
	char type;
	PGconn *conn;
}OidPGconn;

const PQNHookFunctions PQNDefaultHookFunctions =
{
	PQNDefHookError,
	PQNDefHookCopyOut,
	PQNDefHookCopyInOnly,
	PQNDefHookResult
};

const PQNHookFunctions PQNFalseHookFunctions =
{
	PQNDefHookError,
	PQNFalseHookCopyOut,
	PQNDefHookCopyInOnly,
	PQNDefHookResult
};

extern char *PGXCNodeName;	/* GUC */
extern bool enable_readsql_on_slave;	/* GUC */
static HTAB *htab_oid_pgconn = NULL;
bool auto_release_connect = false;	/* guc */
static bool force_release_connect = false;
static bool force_close_connect = false;

static void init_htab_oid_pgconn(void);
static List* apply_for_node_use_oid(List *oid_list);
//#ifndef WITH_RDMA
static List* pg_conn_attach_socket(int *fds, Size n);
//#endif
static bool PQNExecFinish(PGconn *conn, const PQNHookFunctions *hook);
static int PQNIsConnecting(PGconn *conn);
static void check_is_all_socket_correct(List *oid_list);

void PQNForceReleaseWhenTransactionFinish()
{
	force_release_connect = true;
}

List *PQNGetConnUseOidList(List *oid_list)
{
	if(htab_oid_pgconn == NULL)
		init_htab_oid_pgconn();
	return apply_for_node_use_oid(oid_list);
}

struct pg_conn* PQNFindConnUseOid(Oid oid)
{
	OidPGconn *op;
	if (htab_oid_pgconn == NULL)
		return NULL;

	op = hash_search(htab_oid_pgconn, &oid, HASH_FIND, NULL);
	return op ? op->conn : NULL;
}

List* PQNGetAllConns(void)
{
	List *result;
	OidPGconn *op;
	HASH_SEQ_STATUS seq;

	if (htab_oid_pgconn == NULL)
		return NIL;

	result = NIL;
	hash_seq_init(&seq, htab_oid_pgconn);
	while((op = hash_seq_search(&seq)) != NULL)
	{
		result = lappend(result, op->conn);
	}
	
	return result;
}

static void init_htab_oid_pgconn(void)
{
	HASHCTL hctl;
	long size;
	Assert(htab_oid_pgconn == NULL);

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(OidPGconn);
	hctl.hash = oid_hash;
	hctl.hcxt = TopMemoryContext;
	size = 16;
	while(size < MaxCoords + MaxDataNodes)
		size <<= 1;	/* size = size*2 */
	htab_oid_pgconn = hash_create("hash oid to PGconn", size, &hctl
				, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
/*	pg_atexit*/
}

static void check_is_all_socket_correct(List *oid_list)
{
	struct pollfd *pfds;
	ListCell *lc;
	OidPGconn *op;
	int i,n;

	pfds = palloc(sizeof(pfds[0]) * list_length(oid_list));
	i = 0;
	foreach(lc, oid_list)
	{
		if((op=hash_search(htab_oid_pgconn, &(lfirst_oid(lc)), HASH_FIND, NULL)) != NULL)
		{
			pfds[i].events = POLLIN;
			pfds[i].fd = PQsocket(op->conn);
			i++;
		}
	}
#ifdef WITH_RDMA
	n = adb_rpoll(pfds, i, 0);
#else
	n = poll(pfds, i, 0);
#endif
	if(n > 0)
	{
		PQNForceReleaseWhenTransactionFinish();
		PQNReleaseAllConnect(0);
		init_htab_oid_pgconn();
	}
	pfree(pfds);
	return;
}
/*
 * save apply for socket to result list,
 * if we has socket for node oid, save PGINVALID_SOCKET in list item
 */
static List* apply_for_node_use_oid(List *oid_list)
{
	List * volatile need_list = NIL;
	List *result = NIL;
	ListCell *lc, *lc2;
	TransactionId	cureent_txid;
	OidPGconn *op;
	
	const char *param_str;
	cureent_txid = GetTopTransactionIdIfAny();

	/* not in transaction, check broken connection */
	if (!TransactionIdIsValid(cureent_txid))
		check_is_all_socket_correct(oid_list);

	foreach(lc, oid_list)
	{
		if((op=hash_search(htab_oid_pgconn, &(lfirst_oid(lc)), HASH_FIND, NULL)) != NULL)
		{
			result = lappend(result, op->conn);
		}else
		{
			result = lappend(result, NULL);
			need_list = lappend_oid(need_list, lfirst_oid(lc));
		}
	}
	Assert(list_length(result) == list_length(oid_list));

	if (need_list != NIL)
	{
		List * volatile conn_list = NIL;
		pgsocket * volatile fds = NULL;

		PG_TRY();
		{
			fds = PoolManagerGetConnectionsOid(need_list);
			if(fds == NULL)
			{
				/* this error message copy from pgxcnode.c */
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("Failed to get pooled connections")));
			}

			conn_list = pg_conn_attach_socket(fds, list_length(need_list));
			/* at here don't need fds */
			pfree(fds);
			fds = NULL;

			Assert(list_length(conn_list) == list_length(need_list));
			PQNListExecFinish(conn_list, NULL, &PQNDefaultHookFunctions, true);

			foreach(lc, need_list)
			{
				op = hash_search(htab_oid_pgconn, &lfirst_oid(lc), HASH_ENTER, NULL);
				op->conn = linitial(conn_list);
				op->type = '\0';
				conn_list = list_delete_first(conn_list);

				param_str = PQparameterStatus(op->conn, "adb_version");
				if (param_str &&
					strcmp(param_str, ADB_VERSION) != 0)
				{
					ereport(ERROR,
							(errmsg("node %u version is \"%s\" not same to coordinator version \"%s\"",
									lfirst_oid(lc), param_str, ADB_VERSION)));
				}

				param_str = PQparameterStatus(op->conn, "pgxc_node_name");
				if (param_str && pg_strcasecmp(param_str, PGXCNodeName) == 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("duplicate pgxc_node_name \"%s\"", param_str)));
				}
			}
		}PG_CATCH();
		{
			if(fds)
			{
				int count = list_length(need_list);
				while (count-- > 0)
					closesocket(fds[count]);
			}else
			{
				while(conn_list != NIL)
				{
					PQfinish(linitial(conn_list));
					conn_list = list_delete_first(conn_list);
				}
			}
			/**
			 * when the slave node stops responding, 
			 * the read-write separation is stopped.
			 */
			if (enable_readsql_on_slave)
			{
				foreach(lc, need_list)
				{
					NodeHandle *handle = GetNodeHandle(lfirst_oid(lc), false, NULL);
					if (handle->node_type == TYPE_DN_SLAVENODE)
					{
						enable_readsql_on_slave = false;
						break;
					}
				}
			}
			force_release_connect = force_close_connect = true;
			PG_RE_THROW();
		}PG_END_TRY();
		list_free(need_list);
	}else
	{
		Assert(list_member_ptr(result, NULL) == false);
		return result;
	}

	forboth(lc, result, lc2, oid_list)
	{
		if(lfirst(lc) == NULL)
		{
			op = hash_search(htab_oid_pgconn, &(lfirst_oid(lc2)), HASH_FIND, NULL);
			Assert(op != NULL);
			lfirst(lc) = op->conn;
		}
	}

	Assert(list_member_ptr(result, NULL) == false);
	return result;
}

//#ifndef WITH_RDMA
static List* pg_conn_attach_socket(int *fds, Size n)
{
	Size i;
	List * volatile list = NIL;

	PG_TRY();
	{
		for(i=0;i<n;++i)
		{
			PGconn *conn = PQbeginAttach(fds[i], NULL, true, PG_PROTOCOL_LATEST);
			if(conn == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("Out of memory")));
			}else
			{
				list = lappend(list, conn);
			}
		}
	}PG_CATCH();
	{
		while(list != NIL)
		{
			PQNExecFinish_trouble(linitial(list), -1);
			list = list_delete_first(list);
		}
		PG_RE_THROW();
	}PG_END_TRY();

	return list;
}
//#endif

bool PQNOneExecFinish(struct pg_conn *conn, const PQNHookFunctions *hook, bool blocking)
{
	struct pollfd pfd;
	int connecting_status;
	int poll_res;
	AssertArg(conn && hook);

	connecting_status = PQNIsConnecting(conn);
	if(connecting_status == 0)
	{
		while(PQflush(conn) > 0)
		{
			pfd.fd = PQsocket(conn);
			pfd.events = POLLOUT;
#ifdef WITH_RDMA
			poll_res = adb_rpoll(&pfd, 1, blocking ? -1:0);
#else
			poll_res = poll(&pfd, 1, blocking ? -1:0);
#endif
			if(poll_res == 0)
			{
				/* timeout */
				return false;
			}else if(poll_res < 0)
			{
				if(errno == EINTR)
				{
					CHECK_FOR_INTERRUPTS();
					continue;
				}
				if ((*hook->HookError)((PQNHookFunctions*)hook))
					return true;
			}
		}
	}

	while(connecting_status != 0)
	{
		pfd.fd = PQsocket(conn);
		if(connecting_status > 0)
			pfd.events = POLLOUT;
		else
			pfd.events = POLLIN;
#ifdef WITH_RDMA
		poll_res = adb_rpoll(&pfd, 1, blocking ? -1:0);
#else
		poll_res = poll(&pfd, 1, blocking ? -1:0);
#endif
		CHECK_FOR_INTERRUPTS();
		if(poll_res == 0)
		{
			/* timeout */
			return false;
		}else if(poll_res > 0)
		{
			PostgresPollingStatusType pstatus;
			pstatus = PQconnectPoll(conn);
			if(pstatus == PGRES_POLLING_READING)
				connecting_status = -1;
			else if(pstatus == PGRES_POLLING_WRITING)
				connecting_status = 1;
			else
				connecting_status = 0;
		}else
		{
			if(errno != EINTR)
			{
				if ((*hook->HookError)((PQNHookFunctions*)hook))
					return true;
			}
		}
	}

	if(PQNExecFinish(conn, hook))
		return true;
	if(PQstatus(conn) == CONNECTION_BAD
		|| (PQisCopyInState(conn) && ! PQisCopyOutState(conn)))
		return false;

	pfd.fd = PQsocket(conn);
	pfd.events = POLLIN;
	for(;;)
	{
		if(PQstatus(conn) == CONNECTION_BAD
			|| PQtransactionStatus(conn) != PQTRANS_ACTIVE)
			break;

#ifdef WITH_RDMA
		poll_res = adb_rpoll(&pfd, 1, blocking ? -1:0);
#else
		poll_res = poll(&pfd, 1, blocking ? -1:0);
#endif
		if(poll_res < 0)
		{
			if(errno == EINTR)
			{
				CHECK_FOR_INTERRUPTS();
				continue;
			}
			if ((*hook->HookError)((PQNHookFunctions*)hook))
				return true;
			continue;
		}else if(poll_res == 0)
		{
			return false;
		}
		Assert(poll_res > 0);
		PQconsumeInput(conn);
		if(PQNExecFinish(conn, hook))
			return true;
	}

	return false;
}

bool
PQNListExecFinish(List *conn_list, GetPGconnHook get_pgconn_hook,
				  const PQNHookFunctions *hook, bool blocking)
{
	List *list;
	ListCell *lc;
	PGconn *conn;
	struct pollfd *pfds;
	int i,n;
	bool res;

	if(conn_list == NIL)
		return false;
	else if(list_length(conn_list) == 1)
	{
		if (get_pgconn_hook)
			conn = (*get_pgconn_hook)(linitial(conn_list));
		else
			conn = linitial(conn_list);
		return PQNOneExecFinish(conn, hook, blocking);
	}

	/* first try got data */
	foreach(lc, conn_list)
	{
		if (get_pgconn_hook)
			conn = (*get_pgconn_hook)(lfirst(lc));
		else
			conn = lfirst(lc);
		if(PQNIsConnecting(conn) == 0 &&
			!PQisIdle(conn) &&
			(res = PQNExecFinish(conn, hook)) != false)
			return res;
	}

	list = NIL;
	foreach(lc,conn_list)
	{
		if (get_pgconn_hook)
			conn = (*get_pgconn_hook)(lfirst(lc));
		else
			conn = lfirst(lc);
		if(PQNIsConnecting(conn) == 0
			&& PQstatus(conn) != CONNECTION_BAD
			&& PQtransactionStatus(conn) != PQTRANS_ACTIVE)
			continue;
		list = lappend(list, conn);
	}
	if(list == NIL)
		return false;

	res = false;
	pfds = palloc(sizeof(pfds[0]) * list_length(list));
	while(list != NIL)
	{
		int fres;
		foreach (lc, list)
		{
			conn = lfirst(lc);
			if((fres = PQflush(conn)) != 0)
			{
				if(fres > 0)
				{
					pfds[lc__state.i].events = POLLOUT;
				}else
				{
					list = foreach_delete_current(list, lc);
				}
			}else if((n=PQNIsConnecting(conn)) != 0)
			{
				if(n > 0)
					pfds[lc__state.i].events = POLLOUT;
				else
					pfds[lc__state.i].events = POLLIN;
			}else if(PQisCopyInState(conn) && !PQisCopyOutState(conn))
			{
				list = foreach_delete_current(list, lc);
				continue;
			}else if (PQstatus(conn) == CONNECTION_BAD)
			{
				if (PQNExecFinish(conn, hook))
				{
					res = true;
					goto end_loop_;
				}
				list = foreach_delete_current(list, lc);
				continue;
			}else
			{
				pfds[lc__state.i].events = POLLIN;
			}
			pfds[lc__state.i].fd = PQsocket(conn);
		}

re_poll_:
#ifdef WITH_RDMA
		n = adb_rpoll(pfds, list_length(list), blocking ? -1:0);
#else
		n = poll(pfds, list_length(list), blocking ? -1:0);
#endif
		if(n < 0)
		{
			if(errno == EINTR)
			{
				CHECK_FOR_INTERRUPTS();
				goto re_poll_;
			}
			res = (*hook->HookError)((PQNHookFunctions*)hook);
			if(res)
				break;
		}else if(n == 0)
		{
			/* timeout */
			return false;
		}

		/* first consume all socket data */
		for(i=0,lc=list_head(list);lc!=NULL;lc=lnext(list, lc),++i)
		{
			if(pfds[i].revents != 0)
			{
				conn = lfirst(lc);
				if(PQNIsConnecting(conn))
				{
					PQconnectPoll(conn);
				}else if(pfds[i].revents & POLLOUT)
				{
					PQflush(conn);
				}else
				{
					PQconsumeInput(conn);
				}
			}
		}

		/* second analyze socket data one by one */
		i = 0;
		foreach (lc, list)
		{
			if ((pfds[i++].revents & POLLIN) == 0)
				continue;

			conn = lfirst(lc);
			res = PQNExecFinish(conn, hook);
			if(res)
				goto end_loop_;
			if (PQstatus(conn) == CONNECTION_BAD ||
				PQtransactionStatus(conn) != PQTRANS_ACTIVE)
				list = foreach_delete_current(list, lc);
		}
	}

end_loop_:
	pfree(pfds);
	list_free(list);
	return res;
}

bool PQNEFHNormal(void *context, struct pg_conn *conn, PQNHookFuncType type,...)
{
	if(type ==PQNHFT_ERROR)
		ereport(ERROR, (errmsg("%m")));
	return false;
}

static bool PQNExecFinish(PGconn *conn, const PQNHookFunctions *hook)
{
	PGresult   *res;
	bool		hook_res;

re_get_:
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		res = PQgetResult(conn);
		hook_res = (*hook->HookResult)((PQNHookFunctions*)hook, conn, res);
		PQclear(res);
		if (hook_res)
			return true;
	} else if (!PQisBusy(conn))
	{
		if (PQisCopyOutState(conn))
		{
			const char	   *buf;
			int				n;

			n = PQgetCopyDataBuffer(conn, &buf, true);
			if (n > 0)
			{
				if ((*hook->HookCopyOut)((PQNHookFunctions*)hook, conn, buf, n))
					return true;
				goto re_get_;
			} else if (n < 0)
			{
				goto re_get_;
			} else if (n == 0)
			{
				return false;
			}
		} else if (PQisCopyInState(conn))
		{
			if ((*hook->HookCopyInOnly)((PQNHookFunctions*)hook, conn))
				return true;
			if (!PQisCopyInState(conn))
				goto re_get_;
		} else
		{
			res = PQgetResult(conn);
			hook_res = (*hook->HookResult)((PQNHookFunctions*)hook, conn, res);
			PQclear(res);
			if (hook_res)
				return true;
			if (!PQisIdle(conn))
				goto re_get_;

		}
	}

	return false;
}

/*
 * return 0 for not connectiong
 * <0 for need input
 * >0 for need output
 */
static int PQNIsConnecting(PGconn *conn)
{
	AssertArg(conn);
	switch(PQstatus(conn))
	{
	case CONNECTION_OK:
	case CONNECTION_BAD:
		break;
	case CONNECTION_STARTED:
	case CONNECTION_MADE:
	case CONNECTION_CHECK_TARGET:
	case CONNECTION_CHECK_STANDBY:
		return 1;
	case CONNECTION_AWAITING_RESPONSE:
	case CONNECTION_AUTH_OK:
		return -1;
	case CONNECTION_SETENV:
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("No support protocol 2.0 version for remote node")));
		break;
	case CONNECTION_SSL_STARTUP:
	case CONNECTION_GSS_STARTUP:
	case CONNECTION_CHECK_WRITABLE:
	case CONNECTION_CONSUME:
		return -1;
	case CONNECTION_NEEDED:
		switch(PQconnectPoll(conn))
		{
		case PGRES_POLLING_READING:
			return -1;
		case PGRES_POLLING_WRITING:
			return 1;
		default:
			break;
		}
		break;
	}
	return 0;
}

bool PQNExecFinish_trouble(PGconn *conn, int timeout_sec)
{
	PGresult   *res;
	time_t		time_last;
	struct timeval tv;
	fd_set		fds;
	fd_set		efds;
	pgsocket	sock;
	int			ret;

	time_last = time(NULL);
	MemSet(&tv, 0, sizeof(tv));
	if (timeout_sec > 0)
		tv.tv_sec = 1;
	HOLD_CANCEL_INTERRUPTS();
	for(;;)
	{
		if(PQstatus(conn) == CONNECTION_BAD)
			break;
		if(PQisCopyInState(conn))
		{
			PQputCopyEnd(conn, NULL);
re_flush_:
			ret = PQflush(conn);
			if(ret > 0)
			{
re_select_:
				if ((timeout_sec > 0 && time(NULL) - time_last > timeout_sec) ||
					timeout_sec == 0)
				{
					RESUME_CANCEL_INTERRUPTS();
					return false;
				}
				FD_ZERO(&fds);
				FD_ZERO(&efds);
				sock = PQsocket(conn);
				FD_SET(sock, &fds);
				FD_SET(sock, &efds);
#ifdef WITH_RDMA
				ret = adb_rselect(sock + 1, NULL, &wfds, &efds, timeout_sec >= 0 ? &tv:NULL);
#else
				ret = select(sock + 1, NULL, &fds, &efds, timeout_sec >= 0 ? &tv:NULL);
#endif
				CHECK_FOR_INTERRUPTS();
				if (ret < 0)
				{
					if (errno == EINTR)
					{
						goto re_select_;
					}else
					{
						/* unknown error, just sleep */
						pg_usleep(1000);
					}
				}
				goto re_flush_;
			}
		}
		while(PQisCopyOutState(conn))
		{
			ret = PQgetCopyDataBuffer(conn, (const char**)&res, true);
			if (ret < 0)
			{
				/* end of copy or error */
				break;
			}else if (ret == 0)
			{
				if ((timeout_sec > 0 && time(NULL) - time_last > timeout_sec) ||
					timeout_sec == 0)
				{
					RESUME_CANCEL_INTERRUPTS();
					return false;
				}
				FD_ZERO(&fds);
				FD_ZERO(&efds);
				sock = PQsocket(conn);
				FD_SET(sock, &fds);
				FD_SET(sock, &efds);
#ifdef WITH_RDMA
				ret = rselect(sock + 1, NULL, &fds, &efds, timeout_sec >= 0 ? &tv:NULL);
#else
				ret = select(sock + 1, NULL, &fds, &efds, timeout_sec >= 0 ? &tv:NULL);
#endif
				CHECK_FOR_INTERRUPTS();
				if (ret > 0)
					PQconsumeInput(conn);
			}
		}

		res = PQgetResult(conn);
		if(res)
			PQclear(res);
		else
			break;
	}
	RESUME_CANCEL_INTERRUPTS();
	return true;
}

/*
 * request_cancel_after < 0 don't request cancel
 * request_cancel_after == 0 request first
 * request_cancel_after > 0 request cancel after request_cancel_after second(s)
 */
void PQNReleaseAllConnect(int request_cancel_after)
{
	HASH_SEQ_STATUS	seq_status;
	OidPGconn	   *op;
	time_t			time_last;
	bool			release_connect;
	bool			force_close;
	bool			sended_cancel;

	if (htab_oid_pgconn == NULL ||
		hash_get_num_entries(htab_oid_pgconn) == 0)
	{
		if (force_close_connect)
		{
			PoolManagerReleaseConnections(force_close_connect);
			force_close_connect = false;
		}
		return;
	}

	if (request_cancel_after == 0)
		PQNRequestCancelAllconnect();
	else if (request_cancel_after > 0)
		time_last = time(NULL);

	sended_cancel = false;
	release_connect = force_close = false;
	hash_seq_init(&seq_status, htab_oid_pgconn);
	while((op = hash_seq_search(&seq_status)) != NULL)
	{
		if (sended_cancel == false &&
			request_cancel_after > 0)
		{
re_check_:
			if (time(NULL) - time_last <= request_cancel_after)
			{
				if (PQNExecFinish_trouble(op->conn, 1) == false)
					goto re_check_;
			}else
			{
				PQNRequestCancelAllconnect();
				PQNExecFinish_trouble(op->conn, -1);
			}
		}else
		{
			PQNExecFinish_trouble(op->conn, -1);
		}

		switch (PQtransactionStatus(op->conn))
		{
		case PQTRANS_IDLE:
			break;
		case PQTRANS_UNKNOWN:
			/* connect can not reuse, so we need release */
			release_connect = true;
			break;
		default:
			/* should not happy, can not reset connect, so close it */
			release_connect = force_close = true;
			break;
		}
	}

	if (release_connect ||
		auto_release_connect ||
		force_release_connect)
	{
		hash_seq_init(&seq_status, htab_oid_pgconn);
		while((op = hash_seq_search(&seq_status)) != NULL)
			PQdetach(op->conn);
		hash_destroy(htab_oid_pgconn);
		htab_oid_pgconn = NULL;
		PoolManagerReleaseConnections(force_close || force_close_connect);
		force_release_connect = false;
		force_close_connect = false;
		StopDynamicReduceWorker();
	}
}

void PQNRequestCancel(struct pg_conn *conn)
{
	PGTransactionStatusType ts;
	if (PQstatus(conn) != CONNECTION_BAD)
	{
		ts = PQtransactionStatus(conn);
		if (ts == PQTRANS_ACTIVE ||
			ts == PQTRANS_UNKNOWN)
		{
			PQrequestCancel(conn);
		}
	}
}

void PQNRequestCancelAllconnect(void)
{
	HASH_SEQ_STATUS seq_status;
	OidPGconn *op;

	if (htab_oid_pgconn == NULL ||
		hash_get_num_entries(htab_oid_pgconn) == 0)
		return; /* quick quit */

	hash_seq_init(&seq_status, htab_oid_pgconn);
	while((op = hash_seq_search(&seq_status)) != NULL)
		PQNRequestCancel(op->conn);
}

void PQNReportResultError(struct pg_result *result, struct pg_conn *conn, int elevel, bool free_result)
{
	AssertArg(result);
	PG_TRY();
	{
		const char *str;
		const char *file_name;
		const char *file_line;
		const char *func_name;

		if (elevel <= 0)
		{
			const char * err = PQresultErrorField(result, PG_DIAG_SEVERITY);
			if (err)
				elevel = get_str_elevel(err);
			if (elevel <= 0)
				elevel = LOG;
			else if (elevel >= ERROR)
				elevel = ERROR;
		}

		if(errstart(elevel, TEXTDOMAIN))
		{
			if((str = PQresultErrorField(result, PG_DIAG_SQLSTATE)) != NULL)
				errcode(MAKE_SQLSTATE(str[0], str[1], str[2], str[3], str[4]));
			else
				errcode(ERRCODE_CONNECTION_FAILURE);

			str = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
			if(str == NULL && conn)
				str = PQerrorMessage(conn);
			if(str != NULL)
				errmsg_internal("%s", str);

			str = PQresultErrorField(result, PG_DIAG_NODE_NAME);
			if(str == NULL && conn)
			{
				str = PQparameterStatus(conn, "pgxc_node_name");
				if(str == NULL)
					str = PQNConnectName(conn);
			}
			if(str != NULL)
				errnode(str);

#define GENERIC_ERROR(diag, func)									\
			if((str = PQresultErrorField(result, diag)) != NULL)	\
				func("%s", str)

			GENERIC_ERROR(PG_DIAG_MESSAGE_DETAIL, errdetail_internal);
			GENERIC_ERROR(PG_DIAG_MESSAGE_HINT, errhint);
			GENERIC_ERROR(PG_DIAG_CONTEXT, errcontext);

#undef GENERIC_ERROR
#define GENERIC_ERROR(diag)											\
			if((str = PQresultErrorField(result, diag)) != NULL)	\
				err_generic_string(diag, str)

			GENERIC_ERROR(PG_DIAG_SCHEMA_NAME);
			GENERIC_ERROR(PG_DIAG_TABLE_NAME);
			GENERIC_ERROR(PG_DIAG_COLUMN_NAME);
			GENERIC_ERROR(PG_DIAG_DATATYPE_NAME);
			GENERIC_ERROR(PG_DIAG_CONSTRAINT_NAME);
#undef GENERIC_ERROR

			file_name = PQresultErrorField(result, PG_DIAG_SOURCE_FILE);
			file_line = PQresultErrorField(result, PG_DIAG_SOURCE_LINE);
			func_name = PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION);
			errfinish(file_name ? file_name : __FILE__,
					  file_line ? atoi(file_line) : __LINE__,
					  func_name ? func_name : PG_FUNCNAME_MACRO);

			if (elevel >= ERROR)
				pg_unreachable();
		}
	}PG_CATCH();
	{
		if(free_result)
			PQclear(result);
		PG_RE_THROW();
	}PG_END_TRY();
	if(free_result)
		PQclear(result);
}

const char *PQNConnectName(struct pg_conn *conn)
{
	OidPGconn *op;
	HASH_SEQ_STATUS status;
	if(htab_oid_pgconn)
	{
		hash_seq_init(&status, htab_oid_pgconn);
		while((op = hash_seq_search(&status)) != NULL)
		{
			if(op->conn == conn)
			{
				hash_seq_term(&status);
				return GetNodeName(op->oid);
			}
		}
	}
	return NULL;
}

Oid PQNConnectOid(struct pg_conn *conn)
{
	OidPGconn *op;
	HASH_SEQ_STATUS status;
	if(htab_oid_pgconn)
	{
		hash_seq_init(&status, htab_oid_pgconn);
		while((op = hash_seq_search(&status)) != NULL)
		{
			if(op->conn == conn)
			{
				hash_seq_term(&status);
				return op->oid;
			}
		}
	}
	return InvalidOid;
}

/*
 * return flush finish count
 */
int PQNFlush(List *conn_list, bool blocking)
{
	ListCell *lc;
	Bitmapset *bms_need = NULL;
	struct pollfd *pfd,*tmp;
	int result = 0;
	int i = 0;
	int count;

	foreach(lc, conn_list)
	{
		int r = PQflush(lfirst(lc));
		if (r == 0)
		{
			result++;
		}else if (r < 0)
		{
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(lfirst(lc))),
					 errnode(PQNConnectName(lfirst(lc)))));
		}else if(blocking)
		{
			bms_need = bms_add_member(bms_need, i);
		}
		++i;
	}

	if (bms_need == NULL)
		return result;

	count = bms_num_members(bms_need);
	pfd = palloc(sizeof(*pfd) * count);

re_set_:
	tmp = pfd;
	i = 0;
	foreach(lc, conn_list)
	{
		if (bms_is_member(i, bms_need))
		{
			tmp->fd = PQsocket(lfirst(lc));
			tmp->events = POLLOUT;
			tmp = &tmp[1];
		}
		++i;
	}
re_select_:
#ifdef WITH_RDMA
	result = adb_rpoll(pfd, count, -1);
#else
	result = poll(pfd, count, -1);
#endif
	CHECK_FOR_INTERRUPTS();
	if (result < 0)
	{
		if (errno == EINTR)
			goto re_select_;
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("Can not poll sockets for flush")));
	}
	tmp = pfd;
	i=0;
	foreach(lc, conn_list)
	{
		if (bms_is_member(i, bms_need))
		{
			Assert(PQsocket(lfirst(lc)) == tmp->fd);
			if (tmp->revents != 0)
			{
				int r = PQflush(lfirst(lc));
				if (r == 0)
				{
					if (--count == 0)
						break;
					bms_need = bms_del_member(bms_need, i);
				}else if (r < 0)
				{
					ereport(ERROR,
							(errmsg("%s", PQerrorMessage(lfirst(lc))),
								errnode(PQNConnectName(lfirst(lc)))));
				}
			}
			tmp = &tmp[1];
		}
		++i;
	}

	if (count == 0)
	{
		pfree(pfd);
		bms_free(bms_need);
		return list_length(conn_list);
	}
	goto re_set_;
}

void PQNputCopyData(List *conn_list, const char *buffer, int nbytes)
{
	ListCell *lc;
	List *list;
	int result;

	if (conn_list == NIL)
		return;

	list = NIL;
	foreach(lc, conn_list)
	{
		result = PQputCopyData(lfirst(lc), buffer, nbytes);
		if (result == 0)
		{
			list = lappend(list, lfirst(lc));
		}else if (result < 0)
		{
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(lfirst(lc))),
					 errnode(PQNConnectName(lfirst(lc)))));
		}
	}

	while(list != NIL)
	{
		PQNFlush(list, true);

		foreach (lc, list)
		{
			result = PQputCopyData(lfirst(lc), buffer, nbytes);
			if (result > 0)
			{
				list = foreach_delete_current(list, lc);
				continue;
			}else if(result < 0)
			{
				ereport(ERROR,
						(errmsg("%s", PQerrorMessage(lfirst(lc))),
						 errnode(PQNConnectName(lfirst(lc)))));
			}
		}
	}
}

void PQNPutCopyEnd(List *conn_list)
{
	List	   *block_list;
	List	   *list;
	ListCell   *lc;
	int			res;

	list = conn_list;
re_do_:
	block_list = NIL;
	foreach (lc, list)
	{
		res = PQputCopyEnd(lfirst(lc), NULL);
		if (res < 0)
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(lfirst(lc))),
					 errnode(PQNConnectName(lfirst(lc)))));
		else if (res == 0)
			block_list = lappend(block_list, lfirst(lc));
	}
	if (list != conn_list)
		list_free(list);

	if (block_list)
	{
		list = block_list;
		goto re_do_;
	}
}

void* PQNMakeDefHookFunctions(Size size)
{
	PQNHookFunctions *pub;
	if (size < sizeof(*pub))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid size %zu of PQNHookFunctions", size)));
	
	pub = palloc0(size);
	memcpy(pub, &PQNDefaultHookFunctions, sizeof(*pub));

	return pub;
}

bool PQNDefHookError(PQNHookFunctions *pub)
{
	ereport(ERROR, (errmsg("%m")));
	return false;	/* keep compiler quiet */
}

bool PQNDefHookCopyOut(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	return clusterRecvTuple(NULL, buf, len, NULL, conn);
}

bool PQNFalseHookCopyOut(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	clusterRecvTuple(NULL, buf, len, NULL, conn);
	return false;
}

bool PQNDefHookCopyInOnly(PQNHookFunctions *pub, struct pg_conn *conn)
{
	PQputCopyEnd(conn, NULL);
	return false;
}

bool PQNDefHookResult(PQNHookFunctions *pub, struct pg_conn *conn, struct pg_result *res)
{
	ExecStatusType status;
	
	if (res)
	{
		status = PQresultStatus(res);

		if(status == PGRES_FATAL_ERROR || status == PGRES_BAD_RESPONSE)
			PQNReportResultError(res, conn, ERROR, true);
		else if(status == PGRES_NONFATAL_ERROR)
			PQNReportResultError(res, conn, -1, true);
	}
	return false;
}
