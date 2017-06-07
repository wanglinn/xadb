#include "postgres.h"

#include "catalog/pgxc_node.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqcomm.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "utils/hsearch.h"

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

static HTAB *htab_oid_pgconn = NULL;

static void init_htab_oid_pgconn(void);
static List* apply_for_node_use_oid(List *oid_list);
static OidPGconn* insert_pgconn_to_htab(int index, char type, PGconn *conn);
static List* pg_conn_attach_socket(int *fds, Size n);
static void PQNListExecFinish_trouble(List *list);
static void PQNExecFinsh_trouble(PGconn *conn);

List *PQNGetConnUseOidList(List *oid_list)
{
	if(htab_oid_pgconn == NULL)
		init_htab_oid_pgconn();
	return apply_for_node_use_oid(oid_list);
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
	while(size < NumCoords + NumDataNodes)
		size <<= 1;	/* size = size*2 */
	htab_oid_pgconn = hash_create("hash oid to PGconn", size, &hctl
				, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
/*	pg_atexit*/
}

/*
 * save apply for socket to result list,
 * if we has socket for node oid, save PGINVALID_SOCKET in list item
 */
static List* apply_for_node_use_oid(List *oid_list)
{
	List *co_list = NIL;
	List *dn_list = NIL;
	List *result = NIL;
	ListCell *lc, *lc2;
	OidPGconn *op;
	int id;

	foreach(lc, oid_list)
	{
		if((op=hash_search(htab_oid_pgconn, &(lfirst_oid(lc)), HASH_FIND, NULL)) != NULL)
		{
			result = lappend(result, op->conn);
			continue;
		}else
		{
			result = lappend(result, NULL);
		}

		if((id = PGXCNodeGetNodeId(lfirst_oid(lc), PGXC_NODE_DATANODE)) != -1)
		{
			dn_list = lappend_int(dn_list, id);
		}else if((id = PGXCNodeGetNodeId(lfirst_oid(lc), PGXC_NODE_COORDINATOR)) != -1)
		{
			co_list = lappend_int(co_list, id);
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				(errmsg("Can not found node for oid '%u'", lfirst_oid(lc)))));
		}
	}
	Assert(list_length(result) == list_length(oid_list));

	if(co_list != NIL || dn_list != NIL)
	{
		List *conns;
		int *fds = PoolManagerGetConnections(dn_list, co_list);
		if(fds == NULL)
		{
			/* this error message copy from pgxcnode.c */
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("Failed to get pooled connections")));
		}

		conns = pg_conn_attach_socket(fds, list_length(dn_list) + list_length(co_list));
		PQNListExecFinish(conns, true);

		foreach(lc,dn_list)
		{
			insert_pgconn_to_htab(lfirst_int(lc), PGXC_NODE_DATANODE, linitial(conns));
			conns = list_delete_first(conns);
		}
		list_free(dn_list);

		foreach(lc, co_list)
		{
			insert_pgconn_to_htab(lfirst_int(lc), PGXC_NODE_COORDINATOR, linitial(conns));
			conns = list_delete_first(conns);
		}
		list_free(co_list);

		Assert(conns == NIL);
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

static OidPGconn* insert_pgconn_to_htab(int index, char type, PGconn *conn)
{
	OidPGconn *op;
	Oid oid;
	bool found;
	AssertArg(index >= 0 && conn != NULL);

	oid = PGXCNodeGetNodeOid(index, type);
	Assert(OidIsValid(oid));
	op = hash_search(htab_oid_pgconn, &oid, HASH_ENTER, &found);
	Assert(found == false && op->oid == oid);
	op->conn = conn;
	op->type = type;

	return op;
}

static List* pg_conn_attach_socket(int *fds, Size n)
{
	Size i;
	List *list = NIL;

	for(i=0;i<n;++i)
	{
		PGconn *conn = PQbeginAttach(fds[i], NULL, true, PG_PROTOCOL_LATEST);
		if(conn == NULL)
		{
			ListCell *lc;
			PQNListExecFinish(list, false);
			foreach(lc, list)
				PQdetach(lfirst(lc));
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("Out of memory")));
		}else
		{
			list = lappend(list, conn);
		}
	}
	return list;
}

/*
 * return index of conn_list (start with 1)
 * when result==0 is no connection need wait
 * when result<0 is error number
 */
int PQNWaitResult(List *conn_list, struct pg_conn **ppconn, bool noError)
{
	ListCell *lc;
	PGconn *conn;
	struct pollfd *pfd = NULL;
	int i,n,ret;

	*ppconn = NULL;
	for(;;)
	{
		i = 1;
		foreach(lc, conn_list)
		{
			conn = lfirst(lc);
			Assert(conn != NULL);
			n = PQisCopyOutState(conn);
			if((n == true && PQgetCopyDataBuffer(conn, NULL, true) > 0)
				|| (n == false && PQisBusy(conn) == false)
				|| PQstatus(conn) == CONNECTION_BAD
				|| PQflush(conn) != 0)
			{
				if(pfd)
					pfree(pfd);
				*ppconn = conn;
				return i;
			}
			++i;
		}
		if(pfd == NULL)
		{
			pfd = palloc_extended(sizeof(pfd[0]) * list_length(conn_list)
				, noError ? MCXT_ALLOC_NO_OOM:0);
			if(pfd == NULL)
				return -ENOMEM;
		}

		n = 0;
		foreach(lc, conn_list)
		{
			pfd[n].events = 0;
			conn = lfirst(lc);
switch_again_:
			switch(PQstatus(conn))
			{
			case CONNECTION_OK:
				switch(PQtransactionStatus(conn))
				{
				case PQTRANS_ACTIVE:
					pfd[n].events = POLLIN;
					break;
				case PQTRANS_UNKNOWN:
					*ppconn = conn;
					goto return_pqn_wait_;
				default:
					break;
				}
				break;
			case CONNECTION_BAD:
				*ppconn = conn;
				goto return_pqn_wait_;
			case CONNECTION_NEEDED:
				(void)PQconnectPoll(conn);
				goto switch_again_;
			case CONNECTION_STARTED:
			case CONNECTION_MADE:
				pfd[n].events = POLLOUT;
				break;
			case CONNECTION_AWAITING_RESPONSE:
			case CONNECTION_AUTH_OK:
			case CONNECTION_SSL_STARTUP:
				pfd[n].events = POLLIN;
				break;
			case CONNECTION_SETENV:
				pfree(pfd);
				if(noError == false)
				{
					ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("No support protocol 2.0 version for remote node")));
				}
				return -EINVAL;
			}
			if(pfd[n].events != 0)
			{
				pfd[n].fd = PQsocket(conn);
				++n;
			}
		}

		if(n == 0)
		{
			pfree(pfd);
			*ppconn = NULL;
			return 0;
		}

		ret = poll(pfd, n, -1);
		if(ret < 0)
		{
			*ppconn = NULL;
			pfree(pfd);
			return -errno;
		}

		for(i=0,lc=list_head(conn_list);i<n;++i)
		{
			if(pfd[i].revents == 0)
				continue;

			for(;;)
			{
				conn = lfirst(lc);
				if(PQsocket(conn) != pfd[i].fd)
					lc = lnext(lc);
				else
					break;
			}

			Assert(pfd[i].fd == PQsocket(conn));
			switch(PQstatus(conn))
			{
			case CONNECTION_OK:
				PQconsumeInput(conn);
				break;
			case CONNECTION_STARTED:
			case CONNECTION_MADE:
			case CONNECTION_AWAITING_RESPONSE:
			case CONNECTION_AUTH_OK:
			case CONNECTION_SETENV:
			case CONNECTION_SSL_STARTUP:
				(void)PQconnectPoll(conn);
				break;
			default:
				ExceptionalCondition("Status error", "FailedAssertion", __FILE__, __LINE__);
			}
		}
	}

return_pqn_wait_:
	if(*ppconn == NULL)
		return 0;
	i=1;
	foreach(lc, conn_list)
	{
		if(lfirst(lc) == *ppconn)
			return i;
		++i;
	}
	Assert(0);
	return 0;
}

void PQNListExecFinish(List *conn_list, bool report_error)
{
	List *list;
	ListCell *lc;
	PGconn *conn,*error_conn = NULL;
	PGresult *res,*error_res = NULL;
	int index;

	/* copy list */
	list = NIL;
	foreach(lc, conn_list)
	{
		conn = lfirst(lc);
		if(PQisCopyInState(conn))
			PQputCopyEnd(conn, NULL);
		list = lappend(list, conn);
	}

	while(list != NIL)
	{
		index = PQNWaitResult(list, &conn, true);
		if(index < 0)
		{
			if(index == -EINTR)
				continue;

			PQNListExecFinish_trouble(list);
			list_free(list);

			if(error_res && report_error)
				PQNReportResultError(error_res, conn, ERROR, true);
			PQclear(error_res);
			return;
		}else if(index == 0)
		{
			break;
		}
		if(PQisCopyOutState(conn))
		{
			/* we just use res for temp, PQgetCopyDataBuffer don't need free result memory point */
			PQgetCopyDataBuffer(conn, (const char**)&res, true);
		}else
		{
			res = PQgetResult(conn);
			if(res)
			{
				switch(PQresultStatus(res))
				{
				case PGRES_COPY_IN:
					PQputCopyEnd(conn, NULL);
					break;
				case PGRES_FATAL_ERROR:
					if(report_error && error_res == NULL)
					{
						error_res = res;
						error_conn = conn;
						res = NULL;
					}
					break;
				default:
					break;
				}
				PQclear(res);
			}else
			{
				list = list_delete_ptr(list, conn);
			}
		}
	}

	if(report_error && error_res)
		PQNReportResultError(error_res, error_conn, ERROR, true);
}

static void PQNListExecFinish_trouble(List *list)
{
	ListCell *lc;
	foreach(lc, list)
		PQNExecFinsh_trouble(lfirst(lc));
}

static void PQNExecFinsh_trouble(PGconn *conn)
{
	PGresult *res;
	for(;;)
	{
		if(PQstatus(conn) == CONNECTION_BAD)
			break;
		if(PQisCopyInState(conn))
			PQputCopyEnd(conn, NULL);
		while(PQisCopyOutState(conn))
		{
			if(PQgetCopyDataBuffer(conn, (const char**)&res, false) < 0)
				break;
		}
		res = PQgetResult(conn);
		if(res)
			PQclear(res);
		else
			break;
	}
}

void PQNReleaseAllConnect(void)
{
	HASH_SEQ_STATUS seq_status;
	OidPGconn *op;
	if(htab_oid_pgconn == NULL || hash_get_num_entries(htab_oid_pgconn) == 0)
		return;

	hash_seq_init(&seq_status, htab_oid_pgconn);
	while((op = hash_seq_search(&seq_status)) != NULL)
	{
		PQNExecFinsh_trouble(op->conn);
		PQdetach(op->conn);
		op->conn = NULL;
	}
	hash_destroy(htab_oid_pgconn);
	htab_oid_pgconn = NULL;
	PoolManagerReleaseConnections(false);
}

int PQNListGetReslut(List *conn_list, struct pg_result **ppres, bool noError)
{
	int index;
	PGconn *conn;

	*ppres = NULL;
	index = PQNWaitResult(conn_list, &conn, noError);
	if(index <= 0)
		return index;
	*ppres = PQgetResult(conn);
	return index;
}

void PQNReportResultError(struct pg_result *result, struct pg_conn *conn, int elevel, bool free_result)
{
	AssertArg(result);
	PG_TRY();
	{
		char	   *file_name = PQresultErrorField(result, PG_DIAG_SOURCE_FILE);
		char	   *file_line = PQresultErrorField(result, PG_DIAG_SOURCE_LINE);
		char	   *func_name = PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION);

		if(errstart(elevel, file_name ? file_name : __FILE__,
			file_line ? atoi(file_line) : __LINE__,
			func_name ? func_name : PG_FUNCNAME_MACRO,
			TEXTDOMAIN))
		{
			const char *str;
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
				str = PQparameterStatus(conn, "pgxc_node_name");
			if(str == NULL)
				str = PQNConnectName(conn);
			if(str != NULL);
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

			errfinish(0);
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

extern const char *PQNConnectName(struct pg_conn *conn)
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
				return PGXCNodeOidGetName(op->oid);
			}
		}
	}
	return NULL;
}
