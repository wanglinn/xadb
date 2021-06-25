#include "postgres.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/rxact_comm.h"
#include "access/rxact_mgr.h"
#include "access/rxact_msg.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "catalog/pgxc_node.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/builtins.h"
#include "replication/snapsender.h"
#include "replication/snapreceiver.h"

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#define RETRY_TIME			1	/* 1 second */
#define EXIT_MINIMUM_NUMBER		2	/* The minimum number of events that Rxact can exit */
#define MAX_RXACT_BUF_SIZE	4096
#if defined(EAGAIN) && EAGAIN != EINTR
#define IS_ERR_INTR() (errno == EINTR || errno == EAGAIN)
#else
#define IS_ERR_INTR() (errno == EINTR)
#endif

#define REMOTE_IDLE_TIMEOUT			60	/* close remote connect if it idle in second */
#ifndef RXACT_LOG_LEVEL
#	define RXACT_LOG_LEVEL			DEBUG1
#endif /* RXACT_LOG_LEVEL */
#define RXACT_TYPE_IS_VALID(t) (t == RX_PREPARE || t == RX_COMMIT || t == RX_ROLLBACK)

#define RXACT_START_TRANS(in_transaction, oldcontext, CurrentMemoryContext)	\
do {																		\
	if (in_transaction == false)														\
	{																		\
		oldcontext = CurrentMemoryContext;									\
		StartTransactionCommand();											\
		CurrentMemoryContext = oldcontext;									\
		in_transaction = true;												\
	}																		\
} while(0)

#define RXACT_END_TRANS(in_transaction, oldcontext, CurrentMemoryContext)	\
do {																		\
	if (in_transaction)														\
	{																		\
		AbortCurrentTransaction();											\
		CurrentMemoryContext = oldcontext;									\
	}																		\
} while(0)

typedef struct RxactAgent
{
	pgsocket sock;
	Oid		dboid;
	bool	in_error;
	bool	waiting_gid;
	char	last_gid[NAMEDATALEN];
	StringInfoData out_buf;
	StringInfoData in_buf;
	XLogRecPtr		need_flush;		/* XLog flush number */
	char			reply_msg;		/* reply flush state */
}RxactAgent;

typedef struct DbAndNodeOid
{
	Oid node_oid;
	Oid db_oid;
}DbAndNodeOid;

typedef struct NodeConn
{
	DbAndNodeOid oids;	/* must be first */
	PGconn *conn;
	time_t last_use;	/* when conn != NULL: last use time
						 * when conn == NULL: next connect time */
	PostgresPollingStatusType
			status;
	char doing_gid[NAMEDATALEN];
	char clean_gid[NAMEDATALEN];
}NodeConn;

typedef enum WaiteEventTag
{
	T_Event_Start = 0,
	T_Event_Agent = T_Event_Start,
	T_Event_Node,
	T_Event_Socket,
	T_Event_Signal,
	T_Event_Last
}WaiteEventTag;

typedef struct RxactWaitEventData
{	
	WaiteEventTag	type;
	RxactAgent		*agent;
	NodeConn		*pconn;
	pgsocket		pconn_fd_dup;
	int				event_pos;	/* position in the event data structure */
	void			(*fun)(WaitEvent *event);
}RxactWaitEventData;

/*
 * don't use FeBeWaitSet, function secure_read call WaitEventSetWait
 * pass in only one WaitEvent parameter, when pq_comm_node have or listen
 * has event, it does not have a correspoding processing flow
 */
static WaitEventSet *rxact_wait_event_set = NULL;
static WaitEvent *rxact_wait_event = NULL;
static Size rxact_event_max_count = 32;
static Size rxact_event_cur_count = 0;
static long waiting_time = -1L;
#define RXACT_WAIT_EVENT_ENLARGE_STEP	32

static void AddRxactEventToSet(WaitEventSet *set, WaiteEventTag type, pgsocket fd, uint32 events, NodeConn *pconn);
static void RemoveRxactWaitEvent(WaitEventSet *set, RxactAgent *agent, NodeConn *pconn);
static void OnListenServerSocketConnEvent(WaitEvent *event);
static void OnListenAgentConnEvent(WaitEvent *event);
static void OnListenNodeConnEvent(WaitEvent *event);
static int getNodeConnPos(NodeConn *checkCoon);

static XLogRecPtr last_flush = 0;
static XLogRecPtr need_flush = 0;

extern bool adb_check_sync_nextid;	/* in snapsender.c */
static HTAB *htab_node_conn = NULL;		/* NodeConn */
static HTAB *htab_rxid = NULL;			/* RxactTransactionInfo */

/* remote xact log files */
static const char rxlf_xact_filename[] = {"rxact"};
static const char rxlf_directory[] = {"pg_rxlog"};
static StringInfoData rxlf_xlog_buf = {NULL, 0, 0, 0};
#define MAX_RLOG_FILE_NAME 24

static pgsocket rxact_server_fd = PGINVALID_SOCKET;
static volatile pgsocket rxact_client_fd = PGINVALID_SOCKET;
static bool sended_db_info = false;
static volatile bool rxact_need_exit = false;
static bool is_rxact_worker = false;

/*
 * Flag to mark SIGHUP. Whenever the main loop comes around it
 * will reread the configuration file. (Better than doing the
 * reading in the signal handler, ey?)
 */
static volatile sig_atomic_t got_SIGHUP = false;
static void RxactHupHandler(SIGNAL_ARGS);

static RxactAgent* CreateRxactAgent(int agent_fd);
static void RxactMgrQuickdie(SIGNAL_ARGS);
static void RxactMarkAutoTransaction(RxactTransactionInfo *rinfo);
static void RxactLoop(void);
static void RemoteXactBaseInit(void);
static void RemoteXactMgrInit(void);
static void RemoteXactHtabInit(void);
static void DestroyRemoteConnHashTab(void);
static void RxactLoadLog(bool is_main);
static void RxactSaveLog(bool flush);
static void on_exit_rxact_mgr(int code, Datum arg);

static bool rxact_agent_recv_data(RxactAgent *agent);
static void rxact_agent_input(RxactAgent *agent);
static void agent_error_hook(void *arg);
static void rxact_agent_output(RxactAgent *agent);
static void rxact_agent_destroy(RxactAgent *agent);
static void rxact_agent_end_msg(RxactAgent *agent, StringInfo msg);
static void rxact_agent_simple_msg(RxactAgent *agent, char msg_type);

/* parse message from backend */
static void rxact_agent_connect(RxactAgent *agent, StringInfo msg);
static void rxact_agent_do(RxactAgent *agent, StringInfo msg);
static void rxact_agent_mark(RxactAgent *agent, StringInfo msg, bool success);
static void rxact_agent_checkpoint(RxactAgent *agent, StringInfo msg);
static void rxact_gent_auto_txid(RxactAgent *agent, StringInfo msg);
static void rxact_agent_get_running(RxactAgent *agent);
static void rxact_agent_wait_gid(RxactAgent *agent, StringInfo msg);

/* htab functions */
static uint32 hash_DbAndNodeOid(const void *key, Size keysize);
static int match_DbAndNodeOid(const void *key1, const void *key2,
											Size keysize);
static void
rxact_insert_gid(const char *gid, const Oid *oids, int count, RemoteXactType type,
			Oid db_oid, bool is_redo, bool is_cleanup);
static void rxact_mark_gid(const char *gid, RemoteXactType type, bool success, bool is_redo);
static void rxact_auto_gid(const char *gid, TransactionId txid, bool is_redo);

/* 2pc redo functions */
static void rxact_2pc_do(void);
static void rxact_2pc_result(NodeConn *conn);
static NodeConn* rxact_get_node_conn(Oid db_oid, Oid node_oid, time_t cur_time);
static bool rxact_check_node_conn(NodeConn *conn);
static void rxact_finish_node_conn(NodeConn *conn);
static void rxact_build_2pc_cmd(StringInfo cmd, const char *gid, RemoteXactType type);
static void rxact_close_timeout_remote_conn(time_t cur_time);
static File rxact_log_open_file(const char *log_name, int fileFlags, int fileMode);
static void rxact_xlog_insert(char *data, int len, uint8 info);
static const char* RemoteXactType2String(RemoteXactType type);

/* interface for client */
static bool record_rxact_status(const char *gid, RemoteXactType type, bool success, bool no_error);
static bool send_msg_to_rxact(StringInfo buf, bool no_error);
static bool recv_msg_from_rxact(StringInfo buf, bool no_error);
static bool wait_socket(pgsocket sock, bool wait_send, bool block);
static bool recv_socket(pgsocket sock, StringInfo buf, int max_recv, bool no_error);
static bool connect_rxact(bool no_error);
static bool rxact_begin_db_info(StringInfo buf, Oid dboid, bool no_error);

static RxactAgent *
CreateRxactAgent(pgsocket agent_fd)
{
	RxactAgent *agent = NULL;

	AssertArg(agent_fd != PGINVALID_SOCKET);
	MemoryContextSwitchTo(TopMemoryContext);
	agent = MemoryContextAlloc(TopMemoryContext, sizeof(RxactAgent));
	Assert(agent != NULL);

	agent->sock = agent_fd;
	agent->reply_msg = 0;
	pg_set_noblock(agent_fd);
	agent->in_error = agent->waiting_gid = false;

	initStringInfo(&(agent->in_buf));
	initStringInfo(&(agent->out_buf));
	resetStringInfo(&(agent->in_buf));
	resetStringInfo(&(agent->out_buf));
	return agent;
}

static void
RxactMgrQuickdie(SIGNAL_ARGS)
{
	rxact_need_exit = true;
}

static void RxactLoop(void)
{
	sigjmp_buf			local_sigjmp_buf;
	RxactAgent 			*agent;
	StringInfoData		message;
	time_t				last_time,cur_time;
	NodeConn			*pconn;
	HASH_SEQ_STATUS		seq_status;

	Assert(rxact_server_fd != PGINVALID_SOCKET);
	if(pg_set_noblock(rxact_server_fd) == false)
		ereport(FATAL, (errmsg("Can not set RXACT listen socket to noblock:%m")));

	MemoryContextSwitchTo(TopMemoryContext);
	initStringInfo(&message);

	/* create WaitEventSet */
	if(rxact_wait_event_set == NULL)
	{
		rxact_wait_event_set = CreateWaitEventSet(TopMemoryContext,
												rxact_event_max_count);
		rxact_wait_event = palloc(rxact_event_max_count * sizeof(WaitEvent));
	}
	/* add server soket event to eventSet */
	AddRxactEventToSet(rxact_wait_event_set, T_Event_Socket, rxact_server_fd, WL_SOCKET_READABLE, NULL);
	/* add POSTMASTER exit signal */
	AddRxactEventToSet(rxact_wait_event_set, T_Event_Signal, PGINVALID_SOCKET, WL_POSTMASTER_DEATH, NULL);

	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Cleanup something */
		EmitErrorReport();
		FlushErrorState();
		AtEOXact_HashTables(false);
		AbortCurrentTransaction();
		error_context_stack = NULL;
	}
	PG_exception_stack = &local_sigjmp_buf;
	(void)MemoryContextSwitchTo(MessageContext);
	if (hash_get_num_entries(htab_rxid) > 0)
		waiting_time = 1000L;

	last_time = cur_time = time(NULL);
	while(rxact_need_exit == false)
	{
		int					nevents, i;
		int					skip_pos = -1;
		WaitEvent			*waitEvent;
		RxactWaitEventData	*user_data;

		MemoryContextResetAndDeleteChildren(MessageContext);

		if (!PostmasterIsAlive() && rxact_event_cur_count <= EXIT_MINIMUM_NUMBER)
			exit(0);

		for (i = rxact_event_cur_count -1; i >= EXIT_MINIMUM_NUMBER; i--)
		{
			user_data = (RxactWaitEventData *)GetWaitEventData(rxact_wait_event_set, i);
			if(user_data->type == T_Event_Agent)
			{
				agent = user_data->agent;
				
				Assert(agent->sock != PGINVALID_SOCKET);
				if(agent->waiting_gid)
				{
					Assert(agent->last_gid[0] != '\0');
					if(hash_search(htab_rxid, agent->last_gid, HASH_FIND, NULL) == NULL)
					{
						agent->waiting_gid = false;
						agent->last_gid[0] = '\0';
						rxact_agent_simple_msg(agent, RXACT_MSG_OK);
						if(agent->sock == PGINVALID_SOCKET)
							continue;
					}
				}
				/* Check and modify wait events */
				if(agent->out_buf.len > agent->out_buf.cursor)
					ModifyWaitEvent(rxact_wait_event_set, i, WL_SOCKET_WRITEABLE, NULL);
				else if(agent->waiting_gid == false)
					ModifyWaitEvent(rxact_wait_event_set, i, WL_SOCKET_READABLE, NULL);
				else
					ModifyWaitEvent(rxact_wait_event_set, i, 0, NULL);

			}else if (user_data->type == T_Event_Node)
			{
				pconn = NULL;
				hash_seq_init(&seq_status, htab_node_conn);
				while((pconn = hash_seq_search(&seq_status)) != NULL)
				{
					if (pconn == user_data->pconn)
					{
						hash_seq_term(&seq_status);
						break;
					}
				}
				if (pconn == NULL || PQstatus(user_data->pconn->conn) == CONNECTION_BAD || 
				 	user_data->pconn->status == PGRES_POLLING_FAILED ||
				 	user_data->pconn->status == PGRES_POLLING_ACTIVE)
				{
					if (user_data->pconn)
					{
						RemoveWaitEvent(rxact_wait_event_set, i);
						closesocket(user_data->pconn_fd_dup);
						rxact_finish_node_conn(user_data->pconn);
						--rxact_event_cur_count;
					}
				}
			}
		}

		/* append node sockets */
		hash_seq_init(&seq_status, htab_node_conn);
		while((pconn = hash_seq_search(&seq_status)) != NULL)
		{
			bool wait_write;
			int pos = 0;

			if(pconn->conn == NULL)
				continue;
			if(PQstatus(pconn->conn) == CONNECTION_BAD)
			{
				RemoveRxactWaitEvent(rxact_wait_event_set, NULL, pconn);
				continue;
			}

			switch(pconn->status)
			{
			case PGRES_POLLING_ACTIVE:
			case PGRES_POLLING_FAILED:
				RemoveRxactWaitEvent(rxact_wait_event_set, NULL, pconn);
				continue;
			case PGRES_POLLING_OK:
				if(pconn->doing_gid[0] == '\0')
				{
					pos = getNodeConnPos(pconn);
					if (pos)
					{
						user_data = (RxactWaitEventData *)GetWaitEventData(rxact_wait_event_set, pos);
						/* 
						 * If the connection exists in the event set, 
						 * move it out only, but do not close it.
						 */
						RemoveWaitEvent(rxact_wait_event_set, pos);
						--rxact_event_cur_count;
						closesocket(user_data->pconn_fd_dup);
					}
					continue;
				}
				wait_write = false;
				break;
			case PGRES_POLLING_WRITING:
				wait_write = true;
				break;
			case PGRES_POLLING_READING:
				wait_write = false;
				break;
			default:
				Assert(0);
			}
			
			pos = getNodeConnPos(pconn);
			if(pos > 0)
			{
				RxactWaitEventData *rxactEventData;
				rxactEventData = (RxactWaitEventData *)GetWaitEventData(rxact_wait_event_set, pos);

				Assert(rxactEventData->type == T_Event_Node);
				/* change node evnet */
				ModifyWaitEvent(rxact_wait_event_set, pos, wait_write ? WL_SOCKET_WRITEABLE:WL_SOCKET_READABLE, NULL);
				rxactEventData = (RxactWaitEventData *)GetWaitEventData(rxact_wait_event_set, pos);
				Assert(rxactEventData->type >= T_Event_Start && rxactEventData->type < T_Event_Last);
			}else
				AddRxactEventToSet(rxact_wait_event_set, T_Event_Node, PQsocket(pconn->conn), wait_write ? WL_SOCKET_WRITEABLE:WL_SOCKET_READABLE, pconn);
		}

		/* wait event  nevents */
		nevents = WaitEventSetWaitSignal(rxact_wait_event_set,
								   waiting_time, //-1L or 1000,
								   rxact_wait_event,
								   rxact_event_cur_count,
								   WAIT_EVENT_CLIENT_WRITE,
								   true);
		CHECK_FOR_INTERRUPTS();
		for(i=0; i<nevents; ++i)
		{	
			waitEvent = &rxact_wait_event[i];
			user_data = waitEvent->user_data;
			if (user_data->type == T_Event_Socket)
			{
				(*user_data->fun)(waitEvent);
				skip_pos = i;
				break;
			}
		} 
		for(i=0; i<nevents; ++i)
		{	
			if (skip_pos == i)
				continue;
			waitEvent = &rxact_wait_event[i];
			user_data = waitEvent->user_data;
			if(user_data->fun)
				(*user_data->fun)(waitEvent);
		}
		if(last_flush != 0)
			XLogFlush(last_flush);

		/* Send log flush results */
		for(i=0; i<nevents; i++)
		{
			waitEvent = &rxact_wait_event[i];
			user_data = waitEvent->user_data;
			agent = user_data->agent;
			if(user_data->type == T_Event_Agent && agent->reply_msg)
			{
				if(last_flush >= agent->need_flush)
				{
					if(agent->reply_msg == RXACT_MSG_OK)
					{
						/* flush ok */
						rxact_agent_simple_msg(agent, RXACT_MSG_OK);
					}
				}else
				{
					/* flush error */
					rxact_agent_simple_msg(agent, RXACT_MSG_ERROR);
				}
				/* clear reply massage */
				agent->reply_msg = 0;
			}
		}
		last_flush = 0;
		rxact_2pc_do();

		cur_time = time(NULL);
		if(last_time != cur_time)
		{
			rxact_close_timeout_remote_conn(cur_time);
			last_time = cur_time;
		}
	}
	FreeWaitEventSet(rxact_wait_event_set);
}

static void RemoteXactBaseInit(void)
{
	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, RxactMgrQuickdie);
	pqsignal(SIGQUIT, RxactMgrQuickdie);
	pqsignal(SIGHUP, RxactHupHandler);
	InitializeTimeouts();		/* establishes SIGALRM handler */
	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);
}

static void RemoteXactMgrInit(void)
{
	/* init listen socket */
	Assert(rxact_server_fd == PGINVALID_SOCKET);
	rxact_server_fd = rxact_listen();
	if(rxact_server_fd == PGINVALID_SOCKET)
	{
		ereport(FATAL,
			(errmsg("Remote xact can not create listen socket on \"%s\":%m", rxact_get_sock_path())));
	}

	initStringInfo(&rxlf_xlog_buf);

	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);

	on_proc_exit(on_exit_rxact_mgr, (Datum)0);
}

static void RemoteXactHtabInit(void)
{
	HASHCTL hctl;

	/* create HTAB for NodeConn */
	Assert(htab_node_conn == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(DbAndNodeOid);
	hctl.entrysize = sizeof(NodeConn);
	hctl.hash = hash_DbAndNodeOid;
	hctl.match = match_DbAndNodeOid;
	hctl.hcxt = TopMemoryContext;
	htab_node_conn = hash_create("NodeConnect",
								 64,
								 &hctl,
								 HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* create HTAB for RxactTransactionInfo */
	Assert(htab_rxid == NULL);
	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(((RxactTransactionInfo*)0)->gid);
	hctl.entrysize = sizeof(RxactTransactionInfo);
	hctl.hcxt = TopMemoryContext;
	htab_rxid = hash_create("RxactInfo",
							512,
							&hctl,
							HASH_ELEM | HASH_CONTEXT | HASH_STRINGS);
}

static void
DestroyRemoteConnHashTab(void)
{
	RxactTransactionInfo *rinfo;
	HASH_SEQ_STATUS hash_status;

	hash_seq_init(&hash_status, htab_rxid);
	while((rinfo=hash_seq_search(&hash_status))!=NULL)
	{
		if(rinfo->remote_nodes)
			pfree(rinfo->remote_nodes);
	}
	hash_destroy(htab_rxid);
	htab_rxid = NULL;
}

bool IsRXACTWorker(void)
{
	return is_rxact_worker;
}

void
RemoteXactMgrMain(void)
{
	is_rxact_worker = true;
	PG_exception_stack = NULL;

	RemoteXactBaseInit();

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	/* Initinalize something */
	RemoteXactHtabInit();
	RemoteXactMgrInit();
	(void)MemoryContextSwitchTo(MessageContext);

	RxactLoadLog(true);

	/* Server loop */
	RxactLoop();

	proc_exit(0);
}

static void RxactLoadLog(bool is_main)
{
	RXactLog rlog;
	File rfile;
	int res;

	/* mkdir rxlog directory if not exists */
	res = mkdir(rxlf_directory, S_IRWXU);
	if(res < 0)
	{
		if(errno != EEXIST)
		{
			ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\":%m", rxlf_directory)));
		}
	}

	/* load xact */
	rfile = rxact_log_open_file(rxlf_xact_filename, O_RDONLY|O_CREAT|PG_BINARY, 0600);
	rlog = rxact_begin_read_log(rfile);
	for(;;)
	{
		const char *gid;
		Oid *oids;
		Oid db_oid;
		TransactionId tid;
		int count;
		char c;
		rxact_log_reset(rlog);
		if(rxact_log_is_eof(rlog))
			break;

		waiting_time = 100;
		gid = rxact_log_get_string(rlog);
		rxact_log_read_bytes(rlog, (char*)&db_oid, sizeof(db_oid));
		rxact_log_read_bytes(rlog, (char*)&count, sizeof(count));
		if(count > 0)
			oids = rxact_log_get_bytes(rlog, count*sizeof(oids[0]));
		else
			oids = NULL;
		rxact_log_read_bytes(rlog, &c, 1);
		rxact_insert_gid(gid, oids, count, (RemoteXactType)c, db_oid, true, is_main);
		if(c == RX_AUTO)
		{
			rxact_log_read_bytes(rlog, &tid, sizeof(tid));
			rxact_auto_gid(gid, tid, true);
		}
	}
	rxact_end_read_log(rlog);
	FileClose(rfile);
}

static void RxactSaveLog(bool flush)
{
	RxactTransactionInfo *rinfo;
	RXactLog rlog;
	HASH_SEQ_STATUS hash_status;
	File rfile;

	/* save xact */
	hash_seq_init(&hash_status, htab_rxid);
	rfile = rxact_log_open_file(rxlf_xact_filename, O_WRONLY | O_TRUNC | PG_BINARY, 0);
	rlog = rxact_begin_write_log(rfile);
	while((rinfo = hash_seq_search(&hash_status)) != NULL)
	{
		rxact_log_write_string(rlog, rinfo->gid);
		rxact_log_write_bytes(rlog, &(rinfo->db_oid), sizeof(rinfo->db_oid));
		rxact_log_write_int(rlog, rinfo->count_nodes);
		rxact_log_write_bytes(rlog, rinfo->remote_nodes
			, sizeof(rinfo->remote_nodes[0]) * (rinfo->count_nodes));
		rxact_log_write_byte(rlog, (char)(rinfo->type));
		if(rinfo->type == RX_AUTO)
			rxact_log_write_bytes(rlog, &rinfo->auto_tid, sizeof(rinfo->auto_tid));
		rxact_write_log(rlog);
	}
	rxact_end_write_log(rlog);
	if (flush &&
		FileSync(rfile, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
	{
		FileClose(rfile);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						FilePathName(rfile))));
	}
	FileClose(rfile);
}

static void
on_exit_rxact_mgr(int code, Datum arg)
{
	if(rxact_server_fd != PGINVALID_SOCKET)
	{
		closesocket(rxact_server_fd);
		rxact_server_fd = PGINVALID_SOCKET;
	}
	if (htab_rxid != NULL)
	{
		RxactSaveLog(true);
		DestroyRemoteConnHashTab();
	}
}

/*
 * Destroy RxactAgent
 */
static void
rxact_agent_destroy(RxactAgent *agent)
{
	
	/* remove agent wait event */
	RemoveRxactWaitEvent(rxact_wait_event_set, agent, NULL);
	/* Destroy agent */
	closesocket(agent->sock);
	agent->sock = PGINVALID_SOCKET;
	agent->dboid = InvalidOid;
	if(agent->last_gid[0] != '\0' && agent->waiting_gid == false)
	{
		RxactTransactionInfo *rinfo;
		bool found;
		rinfo = hash_search(htab_rxid, agent->last_gid, HASH_FIND, &found);
		if(!found)
		{
			ereport(WARNING,
				(errmsg("Can not found gid '%s' at destroy agent", agent->last_gid)));
		}else
		{
			Assert(rinfo && rinfo->failed == false);
			rxact_mark_gid(rinfo->gid, rinfo->type, false, false);
			ereport(RXACT_LOG_LEVEL, (errmsg("agent destroy mark '%s' %s %s"
				, rinfo->gid, RemoteXactType2String(rinfo->type), "failed")));
		}
		agent->last_gid[0] = '\0';
	}
}

static void rxact_agent_end_msg(RxactAgent *agent, StringInfo msg)
{
	AssertArg(agent && msg);
	if(agent->sock != PGINVALID_SOCKET)
	{
		bool need_try;
		if(agent->out_buf.len > agent->out_buf.cursor)
		{
			need_try = false;
		}else
		{
			need_try = true;
			resetStringInfo(&(agent->out_buf));
		}
		rxact_put_finsh(msg, false);
		appendBinaryStringInfo(&(agent->out_buf), msg->data, msg->len);
		if(need_try)
			rxact_agent_output(agent);
	}
	pfree(msg->data);
}

static void rxact_agent_simple_msg(RxactAgent *agent, char msg_type)
{
	union
	{
		int len;
		char str[5];
	}msg;
	bool need_try;
	AssertArg(agent);

	if(agent->sock == PGINVALID_SOCKET)
		return;
	if(agent->out_buf.len > agent->out_buf.cursor)
	{
		need_try = false;
	}else
	{
		need_try = true;
		resetStringInfo(&(agent->out_buf));
	}
	enlargeStringInfo(&(agent->out_buf), 5);
	msg.len = 5;
	msg.str[4] = msg_type;
	appendBinaryStringInfo(&(agent->out_buf), msg.str, 5);
	if(need_try)
		rxact_agent_output(agent);
}

/* true for recv some data, false for closed by remote */
static bool
rxact_agent_recv_data(RxactAgent *agent)
{
	StringInfo buf;
	ssize_t recv_res;

	AssertArg(agent && agent->sock != PGINVALID_SOCKET);
	buf = &(agent->in_buf);

	if(buf->cursor > 0)
	{
		if(buf->len > buf->cursor)
		{
			memmove(buf->data, buf->data + buf->cursor, buf->len - buf->cursor);
			buf->len -= buf->cursor;
			buf->cursor = 0;
		}else
		{
			buf->cursor = buf->len = 0;
		}
	}

	if(buf->len >= buf->maxlen)
	{
		if(buf->len >= MAX_RXACT_BUF_SIZE)
		{
			ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("too many data from backend for remote xact manager")));
			return false;
		}
		enlargeStringInfo(buf, buf->maxlen + 1024);
		if(buf->len >= buf->maxlen)
			return false;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		recv_res = recv(agent->sock
			, buf->data + buf->len
			, buf->maxlen - buf->len, 0);
		CHECK_FOR_INTERRUPTS();

		if (recv_res < 0)
		{
			if (IS_ERR_INTR())
				continue;		/* Ok if interrupted */

			/*
			 * Report broken connection
			 */
			ereport(WARNING,
					(errcode_for_socket_access(),
					 errmsg("RXACT could not receive data from client: %m")));
			return false;
		}else if (recv_res == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return false;
		}
		/* rval contains number of bytes read, so just incr length */
		buf->len += recv_res;
		Assert(buf->len <= buf->maxlen);
		break;
	}
	return true;
}

/* get message if has completion message */
static bool
agent_has_completion_msg(RxactAgent *agent, StringInfo msg, uint8 *msg_type)
{
	StringInfo buf;
	Size unread_len;
	int len;
	AssertArg(agent && msg);

	buf = &(agent->in_buf);

	unread_len = buf->len - buf->cursor;
	/* 5 is message type (char) and message length(int) */
	if(unread_len < 5)
		return false;

	/* get message length */
	memcpy(&len, buf->data + buf->cursor, 4);
	if(len > unread_len)
		return false;

	*msg_type = buf->data[buf->cursor + 4];

	/* okay, copy message */
	resetStringInfo(msg);
	enlargeStringInfo(msg, len);
	memcpy(msg->data, buf->data + buf->cursor, len);
	msg->len = len;
	msg->cursor = 5; /* skip message type and length */
	buf->cursor += len;
	return true;
}

static void
rxact_agent_input(RxactAgent *agent)
{
	ErrorContextCallback	err_calback;
	StringInfoData			s;
	uint8					qtype;

	/* try recv data */
	if(rxact_agent_recv_data(agent) == false)
	{
		/* closed by remote */
		rxact_agent_destroy(agent);
		return;
	}
	Assert(agent->waiting_gid == false);

	/* setup error callback */
	err_calback.arg = agent;
	err_calback.callback = agent_error_hook;
	err_calback.previous = error_context_stack;
	error_context_stack = &err_calback;

	initStringInfo(&s);
	while(agent_has_completion_msg(agent, &s, &qtype))
	{
		agent->in_error = false;
		switch(qtype)
		{
		case RXACT_MSG_CONNECT:
			rxact_agent_connect(agent, &s);
			break;
		case RXACT_MSG_DO:
			rxact_agent_do(agent, &s);
			break;
		case RXACT_MSG_SUCCESS:
			rxact_agent_mark(agent, &s, true);
			break;
		case RXACT_MSG_FAILED:
			rxact_agent_mark(agent, &s, false);
			break;
		case RXACT_MSG_CHECKPOINT:
			rxact_agent_checkpoint(agent, &s);
			break;
		case RXACT_MSG_AUTO:
			rxact_gent_auto_txid(agent, &s);
			break;
		case RXACT_MSG_RUNNING:
			rxact_agent_get_running(agent);
			break;
		case RXACT_MSG_WAIT_GID:
			rxact_agent_wait_gid(agent, &s);
			break;
		default:
			PG_TRY();
			{
				ereport(ERROR, (errmsg("unknown message type %d", qtype)));
			}PG_CATCH();
			{
				rxact_agent_destroy(agent);
				PG_RE_THROW();
			}PG_END_TRY();
		}
		PG_TRY();
		{
			rxact_get_msg_end(&s);
		}PG_CATCH();
		{
			rxact_agent_destroy(agent);
			PG_RE_THROW();
		}PG_END_TRY();
	}
	error_context_stack = err_calback.previous;
	if (s.data)
		pfree(s.data);
}

static void agent_error_hook(void *arg)
{
	const ErrorData *err;
	RxactAgent *agent = arg;
	AssertArg(agent);

	if(agent->in_error == false
		&& agent->sock != PGINVALID_SOCKET
		&& (err = err_current_data()) != NULL
		&& err->elevel >= ERROR)
	{
		StringInfoData msg;
		agent->in_error = true;
		rxact_begin_msg(&msg, RXACT_MSG_ERROR, false);
		rxact_put_string(&msg, err->message ? err->message : "miss error message", false);
		rxact_agent_end_msg(arg, &msg);
	}
}

static void rxact_agent_output(RxactAgent *agent)
{
	ssize_t send_res;
	AssertArg(agent && agent->sock != PGINVALID_SOCKET);
	AssertArg(agent->out_buf.len > agent->out_buf.cursor);

re_send_:
	send_res = send(agent->sock
		, agent->out_buf.data + agent->out_buf.cursor
		, agent->out_buf.len - agent->out_buf.cursor
#ifdef MSG_DONTWAIT
		, MSG_DONTWAIT);
#else
		, 0);
#endif
	CHECK_FOR_INTERRUPTS();

	if(send_res == 0)
	{
		rxact_agent_destroy(agent);
	}else if(send_res > 0)
	{
		agent->out_buf.cursor += send_res;
		if(agent->out_buf.cursor >= agent->out_buf.len)
			resetStringInfo(&(agent->out_buf));
	}else
	{
		if(IS_ERR_INTR())
			goto re_send_;
		if(errno != EAGAIN
#if defined(EWOULDBLOCK) && EWOULDBLOCK != EAGAIN
			&& errno != EWOULDBLOCK
#endif
			)
		{
			ereport(WARNING, (errcode_for_socket_access()
				, errmsg("Can not send message to RXACT client:%m")));
			rxact_agent_destroy(agent);
		}
	}
}

static void rxact_agent_connect(RxactAgent *agent, StringInfo msg)
{
	AssertArg(agent && msg);
	agent->dboid = (Oid)rxact_get_int(msg);
	if(OidIsValid(agent->dboid))
	{
		(void)rxact_get_string(msg);
		(void)rxact_get_string(msg);
		/*rxact_insert_database(agent->dboid, dbname, owner, false);*/
	}
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_agent_do(RxactAgent *agent, StringInfo msg)
{
	Oid *oids;
	char *gid;
	RemoteXactType type;
	int count;
	AssertArg(agent && msg);

	if(!OidIsValid(agent->dboid))
	{
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			,errmsg("not got database oid")));
	}

	type = (RemoteXactType)rxact_get_int(msg);
	if(!RXACT_TYPE_IS_VALID(type))
		ereport(ERROR, (errmsg("Unknown remote xact type %d", type)));
	count = rxact_get_int(msg);
	if(count < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("insufficient data left in message")));
	}
	if(count > 0)
		oids = rxact_get_bytes(msg, sizeof(Oid)*count);
	else
		oids = NULL;
	gid = rxact_get_string(msg);

	rxact_insert_gid(gid, oids, count, type, agent->dboid, false, false);
	ereport(RXACT_LOG_LEVEL, (errmsg("backend begin '%s' %s"
		, gid, RemoteXactType2String(type))));
	strncpy(agent->last_gid, gid, sizeof(agent->last_gid)-1);
	agent->reply_msg = RXACT_MSG_OK;
}

static void rxact_agent_mark(RxactAgent *agent, StringInfo msg, bool success)
{
	const char *gid;
	RemoteXactType type;
	AssertArg(agent && msg);

	type = (RemoteXactType)rxact_get_int(msg);
	gid = rxact_get_string(msg);
	rxact_mark_gid(gid, type, success, false);
	ereport(RXACT_LOG_LEVEL, (errmsg("backend mark '%s' %s %s"
		, gid, RemoteXactType2String(type), success ? "success":"failed")));
	agent->last_gid[0] = '\0';
	agent->reply_msg = RXACT_MSG_OK;
}

static void rxact_agent_checkpoint(RxactAgent *agent, StringInfo msg)
{
	int flags = rxact_get_int(msg);
	RxactSaveLog(flags & CHECKPOINT_IMMEDIATE ? false:true);
	rxact_agent_simple_msg(agent, RXACT_MSG_OK);
}

static void rxact_gent_auto_txid(RxactAgent *agent, StringInfo msg)
{
	const char *gid;
	TransactionId tid;
	AssertArg(agent && msg);

	tid = (TransactionId)rxact_get_int(msg);		StaticAssertExpr(sizeof(tid) == sizeof(int),"");
	gid = rxact_get_string(msg);
	rxact_auto_gid(gid, tid, false);
	ereport(RXACT_LOG_LEVEL, (errmsg("backend auto '%s' for transaction id %u", gid, tid)));
	agent->reply_msg = RXACT_MSG_OK;
}

static void rxact_agent_get_running(RxactAgent *agent)
{
	RxactTransactionInfo *info;
	StringInfoData buf;
	HASH_SEQ_STATUS seq_status;
	Oid oid;

	hash_seq_init(&seq_status, htab_rxid);
	rxact_begin_msg(&buf, RXACT_MSG_RUNNING, false);
	oid = InvalidOid;
	while((info = hash_seq_search(&seq_status)) != NULL)
	{
		rxact_put_string(&buf, info->gid, false);
		rxact_put_int(&buf, info->count_nodes, false);
		if(info->count_nodes > 0)
		{
			StaticAssertStmt(sizeof(oid) == sizeof(info->remote_nodes[0]), "change oid type");
			rxact_put_bytes(&buf, info->remote_nodes
				, sizeof(info->remote_nodes[0]) * (info->count_nodes), false);
		}
		rxact_put_bytes(&buf, info->remote_success
			, sizeof(info->remote_success[0]) * info->count_nodes, false);
		StaticAssertStmt(sizeof(info->db_oid) == sizeof(int), "change code off get");
		rxact_put_int(&buf, info->db_oid, false);
		rxact_put_int(&buf, info->type, false);
		rxact_put_short(&buf, info->failed, false);
	}
	rxact_agent_end_msg(agent, &buf);
}

static void rxact_agent_wait_gid(RxactAgent *agent, StringInfo msg)
{
	char *gid;
	RxactTransactionInfo *info;
	AssertArg(agent && msg);

	gid = rxact_get_string(msg);
	info = hash_search(htab_rxid, gid, HASH_FIND, NULL);
	if(info == NULL)
	{
		rxact_agent_simple_msg(agent, RXACT_MSG_OK);
	}else
	{
		strcpy(agent->last_gid, gid);
		agent->waiting_gid = true;
	}
}

/* HTAB functions */
static uint32 hash_DbAndNodeOid(const void *key, Size keysize)
{
	Datum datum;
	Assert(keysize == sizeof(DbAndNodeOid));

	datum = hash_any((const unsigned char *) key
		, sizeof(DbAndNodeOid));
	return DatumGetUInt32(datum);
}

static int match_DbAndNodeOid(const void *key1, const void *key2,
											Size keysize)
{
	const DbAndNodeOid *l;
	const DbAndNodeOid *r;
	Assert(keysize == sizeof(DbAndNodeOid));
	AssertArg(key1 && key2);

	l = key1;
	r = key2;
	if(l->node_oid > r->node_oid)
		return 1;
	else if(l->node_oid < r->node_oid)
		return -1;
	else if(l->db_oid > r->db_oid)
		return 1;
	else if(l->db_oid < r->db_oid)
		return -1;
	return 0;
}

static void
rxact_insert_gid(const char *gid, const Oid *oids, int count, RemoteXactType type, Oid db_oid, bool is_redo, bool is_cleanup)
{
	RxactTransactionInfo *rinfo;
	bool found;
	AssertArg(gid && gid[0] && count >= 0);
	if (type == RX_AUTO)
	{
		if(!is_redo)
			ereport(ERROR, (errmsg("Can not record auto transaction")));
	}else if (!RXACT_TYPE_IS_VALID(type))
	{
		ereport(ERROR, (errmsg("Unknown remote xact type %d", type)));
	}

	rinfo = hash_search(htab_rxid, gid, HASH_ENTER, &found);
	rinfo->is_cleanup = is_cleanup;
	if(found)
	{
		if(is_redo)
			return;		/* ADBQ:need change exist info ? */
		ereport(ERROR, (errmsg("gid '%s' exists", gid)));
	}

	PG_TRY();
	{
		/* insert into log file */
		if(!is_redo)
		{
			resetStringInfo(&rxlf_xlog_buf);
			appendBinaryStringInfo(&rxlf_xlog_buf, (char*)&db_oid, sizeof(db_oid));
			appendStringInfoChar(&rxlf_xlog_buf, (char)type);
			appendBinaryStringInfo(&rxlf_xlog_buf, (char*)&count, sizeof(count));
			if(count > 0)
			{
				AssertArg(oids);
				appendBinaryStringInfo(&rxlf_xlog_buf, (char*)oids, count*(sizeof(oids[0])));
			}
			appendStringInfoString(&rxlf_xlog_buf, gid);
			/* include gid's '\0' */
			rxact_xlog_insert(rxlf_xlog_buf.data, rxlf_xlog_buf.len+1, RXACT_MSG_DO);
		}

		rinfo->remote_nodes
			= MemoryContextAllocZero(TopMemoryContext
				, (sizeof(Oid)+sizeof(bool))*(count+1));
		if(count > 0)
			memcpy(rinfo->remote_nodes, oids, sizeof(Oid)*(count));
		rinfo->remote_success = (bool*)(&rinfo->remote_nodes[count]);
		rinfo->count_nodes = count;

		rinfo->type = type;
		rinfo->failed = is_redo;
		rinfo->db_oid = db_oid;
	}PG_CATCH();
	{
		hash_search(htab_rxid, gid, HASH_REMOVE, NULL);
		PG_RE_THROW();
	}PG_END_TRY();

	/*if (is_cleanup)
	{
		if (IsGTMCnNode())
		{
			SnapSendTransactionAssign(pg_strtouint64(&rinfo->gid[1], NULL, 10), 1, InvalidTransactionId, true);
		}
		else
		{
			SnapRecvAddXactPrepareXid(pg_strtouint64(&rinfo->gid[1], NULL, 10));
		}
	}*/
}

static void RxactMarkAutoTransaction(RxactTransactionInfo *rinfo)
{
	int i;
	char buf[1024];
	PGresult *res;
	bool is_commited = false;
	NodeConn *node_conn;

	bool transfor_auto_ok = true;
	bool in_transaction = false;
	MemoryContext oldcontext = CurrentMemoryContext;

	Assert(rinfo->type == RX_AUTO);
	for(i=0;i<rinfo->count_nodes;++i)
	{
		/* skip successed node */
		if(rinfo->remote_success[i])
			continue;

		/* get node connection, skip if not connectiond */
		RXACT_START_TRANS(in_transaction, oldcontext, CurrentMemoryContext);
		/* get node connection, skip if not connectiond */
		node_conn = rxact_get_node_conn(rinfo->db_oid, rinfo->remote_nodes[i], time(NULL));
		if(node_conn == NULL || node_conn->conn == NULL || node_conn->doing_gid[0] != '\0')
		{
			/* if cann not get the connect now, try next time */
			transfor_auto_ok = false;
			break;
		}

		snprintf(buf, sizeof(buf), "SELECT pg_xact_status(%u)", rinfo->auto_tid);
		res = PQexec(node_conn->conn, buf);
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			if (strcmp(PQgetvalue(res, 0, 0), "COMMITTED") == 0)
				is_commited = true;
		}
		else
		{
			transfor_auto_ok = false;
			ereport(WARNING,(errmsg("pg_xact_status xid %u failed in node oid %u\n",rinfo->auto_tid, rinfo->remote_nodes[i])));
		}
			
		if (res)
			PQclear(res);
	}
	RXACT_END_TRANS(in_transaction, oldcontext, CurrentMemoryContext);

	if (transfor_auto_ok)
	{
		if (is_commited)
			rinfo->type = RX_COMMIT;
		else
			rinfo->type = RX_ROLLBACK;
	}
}

static void rxact_mark_gid(const char *gid, RemoteXactType type, bool success, bool is_redo)
{
	RxactTransactionInfo *rinfo;
	bool found;
	AssertArg(gid && gid[0]);

	/* save to log file */
	if(!is_redo && success)
	{
		/* we don't need save faile log */
		resetStringInfo(&rxlf_xlog_buf);
		appendStringInfoChar(&rxlf_xlog_buf, (char)type);
		appendStringInfoString(&rxlf_xlog_buf, gid);
		rxact_xlog_insert(rxlf_xlog_buf.data, rxlf_xlog_buf.len+1, RXACT_MSG_SUCCESS);
	}

	rinfo = hash_search(htab_rxid, gid, HASH_FIND, &found);
	if(found)
	{
		Assert(rinfo->type == RX_AUTO || rinfo->type == type);
		if(success)
		{
			pfree(rinfo->remote_nodes);
			hash_search(htab_rxid, gid, HASH_REMOVE, NULL);

			if (rinfo->is_cleanup)
			{
				if (IsGTMCnNode())
				{
					SnapSendTransactionFinish(pg_strtouint64(&rinfo->gid[1], NULL, 10), SNAP_XID_RXACT);
				}
				else
				{
					int snap_finish_option = rinfo->type == RX_COMMIT ? SNAP_XID_COMMIT : SNAP_XID_NONE;
					SnapRcvCommitTransactionId(pg_strtouint64(&rinfo->gid[1], NULL, 10), snap_finish_option | SNAP_XID_RXACT);
				}
			}
		}else
		{
			rinfo->failed = true;
			/* end of redo while change all RX_AUTO(function RxactMarkAutoTransaction) */
			if (!is_redo && rinfo->type == RX_AUTO)
			{
				if (TransactionIdDidCommit(rinfo->auto_tid))
					rinfo->type = RX_COMMIT;
				else if (TransactionIdDidAbort(rinfo->auto_tid))
					rinfo->type = RX_ROLLBACK;
				else
					RxactMarkAutoTransaction(rinfo);
			}
		}
	}else
	{
		if(!is_redo)
			ereport(ERROR, (errmsg("gid '%s' not exists", gid)));
	}
}

static void rxact_auto_gid(const char *gid, TransactionId txid, bool is_redo)
{
	RxactTransactionInfo *rinfo;
	bool found;
	if(gid == NULL || gid[0] == '\0')
		ereport(ERROR, (errmsg("invalid gid")));

	rinfo = hash_search(htab_rxid, gid, HASH_FIND, &found);
	if(!found)
	{
		if(!is_redo)
			ereport(ERROR, (errmsg("gid '%s' not exists", gid)));
		return; /* for redo */
	}
	if (rinfo->type != RX_PREPARE)
	{
		if(!is_redo)
			ereport(WARNING
				, (errmsg("change rxact \"%s\" is not prepared ", gid)));
	}else
	{
		if(!is_redo)
		{
			/* save to log file */
			resetStringInfo(&rxlf_xlog_buf);
			appendBinaryStringInfo(&rxlf_xlog_buf, (const char*)&txid, sizeof(txid));
			appendStringInfoString(&rxlf_xlog_buf, gid);
			rxact_xlog_insert(rxlf_xlog_buf.data, rxlf_xlog_buf.len+1, RXACT_MSG_AUTO);
		}
		rinfo->type = RX_AUTO;
	}
	rinfo->auto_tid = txid;
}

static void rxact_2pc_do(void)
{
	RxactTransactionInfo *rinfo;
	NodeConn *node_conn;
	HASH_SEQ_STATUS hstatus;
	StringInfoData buf;
	int i;
	bool cmd_is_ok;
	bool wait_block = true;
	bool in_transaction = false;
	MemoryContext oldcontext = CurrentMemoryContext;

	hash_seq_init(&hstatus, htab_rxid);
	buf.data = NULL;
	while((rinfo = hash_seq_search(&hstatus)) != NULL)
	{
#ifdef ADB_EXT
		if (rinfo->count_nodes == 0)
		{
			ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("prepared transaction with identifier \"%s\" does not exist int this node",
					rinfo->gid)));
		}
#else
		Assert(rinfo->count_nodes > 0);
#endif /* ADB_EXT */
		if(rinfo->failed == false)
			continue;

		if (rinfo->type == RX_AUTO)
		{
			if (TransactionIdDidCommit(rinfo->auto_tid))
				rinfo->type = RX_COMMIT;
			else if (TransactionIdDidAbort(rinfo->auto_tid))
				rinfo->type = RX_ROLLBACK;
			else
				RxactMarkAutoTransaction(rinfo);
		}
		
		if (rinfo->type == RX_AUTO)
			continue;

		cmd_is_ok = false;
		for(i=0;i<rinfo->count_nodes;++i)
		{
			/* skip successed node */
			if(rinfo->remote_success[i])
				continue;

			/* get node connection, skip if not connectiond */
			RXACT_START_TRANS(in_transaction, oldcontext, CurrentMemoryContext);
			node_conn = rxact_get_node_conn(rinfo->db_oid, rinfo->remote_nodes[i], time(NULL));
			if(node_conn == NULL || node_conn->conn == NULL || node_conn->doing_gid[0] != '\0')
			{
				/*
				* Avoid rxact blocking the wait event, 
				* resulting in two-phase transactions not being executed.
				*/
				wait_block = false;
				continue;
			}

			/* when SQL not maked, make it */
			if(cmd_is_ok == false)
			{
				rxact_build_2pc_cmd(&buf, rinfo->gid, rinfo->type);
				cmd_is_ok = true;
			}

			if(PQsendQuery(node_conn->conn, buf.data))
			{
				strcpy(node_conn->doing_gid, rinfo->gid);
				node_conn->last_use = time(NULL);
				ereport(RXACT_LOG_LEVEL, (errmsg("send \"%s\" to %u", buf.data, rinfo->remote_nodes[i])));
			}else
			{
				ereport(RXACT_LOG_LEVEL, (errmsg("send \"%s\" to %u %s", buf.data, rinfo->remote_nodes[i], "failed")));
				rxact_finish_node_conn(node_conn);
			}
		}
	}

	if (wait_block)
		waiting_time = -1L;
	else
		waiting_time = 1000L;

	RXACT_END_TRANS(in_transaction, oldcontext, CurrentMemoryContext);

	if(buf.data)
		pfree(buf.data);
}

static void rxact_2pc_result(NodeConn *conn)
{
	RxactTransactionInfo *rinfo;
	PGresult *res;
	ExecStatusType status;
	int i;
	bool finish;
	char gid[lengthof(conn->doing_gid)];
	Assert(conn->doing_gid[0] != '\0');

	res = PQgetResult(conn->conn);
	status = PQresultStatus(res);
	PQclear(res);
	conn->last_use = time(NULL);
	strcpy(gid, conn->doing_gid);
	conn->doing_gid[0] = '\0';
	if(status != PGRES_COMMAND_OK)
	{
		ereport(RXACT_LOG_LEVEL,
			(errmsg("gid '%s' from %u %s:%s"
				, gid
				, conn->oids.node_oid
				, "failed"
				, PQerrorMessage(conn->conn))));
		return;
	}
	ereport(RXACT_LOG_LEVEL,
			(errmsg("gid '%s' from %u %s"
				, gid
				, conn->oids.node_oid
				, "success")));
	rinfo = hash_search(htab_rxid, gid, HASH_FIND, NULL);
	Assert(rinfo != NULL && rinfo->failed == true);
	Assert(conn->oids.db_oid == rinfo->db_oid);
	finish = true;
	for(i=0;i<rinfo->count_nodes;++i)
	{
		if(rinfo->remote_nodes[i] == conn->oids.node_oid)
		{
			Assert(rinfo->remote_success[i] == false);
			rinfo->remote_success[i] = true;
			break;
		}
		if(rinfo->remote_success[i] == false)
			finish = false;
	}
	Assert(i < rinfo->count_nodes);

	if(finish)
	{
		for(++i;i<rinfo->count_nodes;++i)
		{
			if(rinfo->remote_success[i] == false)
			{
				finish = false;
				break;
			}
		}
		if(finish)
		{
			rxact_mark_gid(rinfo->gid, rinfo->type, true, false);
			ereport(RXACT_LOG_LEVEL, (errmsg("rxact mark '%s' %s %s"
				, rinfo->gid, RemoteXactType2String(rinfo->type), "success")));
		}
	}
	/* 
	 * When the datanode connection fails, adjust the epoll waiting time to 
	 * avoid that the unfinished two-phase transaction 
	 * cannot continue after the datanode recovery.
	 */
	if(finish)
		waiting_time = -1L;
	else
		waiting_time = 1000L;
}

static char* rxact_get_node_conn_str(Oid dboid, Oid nodeoid)
{
	StringInfoData	buf;
	HeapTuple		tupNode = NULL;
	HeapTuple		tupDB = NULL;
	HeapTuple		tupAuth = NULL;
	Form_pgxc_node	pgxc_node;
	Form_pg_database pg_db;
	Form_pg_authid	pg_authid;

	buf.data = NULL;
	tupNode = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeoid));
	if (!HeapTupleIsValid(tupNode))
		goto end_get_conn_;
	tupDB = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid));
	if (!HeapTupleIsValid(tupDB))
		goto end_get_conn_;

	pgxc_node = (Form_pgxc_node)GETSTRUCT(tupNode);
	pg_db = (Form_pg_database)GETSTRUCT(tupDB);
	tupAuth = SearchSysCache1(AUTHOID, ObjectIdGetDatum(pg_db->datdba));
	if (!HeapTupleIsValid(tupAuth))
		goto end_get_conn_;
	pg_authid = (Form_pg_authid)GETSTRUCT(tupAuth);

	initStringInfo(&buf);
	appendStringInfo(&buf, "host='%s' port=%u user='%s' dbname='%s'",
					 NameStr(pgxc_node->node_host),
					 pgxc_node->node_port,
					 NameStr(pg_authid->rolname),
					 NameStr(pg_db->datname));
	appendStringInfoString(&buf, " options='-c remotetype=rxactmgr'");

end_get_conn_:
	if (HeapTupleIsValid(tupAuth))
		ReleaseSysCache(tupAuth);
	if (HeapTupleIsValid(tupDB))
		ReleaseSysCache(tupDB);
	if (HeapTupleIsValid(tupNode))
		ReleaseSysCache(tupNode);
	return buf.data;
}

static NodeConn* rxact_get_node_conn(Oid db_oid, Oid node_oid, time_t cur_time)
{
	NodeConn *conn;
	DbAndNodeOid key;
	bool found;

	key.node_oid = node_oid;
	key.db_oid = db_oid;

	conn = hash_search(htab_node_conn, &key, HASH_ENTER, &found);
	if(!found)
	{
		conn->conn = NULL;
		conn->last_use = 0;
		conn->status = PGRES_POLLING_FAILED;
		conn->doing_gid[0] = '\0';
	}
	Assert(conn && conn->oids.node_oid == node_oid);
	Assert(conn->oids.db_oid == db_oid);

	if(conn->conn != NULL && PQstatus(conn->conn) == CONNECTION_BAD)
		rxact_finish_node_conn(conn);

	if(conn->last_use > cur_time)
		conn->last_use = (time_t)0;
	if (conn->conn == NULL &&
		conn->last_use < cur_time)
	{
		char *conn_str = rxact_get_node_conn_str(db_oid, node_oid);
		if (conn_str != NULL)
		{
			conn->conn = PQconnectStart(conn_str);
			conn->status = PGRES_POLLING_WRITING;
			pfree(conn_str);
		}
	}
	if(conn->status == PGRES_POLLING_OK)
		return conn;

	return rxact_check_node_conn(conn) ? conn:NULL;
}

static bool rxact_check_node_conn(NodeConn *conn)
{
	AssertArg(conn);
	if(conn->conn == NULL)
		return false;
	if(PQstatus(conn->conn) == CONNECTION_BAD)
	{
		rxact_finish_node_conn(conn);
		return false;
	}

re_poll_conn_:
	switch(conn->status)
	{
	case PGRES_POLLING_READING:
		if(wait_socket(PQsocket(conn->conn), false, false) == false)
			return false;
		break;
	case PGRES_POLLING_WRITING:
		if(wait_socket(PQsocket(conn->conn), true, false) == false)
			return false;
		break;
	case PGRES_POLLING_OK:
		conn->last_use = time(NULL);
		return true;
	case PGRES_POLLING_FAILED:
	case PGRES_POLLING_ACTIVE:	/* should be not happen */
		rxact_finish_node_conn(conn);
		return false;
	}
	conn->status = PQconnectPoll(conn->conn);
	goto re_poll_conn_;

	return false;
}

static void rxact_finish_node_conn(NodeConn *conn)
{
	AssertArg(conn);
	if(conn->conn != NULL)
	{
		PQfinish(conn->conn);
		conn->conn = NULL;
		conn->last_use = time(NULL);
	}
	conn->status = PGRES_POLLING_FAILED;
	conn->doing_gid[0] = '\0';
}

static void rxact_build_2pc_cmd(StringInfo cmd, const char *gid, RemoteXactType type)
{
	AssertArg(cmd);
	if(cmd->data == NULL)
		initStringInfo(cmd);
	else
		resetStringInfo(cmd);

	switch(type)
	{
	case RX_PREPARE:
	case RX_ROLLBACK:
		appendStringInfoString(cmd, "rollback");
		break;
	case RX_COMMIT:
		appendStringInfoString(cmd, "commit");
		break;
	case RX_AUTO:
	default:
		ereport(FATAL, (errmsg("error remote xact type %d", (int)type)));
		break;
	}
	appendStringInfoString(cmd, " prepared if exists '");
	appendStringInfoString(cmd, gid);
	appendStringInfoChar(cmd, '\'');
}

/*
 * close idle timeout connection node
 * and remove one closed connection(not include AGTM)
 */
static void rxact_close_timeout_remote_conn(time_t cur_time)
{
	NodeConn *node_conn;
	HASH_SEQ_STATUS seq_status;
	DbAndNodeOid key;
	bool hint;

	hint = false;
	hash_seq_init(&seq_status, htab_node_conn);
	while((node_conn = hash_seq_search(&seq_status)) != NULL)
	{
		if(node_conn->conn != NULL
			&& PQstatus(node_conn->conn) == CONNECTION_OK
			&& node_conn->doing_gid[0] == '\0'
			&& cur_time - node_conn->last_use >= REMOTE_IDLE_TIMEOUT)
		{
			rxact_finish_node_conn(node_conn);
		}
		if (hint == false &&
			node_conn->conn == NULL)
		{
			key = node_conn->oids;
			hint = true;
		}
	}
	if(hint)
	{
		hash_search(htab_node_conn, &key, HASH_REMOVE, NULL);
	}
}

static File rxact_log_open_file(const char *log_name, int fileFlags, int fileMode)
{
	File rfile;
	char file_name[MAX_RLOG_FILE_NAME];
	AssertArg(log_name);

	snprintf(file_name, sizeof(file_name), "%s/%s", rxlf_directory, log_name);
	rfile = PathNameOpenFilePerm(file_name, fileFlags, fileMode);
	if(rfile == -1)
	{
		const char *tmp = (fileFlags & O_CREAT) == O_CREAT ? " or create" : "";
		ereport(FATAL,
			(errcode_for_file_access(),
			errmsg("could not open%s file \"%s\":%m", tmp, file_name)));
	}
	return rfile;
}

static void rxact_xlog_insert(char *data, int len, uint8 info)
{
	XLogRecPtr xptr;
	XLogBeginInsert();
	XLogRegisterData(data, len);
	xptr = XLogInsert(RM_RXACT_MGR_ID, info);
	Assert(xptr > last_flush);
	/* Last position */
	last_flush = xptr;	
	if(!need_flush)
		need_flush = xptr;
}

static const char* RemoteXactType2String(RemoteXactType type)
{
	switch(type)
	{
	case RX_PREPARE:
		return "prepare";
	case RX_COMMIT:
		return "commit";
	case RX_ROLLBACK:
		return "rollback";
	case RX_AUTO:
		return "auto";
	}
	return "unknown";
}

/* ---------------------- interface for xlog ---------------------------------*/

void rxact_redo(XLogReaderState *record)
{
	const char *gid;
	RemoteXactType type;
	StringInfoData buf;
	int count;
	Oid db_oid;
	Oid *oids;
	TransactionId txid;

	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	buf.data = XLogRecGetData(record);
	buf.len = XLogRecGetDataLen(record);
	buf.cursor = 0;

	if (!adb_check_sync_nextid)
		return;

	switch(info)
	{
	case RXACT_MSG_DO:
		pq_copymsgbytes(&buf, (char*)&db_oid, sizeof(db_oid));
		type = (RemoteXactType)pq_getmsgbyte(&buf);
		pq_copymsgbytes(&buf, (char*)&count, sizeof(count));
		oids = (Oid*)pq_getmsgbytes(&buf, count*sizeof(oids[0]));
		gid = pq_getmsgstring(&buf);
		rxact_insert_gid(gid, oids, count, type, db_oid, true, false);
		break;
	case RXACT_MSG_SUCCESS:
		type = (RemoteXactType)pq_getmsgbyte(&buf);
		gid = pq_getmsgstring(&buf);
		rxact_mark_gid(gid, type, true, true);
		break;
	case RXACT_MSG_AUTO:
		pq_copymsgbytes(&buf, (char*)&txid, sizeof(txid));
		gid = pq_getmsgstring(&buf);
		rxact_auto_gid(gid, txid, true);
		break;
	default:
		ereport(PANIC,
			(errmsg("rxact_redo: unknown op code %u", info)));
	}
}

void rxact_xlog_startup(void)
{
	RemoteXactHtabInit();
	RxactLoadLog(false);
}

void rxact_xlog_cleanup(void)
{
	RxactSaveLog(false);
	DestroyRemoteConnHashTab();
}

void CheckPointRxact(int flags)
{
	StringInfoData buf;
	if(!IS_PGXC_COORDINATOR || !IsUnderPostmaster || (flags & CHECKPOINT_END_OF_RECOVERY))
		return;

	if (RecoveryInProgress())
		return;

	connect_rxact(false);
	Assert(rxact_client_fd != PGINVALID_SOCKET);

	rxact_begin_msg(&buf, RXACT_MSG_CHECKPOINT, false);
	rxact_put_int(&buf, flags, false);
	send_msg_to_rxact(&buf, false);

	recv_msg_from_rxact(&buf, false);
	pfree(buf.data);
}

/* ---------------------- interface for xact client --------------------------*/

bool RecordRemoteXact(const char *gid, Oid *nodes, int count, RemoteXactType type, bool no_error)
{
	StringInfoData buf;
	AssertArg(gid && gid[0] && count >= 0);
	AssertArg(RXACT_TYPE_IS_VALID(type));

	ereport(DEBUG1, (errmsg("[ADB_EXT]Record %s rxact %s", RemoteXactType2String(type), gid)));

	if(connect_rxact(no_error) == false)
		return false;
	Assert(rxact_client_fd != PGINVALID_SOCKET);

	buf.data = NULL;
	if (rxact_begin_msg(&buf, RXACT_MSG_DO, no_error) == false ||
		rxact_put_int(&buf, (int)type, no_error) == false ||
		rxact_put_int(&buf, count, no_error) == false)
		goto record_gid_failed_;
	if(count > 0)
	{
		AssertArg(nodes);
		if (rxact_put_bytes(&buf, nodes, sizeof(nodes[0]) * count, no_error) == false)
			goto record_gid_failed_;
	}
	if (rxact_put_string(&buf, gid, no_error) == false)
		goto record_gid_failed_;

	if (send_msg_to_rxact(&buf, no_error) == false ||
		recv_msg_from_rxact(&buf, no_error) == false)
	{
		DisconnectRemoteXact();
		goto record_gid_failed_;
	}
	pfree(buf.data);
	return true;

record_gid_failed_:
	if(buf.data)
		pfree(buf.data);
	return false;
}

bool RecordRemoteXactSuccess(const char *gid, RemoteXactType type, bool no_error)
{
	return record_rxact_status(gid, type, true, no_error);
}

bool RecordRemoteXactFailed(const char *gid, RemoteXactType type, bool no_error)
{
	return record_rxact_status(gid, type, false, no_error);
}

bool RecordRemoteXactAuto(const char *gid, TransactionId tid, bool no_error)
{
	StringInfoData buf;
	AssertArg(gid && gid[0]);

	ereport(DEBUG1, (errmsg("[ADB]Record rxact %s to auto", gid)));

	if(connect_rxact(no_error) == false)
		return false;
	Assert(rxact_client_fd != PGINVALID_SOCKET);

	buf.data = NULL;

	StaticAssertStmt(sizeof(tid) == sizeof(int),"");
	if (rxact_begin_msg(&buf, RXACT_MSG_AUTO, no_error) == false ||
		rxact_put_int(&buf, (int)tid, no_error) == false ||
		rxact_put_string(&buf, gid, no_error) == false ||
		send_msg_to_rxact(&buf, no_error) == false ||
		recv_msg_from_rxact(&buf, no_error) == false)
	{
		if(buf.data)
			pfree(buf.data);
		DisconnectRemoteXact();
		return false;
	}

	pfree(buf.data);
	return true;
}

static bool record_rxact_status(const char *gid, RemoteXactType type, bool success, bool no_error)
{
	StringInfoData buf;
	AssertArg(gid && gid[0] && RXACT_TYPE_IS_VALID(type));

	ereport(DEBUG1,
			(errmsg("[ADB]Record %s rxact %s %s",
					RemoteXactType2String(type), gid, success ? "SUCCESS" : "FAILED")));

	if(connect_rxact(no_error) == false)
		return false;
	Assert(rxact_client_fd != PGINVALID_SOCKET);

	buf.data = NULL;
	if (rxact_begin_msg(&buf, success ? RXACT_MSG_SUCCESS : RXACT_MSG_FAILED, no_error) == false ||
		rxact_put_int(&buf, (int)type, no_error) == false ||
		rxact_put_string(&buf, gid, no_error) == false ||
		send_msg_to_rxact(&buf, no_error) == false ||
		recv_msg_from_rxact(&buf, no_error) == false)
	{
		if(buf.data)
			pfree(buf.data);
		DisconnectRemoteXact();
		return false;
	}

	pfree(buf.data);
	return true;
}

static bool send_msg_to_rxact(StringInfo buf, bool no_error)
{
	ssize_t send_res;
	AssertArg(buf);

	Assert(rxact_client_fd != PGINVALID_SOCKET);

	if(rxact_put_finsh(buf, no_error) == false)
		return false;
	Assert(buf->len >= 5);

	HOLD_CANCEL_INTERRUPTS();

re_send_:
	Assert(buf->len > buf->cursor);
	send_res = send(rxact_client_fd,
					buf->data + buf->cursor,
					buf->len - buf->cursor, 0);
	CHECK_FOR_INTERRUPTS();
	if(send_res == 0)
	{
		if(no_error)
			goto send_msg_failed_;
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("Send message to RXACT manager close by remote")));
	}else if(send_res < 0)
	{
		if(IS_ERR_INTR())
			goto re_send_;
		if(no_error)
			goto send_msg_failed_;
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("Can not send message to RXACT manager:%m")));
	}
	buf->cursor += send_res;
	if(buf->len > buf->cursor)
	{
		wait_socket((pgsocket)rxact_client_fd, true, true);
		goto re_send_;
	}

	RESUME_CANCEL_INTERRUPTS();
	Assert(buf->cursor == buf->len);
	return true;

send_msg_failed_:
	RESUME_CANCEL_INTERRUPTS();
	DisconnectRemoteXact();
	return false;
}

static bool recv_msg_from_rxact(StringInfo buf, bool no_error)
{
	int len;
	uint8 msg_type;

	AssertArg(buf);
	Assert(rxact_client_fd != PGINVALID_SOCKET);
	resetStringInfo(buf);

	/* get message head */
	while(buf->len < 5)
	{
		wait_socket(rxact_client_fd, false, true);
		if(recv_socket(rxact_client_fd, buf, 5-buf->len, no_error) == false)
			goto recv_msg_failed_;
	}

	/* get full message data */
	len = *(int*)(buf->data);
	while(buf->len < len)
	{
		wait_socket(rxact_client_fd, false, true);
		if(recv_socket(rxact_client_fd, buf, len-buf->len, no_error) == false)
			goto recv_msg_failed_;
	}

	/* parse message */
	buf->cursor += 5;	/* 5 is sizeof(length) and message type */
	msg_type = buf->data[4];
	if(msg_type == RXACT_MSG_OK)
	{
		rxact_get_msg_end(buf);
		return true;
	}else if(msg_type == RXACT_MSG_ERROR)
	{
		if(no_error)
			return false;
		ereport(ERROR, (errmsg("error message from RXACT manager:%s", rxact_get_string(buf))));
	}else if(msg_type != RXACT_MSG_RUNNING)
	{
		if(no_error)
			goto recv_msg_failed_;
		ereport(ERROR, (errmsg("Unknown message type %d from RXACT manager", msg_type)));
	}
	return true;

recv_msg_failed_:
	DisconnectRemoteXact();
	return false;
}

static bool recv_socket(pgsocket sock, StringInfo buf, int max_recv, bool no_error)
{
	ssize_t recv_res;
	AssertArg(sock != PGINVALID_SOCKET && buf && max_recv > 0);

	if(rxact_enlarge_msg(buf, max_recv, no_error) == false)
		return false;

re_recv_:
	CHECK_FOR_INTERRUPTS();
	recv_res = recv(sock, buf->data + buf->len, max_recv, 0);
	if(recv_res == 0)
	{
		if(no_error)
			return false;
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("Recv message from RXACT manager close by remote")));
	}else if(recv_res < 0)
	{
		if(IS_ERR_INTR())
			goto re_recv_;
		if(no_error)
			return false;
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("Can recv message from RXACT manager:%m")));
	}
	buf->len += recv_res;
	return true;
}

static bool wait_socket(pgsocket sock, bool wait_send, bool block)
{
	int ret;
#ifdef HAVE_POLL
	struct pollfd poll_fd;
	poll_fd.fd = sock;
	poll_fd.events = (wait_send ? POLLOUT : POLLIN) | POLLERR;
	poll_fd.revents = 0;
re_poll_:
	CHECK_FOR_INTERRUPTS();
	ret = poll(&poll_fd, 1, block ? -1:0);
#else
	fd_set mask;
	struct timeval tv;
re_poll_:
	CHECK_FOR_INTERRUPTS();
	tv.tv_sec 0;
	tv.tv_usec = 0;
	FD_ZERO(&mask);
	FD_SET(sock, &mask);
	if(wait_send)
		ret = select(sock+1, NULL, &mask, NULL, block ? NULL:&tv);
	else
		ret = select(sock+1, &mask, NULL, NULL, block ? NULL:&tv);
#endif

	if(ret < 0)
	{
		if(IS_ERR_INTR())
			goto re_poll_;
		ereport(WARNING,
				(errcode_for_socket_access(),
				 errmsg("wait socket failed:%m")));
	}
	return ret == 0 ? false:true;
}

static bool connect_rxact(bool no_error)
{
	StringInfoData buf;
	bool need_send_db_info = false;

	if(rxact_client_fd == PGINVALID_SOCKET)
	{
		rxact_client_fd = rxact_connect();
		if(rxact_client_fd == PGINVALID_SOCKET)
		{
			if(no_error)
				return false;
			ereport(ERROR, (errcode_for_socket_access()
				, errmsg("Can not connect to RXACT manager:%m")));
		}
		need_send_db_info = true;
		sended_db_info = false;
	}

	buf.data = NULL;
	if (need_send_db_info)
	{
		if (IsTransactionState() &&
			OidIsValid(MyDatabaseId))
		{
			if(rxact_begin_db_info(&buf, MyDatabaseId, no_error) == false)
				goto connection_failed_;
			sended_db_info = true;
		}else
		{
			if(rxact_begin_db_info(&buf, InvalidOid, no_error) == false)
				goto connection_failed_;
			sended_db_info = false;
		}
	}else if(sended_db_info == false &&
			 IsTransactionState() &&
			 OidIsValid(MyDatabaseId))
	{
		if(rxact_begin_db_info(&buf, MyDatabaseId, no_error) == false)
			goto connection_failed_;
		sended_db_info = true;
	}

	if(buf.data)
	{
		if (send_msg_to_rxact(&buf, no_error) == false ||
			recv_msg_from_rxact(&buf, no_error) == false)
			goto connection_failed_;
		pfree(buf.data);
	}
	return true;

connection_failed_:
	if(buf.data)
		pfree(buf.data);
	DisconnectRemoteXact();
	return false;
}

static bool rxact_begin_db_info(StringInfo buf, Oid dboid, bool no_error)
{
	if (rxact_begin_msg(buf, RXACT_MSG_CONNECT, no_error) == false ||
		rxact_put_int(buf, dboid, no_error) == false)
		return false;

	if(OidIsValid(dboid))
	{
		HeapTuple tuple;
		Form_pg_database form_db;
		Form_pg_authid form_authid;
		Oid owner;
		/* put database name */
		tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid));
		Assert(HeapTupleIsValid(tuple));
		form_db = (Form_pg_database)GETSTRUCT(tuple);
		if (rxact_put_string(buf, NameStr(form_db->datname), no_error) == false)
		{
			ReleaseSysCache(tuple);
			return false;
		}
		owner = form_db->datdba;
		ReleaseSysCache(tuple);

		/* put Database owner name */
		tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner));
		Assert(HeapTupleIsValid(tuple));
		form_authid = (Form_pg_authid)GETSTRUCT(tuple);
		if (rxact_put_string(buf, NameStr(form_authid->rolname), no_error) == false)
		{
			ReleaseSysCache(tuple);
			return false;
		}
		ReleaseSysCache(tuple);
	}
	return true;
}

List *RxactGetRunningList(void)
{
	List *list;
	RxactTransactionInfo *info;
	StringInfoData buf;

	connect_rxact(false);
	Assert(rxact_client_fd != PGINVALID_SOCKET);

	rxact_begin_msg(&buf, RXACT_MSG_RUNNING, false);
	send_msg_to_rxact(&buf, false);

	recv_msg_from_rxact(&buf, false);

	list = NIL;
	while(buf.len > buf.cursor)
	{
		info = palloc(sizeof(*info));
		strncpy(info->gid, rxact_get_string(&buf), lengthof(info->gid));
		info->count_nodes = rxact_get_int(&buf);
		info->remote_nodes = palloc((info->count_nodes) *
			(sizeof(info->remote_nodes[0]) + sizeof(info->remote_success[0])) );
		rxact_copy_bytes(&buf, info->remote_nodes, info->count_nodes * sizeof(info->remote_nodes[0]));
		info->remote_success = (bool*)&(info->remote_nodes[info->count_nodes]);
		rxact_copy_bytes(&buf, info->remote_success, info->count_nodes * sizeof(info->remote_success[0]));
		StaticAssertStmt(sizeof(info->db_oid) == sizeof(int), "change code off get");
		info->db_oid = (Oid)rxact_get_int(&buf);
		info->type = (RemoteXactType)rxact_get_int(&buf);
		info->failed = rxact_get_short(&buf) ? true:false;
		list = lappend(list, info);
	}
	rxact_get_msg_end(&buf);
	pfree(buf.data);

	return list;
}

void FreeRxactTransactionInfo(RxactTransactionInfo *rinfo)
{
	if(rinfo)
	{
		if(rinfo->remote_nodes)
			pfree(rinfo->remote_nodes);
		pfree(rinfo);
	}
}

void FreeRxactTransactionInfoList(List *list)
{
	ListCell *lc;
	foreach(lc,list)
		FreeRxactTransactionInfo(lfirst(lc));
	list_free(list);
}

void DisconnectRemoteXact(void)
{
	if(rxact_client_fd != PGINVALID_SOCKET)
	{
		closesocket(rxact_client_fd);
		rxact_client_fd = PGINVALID_SOCKET;
		sended_db_info = false;
	}
}

bool RxactWaitGID(const char *gid, bool no_error)
{
	StringInfoData buf;

	if(gid == NULL || gid[0] == '\0')
		return false;
	if(strlen(gid) >= NAMEDATALEN)
	{
		if(no_error)
			return false;
		ereport(ERROR, (errmsg("argument \"%s\" too long", gid)));
	}

	if (connect_rxact(no_error) == false)
		return false;
	Assert(rxact_client_fd != PGINVALID_SOCKET);

	buf.data = NULL;
	if (rxact_begin_msg(&buf, RXACT_MSG_WAIT_GID, no_error) == false ||
		rxact_put_string(&buf, gid, no_error) == false ||
		send_msg_to_rxact(&buf, no_error) == false ||
		recv_msg_from_rxact(&buf, no_error) == false)
	{
		if(buf.data)
			pfree(buf.data);
		return false;
	}

	pfree(buf.data);
	return true;
}

static void RxactHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/* Listen new socket connected event */
void OnListenServerSocketConnEvent(WaitEvent *event)
{	
	pgsocket	agent_fd;

	for(;;)
	{
		agent_fd = accept(rxact_server_fd, NULL, NULL);
		if(agent_fd == PGINVALID_SOCKET)
		{
			if(errno != EWOULDBLOCK)
				ereport(WARNING, (errcode_for_socket_access()
					,errmsg("RXACT accept new connect failed:%m")));
			break;
		}
		AddRxactEventToSet(rxact_wait_event_set, T_Event_Agent, agent_fd, WL_SOCKET_READABLE, NULL);
	}
}

/** Description: 
 * 	Callback function for node connection.
 */
static void
OnListenNodeConnEvent(WaitEvent *event)
{
	NodeConn			*pconn;
	RxactWaitEventData	*user_data;

	if(event->events == 0)
		return;

	user_data = event->user_data;
	pconn = user_data->pconn;
	Assert(pconn != NULL);
	if(pconn->status != PGRES_POLLING_OK)
	{
		pconn->status = PQconnectPoll(pconn->conn);
		if(pconn->status == PGRES_POLLING_FAILED)
			RemoveRxactWaitEvent(rxact_wait_event_set, NULL, pconn);
	}else
	{
		Assert(pconn->doing_gid[0] != '\0');
		rxact_2pc_result(pconn);
	}
}

/** Description: 
 * 	Agent connection callback function.
 */
static void
OnListenAgentConnEvent(WaitEvent *event)
{	
	RxactWaitEventData	*user_data;
	RxactAgent			*agent;
	
	user_data = event->user_data;
	agent = user_data ->agent;
	if(event->events & WL_SOCKET_WRITEABLE)
	{
		Assert(agent->out_buf.len > agent->out_buf.cursor);
		rxact_agent_output(agent);
	}else if(event->events & (WL_SOCKET_READABLE | 0))
	{	
		if(agent->waiting_gid)
		{	
			rxact_agent_destroy(agent);
		}else
		{
			rxact_agent_input(agent);
			agent->need_flush =  need_flush;
			need_flush = 0;
		}
	}
}

/** Description: add a focused event to an event collection.
 * 	set:	event collection
 * 	type:	the corresponding callback function type
 * 	fd:		connection of interest
 * 	events:	events of interest
 */
static void
AddRxactEventToSet(WaitEventSet *set, WaiteEventTag type, pgsocket fd, uint32 events, NodeConn *pconn)
{	
	RxactWaitEventData *rxactEventData;
	RxactAgent			*agent;
	
	/* Enlarge wait event set */
	if(rxact_event_cur_count == rxact_event_max_count)
	{
		rxact_event_max_count += RXACT_WAIT_EVENT_ENLARGE_STEP;
		set = EnlargeWaitEventSet(set, rxact_event_max_count);
		rxact_wait_event = repalloc(rxact_wait_event, sizeof(WaitEvent) * rxact_event_max_count);
		/* update global EventSet pointer address */
		rxact_wait_event_set = set;
	}
	/* add sockent event to set */
	if(rxact_event_cur_count >= rxact_event_max_count)
		ereport(ERROR, (errmsg("No extra space to store new pgsocket.")));

	rxactEventData = (RxactWaitEventData *)MemoryContextAlloc(TopMemoryContext, sizeof(RxactWaitEventData));
	switch(type)
	{
		case T_Event_Agent:
			agent = CreateRxactAgent(fd);
			if(agent)
			{
				rxactEventData->type = T_Event_Agent;
				rxactEventData->agent = agent;
				rxactEventData->pconn = NULL;
				rxactEventData->pconn_fd_dup = PGINVALID_SOCKET;
				rxactEventData->fun = &OnListenAgentConnEvent;
				/* add wait event */
				rxactEventData->event_pos = AddWaitEventToSet(set,
																events,
																fd,
																NULL,
																(void*)rxactEventData);
			}
			break;
		case T_Event_Node:
			rxactEventData->type = T_Event_Node;
			rxactEventData->agent = NULL;
			rxactEventData->pconn = pconn;
			rxactEventData->pconn_fd_dup = dup(fd);
			rxactEventData->fun = &OnListenNodeConnEvent;
			rxactEventData->event_pos = AddWaitEventToSet(set,
															events,
															rxactEventData->pconn_fd_dup,
															NULL,
															(void*)rxactEventData);
			pconn->clean_gid[0] = '\0';
			break;
		case T_Event_Socket:
			rxactEventData->type = T_Event_Socket;
			rxactEventData->agent = NULL;
			rxactEventData->pconn = NULL;
			rxactEventData->pconn_fd_dup = PGINVALID_SOCKET;
			rxactEventData->fun = &OnListenServerSocketConnEvent;
			rxactEventData->event_pos = AddWaitEventToSet(set,
															events,
															fd,
															NULL,
															(void*)rxactEventData);
			break;
		case T_Event_Signal:
			rxactEventData->type = T_Event_Signal;
			rxactEventData->agent = NULL;
			rxactEventData->pconn = NULL;
			rxactEventData->pconn_fd_dup = PGINVALID_SOCKET;
			rxactEventData->fun = NULL;
			rxactEventData->event_pos = AddWaitEventToSet(set,
															events,
															fd,
															NULL,
															(void*)rxactEventData);
			break;
		default:
			ereport(ERROR,
				(errmsg("Unrecognized event tag: %d", type)));
			break;
	}
	++rxact_event_cur_count;
}

/** Description: remove obsolete connections from the event collection.
 * 	set:	event collection
 * 	fd:		deleted connection
 */
void RemoveRxactWaitEvent(WaitEventSet *set, RxactAgent *agent, NodeConn *pconn)
{
	int		pos;
	RxactWaitEventData	*rxactEventData;

	Assert(agent || pconn);
	for(pos = 1; pos < rxact_event_cur_count; ++pos)
	{	
		rxactEventData = (RxactWaitEventData *)GetWaitEventData(set, pos);
		if((agent && agent->sock == GetWaitEventSocket(set, pos)) || 
		   (pconn && pconn == rxactEventData->pconn))
			break;
	}
	
	Assert(pos <= rxact_event_cur_count);
	if(pos < rxact_event_cur_count)
	{
		RemoveWaitEvent(set, pos);
		if (agent)
			closesocket(agent->sock);
		if (pconn)
		{
			closesocket(rxactEventData->pconn_fd_dup);
			rxact_finish_node_conn(pconn);
		}
		--rxact_event_cur_count;
	}
}


static int
getNodeConnPos(NodeConn *checkConn)
{
	int pos;
	RxactWaitEventData	*rxactEventData;

	/* find pgsocket */
	for(pos = EXIT_MINIMUM_NUMBER -1; pos < rxact_event_cur_count; ++pos)
	{
		rxactEventData = (RxactWaitEventData *)GetWaitEventData(rxact_wait_event_set, pos);
		if(rxactEventData->pconn == checkConn &&
			rxactEventData->pconn_fd_dup == GetWaitEventSocket(rxact_wait_event_set, pos))
			return pos;
	}

	return 0;
}