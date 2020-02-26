
#include "postgres.h"

#include <poll.h>
#include <signal.h>
#include <time.h>
#include <math.h>
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "postmaster/postmaster.h"		/* For Unix_socket_directories */
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/varlena.h"
#include "utils/syscache.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "pgxc/pause.h"
#include "intercomm/inter-comm.h"

#define START_POOL_ALLOC	512
#define STEP_POLL_ALLOC		8

#ifndef PMGRLOG
#define PMGRLOG DEBUG5
#endif
#ifdef PMGR_LOG_USE_BT
#include <execinfo.h>
static int PMGR_BACKTRACE_DETIAL()
{
	void *addrs[5];
	char addr_str[lengthof(addrs)*((SIZEOF_VOID_P*2)+1)]={'\0'};
	int n = backtrace(addrs, lengthof(addrs));
	while(n--)
		sprintf(&addr_str[strlen(addr_str)],
				"%p%c", addrs[n], n ? ',':'\0');
	errdetail("%s", addr_str);
	return 0;
}
#else
#define PMGR_BACKTRACE_DETIAL() 0
#endif

#define PM_MSG_ABORT_TRANSACTIONS	'a'
#define PM_MSG_SEND_LOCAL_COMMAND	'b'
#define PM_MSG_CONNECT				'c'
#define PM_MSG_DISCONNECT			'd'
#define PM_MSG_CLEAN_CONNECT		'f'
#define PM_MSG_GET_CONNECT			'g'
#define PM_MSG_RELEASE_CONNECT		'r'
#define PM_MSG_SET_COMMAND			's'
#define PM_MSG_CLOSE_CONNECT		'C'
#define PM_MSG_ERROR				'E'
#define PM_MSG_CLOSE_IDLE_CONNECT	'S'
#define PM_MSG_GET_DBINFO_CONNECT	'T'

typedef enum SlotStateType
{
	 SLOT_STATE_UNINIT = 0
	,SLOT_STATE_IDLE
	,SLOT_STATE_LOCKED
	,SLOT_STATE_RELEASED
	,SLOT_STATE_CONNECTING				/* connecting remote */
	,SLOT_STATE_ERROR					/* got an error message */

	,SLOT_STATE_QUERY_AGTM_PORT			/* sended agtm port */
	,SLOT_STATE_END_AGTM_PORT = SLOT_STATE_QUERY_AGTM_PORT+1			/* recved agtm port */

	,SLOT_STATE_QUERY_PARAMS_SESSION	/* sended session params */
	,SLOT_STATE_END_PARAMS_SESSION = SLOT_STATE_QUERY_PARAMS_SESSION+1

	,SLOT_STATE_QUERY_PARAMS_LOCAL		/* sended local params */
	,SLOT_STATE_END_PARAMS_LOCAL = SLOT_STATE_QUERY_PARAMS_LOCAL+1

	,SLOT_STATE_QUERY_RESET_ALL			/* sended "reset all" */
	,SLOT_STATE_END_RESET_ALL = SLOT_STATE_QUERY_RESET_ALL+1
}SlotStateType;

typedef enum SlotCurrentList
{
	UNINIT_SLOT = 0,
	IDLE_SLOT,
	RELEASED_SLOT,
	BUSY_SLOT,
	NULL_SLOT
} SlotCurrentList;

#define PFREE_SAFE(p)				\
	do{								\
		void *p_ = (p);				\
		if(p_)	pfree(p_);			\
	}while(0)

#define INIT_PARAMS_MAGIC(agent_, member)	((agent_)->member = 0)
#define INIT_SLOT_PARAMS_MAGIC(slot_, member) ((slot_)->member = 0)
#define UPDATE_PARAMS_MAGIC(agent_, member)	(++((agent_)->member))
#define COPY_PARAMS_MAGIC(dest_, src_)		((dest_) = (src_))
#define EQUAL_PARAMS_MAGIC(l_, r_)			((l_) == (r_))

#if	0
#include <execinfo.h>
#define SET_SLOT_OWNER(slot_, owner_)									\
	do{																	\
		void *addrs[5];													\
		char addr_str[lengthof(addrs)*((SIZEOF_VOID_P*2)+1)]={'\0'};	\
		int n_ = backtrace(addrs, lengthof(addrs));						\
		while(n_--)														\
			sprintf(&addr_str[strlen(addr_str)],						\
					"%p%c", addrs[n_], n_ ? ',':'\0');					\
		printf("slot %p owner from %p set to %p:%d:%s\n",				\
			   slot_, slot_->owner, owner_, __LINE__,addr_str);			\
		fflush(stdout);													\
		slot_->owner = owner_;											\
	}while(0)
#define SET_SLOT_LIST(slot_, list_)										\
	do{																	\
		void *addrs[5];													\
		char addr_str[lengthof(addrs)*((SIZEOF_VOID_P*2)+1)]={'\0'};	\
		int n_ = backtrace(addrs, lengthof(addrs));						\
		while(n_--)														\
			sprintf(&addr_str[strlen(addr_str)],						\
					"%p%c", addrs[n_], n_ ? ',':'\0');					\
		printf("slot %p list from %d set to %d:%d:%s\n",				\
			   slot_, slot_->current_list, list_, __LINE__,addr_str);	\
		fflush(stdout);													\
		slot_->current_list = list_;									\
	}while(0)
#else
#define SET_SLOT_OWNER(slot_, owner_)	(slot_->owner = owner_)
#define SET_SLOT_LIST(slot_, list_)		(slot_->current_list = list_)
#endif

/* Connection pool entry */
typedef struct ADBNodePoolSlot
{
	dlist_node			dnode;
	PGconn				*conn;
	struct ADBNodePool	*parent;
	struct PoolAgent	*owner;				/* using bye */
	char				*last_error;		/* palloc in PoolerMemoryContext */
	time_t				released_time;
	PostgresPollingStatusType
						poll_state;			/* when state equal SLOT_BUSY_CONNECTING
											 * this value is valid */
	SlotStateType		slot_state;			/* SLOT_BUSY_*, valid when in ADBNodePool::busy_slot */
	int					last_user_pid;
	int					last_agtm_port;		/* last send agtm port */
	bool				has_temp;			/* have temp object? */
	int					retry;				/* try to reconnect times, at most three times */
	int64				last_retry_time;	/* last retry time, in order to do backoff time wait retry */
	uint32				session_magic;		/* sended session params magic number */
	uint32				local_magic;		/* sended local params magic number */
	SlotCurrentList		current_list;
} ADBNodePoolSlot;

typedef struct HostInfo
{
	char	   *hostname;
	uint16		port;
}HostInfo;

/* Pool of connections to specified pgxc node */
typedef struct ADBNodePool
{
	HostInfo	hostinfo;	/* Node Oid related to this pool */
	dlist_head	uninit_slot;
	dlist_head	released_slot;
	dlist_head	idle_slot;
	dlist_head	busy_slot;
	char	   *connstr;
	Size		last_idle;
	struct DatabasePool *parent;
} ADBNodePool;

typedef struct DatabaseInfo
{
	char	   *database;
	char	   *user_name;
	char	   *pgoptions;		/* Connection options */
}DatabaseInfo;

/* All pools for specified database */
typedef struct DatabasePool
{
	DatabaseInfo	db_info;
	HTAB		   *htab_nodes; 		/* Hashtable of ADBNodePool, one entry for each
										 * Coordinator or DataNode */
} DatabasePool;

typedef struct ConnectedInfo
{
	HostInfo			info;
	ADBNodePoolSlot	   *slot;
}ConnectedInfo;

/*
 * Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 * and managing session parameters
 */
typedef struct PoolAgent
{
	/* communication channel */
	PoolPort		port;
	DatabasePool   *db_pool;
	HTAB		   *connected_node;	/* ConnectedInfo */
	char		   *session_params;
	char		   *local_params;
	uint32			session_magic;	/* magic number for session_params */
	uint32			local_magic;	/* magic number for local_params */
	List		   *list_wait;		/* List of ADBNodePoolSlot in connecting */
	MemoryContext	mctx;
	/* Process ID of postmaster child process associated to pool agent */
	int				pid;
	int				agtm_port;
	bool			is_temp; /* Temporary objects used for this pool session? */
} PoolAgent;

struct PoolHandle
{
	/* communication channel */
	PoolPort	port;
};

/* Configuration options */
int			MinPoolSize = 1;
int			MaxPoolSize = 100;
int			PoolRemoteCmdTimeout = 0;

bool		PersistentConnections = false;

/* pool time out */
extern int pool_time_out;
extern int pool_release_to_idle_timeout;

/* connect retry times */
int 		RetryTimes = 3;

/* receive sigquit signal */
volatile bool signal_quit = false;

/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

/* The root memory context */
static MemoryContext PoolerMemoryContext;

/* PoolAgents */
static volatile Size	agentCount;
static Size max_agent_count;
static PoolAgent **poolAgents;

static PoolHandle *poolHandle = NULL;

static int	is_pool_locked = false;
static pgsocket server_fd = PGINVALID_SOCKET;
static volatile sig_atomic_t got_SIGHUP = false;

/* Signal handlers */

static void pooler_quickdie(SIGNAL_ARGS);
static void pooler_sighup(SIGNAL_ARGS);
static void PoolerLoop(void) __attribute__((noreturn));

static void agent_handle_input(PoolAgent * agent, StringInfo s);
static void agent_handle_output(PoolAgent *agent);
static void agent_destroy(PoolAgent *agent);
static void agent_error_hook(void *arg);
static void agent_check_waiting_slot(PoolAgent *agent);
static bool pool_recv_data(PoolPort *port);
static bool agent_has_completion_msg(PoolAgent *agent, StringInfo msg, int *msg_type);
static int *abort_pids(int *count, int pid, const char *database, const char *user_name);
static int clean_connection(StringInfo msg);
static bool check_slot_status(ADBNodePoolSlot *slot);

static void agent_create(volatile pgsocket new_fd);
static void agent_release_connections(PoolAgent *agent, bool force_destroy);
static void agent_idle_connections(PoolAgent *agent, bool force_destroy);
static void process_slot_event(ADBNodePoolSlot *slot);
static void save_slot_error(ADBNodePoolSlot *slot);
static bool get_slot_result(ADBNodePoolSlot *slot);
static void agent_acquire_connections(PoolAgent *agent, StringInfo msg);
static int agent_session_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type, StringInfo errMsg);
static int send_local_commands(PoolAgent *agent, StringInfo msg);

static void destroy_slot(ADBNodePoolSlot *slot, bool send_cancel);
static void release_slot(ADBNodePoolSlot *slot, bool force_close);
static void idle_slot(ADBNodePoolSlot *slot, bool reset);
static void destroy_node_pool(ADBNodePool *node_pool, bool bfree);
static bool node_pool_in_using(ADBNodePool *node_pool);
static time_t close_timeout_idle_slots(time_t cur_time);
static time_t idle_timeout_released_slots(time_t cur_time);
static bool pool_exec_set_query(PGconn *conn, const char *query, StringInfo errMsg);
static int pool_wait_pq(PGconn *conn);
static int pq_custom_msg(PGconn *conn, char id, int msgLength);
static void close_idle_connection(void);
static void check_idle_slot(void);

#ifdef WITH_RDMA
static bool pool_getstringdata(PoolPort *port, StringInfo msg);
#endif

/* for hash DatabasePool */
static HTAB *htab_database;
static uint32 hash_any_v(const void *key, int keysize, ...);
static uint32 hash_database_info(const void *key, Size keysize);
static int match_database_info(const void *key1, const void *key2, Size keysize);
static uint32 hash_host_info(const void *key, Size keysize);
static int match_host_info(const void *key1, const void *key2, Size keysize);
static void create_htab_database(void);
static void destroy_htab_database(void);
static DatabasePool *get_database_pool(const char *database, const char *user_name, const char *pgoptions);
static void destroy_database_pool(DatabasePool *db_pool, bool bfree);

static void pool_end_flush_msg(PoolPort *port, StringInfo buf);
static void pool_sendstring(StringInfo buf, const char *str);
static const char *pool_getstring(StringInfo buf);
static void pool_sendint(StringInfo buf, int ival);
static int pool_getint(StringInfo buf);
static void on_exit_pooler(int code, Datum arg);
/*
 * usage:
 *  HostInfo info;
 *  foreach_recv_hostinfo(info, msg)
 *  {
 *     same code
 *  }end_recv_hostinfo(&info, msg)
 */
#define foreach_recv_hostinfo(host_, buf_)							\
		{															\
			int count_ = pool_getint(buf_);							\
			while(count_-- > 0)										\
			{														\
				(host_).hostname = (char*) pool_getstring(buf_);	\
				(host_).port = (uint16) pool_getint(buf_);
#define end_recv_hostinfo(host_, buf_)								\
			}														\
		}
static void send_host_info(StringInfo buf, List *oidlist);
static List* recv_host_info(StringInfo buf, bool dup_str);

/* check slot state */
#if 0
static void check_all_slot_list(void)
{
	HASH_SEQ_STATUS hash_database_stats;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	ADBNodePool *node_pool;
	ADBNodePoolSlot *slot;
	dlist_iter iter;

	if(htab_database == NULL)
		return;

	hash_seq_init(&hash_database_stats, htab_database);
	while((db_pool = hash_seq_search(&hash_database_stats)) != NULL)
	{
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			dlist_foreach(iter, &node_pool->uninit_slot)
			{
				slot = dlist_container(ADBNodePoolSlot, dnode, iter.cur);
				AssertState(slot->slot_state == SLOT_STATE_UNINIT);
			}

			dlist_foreach(iter, &node_pool->released_slot)
			{
				slot = dlist_container(ADBNodePoolSlot, dnode, iter.cur);
				AssertState(slot->slot_state == SLOT_STATE_RELEASED);
			}

			dlist_foreach(iter, &node_pool->idle_slot)
			{
				slot = dlist_container(ADBNodePoolSlot, dnode, iter.cur);
				AssertState(slot->slot_state == SLOT_STATE_IDLE);
			}
		}
	}
}
#else
#define check_all_slot_list() ((void)0)
#endif

void PGXCPoolerProcessIam(void)
{
	am_pgxc_pooler = true;
}

bool IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	/* set it to NULL well exit wen has an error */
	PG_exception_stack = NULL;

	elog(DEBUG1, "Pooler process is started: %d", getpid());

	/*
	 * Set up memory contexts for the pooler objects
	 */
	PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												"PoolerMemoryContext",
												ALLOCSET_DEFAULT_SIZES);

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, pooler_quickdie);
//	pqsignal(SIGQUIT, SIG_IGN);
	pqsignal(SIGHUP, pooler_sighup);
	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/* Allocate pooler structures in the Pooler context */
	MemoryContextSwitchTo(PoolerMemoryContext);

	max_agent_count = (MaxConnections << 1) + max_worker_processes;
	poolAgents = (PoolAgent **) palloc(max_agent_count * sizeof(PoolAgent *));
	agentCount = 0;

	create_htab_database();

	PoolerLoop();	/* should never return */
	proc_exit(1);
}

static void PoolerLoop(void)
{
	MemoryContext volatile context;
	volatile Size poll_max;
	Size i,count,poll_count;
	struct pollfd * volatile poll_fd;
	struct pollfd *pollfd_tmp;
	PoolAgent *agent;
	List *polling_slot;		/* polling slot */
	ListCell *lc;
	DatabasePool *db_pool;
	ADBNodePool *nodes_pool;
	ADBNodePoolSlot *slot;
	dlist_mutable_iter miter;
	HASH_SEQ_STATUS hseq1,hseq2;
	sigjmp_buf	local_sigjmp_buf;
	time_t next_close_idle_time, next_idle_released_time, cur_time;
	StringInfoData input_msg;
	int rval;
	pgsocket new_socket;

	server_fd = pool_listen();
	if(server_fd == PGINVALID_SOCKET)
	{
		ereport(PANIC, (errcode_for_socket_access(),
			errmsg("Can not listen pool manager unix socket on %s:%m", pool_get_sock_path())));
	}
	if(!pg_set_noblock(server_fd))
	{
		ereport(PANIC,
				(errcode_for_socket_access(),
				 errmsg("could not set pool manager listen socket to nonblocking mode: %m")));
	}

	poll_max = START_POOL_ALLOC;
	poll_fd = palloc(START_POOL_ALLOC * sizeof(struct pollfd));
	initStringInfo(&input_msg);
	context = AllocSetContextCreate(CurrentMemoryContext,
									"PoolerMemoryContext",
									ALLOCSET_DEFAULT_SIZES);
	poll_fd[0].fd = server_fd;
	poll_fd[0].events = POLLIN;
	for(i=1;i<START_POOL_ALLOC;++i)
	{
		poll_fd[i].fd = PGINVALID_SOCKET;
		poll_fd[i].events = POLLIN | POLLPRI | POLLRDNORM | POLLRDBAND;
	}
	on_proc_exit(on_exit_pooler, (Datum)0);
	cur_time = time(NULL);
	next_close_idle_time = cur_time + pool_time_out;
	next_idle_released_time = cur_time + pool_release_to_idle_timeout;

	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Cleanup something */
		EmitErrorReport();
		FlushErrorState();
		AtEOXact_HashTables(false);
		error_context_stack = NULL;
	}
	PG_exception_stack = &local_sigjmp_buf;
	(void)MemoryContextSwitchTo(context);

	for(;;)
	{
		MemoryContextResetAndDeleteChildren(context);

		if(!PostmasterIsAlive())
			proc_exit(1);

		/* receive signal_quit and all agents had destory.exit poolmgr */
		if (signal_quit && 0 == agentCount)
			proc_exit(0);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
			next_close_idle_time = cur_time;
			next_idle_released_time = cur_time;
		}

		for(i=agentCount;i--;)
			agent_check_waiting_slot(poolAgents[i]);

		poll_count = 1;	/* listen socket */
		for(i=0;i<agentCount;++i)
		{
			rval = 0;	/* temp use rval for poll event */
			agent = poolAgents[i];
			if(agent->list_wait == NIL)
			{
				/* when agent waiting connect remote
				 * we just wait agent data
				 * if poll result it can recv we consider is closed
				 */
				rval = POLLIN;
			}else if(agent->port.SendPointer > 0)
			{
				rval = POLLOUT;
			}else
			{
				rval = POLLIN;
			}
			if(poll_count == poll_max)
			{
				poll_fd = repalloc(poll_fd, (poll_max+STEP_POLL_ALLOC)*sizeof(*poll_fd));
				poll_max += STEP_POLL_ALLOC;
			}
			Assert(poll_count < poll_max);
			poll_fd[poll_count].fd = Socket(agent->port);
			poll_fd[poll_count].events = POLLERR|POLLHUP|rval;
			++poll_count;
		}

		/* poll busy slots */
		polling_slot = NIL;
		hash_seq_init(&hseq1, htab_database);
		while((db_pool = hash_seq_search(&hseq1)) != NULL)
		{
			hash_seq_init(&hseq2, db_pool->htab_nodes);
			while((nodes_pool = hash_seq_search(&hseq2)) != NULL)
			{
				dlist_foreach_modify(miter, &(nodes_pool->busy_slot))
				{
					slot = dlist_container(ADBNodePoolSlot, dnode, miter.cur);
					rval = 0;	/* temp use rval for poll event */
					switch(slot->slot_state)
					{
					case SLOT_STATE_CONNECTING:
						if(slot->poll_state == PGRES_POLLING_READING)
							rval = POLLIN;
						else if(slot->poll_state == PGRES_POLLING_WRITING)
							rval = POLLOUT;
						break;
					case SLOT_STATE_QUERY_AGTM_PORT:
					case SLOT_STATE_QUERY_PARAMS_SESSION:
					case SLOT_STATE_QUERY_PARAMS_LOCAL:
					case SLOT_STATE_QUERY_RESET_ALL:
						rval = POLLIN;
						break;
					case SLOT_STATE_ERROR:
						if(PQisBusy(slot->conn))
						{
							rval = POLLIN;
						}else if(slot->owner == NULL)
						{
							dlist_delete(&slot->dnode);
							SET_SLOT_LIST(slot, NULL_SLOT);
							destroy_slot(slot, false);
							continue;
						}
						break;
					case SLOT_STATE_IDLE:
					case SLOT_STATE_END_AGTM_PORT:
					case SLOT_STATE_END_PARAMS_SESSION:
					case SLOT_STATE_END_PARAMS_LOCAL:
					case SLOT_STATE_END_RESET_ALL:
						if (slot->owner == NULL)
						{
							dlist_delete(&slot->dnode);
							SET_SLOT_LIST(slot, NULL_SLOT);
							if (slot->slot_state == SLOT_STATE_IDLE)
								idle_slot(slot, false);
							else
								destroy_slot(slot, false);
							continue;
						}
						break;
					default:
						break;
					}
					ereport(PMGRLOG,
							(errmsg("test busy slot %p fd %d state %d owner %p pid %d pool(%d)",
									slot, PQsocket(slot->conn), slot->slot_state, slot->owner,
									slot->owner ? slot->owner->pid:0, rval)));
					if(rval != 0)
					{
						if(poll_count == poll_max)
						{
							poll_fd = repalloc(poll_fd, (poll_max+STEP_POLL_ALLOC)*sizeof(*poll_fd));
							poll_max += STEP_POLL_ALLOC;
						}
						Assert(poll_count < poll_max);
						poll_fd[poll_count].fd = PQsocket(slot->conn);
						poll_fd[poll_count].events = POLLERR|POLLHUP|rval;
						++poll_count;
						polling_slot = lappend(polling_slot, slot);
					}
				}
			}
		}

		rval = poll(poll_fd, poll_count, 1000);
		CHECK_FOR_INTERRUPTS();
		if(rval < 0)
		{
			if(errno == EINTR
#if defined(EAGAIN) && EAGAIN != EINTR
				|| errno == EAGAIN
#endif
			)
			{
				continue;
			}
			ereport(PANIC, (errcode_for_socket_access(),
				errmsg("pool failed(%d) in pooler process, error %m", rval)));
		}else if(rval == 0)
		{
			/* nothing to do */
		}

		/* processed socket is 0 */
		count = 0;

		/* process busy slot first */
		pollfd_tmp = &(poll_fd[1]);	/* skip liten socket */
		for(lc=list_head(polling_slot);lc && count < (Size)rval;lc=lnext(lc))
		{
			pgsocket sock;
			slot = lfirst(lc);
			sock = PQsocket(slot->conn);
			/* find pollfd */
			for(;;)
			{
				Assert(pollfd_tmp < &(poll_fd[poll_max]));
				if(pollfd_tmp->fd == sock)
					break;
				pollfd_tmp = &(pollfd_tmp[1]);
			}
			Assert(sock == pollfd_tmp->fd);
			if(pollfd_tmp->revents != 0)
				process_slot_event(slot);
		}
		/* don't need pfree polling_slot */

		for(i=agentCount; i > 0 && count < (Size)rval;)
		{
			pollfd_tmp = &(poll_fd[i]);
			--i;
			agent = poolAgents[i];
			Assert(pollfd_tmp->fd == Socket(agent->port));
			if(pollfd_tmp->revents == 0)
			{
				continue;
			}else if(pollfd_tmp->revents & POLLIN)
			{
				if(agent->list_wait != NIL)
					agent_destroy(agent);
				else
					agent_handle_input(agent, &input_msg);
			}else
			{
				agent_handle_output(agent);
			}
			++count;
		}

		if(poll_fd[0].revents & POLLIN)
		{
			/*
			   when agentCount==max_agent_count some agent should closed,
			   we need process agent message, accept new socket next time
			 */
			while(agentCount<max_agent_count)
			{
				new_socket = accept(server_fd, NULL, NULL);

				if(new_socket == PGINVALID_SOCKET)
					break;
				else
				{
					/* receive signal quit, close new connection */
					if (signal_quit)
					{
						closesocket(new_socket);
						continue;
					}
					else
						agent_create(new_socket);
				}
			}
		}

		cur_time = time(NULL);
		/* close timeout idle slot(s) */
		if(cur_time >= next_close_idle_time)
			next_close_idle_time = close_timeout_idle_slots(cur_time);

		/* idle timeout released slot(s) */
		if (pool_release_to_idle_timeout > 0 &&
			cur_time >= next_idle_released_time)
			next_idle_released_time = idle_timeout_released_slots(cur_time);
	}
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
	if (PoolerMemoryContext)
	{
		MemoryContextDelete(PoolerMemoryContext);
		PoolerMemoryContext = NULL;
	}

	return 0;
}


/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 */
PoolHandle *
GetPoolManagerHandle(void)
{
	PoolHandle *handle;
	int			fdsock;

	/* Connect to the pooler */
	fdsock = pool_connect();
	if (fdsock < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("failed to connect to pool manager: %m")));
	}

	/* Allocate handle */
	PG_TRY();
	{
		handle = MemoryContextAlloc(TopMemoryContext, sizeof(*handle));

		handle->port.fdsock = fdsock;
		handle->port.RecvLength = 0;
		handle->port.RecvPointer = 0;
		handle->port.SendPointer = 0;
	}PG_CATCH();
	{
		closesocket(fdsock);
		PG_RE_THROW();
	}PG_END_TRY();

	return handle;
}


/*
 * Close handle
 */
void
PoolManagerCloseHandle(PoolHandle *handle)
{
	closesocket(Socket(handle->port));
	pfree(handle);
}


/*
 * Create agent
 */
static void agent_create(volatile pgsocket new_fd)
{
	PoolAgent  * agent;
	MemoryContext volatile context = NULL;

	AssertArg(new_fd != PGINVALID_SOCKET);
	Assert(agentCount < max_agent_count);

	PG_TRY();
	{
		HASHCTL ctl;

		/* Allocate MemoryContext */
		context = AllocSetContextCreate(PoolerMemoryContext,
										"PoolAgent",
										ALLOCSET_DEFAULT_SIZES);
		agent = MemoryContextAllocZero(context, sizeof(*agent));
		agent->port.fdsock = new_fd;
		agent->mctx = context;
		INIT_PARAMS_MAGIC(agent, session_magic);
		INIT_PARAMS_MAGIC(agent, local_magic);

		ereport(PMGRLOG,
				(errmsg("accept a new agent %p socket %d", agent, new_fd)));

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(HostInfo);
		ctl.entrysize = sizeof(ConnectedInfo);
		ctl.hash = hash_host_info;
		ctl.match = match_host_info;
		ctl.hcxt = context;
		agent->connected_node = hash_create("agent connected slot",
											1024,
											&ctl,
											HASH_ELEM|HASH_FUNCTION|HASH_COMPARE|HASH_CONTEXT);

	}PG_CATCH();
	{
		closesocket(new_fd);
		if(context)
			MemoryContextDelete(context);
		PG_RE_THROW();
	}PG_END_TRY();

	/* Append new agent to the list */
	poolAgents[agentCount++] = agent;
}

/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char *session_options(void)
{
	int				 i;
	const char		*pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle"};
	StringInfoData	 options;
	List			*value_list;
	char			*value;
	const char		*tmp;
	ListCell		*lc;

	initStringInfo(&options);

	/* first add "lc_monetary" */
	appendStringInfoString(&options, " -c lc_monetary=");
	appendStringInfoString(&options, GetConfigOptionResetString("lc_monetary"));

	/* add other options */
	for (i = 0; i < lengthof(pgoptions); i++)
	{
		appendStringInfo(&options, " -c %s=", pgoptions[i]);

		value = pstrdup(GetConfigOptionResetString(pgoptions[i]));

		SplitIdentifierString(value, ',', &value_list);
		tmp = "";
		foreach(lc, value_list)
		{
			appendStringInfoString(&options, tmp);
			appendStringInfoString(&options, lfirst(lc));
			tmp = ",";
		}
		list_free(value_list);
		pfree(value);
	}

	return options.data;
}

/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void
PoolManagerConnect(PoolHandle *handle,
	               const char *database, const char *user_name,
	               const char *pgoptions)
{
	StringInfoData buf;
	AssertArg(handle && database && user_name);

	/* save the handle */
	poolHandle = handle;

	pq_beginmessage(&buf, PM_MSG_CONNECT);

	/* PID number */
	pq_sendbytes(&buf, (char*)&MyProcPid, sizeof(MyProcPid));
	pool_sendstring(&buf, database);
	pool_sendstring(&buf, user_name);
	pool_sendstring(&buf, pgoptions);

	pool_end_flush_msg(&(handle->port), &buf);
}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
	PoolHandle *handle;
	char *dbname;
	char *username;
	char *options = session_options();

	if (poolHandle)
	{
		PoolManagerDisconnect();
	}

	handle = GetPoolManagerHandle();
	PoolManagerConnect(handle,
					   (dbname=get_database_name(MyDatabaseId)),
					   (username=GetUserNameFromId(GetUserId(), false)),
					   options);
	pfree(username);
	pfree(dbname);
	pfree(options);
}

int
PoolManagerSetCommand(PoolCommandType command_type, const char *set_command)
{
	StringInfoData buf;
	int res = 0;

	if (poolHandle)
	{
		InterXactCacheAll(GetCurrentInterXactState());

		pq_beginmessage(&buf, PM_MSG_SET_COMMAND);

		/*
		 * If SET LOCAL is in use, flag current transaction as using
		 * transaction-block related parameters with pooler agent.
		 */
		if (command_type == POOL_CMD_LOCAL_SET)
			SetCurrentLocalParamStatus(true);

		/* LOCAL or SESSION parameter ? */
		pool_sendint(&buf, command_type);

		pool_sendstring(&buf, set_command);

		pool_end_flush_msg(&(poolHandle->port), &buf);

		/* Get result */
		res = pool_recvres(&poolHandle->port);
	}
	return res == 0 ? 0:-1;
}

/*
 * Send commands to alter the behavior of current transaction and update begin sent status
 */
int
PoolManagerSendLocalCommand(int dn_count, int* dn_list, int co_count, int* co_list)
{
	StringInfoData buf;

	if (poolHandle == NULL)
		return EOF;

	if (dn_count == 0 && co_count == 0)
		return EOF;

	if (dn_count != 0 && dn_list == NULL)
		return EOF;

	if (co_count != 0 && co_list == NULL)
		return EOF;

	pq_beginmessage(&buf, PM_MSG_SEND_LOCAL_COMMAND);

	pq_sendbytes(&buf, (char*)&dn_count, sizeof(dn_count));
	pq_sendbytes(&buf, (char*)dn_list, sizeof(int)*dn_count);
	pq_sendbytes(&buf, (char*)&co_count, sizeof(co_count));
	pq_sendbytes(&buf, (char*)co_list, sizeof(int)*co_count);

	pool_end_flush_msg(&(poolHandle->port), &buf);

	/* Get result */
	return pool_recvres(&poolHandle->port);
}

/*
 * Init PoolAgent
 */
static bool
agent_init(PoolAgent *agent, const char *database, const char *user_name,
           const char *pgoptions)
{
	MemoryContext oldcontext;

	ereport(PMGRLOG,
			(errmsg("agent_init agent %p pid %d database=\"%s\" user_name=\"%s\" pgoptions=\"%s\"",
					agent, agent->pid, database ? database:"NULL",
					user_name ? user_name:"NULL", pgoptions ? pgoptions:"NULL"),
			 PMGR_BACKTRACE_DETIAL()));

	AssertArg(agent);
	if(database == NULL || user_name == NULL)
		return false;
	if(pgoptions == NULL)
		pgoptions = "";

	/* disconnect if we are still connected */
	if (agent->db_pool)
		agent_release_connections(agent, false);

	oldcontext = MemoryContextSwitchTo(agent->mctx);

	/* get database */
	agent->db_pool = get_database_pool(database, user_name, pgoptions);

	MemoryContextSwitchTo(oldcontext);

	return true;
}

/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	Size	i;
	ADBNodePool *node_pool;
	dlist_mutable_iter miter;
	ADBNodePoolSlot *slot;
	HASH_SEQ_STATUS hseq;

	AssertArg(agent);

	ereport(PMGRLOG,
			(errmsg("agent begin destroy %p pid %d", agent, agent->pid),
			 PMGR_BACKTRACE_DETIAL()));

	if(Socket(agent->port) != PGINVALID_SOCKET)
		closesocket(Socket(agent->port));

	/*
	 * idle them all.
	 * Force disconnection if there are temporary objects on agent.
	 */
	agent_idle_connections(agent, agent->is_temp);

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (poolAgents[i] == agent)
		{
			Size end = --agentCount;
			if(end > i)
				memmove(&(poolAgents[i]), &(poolAgents[i+1]), (end-i)*sizeof(agent));
			poolAgents[end] = NULL;
			break;
		}
	}
	Assert(i<=agentCount);

	hash_seq_init(&hseq, agent->db_pool->htab_nodes);
	while((node_pool = hash_seq_search(&hseq)) != NULL)
	{
		dlist_foreach_modify(miter, &node_pool->released_slot)
		{
			slot = dlist_container(ADBNodePoolSlot, dnode, miter.cur);
			Assert(slot->slot_state == SLOT_STATE_RELEASED);
			if(slot->owner == agent)
			{
				Assert(slot->last_user_pid == agent->pid);
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(miter.cur);
				SET_SLOT_LIST(slot, NULL_SLOT);
				idle_slot(slot, true);
			}else
			{
				ereport(PMGRLOG,
						(errmsg("agent %p pid %d not call idle_slot(%p) state %d owner is %p pid %d",
								agent, agent->pid, slot, slot->slot_state,
								slot->owner, slot->owner ? slot->owner->pid:0),
						 PMGR_BACKTRACE_DETIAL()));
			}
		}
	}

	while(agent->list_wait != NIL)
	{
		slot = linitial(agent->list_wait);
		agent->list_wait = list_delete_first(agent->list_wait);
		if(slot != NULL && slot->owner != NULL)
		{
			Assert(slot->owner == agent);

			if(slot->slot_state != SLOT_STATE_CONNECTING &&
				slot->slot_state != SLOT_STATE_QUERY_AGTM_PORT &&
				slot->slot_state != SLOT_STATE_QUERY_PARAMS_SESSION	&&
				slot->slot_state != SLOT_STATE_QUERY_PARAMS_LOCAL &&
				slot->slot_state != SLOT_STATE_QUERY_RESET_ALL &&
				slot->current_list != NULL_SLOT)
			{
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(&slot->dnode);
				SET_SLOT_LIST(slot, NULL_SLOT);
			}

			ereport(PMGRLOG,
					(errmsg("agent %p pid %d will idle waiting slot slot %p state %d",
							agent, agent->pid, slot, slot->slot_state),
					 PMGR_BACKTRACE_DETIAL()));

			idle_slot(slot, true);
		}
	}

	MemoryContextDelete(agent->mctx);
}

/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
#ifdef WITH_RDMA
	PQNForceReleaseWhenTransactionFinish();
	PQNReleaseAllConnect(true);
#endif
	if (poolHandle)
	{

		pool_putmessage(&poolHandle->port, PM_MSG_RELEASE_CONNECT, NULL, 0);
		pool_flush(&poolHandle->port);

		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
	}
}

static void send_host_info(StringInfo buf, List *oidlist)
{
	ListCell *lc;
	HeapTuple		tuple;
	Form_pgxc_node	nodeForm;

	pool_sendint(buf, list_length(oidlist));

	foreach(lc, oidlist)
	{
		tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(lfirst_oid(lc)));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for node %u", lfirst_oid(lc));

		nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);
		pool_sendstring(buf, NameStr(nodeForm->node_host));
		pool_sendint(buf, nodeForm->node_port);

		ReleaseSysCache(tuple);
	}
}

static List* recv_host_info(StringInfo buf, bool dup_str)
{
	List *list = NIL;
	HostInfo *info;
	int count;

	count = pool_getint(buf);
	while (count-- > 0)
	{
		info = palloc(sizeof(*info));
		info->hostname = (char*)pool_getstring(buf);
		info->port = (uint16)pool_getint(buf);
		if (dup_str)
			info->hostname = pstrdup(info->hostname);
		list = lappend(list, info);
	}

	return list;
}

#ifdef WITH_RDMA
List*
PoolManagerGetRsConnectionsOid(List *oidlist)
{
	StringInfoData buf;
	const char *database;
	const char *user_name;
	const char *pgoptions;
	ListCell *lc;
	char*	rs_conn_str;
	HeapTuple		tuple;
	Form_pgxc_node	nodeForm;
	PGconn *pcon;
	List *lcon = NIL;

re_try_:
	if (!poolHandle)
		PoolManagerReconnect();
	Assert(poolHandle != NULL);
	if(oidlist == NIL)
		return NIL;

	pq_beginmessage(&buf, PM_MSG_GET_DBINFO_CONNECT);
	pool_sendint(&buf, 1);

	if (pool_putmessage(&(poolHandle->port), (char)buf.cursor, buf.data, buf.len) != 0 ||
		pool_flush(&(poolHandle->port)) != 0 ||
		poolHandle->port.SendPointer != 0)
	{
		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
		pfree(buf.data);
		goto re_try_;
	}

	pool_recv_data(&(poolHandle->port));
	pool_getstringdata(&(poolHandle->port), &buf);

	database = pool_getstring(&buf);
	user_name = pool_getstring(&buf);
	pgoptions = pool_getstring(&buf);

	foreach(lc, oidlist)
	{
		tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(lfirst_oid(lc)));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for node %u", lfirst_oid(lc));

		nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);

		//rs_conn_str = PGXCNodeConnStr(NameStr(nodeForm->node_host), nodeForm->node_port,
					//database, user_name, pgoptions, IsGTMNode() ? "AGTM" : "coordinator");
		rs_conn_str = PGXCNodeRsConnStr(NameStr(nodeForm->node_host), nodeForm->node_port,
					database, user_name, pgoptions, IsGTMNode() ? "AGTM" : "coordinator");

		//elog(LOG, "rs_conn_str is %s", rs_conn_str);
		pcon = PQconnectdb(rs_conn_str);

		if (pcon->status != CONNECTION_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Connect node %s:%d error, return status %d", 
					NameStr(nodeForm->node_host),
					nodeForm->node_port,
					pcon->status)));
		PQbeginRsAttach(pcon);
		elog(LOG, "get rsocket %d,nonblocking %d", pcon->sock, pcon->nonblocking);
		Assert(pcon);

		lcon = lappend(lcon, pcon);

		ReleaseSysCache(tuple);
		pfree(rs_conn_str);
	}

	pfree(buf.data);
	return lcon;
}
#endif

/*
 * Get pooled connections
 */
pgsocket *
PoolManagerGetConnectionsOid(List *oidlist)
{
	pgsocket *fds;
	StringInfoData buf;
	int val;

re_try_:
	if (!poolHandle)
		PoolManagerReconnect();
	Assert(poolHandle != NULL);
	if(oidlist == NIL)
		return NULL;

	pq_beginmessage(&buf, PM_MSG_GET_CONNECT);

	/* send agtm listen port */
	if (IsGTMNode())
	{
		/* -1 for no GTM */
		val = -1;
	}
	/* no need to connect GTM 4.1 */
	/* else
	{
		val = agtm_GetListenPort();
		if(val < 1 || val > 65535)
			ereport(ERROR, (errmsg("Invalid agtm listen port %d", val)));
	}*/

	pool_sendint(&buf, val);

	send_host_info(&buf, oidlist);

	/* send message */
	if (pool_putmessage(&poolHandle->port, (char)(buf.cursor), buf.data, buf.len) != 0 ||
		pool_flush(&poolHandle->port) != 0 ||
		poolHandle->port.SendPointer != 0)
	{
		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
		pfree(buf.data);
		goto re_try_;
	}

	/* Receive response */
	val = list_length(oidlist);
	/* we reuse buf.data, here palloc maybe failed */
	Assert(buf.maxlen >= sizeof(pgsocket)*val);
	fds = (int*)(buf.data);
	if(pool_recvfds(poolHandle->port.fdsock, fds, val) != 0)
	{
		pfree(fds);
		return NULL;
	}

	return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
	StringInfoData buf;
	AssertArg(proc_pids);

re_try_:
	if (!poolHandle)
		PoolManagerReconnect();
	Assert(poolHandle);

	pq_beginmessage(&buf, PM_MSG_ABORT_TRANSACTIONS);

	/* send database name */
	pool_sendstring(&buf, dbname);

	/* send user name */
	pool_sendstring(&buf, username);

	if (pool_putmessage(&(poolHandle->port), (char)buf.cursor, buf.data, buf.len) != 0 ||
		pool_flush(&(poolHandle->port)) != 0 ||
		poolHandle->port.SendPointer != 0)
	{
		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
		pfree(buf.data);
		goto re_try_;
	}

	return pool_recvpids(&(poolHandle->port), proc_pids);
}


/*
 * Clean up Pooled connections
 */

void PoolManagerCleanConnectionOid(List *oidlist, const char *dbname, const char *username)
{
	StringInfoData buf;
	if (oidlist == NIL)
		return;

	pq_beginmessage(&buf, PM_MSG_CLEAN_CONNECT);

	send_host_info(&buf, oidlist);

	/* send database string */
	pool_sendstring(&buf, dbname);

	/* send user name */
	pool_sendstring(&buf, username);

	pool_end_flush_msg(&(poolHandle->port), &buf);

	/* Receive result message */
	if (pool_recvres(&poolHandle->port) != CLEAN_CONNECTION_COMPLETED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Clean connections not completed")));
}

void PoolManagerReleaseConnections(bool force_close)
{
	if(poolHandle)
	{
		pool_putmessage(&(poolHandle->port),
						force_close ? PM_MSG_CLOSE_CONNECT:PM_MSG_RELEASE_CONNECT,
						NULL,
						0);
		pool_flush(&(poolHandle->port));
	}
}

static void pooler_quickdie(SIGNAL_ARGS)
{
	signal_quit = true;
}

static void pooler_sighup(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

bool IsPoolHandle(void)
{
	return poolHandle != NULL;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
static int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{
	int *pids = NULL;
	DatabaseInfo *db_info;
	int i;
	int count;

	Assert(!is_pool_locked);
	Assert(agentCount > 0);

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	is_pool_locked = true;

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = i = 0; i < agentCount; i++)
	{
		Assert(poolAgents[i] && poolAgents[i]->db_pool);
		if (poolAgents[i]->pid == pid)
			continue;

		db_info = &(poolAgents[i]->db_pool->db_info);
		if (database && db_info->database &&
						strcmp(db_info->database, database) != 0)
			continue;

		if (user_name && db_info->user_name &&
						strcmp(db_info->user_name, user_name) != 0)
			continue;

		if (kill(poolAgents[i]->pid, SIGTERM) < 0)
			elog(ERROR, "kill(%ld,%d) failed: %m",
						(long) poolAgents[i]->pid, SIGTERM);

		pids[count] = poolAgents[i]->pid;
		++count;
	}

	*len = count;

	return pids;
}

static void agent_handle_input(PoolAgent * agent, StringInfo s)
{
	ErrorContextCallback err_calback;
	const char *database;
	const char *user_name;
	const char *pgoptions;
	int			*pids;
	int			len,res;
	int qtype;
#ifdef WITH_RDMA
	StringInfoData buf;
#endif

	/* try recv data */
	if(pool_recv_data(&agent->port) == false)
	{
		/*
		 * closed by remote, maybe it have not normal exit.
		 * PGconn maybe have dirty data in socket buffer,
		 * safety we destroy it
		 */
		agent_release_connections(agent, true);
		agent_destroy(agent);
		return;
	}

	/* setup error callback */
	err_calback.arg = NULL;
	err_calback.callback = agent_error_hook;
	err_calback.previous = error_context_stack;
	error_context_stack = &err_calback;

	while(agent_has_completion_msg(agent, s, &qtype))
	{
		/* set need report error if have */
		err_calback.arg = agent;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_locked
			&& (qtype == PM_MSG_ABORT_TRANSACTIONS ||
				qtype == PM_MSG_GET_CONNECT ||
				qtype == PM_MSG_CONNECT))
			elog(WARNING,"Pool operation cannot run during pool lock");

		switch(qtype)
		{
		case PM_MSG_ABORT_TRANSACTIONS:
			database = pool_getstring(s);
			user_name = pool_getstring(s);

			pids = abort_pids(&len, agent->pid, database, user_name);
			pool_sendpids(&agent->port, pids, len);
			if(pids)
				pfree(pids);
			break;
		case PM_MSG_SEND_LOCAL_COMMAND:
			res = send_local_commands(agent, s);
			pool_sendres(&agent->port, res);
			break;
		case PM_MSG_CONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			pq_copymsgbytes(s, (char*)&(agent->pid), sizeof(agent->pid));
			database = pool_getstring(s);
			user_name = pool_getstring(s);
			pgoptions = pool_getstring(s);
			if(agent_init(agent, database, user_name, pgoptions) == false)
			{
				agent_destroy(agent);
				goto end_agent_input_;
			}
			break;
		case PM_MSG_DISCONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			agent_destroy(agent);
			pq_getmsgend(s);
			goto end_agent_input_;
		case PM_MSG_CLEAN_CONNECT:
			res = clean_connection(s);
			pool_sendres(&agent->port, res);
			break;
		case PM_MSG_GET_CONNECT:
			{
				agent->agtm_port = pool_getint(s);
				/* if (!IsGTMCnNode() &&
					(agent->agtm_port <= 0 || agent->agtm_port > 65535))
				{
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("invalid agtm port number %d", agent->agtm_port)));
				}*/
				agent_acquire_connections(agent, s);
				AssertState(agent->list_wait != NIL);
			}
			break;
		case PM_MSG_RELEASE_CONNECT:
		case PM_MSG_CLOSE_CONNECT:
			err_calback.arg = NULL; /* do not send error if have */
			pq_getmsgend(s);
			agent_release_connections(agent, qtype == PM_MSG_CLOSE_CONNECT);
			break;
		case PM_MSG_SET_COMMAND:
			{
				StringInfoData msg;
				PoolCommandType cmd_type;
				const char *set_cmd;
				cmd_type = (PoolCommandType)pool_getint(s);
				set_cmd = pool_getstring(s);
				msg.data = NULL;
				res = agent_session_command(agent, set_cmd, cmd_type, &msg);
				if(res != 0 && msg.data)
					ereport(ERROR, (errmsg("%s", msg.data)));
				pool_sendres(&agent->port, res);
				if(msg.data)
					pfree(msg.data);
			}
			break;
		case PM_MSG_CLOSE_IDLE_CONNECT:
			{
				close_idle_connection();
			}
			break;
#ifdef WITH_RDMA
		case PM_MSG_GET_DBINFO_CONNECT:
			{
				pool_getint(s);
				initStringInfo(&buf);
				pool_sendstring(&buf, agent->db_pool->db_info.database);
				pool_sendstring(&buf, agent->db_pool->db_info.user_name);
				pool_sendstring(&buf, agent->db_pool->db_info.pgoptions);
				pool_end_flush_msg(&agent->port, &buf);
			}
			break;
#endif
		default:
			agent_destroy(agent);
			ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("invalid backend message type %d", qtype)));
			goto end_agent_input_;
		}
		pq_getmsgend(s);
	}
end_agent_input_:
	error_context_stack = err_calback.previous;
}

static void agent_handle_output(PoolAgent *agent)
{
	ssize_t rval;
re_send_:
	rval = send(Socket(agent->port), agent->port.SendBuffer, agent->port.SendPointer, 0);
	if(rval < 0)
	{
		if(errno == EINTR)
			goto re_send_;
		else if(errno == EAGAIN)
			return;
		agent_destroy(agent);
	}else if(rval == 0)
	{
		agent_destroy(agent);
	}
	if(rval < agent->port.SendPointer)
	{
		memmove(agent->port.SendBuffer, &(agent->port.SendBuffer[rval])
			, agent->port.SendPointer - rval);
		agent->port.SendPointer -= rval;
	}else
	{
		agent->port.SendPointer = 0;
	}
}

static void agent_error_hook(void *arg)
{
	const ErrorData *err;
	if(arg && (err = err_current_data()) != NULL
		&& err->elevel >= ERROR)
	{
		if(err->message)
			pool_putmessage(arg, PM_MSG_ERROR, err->message, strlen(err->message));
		else
			pool_putmessage(arg, PM_MSG_ERROR, NULL, 0);
		pool_flush(arg);
	}
}

static void agent_check_waiting_slot(PoolAgent *agent)
{
	ListCell *lc;
	ADBNodePoolSlot *slot;
	PoolAgent *volatile volAgent;
	ErrorContextCallback err_calback;
	bool all_ready;
	int64 now_val;
	ADBNodePool *node_pool;
	AssertArg(agent);
	if(agent->list_wait == NIL)
		return;

	/* setup error callback */
	err_calback.arg = agent;
	err_calback.callback = agent_error_hook;
	err_calback.previous = error_context_stack;
	error_context_stack = &err_calback;
	volAgent = agent;

	/*
	 * SLOT_STATE_IDLE					-> SLOT_STATE_QUERY_AGTM_PORT
	 * SLOT_STATE_END_RESET_ALL			-> SLOT_STATE_QUERY_AGTM_PORT
	 * SLOT_STATE_END_AGTM_PORT			-> SLOT_STATE_QUERY_PARAMS_SESSION
	 * SLOT_STATE_END_PARAMS_SESSION	-> SLOT_STATE_QUERY_PARAMS_LOCAL
	 * SLOT_STATE_END_PARAMS_LOCAL		-> SLOT_STATE_LOCKED
	 * SLOT_STATE_RELEASED				-> SLOT_STATE_QUERY_RESET_ALL or SLOT_STATE_LOCKED
	 */
	all_ready = true;
	PG_TRY();
	{
		foreach(lc,agent->list_wait)
		{
			slot = lfirst(lc);
			AssertState(slot && slot->owner == agent);
			switch(slot->slot_state)
			{
			case SLOT_STATE_UNINIT:
				ExceptionalCondition("invalid status SLOT_STATE_UNINIT for slot"
					, "BadState", __FILE__, __LINE__);
				break;
			case SLOT_STATE_IDLE:
			case SLOT_STATE_END_RESET_ALL:
send_agtm_port_:
				if (!IsGTMCnNode() &&
					(slot->last_user_pid != agent->pid ||
					 slot->last_agtm_port != agent->agtm_port))
				{
					/* if (agent->agtm_port <= 0 ||
						agent->agtm_port > 65535)
					{
						ereport(FATAL,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("invalid agtm port %d for agent", agent->agtm_port)));
					}*/
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d begin send waiting slot %p state %d agtm port %d",
									agent, agent->pid, slot, slot->slot_state, agent->agtm_port),
							 PMGR_BACKTRACE_DETIAL()));
					if(pqSendAgtmListenPort(slot->conn, slot->owner->agtm_port) < 0)
					{
						save_slot_error(slot);
						ereport(PMGRLOG,
								(errmsg("agent %p pid %d send waiting slot %p new state %d agtm port %d failed:\"%s\"",
										agent, agent->pid, slot, slot->slot_state, agent->agtm_port,
										slot->last_error),
								PMGR_BACKTRACE_DETIAL()));
						break;
					}
					slot->last_agtm_port = agent->agtm_port;
					slot->slot_state = SLOT_STATE_QUERY_AGTM_PORT;
					Assert(slot->current_list != NULL_SLOT);
					if (slot->current_list != BUSY_SLOT)
					{
						dlist_delete(&slot->dnode);
						dlist_push_head(&slot->parent->busy_slot, &slot->dnode);
						SET_SLOT_LIST(slot, BUSY_SLOT);
					}
				}else
				{
					goto send_session_params_;
				}
				break;
			case SLOT_STATE_LOCKED:
				continue;
			case SLOT_STATE_RELEASED:
				if (slot->last_user_pid != agent->pid)
				{
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d begin \"reset all\" waiting slot %p state %d last user pid %d",
									agent, agent->pid, slot, slot->slot_state, slot->last_user_pid),
							 PMGR_BACKTRACE_DETIAL()));
					slot->last_agtm_port = 0;
					if(!PQsendQuery(slot->conn, "reset all"))
					{
						save_slot_error(slot);
						ereport(PMGRLOG,
								(errmsg("agent %p pid %d send \"reset all\" waiting slot %p state %d last user pid %d failed:\"%s\"",
										agent, agent->pid, slot, slot->slot_state, slot->last_user_pid,
										slot->last_error),
								 PMGR_BACKTRACE_DETIAL()));
						break;
					}
					slot->slot_state = SLOT_STATE_QUERY_RESET_ALL;
					Assert(slot->current_list != NULL_SLOT);
					if (slot->current_list != BUSY_SLOT)
					{
						dlist_delete(&slot->dnode);
						dlist_push_head(&slot->parent->busy_slot, &slot->dnode);
						SET_SLOT_LIST(slot, BUSY_SLOT);
					}
				}else if (!IsGTMCnNode() &&
					slot->last_agtm_port != agent->agtm_port)
				{
					goto send_agtm_port_;
				}else if(!EQUAL_PARAMS_MAGIC(slot->session_magic, agent->session_magic))
				{
					goto send_session_params_;
				}else if(!EQUAL_PARAMS_MAGIC(slot->local_magic, agent->local_magic))
				{
					goto send_local_params_;
				}else
				{
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d locked released slot %p", agent, agent->pid, slot),
							 PMGR_BACKTRACE_DETIAL()));
					slot->slot_state = SLOT_STATE_LOCKED;
					slot->last_user_pid = agent->pid;
				}
				break;
			case SLOT_STATE_END_AGTM_PORT:
send_session_params_:
				if(agent->session_params != NULL)
				{
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d begin send waiting slot %p state %d session params \"%s\"",
									agent, agent->pid, slot, slot->slot_state, agent->session_params),
							 PMGR_BACKTRACE_DETIAL()));
					if(!PQsendQuery(slot->conn, agent->session_params))
					{
						save_slot_error(slot);
						ereport(PMGRLOG,
								(errmsg("agent %p pid %d begin send waiting slot %p new state %d session params \"%s\" failed:\"%s\"",
										agent, agent->pid, slot, slot->slot_state, agent->session_params, slot->last_error),
								 PMGR_BACKTRACE_DETIAL()));
						break;
					}
					slot->slot_state = SLOT_STATE_QUERY_PARAMS_SESSION;
					COPY_PARAMS_MAGIC(slot->session_magic, agent->session_magic);
					Assert(slot->current_list != NULL_SLOT);
					if (slot->current_list != BUSY_SLOT)
					{
						dlist_delete(&slot->dnode);
						dlist_push_head(&slot->parent->busy_slot, &slot->dnode);
						SET_SLOT_LIST(slot, BUSY_SLOT);
					}
					break;
				}
				goto send_local_params_;
			case SLOT_STATE_END_PARAMS_SESSION:
send_local_params_:
				if(agent->local_params)
				{
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d begin send waiting slot %p state %d local params \"%s\"",
									agent, agent->pid, slot, slot->slot_state, agent->session_params),
							 PMGR_BACKTRACE_DETIAL()));
					if(!PQsendQuery(slot->conn, agent->local_params))
					{
						save_slot_error(slot);
						ereport(PMGRLOG,
								(errmsg("agent %p pid %d begin send waiting slot %p new state %d local params \"%s\" failed:\"%s\"",
										agent, agent->pid, slot, slot->slot_state, agent->session_params, slot->last_error),
								 PMGR_BACKTRACE_DETIAL()));
						break;
					}
					slot->slot_state = SLOT_STATE_QUERY_PARAMS_LOCAL;
					COPY_PARAMS_MAGIC(slot->local_magic, agent->session_magic);
					Assert(slot->current_list != NULL_SLOT);
					if (slot->current_list != BUSY_SLOT)
					{
						dlist_delete(&slot->dnode);
						dlist_push_head(&slot->parent->busy_slot, &slot->dnode);
						SET_SLOT_LIST(slot, BUSY_SLOT);
					}
					break;
				}
				goto end_params_local_;
			case SLOT_STATE_END_PARAMS_LOCAL:
end_params_local_:
				ereport(PMGRLOG,
						(errmsg("agent %p pid %d locked waiting slot %p last state %d",
								agent, agent->pid, slot, slot->slot_state),
						 PMGR_BACKTRACE_DETIAL()));
				slot->slot_state = SLOT_STATE_LOCKED;
				break;
			default:
				break;
			}
			if(slot->slot_state == SLOT_STATE_ERROR)
			{
				if(slot->retry < RetryTimes)
				{
					struct timeval now;
					gettimeofday(&now, NULL);
					now_val = (int64)now.tv_sec * 1000000 + (int64)now.tv_usec;
					node_pool = slot->parent;
					if (now_val - slot->last_retry_time > (int64)pow(2,(double)slot->retry)*10000)
					{
						if (node_pool->connstr != NULL)
						{
							PQfinish(slot->conn);
							slot->conn = PQconnectStart(node_pool->connstr);

							if(slot->conn == NULL)
							{
								ereport(ERROR,
									(errcode(ERRCODE_OUT_OF_MEMORY)
									,errmsg("out of memory")));
							}else if(PQstatus(slot->conn) != CONNECTION_BAD)
							{
								slot->slot_state = SLOT_STATE_CONNECTING;
								slot->poll_state = PGRES_POLLING_WRITING;
								if (slot->current_list != BUSY_SLOT)
								{
									dlist_delete(&slot->dnode);
									dlist_push_head(&slot->parent->busy_slot, &slot->dnode);
									SET_SLOT_LIST(slot, BUSY_SLOT);
								}
							}
							slot->retry++;
							ereport(PMGRLOG,
									(errmsg("[pool] agent %p pid %d reconnect slot %p thimes %d",
											agent, agent->pid, slot, slot->retry)));
						}
						else
						{
							slot->retry = RetryTimes;
							ereport(WARNING,
								(errmsg("node_pool->connstr is NULL, may be pgxc_pool_reload free wrong datanode connection")));
						}
						slot->last_retry_time = now_val;
					}
				}
			}
			if(slot->slot_state == SLOT_STATE_ERROR && slot->retry >= RetryTimes)
			{
				ereport(ERROR, (errmsg("reconnect three thimes , %s", PQerrorMessage(slot->conn))));
			}
			else if(slot->slot_state != SLOT_STATE_LOCKED)
			{
				all_ready = false;
			}else if(slot->slot_state == SLOT_STATE_LOCKED)
			{
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(&slot->dnode);
				SET_SLOT_LIST(slot, NULL_SLOT);
			}
		}
	}PG_CATCH();
	{
		ConnectedInfo *info;
		agent = volAgent;
		while(agent->list_wait != NIL)
		{
			slot = linitial(agent->list_wait);
			agent->list_wait = list_delete_first(agent->list_wait);
			if(slot->slot_state != SLOT_STATE_CONNECTING &&
				slot->slot_state != SLOT_STATE_QUERY_AGTM_PORT &&
				slot->slot_state != SLOT_STATE_QUERY_PARAMS_SESSION	&&
				slot->slot_state != SLOT_STATE_QUERY_PARAMS_LOCAL &&
				slot->slot_state != SLOT_STATE_QUERY_RESET_ALL &&
				slot->current_list != NULL_SLOT)
			{
				/*
				 * when slot->slot_state == SLOT_STATE_LOCKED, slot->current_list may be null,  had dlist_delete;
				 * or from SLOT_STATE_END_AGTM_PORT to SLOT_STATE_LOCKED in function process_slot_event,
				 * slot->current_list is BUSY_SLOT is not null
				 */
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(&slot->dnode);
				SET_SLOT_LIST(slot, NULL_SLOT);
			}
			info = hash_search(agent->connected_node,
							   &slot->parent->hostinfo,
							   HASH_REMOVE,
							   NULL);
			Assert(info && info->slot == slot);
			ereport(PMGRLOG,
					(errmsg("agent %p pid %d removed slot %p for %s:%d",
							agent, agent->pid, info->slot, info->info.hostname, info->info.port),
					 PMGR_BACKTRACE_DETIAL()));
			pfree(info->info.hostname);
			MemSet(info, 0, sizeof(*info));
			if (slot->slot_state == SLOT_STATE_ERROR)
				destroy_slot(slot, false);
			else
				idle_slot(slot, true);
		}
		PG_RE_THROW();
	}PG_END_TRY();

	if(all_ready)
	{
		static int *pfds = NULL;
		static Size max_fd = 0;
		Size count,index;
		ereport(PMGRLOG,
				(errmsg("agent %p pid %d all waiting slot %d locked",
						agent, agent->pid, list_length(agent->list_wait)),
				 PMGR_BACKTRACE_DETIAL()));
		PG_TRY();
		{
			if(max_fd == 0)
			{
				pfds = MemoryContextAlloc(PoolerMemoryContext, 8*sizeof(int));
				max_fd = 8;
			}
			count = list_length(agent->list_wait);
			if(count > max_fd)
			{
				pfds = repalloc(pfds, count*sizeof(int));
				max_fd = count;
			}

			index = 0;
			foreach(lc, agent->list_wait)
			{
				slot = lfirst(lc);
				Assert(slot->slot_state == SLOT_STATE_LOCKED &&
					   slot->owner == agent);
				slot->last_user_pid = agent->pid;
				pfds[index] = PQsocket(slot->conn);
				Assert(pfds[index] != PGINVALID_SOCKET);
				++index;
			}

			if(pool_sendfds(agent->port.fdsock, pfds, (int)index) != 0)
				ereport(ERROR, (errmsg("can not send fds to backend")));
		}PG_CATCH();
		{
			ConnectedInfo *info;
			agent = volAgent;
			while(agent->list_wait != NIL)
			{
				slot = linitial(agent->list_wait);
				agent->list_wait = list_delete_first(agent->list_wait);
				info = hash_search(agent->connected_node,
								   &slot->parent->hostinfo,
								   HASH_REMOVE,
								   NULL);
				ereport(PMGRLOG,
						(errmsg("agent %p pid %d removed slot %p for %s:%d",
								agent, agent->pid, slot, info->info.hostname, info->info.port),
						 PMGR_BACKTRACE_DETIAL()));
				Assert(info);
				pfree(info->info.hostname);
				release_slot(slot, false);
			}
			PG_RE_THROW();
		}PG_END_TRY();

		foreach(lc,agent->list_wait)
		{
			slot = lfirst(lc);
			slot->has_temp = agent->is_temp;
		}
		list_free(agent->list_wait);
		agent->list_wait = NIL;
	}

	error_context_stack = err_calback.previous;
}

/* true for recv some data, false for closed by remote */
static bool pool_recv_data(PoolPort *port)
{
	int rval;
	AssertArg(port);

	if (port->RecvPointer > 0)
	{
		if (port->RecvLength > port->RecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(port->RecvBuffer, port->RecvBuffer + port->RecvPointer,
					port->RecvLength - port->RecvPointer);
			port->RecvLength -= port->RecvPointer;
			port->RecvPointer = 0;
		}
		else
			port->RecvLength = port->RecvPointer = 0;
	}

	if(port->RecvLength >= POOL_BUFFER_SIZE)
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("too many data from backend for pooler")));
		return false;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		rval = recv(Socket(*port), port->RecvBuffer + port->RecvLength,
				 POOL_BUFFER_SIZE - port->RecvLength, 0);

		if (rval < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if (errno == EINTR
#if defined(EAGAIN) && EINTR != EAGAIN
				|| errno == EAGAIN
#endif
			)
				continue;		/* Ok if interrupted */

			/*
			 * Report broken connection
			 */
			ereport(WARNING,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
			return false;
		}else if (rval == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return false;
		}
		/* rval contains number of bytes read, so just incr length */
		port->RecvLength += rval;
		break;
	}
	return true;
}
#ifdef WITH_RDMA
static bool pool_getstringdata(PoolPort *port, StringInfo msg)
{
	Size unread_len;
	int len;
	AssertArg(port && msg);

	Assert(port->RecvLength >= 0 && port->RecvPointer >= 0
		&& port->RecvLength >= port->RecvPointer);

	unread_len = port->RecvLength - port->RecvPointer;
	if(unread_len < 5)
		return false;

	/* get message length */
	memcpy(&len, port->RecvBuffer + port->RecvPointer + 1, 4);
	len = htonl(len);
	if((len+1) > unread_len)
		return false;

	/* okay, copy message */
	len++;	/* add char length */
	resetStringInfo(msg);
	enlargeStringInfo(msg, len);
	Assert(msg->data);
	memcpy(msg->data, port->RecvBuffer + port->RecvPointer, len);
	port->RecvPointer += len;
	msg->len = len;
	msg->cursor = +5; /* skip length */
	return true;
}
#endif

/* get message if has completion message */
static bool agent_has_completion_msg(PoolAgent *agent, StringInfo msg, int *msg_type)
{
	PoolPort *port;
	Size unread_len;
	int len;
	AssertArg(agent && msg);

	port = &agent->port;
	Assert(port->RecvLength >= 0 && port->RecvPointer >= 0
		&& port->RecvLength >= port->RecvPointer);

	unread_len = port->RecvLength - port->RecvPointer;
	/* 5 is message type (char) and message length(int) */
	if(unread_len < 5)
		return false;

	/* get message length */
	memcpy(&len, port->RecvBuffer + port->RecvPointer + 1, 4);
	len = htonl(len);
	if((len+1) > unread_len)
		return false;

	*msg_type = port->RecvBuffer[port->RecvPointer];

	/* okay, copy message */
	len++;	/* add char length */
	resetStringInfo(msg);
	enlargeStringInfo(msg, len);
	Assert(msg->data);
	memcpy(msg->data, port->RecvBuffer + port->RecvPointer, len);
	port->RecvPointer += len;
	msg->len = len;
	msg->cursor = 5; /* skip message type and length */
	return true;
}

static int clean_connection(StringInfo msg)
{
	DatabasePool *db_pool;
	ADBNodePool *nodes_pool;
	List *list;
	ListCell *lc;
	const char *database;
	const char *user_name;
	HASH_SEQ_STATUS hash_db_status;
	int res;

	list = recv_host_info(msg, false);
	database = pool_getstring(msg);
	user_name = pool_getstring(msg);

	if(htab_database == NULL || list == NIL)
		return CLEAN_CONNECTION_COMPLETED;

	res = CLEAN_CONNECTION_COMPLETED;

	hash_seq_init(&hash_db_status, htab_database);
	while((db_pool = hash_seq_search(&hash_db_status)) != NULL)
	{
		if (db_pool->db_info.database && database &&
			strcmp(db_pool->db_info.database, database) != 0)
			continue;
		if (db_pool->db_info.user_name && user_name &&
			strcmp(db_pool->db_info.user_name, user_name) != 0)
			continue;

		foreach(lc, list)
		{
			nodes_pool = hash_search(db_pool->htab_nodes, lfirst(lc), HASH_FIND, NULL);
			if(nodes_pool == NULL)
				continue;

			/* check slots is using in agents */
			if(node_pool_in_using(nodes_pool) == false)
			{
				destroy_node_pool(nodes_pool, true);
			}else
			{
				res = CLEAN_CONNECTION_NOT_COMPLETED;
			}
		}
	}

	is_pool_locked = false;
	list_free_deep(list);
	return res;
}

static bool check_slot_status(ADBNodePoolSlot *slot)
{
	struct pollfd poll_fd;
	int rval;
	bool status_error;

	if(slot == NULL)
		return true;
	if(PQsocket(slot->conn) == PGINVALID_SOCKET)
	{
		status_error = true;
		goto end_check_slot_status_;
	}

	poll_fd.fd = PQsocket(slot->conn);
	poll_fd.events = POLLIN|POLLPRI;

recheck_slot_status_:
	rval = poll(&poll_fd, 1, 0);
	CHECK_FOR_INTERRUPTS();

	status_error = false;
	if(rval == 0)
	{
		/* timeout */
		return true;
	}else if(rval < 0)
	{
		if(errno == EINTR
#if defined(EAGAIN) && (EAGAIN!=EINTR)
			|| errno == EAGAIN
#endif
			)
		{
			goto recheck_slot_status_;
		}
		ereport(WARNING, (errcode_for_socket_access(),
			errmsg("check_slot_status poll error:%m")));
		status_error = true;
	}else /* rval > 0 */
	{
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("check_slot_status connect has unread data or EOF. last backend is %d", slot->last_user_pid)));
		status_error = true;
	}

end_check_slot_status_:
	if(status_error)
	{
		destroy_slot(slot, false);
		return false;
	}
	return true;
}

/*
 * send transaction local commands if any, set the begin sent status in any case
 */
static int
send_local_commands(PoolAgent *agent, StringInfo msg)
{
	ConnectedInfo *info;
	HostInfo	key;
	int			tmp;
	int			res;
	ADBNodePoolSlot	*slot;

	Assert(agent);
	if (agent->local_params == NULL)
		return 0;

	res = 0;
	foreach_recv_hostinfo(key, msg)
	{
		info = hash_search(agent->connected_node, &key, HASH_FIND, NULL);
		if (info != NULL)
		{
			slot = info->slot;
			tmp = PGXCNodeSendSetQuery((NODE_CONNECTION*)(slot->conn), agent->local_params);
			res += tmp;
		}
	}end_recv_hostinfo(key, msg)

	if (res < 0)
		return -res;
	return 0;
}

/*------------------------------------------------*/

static void destroy_slot(ADBNodePoolSlot *slot, bool send_cancel)
{
	AssertArg(slot);

	ereport(PMGRLOG,
			(errmsg("agent %p pid %d begin destroy slot %p last state %d",
					slot->owner, slot->owner ? slot->owner->pid:0,
					slot, slot->slot_state),
			 PMGR_BACKTRACE_DETIAL()));
#if 0
	{
		/* check slot in using ? */
		ListCell *lc;
		PoolAgent *agent;
		Size i;
		for(i=0;i<agentCount;++i)
		{
			agent = poolAgents[i];
			foreach(lc, agent->list_wait)
				Assert(lfirst(lc) != slot);
		}
	}
#endif

	if(slot->conn)
	{
		if(send_cancel)
			PQrequestCancel(slot->conn);
		PQfinish(slot->conn);
		slot->conn = NULL;
	}
	SET_SLOT_OWNER(slot, NULL);
	slot->last_user_pid = 0;
	slot->last_agtm_port = 0;
	slot->slot_state = SLOT_STATE_UNINIT;
	if(slot->last_error)
	{
		pfree(slot->last_error);
		slot->last_error = NULL;
	}
	Assert(slot->conn == NULL && slot->slot_state == SLOT_STATE_UNINIT);
	Assert(slot->current_list == NULL_SLOT);
	dlist_push_head(&slot->parent->uninit_slot, &slot->dnode);
	SET_SLOT_LIST(slot, UNINIT_SLOT);

	check_all_slot_list();
}

static void release_slot(ADBNodePoolSlot *slot, bool force_close)
{
	AssertArg(slot);
	ereport(PMGRLOG,
			(errmsg("agent %p pid %d begin release slot %p last state %d",
					slot->owner, slot->owner ? slot->owner->pid:0,
					slot, slot->slot_state),
			 PMGR_BACKTRACE_DETIAL()));
	if(force_close)
	{
		destroy_slot(slot, false);
	}else if(check_slot_status(slot) != false)
	{
		if (pool_release_to_idle_timeout == 0)
		{
			/* idle slot immediate */
			idle_slot(slot, true);
		}else
		{
			slot->slot_state = SLOT_STATE_RELEASED;
			Assert(slot->current_list == NULL_SLOT);
			dlist_push_head(&slot->parent->released_slot, &slot->dnode);
			SET_SLOT_LIST(slot, RELEASED_SLOT);
			slot->released_time = time(NULL);
		}
	}

	check_all_slot_list();
}

static void idle_slot(ADBNodePoolSlot *slot, bool reset)
{
	AssertArg(slot);
	ereport(PMGRLOG,
			(errmsg("agent %p pid %d begin idle slot %p last state %d",
					slot->owner, slot->owner ? slot->owner->pid:0,
					slot, slot->slot_state),
			 PMGR_BACKTRACE_DETIAL()));
	SET_SLOT_OWNER(slot, NULL);
	slot->last_user_pid = 0;
	slot->released_time = time(NULL);

	if(slot->slot_state == SLOT_STATE_CONNECTING)
	{
		return;
	}
	else if(slot->has_temp)
	{
		destroy_slot(slot, false);
	}
	else if (slot->slot_state == SLOT_STATE_ERROR)
	{
		destroy_slot(slot, false);
	}
	else if(reset)
	{
		switch(slot->slot_state)
		{
			case SLOT_STATE_QUERY_AGTM_PORT:
			case SLOT_STATE_QUERY_PARAMS_SESSION:
			case SLOT_STATE_QUERY_PARAMS_LOCAL:
			case SLOT_STATE_QUERY_RESET_ALL:
				return;
			default:
				break;
		}
		/*  SLOT_STATE_ERROR  state will be destory */
		ereport(PMGRLOG,
				(errmsg("idle_slot(%p) begin reset slot agent %p pid %d last state %d",
						slot, slot->owner, slot->owner?slot->owner->pid:0, slot->slot_state),
				PMGR_BACKTRACE_DETIAL()));
		if(!PQsendQuery(slot->conn, "reset all"))
		{
			ereport(PMGRLOG,
					(errmsg("idle_slot(%p) begin reset slot agent %p pid %d last state %d failed:\"%s\"",
							slot, slot->owner, slot->owner?slot->owner->pid:0, slot->slot_state,
							PQerrorMessage(slot->conn)),
					 PMGR_BACKTRACE_DETIAL()));
			destroy_slot(slot, false);
			return;
		}
		slot->slot_state = SLOT_STATE_QUERY_RESET_ALL;
		Assert(slot->current_list == NULL_SLOT);
		dlist_push_head(&slot->parent->busy_slot, &slot->dnode);
		SET_SLOT_LIST(slot, BUSY_SLOT);
	}
	else
	{
		slot->last_agtm_port = 0;
		Assert(slot->current_list == NULL_SLOT);
		dlist_push_head(&slot->parent->idle_slot, &slot->dnode);
		SET_SLOT_LIST(slot, IDLE_SLOT);
		slot->slot_state = SLOT_STATE_IDLE;
	}

	check_all_slot_list();
}

static void destroy_node_pool(ADBNodePool *node_pool, bool bfree)
{
	ADBNodePoolSlot *slot;
	dlist_head *dheads[4];
	dlist_node *node;
	Size i;
	AssertArg(node_pool);

	dheads[0] = &(node_pool->uninit_slot);
	dheads[1] = &(node_pool->released_slot);
	dheads[2] = &(node_pool->idle_slot);
	dheads[3] = &(node_pool->busy_slot);
	for(i=0;i<lengthof(dheads);++i)
	{
		while(!dlist_is_empty(dheads[i]))
		{
			node = dlist_pop_head_node(dheads[i]);
			slot = dlist_container(ADBNodePoolSlot, dnode, node);
			if(slot->last_error)
			{
				pfree(slot->last_error);
				slot->last_error = NULL;
			}
			PQfinish(slot->conn);
			pfree(slot);
			slot = NULL;
		}
	}

	if(bfree)
	{
		Assert(node_pool->parent);
		if(node_pool->connstr)
		{
			pfree(node_pool->connstr);
			node_pool->connstr = NULL;
		}
		hash_search(node_pool->parent->htab_nodes, &node_pool->hostinfo, HASH_REMOVE, NULL);
	}
}

static bool node_pool_in_using(ADBNodePool *node_pool)
{
	Size i;
	PoolAgent *agent;
	ListCell *lc;
	ADBNodePoolSlot *slot;
	ConnectedInfo *info;
	HASH_SEQ_STATUS hseq;
	AssertArg(node_pool);

	for(i=0;i<agentCount;++i)
	{
		agent = poolAgents[i];
		Assert(agent);
		hash_seq_init(&hseq, agent->connected_node);
		while ((info=hash_seq_search(&hseq)) != NULL)
		{
			if (info->slot->parent == node_pool)
			{
				hash_seq_term(&hseq);
				return true;
			}
		}
		foreach(lc, agent->list_wait)
		{
			slot = lfirst(lc);
			if(slot && slot->parent == node_pool)
				return true;
		}
	}
	return false;
}

/*
 * close idle slots when slot->released_time <= cur_time - pool_time_out
 * return best next call time
 */
static time_t close_timeout_idle_slots(time_t cur_time)
{
	HASH_SEQ_STATUS hash_database_stats;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	ADBNodePool *node_pool;
	ADBNodePoolSlot *slot;
	dlist_mutable_iter miter;
	time_t earliest_time = cur_time;
	time_t need_close_time = cur_time - pool_time_out;

	hash_seq_init(&hash_database_stats, htab_database);
	while((db_pool = hash_seq_search(&hash_database_stats)) != NULL)
	{
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			dlist_foreach_modify(miter, &node_pool->idle_slot)
			{
				slot = dlist_container(ADBNodePoolSlot, dnode, miter.cur);
				Assert(slot->slot_state == SLOT_STATE_IDLE);
				if(slot->released_time <= need_close_time)
				{
					ereport(PMGRLOG,
							(errmsg("begin destroy timeout idle slot %p last agent %p pid %d",
									slot, slot->owner, slot->owner ? slot->owner->pid:0),
							 PMGR_BACKTRACE_DETIAL()));
					Assert(slot->current_list != NULL_SLOT);
					dlist_delete(miter.cur);
					SET_SLOT_LIST(slot, NULL_SLOT);
					destroy_slot(slot, false);
				}else if(earliest_time > slot->released_time)
				{
					earliest_time = slot->released_time;
				}
			}
		}
	}
	return earliest_time + pool_time_out;
}

/*
 * idle slots when slot->released_time <= cur_time - pool_release_to_idle_timeout
 * return best next call time
 */
static time_t idle_timeout_released_slots(time_t cur_time)
{
	HASH_SEQ_STATUS hash_database_stats;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	ADBNodePool *node_pool;
	ADBNodePoolSlot *slot;
	dlist_mutable_iter miter;
	time_t need_idle_time = cur_time - pool_release_to_idle_timeout;
	time_t earliest_time = cur_time;

	hash_seq_init(&hash_database_stats, htab_database);
	while((db_pool = hash_seq_search(&hash_database_stats)) != NULL)
	{
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			dlist_foreach_modify(miter, &node_pool->released_slot)
			{
				slot = dlist_container(ADBNodePoolSlot, dnode, miter.cur);
				AssertState(slot->slot_state == SLOT_STATE_RELEASED);
				AssertState(slot->current_list == RELEASED_SLOT);
				if (slot->released_time <= need_idle_time)
				{
					ereport(PMGRLOG,
							(errmsg("begin release timeout idle slot %p last agent %p pid %d",
									slot, slot->owner, slot->owner ? slot->owner->pid:0),
							 PMGR_BACKTRACE_DETIAL()));
					dlist_delete(miter.cur);
					SET_SLOT_LIST(slot, NULL_SLOT);
					idle_slot(slot, true);
				}else if(slot->released_time < earliest_time)
				{
					earliest_time = slot->released_time;
				}
			}
		}
	}
	return earliest_time+pool_release_to_idle_timeout;
}

/* find pool, if not exist create a new */
static DatabasePool *get_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
	DatabaseInfo info;
	DatabasePool * dbpool;
	bool found;

	AssertArg(database && user_name);
	Assert(htab_database);
	if(pgoptions == NULL)
		pgoptions = "";

	info.database = (char*)database;
	info.user_name = (char*)user_name;
	info.pgoptions = (char*)pgoptions;
	dbpool = hash_search(htab_database, &info, HASH_ENTER, &found);
	if(!found)
	{
		MemoryContext old_context;
		HASHCTL hctl;
		volatile DatabasePool tmp_pool;
		memset((void*)&tmp_pool, 0, sizeof(tmp_pool));
		PG_TRY();
		{
			old_context = MemoryContextSwitchTo(TopMemoryContext);
			tmp_pool.db_info.database = pstrdup(database);
			tmp_pool.db_info.user_name = pstrdup(user_name);
			tmp_pool.db_info.pgoptions = pstrdup(pgoptions);

			memset(&hctl, 0, sizeof(hctl));
			hctl.keysize = sizeof(HostInfo);
			hctl.entrysize = sizeof(ADBNodePool);
			hctl.hash = hash_host_info;
			hctl.match = match_host_info;
			hctl.hcxt = TopMemoryContext;
			tmp_pool.htab_nodes = hash_create("hash ADBNodePool",
											  1024,
											  &hctl,
											  HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);
			(void)MemoryContextSwitchTo(old_context);
		}PG_CATCH();
		{
			destroy_database_pool((DatabasePool*)&tmp_pool, false);
			hash_search(htab_database, &info, HASH_REMOVE, NULL);
			PG_RE_THROW();
		}PG_END_TRY();
		memcpy(dbpool, (void*)&tmp_pool, sizeof(*dbpool));
	}
	return dbpool;
}

static void destroy_database_pool(DatabasePool *db_pool, bool bfree)
{
	DatabaseInfo info;
	if(db_pool == NULL)
		return;
	if(db_pool->htab_nodes)
	{
		ADBNodePool *node_pool;
		HASH_SEQ_STATUS status;
		hash_seq_init(&status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&status)) != NULL)
			destroy_node_pool(node_pool, true);
		hash_destroy(db_pool->htab_nodes);
	}

	if(bfree)
	{
		memcpy(&info, &(db_pool->db_info), sizeof(info));
		hash_search(htab_database, &info, HASH_REMOVE, NULL);
		if(info.pgoptions)
			pfree(info.pgoptions);
		if(info.user_name)
			pfree(info.user_name);
		if(db_pool->db_info.database)
			pfree(info.database);
	}
}

static void agent_release_connections(PoolAgent *agent, bool force_destroy)
{
	ADBNodePoolSlot *slot;
	ConnectedInfo *info;
	HASH_SEQ_STATUS hseq;
	AssertArg(agent);

	if (!force_destroy && cluster_ex_lock_held)
	{
		elog(LOG, "Not releasing connection with cluster lock");
		return;
	}

	hash_seq_init(&hseq, agent->connected_node);
	while ((info=hash_seq_search(&hseq)) != NULL)
	{
		slot = info->slot;
		ereport(PMGRLOG,
				(errmsg("agent %p pid %d begin release connections for slot %p last state %d",
						agent, agent->pid, slot, slot->slot_state),
				 PMGR_BACKTRACE_DETIAL()));
		if (list_member_ptr(agent->list_wait, slot))
		{
			ereport(PMGRLOG,
					(errmsg("agent %p pid %d not released connections for slot %p last state %d, it in waiting list",
							agent, agent->pid, slot, slot->slot_state),
					 PMGR_BACKTRACE_DETIAL()));
			continue;
		}
		Assert(slot->slot_state == SLOT_STATE_LOCKED
			&& slot->owner == agent
			&& slot->last_user_pid == agent->pid);

		hash_search(agent->connected_node, &info->info, HASH_REMOVE, NULL);
		ereport(PMGRLOG,
				(errmsg("agent %p pid %d removed slot %p for %s:%d",
						agent, agent->pid, slot, info->info.hostname, info->info.port),
				 PMGR_BACKTRACE_DETIAL()));
		pfree(info->info.hostname);
		info->info.hostname = NULL;
		release_slot(slot, force_destroy);
	}
}

/* set agent's all slots to idle, include reset */
static void agent_idle_connections(PoolAgent *agent, bool force_destroy)
{
	ADBNodePoolSlot *slot;
	ConnectedInfo *info;
	HASH_SEQ_STATUS hseq;

	AssertArg(agent);
	hash_seq_init(&hseq, agent->connected_node);
	while ((info=hash_seq_search(&hseq)) != NULL)
	{
		slot = info->slot;
		ereport(PMGRLOG,
				(errmsg("agent %p pid %d begin idle connections for slot %p last state %d for %s:%d",
						agent, agent->pid, slot, slot->slot_state, info->info.hostname, info->info.port),
				 PMGR_BACKTRACE_DETIAL()));
		if((slot->slot_state == SLOT_STATE_RELEASED || slot->slot_state == SLOT_STATE_LOCKED)
			&& slot->last_user_pid == agent->pid)
		{
			idle_slot(slot, true);
		}else
		{
			ereport(PMGRLOG,
					(errmsg("agent %p pid %d not idle connections for slot %p last state %d last user pid %d",
							agent, agent->pid, slot, slot->slot_state, slot->last_user_pid),
					 PMGR_BACKTRACE_DETIAL()));
		}
		hash_search(agent->connected_node, &info->info, HASH_REMOVE, NULL);
		ereport(PMGRLOG,
				(errmsg("agent %p pid %d removed slot %p for %s:%d",
						agent, agent->pid, slot, info->info.hostname, info->info.port),
				 PMGR_BACKTRACE_DETIAL()));
		pfree(info->info.hostname);
		info->info.hostname = NULL;
	}
}

static void process_slot_event(ADBNodePoolSlot *slot)
{
	AssertArg(slot);

	ereport(PMGRLOG,
			(errmsg("begin process slot %p state %d owner %p pid %d last error:\"%s\"",
					slot, slot->slot_state, slot->owner, slot->owner ? slot->owner->pid:0,
					slot->last_error ? slot->last_error:"NULL"),
			 PMGR_BACKTRACE_DETIAL()));
	if(slot->last_error)
	{
		pfree(slot->last_error);
		slot->last_error = NULL;
	}

	switch(slot->slot_state)
	{
	case SLOT_STATE_UNINIT:
	case SLOT_STATE_IDLE:
	case SLOT_STATE_LOCKED:
	case SLOT_STATE_RELEASED:

	case SLOT_STATE_END_AGTM_PORT:
	case SLOT_STATE_END_PARAMS_SESSION:
	case SLOT_STATE_END_PARAMS_LOCAL:
	case SLOT_STATE_END_RESET_ALL:
		break;
	case SLOT_STATE_CONNECTING:
		slot->poll_state = PQconnectPoll(slot->conn);
		switch(slot->poll_state)
		{
		case PGRES_POLLING_FAILED:
			save_slot_error(slot);
			slot->slot_state = SLOT_STATE_ERROR;
			ereport(PMGRLOG,
					(errmsg("process connectiong slot %p owner %p pid %d got error:\"%s\"",
							slot, slot->owner, slot->owner ? slot->owner->pid:0, slot->last_error),
					 PMGR_BACKTRACE_DETIAL()));
			break;
		case PGRES_POLLING_READING:
		case PGRES_POLLING_WRITING:
			break;
		case PGRES_POLLING_OK:
			slot->slot_state = SLOT_STATE_IDLE;
			slot->retry = 0;
			slot->last_retry_time = 0;
			break;
		default:
			break;
		}
		break;
	case SLOT_STATE_ERROR:
		if(PQisBusy(slot->conn)
			&& get_slot_result(slot) == false
			&& slot->owner == NULL)
			{
				ereport(PMGRLOG,
						(errmsg("process slot %p owner %p pid %d got error:\"%s\"",
								slot, slot->owner, slot->owner ? slot->owner->pid:0, PQerrorMessage(slot->conn)),
						 PMGR_BACKTRACE_DETIAL()));
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(&slot->dnode);
				SET_SLOT_LIST(slot, NULL_SLOT);
				destroy_slot(slot, false);
			}
		break;
	case SLOT_STATE_QUERY_AGTM_PORT:
	case SLOT_STATE_QUERY_PARAMS_SESSION:
	case SLOT_STATE_QUERY_PARAMS_LOCAL:
	case SLOT_STATE_QUERY_RESET_ALL:
		if(get_slot_result(slot) == false)
		{
			if(slot->slot_state == SLOT_STATE_ERROR)
			{
				ereport(PMGRLOG,
						(errmsg("process slot %p owner %p pid %d got error:\"%s\"",
								slot, slot->owner, slot->owner ? slot->owner->pid:0, slot->last_error),
						 PMGR_BACKTRACE_DETIAL()));
				if(slot->owner == NULL)
				{
					Assert(slot->current_list != NULL_SLOT);
					dlist_delete(&slot->dnode);
					SET_SLOT_LIST(slot, NULL_SLOT);
					destroy_slot(slot, false);
				}
				break;
			}
			slot->slot_state += 1;
			ereport(PMGRLOG,
					(errmsg("process slot %p owner %p pid %d change to new state %d",
							slot, slot->owner, slot->owner ? slot->owner->pid:0, slot->slot_state),
						PMGR_BACKTRACE_DETIAL()));

			if(slot->slot_state == SLOT_STATE_END_RESET_ALL)
			{
				INIT_SLOT_PARAMS_MAGIC(slot, session_magic);
				INIT_SLOT_PARAMS_MAGIC(slot, local_magic);
			}

			if(slot->owner == NULL)
			{
				if (IsGTMCnNode() &&
					slot->slot_state == SLOT_STATE_END_RESET_ALL)
					slot->slot_state = SLOT_STATE_END_AGTM_PORT;
				if(slot->slot_state == SLOT_STATE_END_RESET_ALL)
				{
					/* let remote close agtm */
					ereport(PMGRLOG,
							(errmsg("begin send standalone slot %p invalid agtm port last port %d",
									slot, slot->last_agtm_port),
							 PMGR_BACKTRACE_DETIAL()));
					slot->last_agtm_port = 0;
					if(pqSendAgtmListenPort(slot->conn, 0) < 0)
					{
						save_slot_error(slot);
						ereport(PMGRLOG,
								(errmsg("send standalone slot %p invalid agtm port got error:\"%s\"",
										slot, slot->last_error),
								 PMGR_BACKTRACE_DETIAL()));
						break;
					}
					slot->slot_state = SLOT_STATE_QUERY_AGTM_PORT;
				}else if(slot->slot_state == SLOT_STATE_END_AGTM_PORT)
				{
					/* let slot to idle queue */
					ereport(PMGRLOG,
							(errmsg("send standalone slot %p invalid agtm port success move to idle slot", slot),
							 PMGR_BACKTRACE_DETIAL()));
					Assert(slot->parent);
					slot->last_agtm_port = 0;
					slot->slot_state = SLOT_STATE_IDLE;
					Assert(slot->current_list != NULL_SLOT);
					dlist_delete(&slot->dnode);
					dlist_push_head(&slot->parent->idle_slot, &slot->dnode);
					SET_SLOT_LIST(slot, IDLE_SLOT);
				}
			}
		}
		break;
	default:
		ExceptionalCondition("invalid status for slot"
			, "BadState", __FILE__, __LINE__);
		break;
	}
}

static void save_slot_error(ADBNodePoolSlot *slot)
{
	AssertArg(slot);
	if(slot->last_error)
	{
		pfree(slot->last_error);
		slot->last_error = NULL;
	}

	slot->slot_state = SLOT_STATE_ERROR;
	slot->last_error = MemoryContextStrdup(PoolerMemoryContext, PQerrorMessage(slot->conn));
}

/*
 * return have other result ?
 */
static bool get_slot_result(ADBNodePoolSlot *slot)
{
	PGresult * volatile result;
	ExecStatusType status;
	AssertArg(slot && slot->conn);

reget_slot_result_:
	result = PQgetResult(slot->conn);
	if(result == NULL)
		return false;	/* have no other result */
	status = PQresultStatus(result);
	if(PGRES_FATAL_ERROR == status)
	{
		slot->slot_state = SLOT_STATE_ERROR;
		slot->last_agtm_port = 0;
		if(slot->last_error == NULL)
		{
			PG_TRY();
			{
				slot->last_error = MemoryContextStrdup(PoolerMemoryContext, PQresultErrorMessage(result));
			}PG_CATCH();
			{
				PQclear(result);
				PG_RE_THROW();
			}PG_END_TRY();
		}
		slot->slot_state = SLOT_STATE_ERROR;
	}else if(PGRES_COPY_OUT == status)
	{
		const char *buf;
		PQgetCopyDataBuffer(slot->conn, &buf, false);
	}else if(PGRES_COPY_BOTH == status ||
			 PGRES_COPY_IN == status)
	{
		PQputCopyEnd(slot->conn, NULL);
	}
	PQclear(result);
	if(PQisBusy(slot->conn))
		return true;
	goto reget_slot_result_;
}

static void agent_acquire_connections(PoolAgent *agent, StringInfo msg)
{
	ADBNodePoolSlot *slot,*tmp_slot;
	ADBNodePool *node_pool;
	MemoryContext old_context;
	ConnectedInfo *connected_info;
	HostInfo info;
	dlist_iter iter;
	int count;
	AssertArg(agent);

	count = pool_getint(msg);
	PG_TRY();
	{
		bool found;
		if (agent->db_pool == NULL)
			ereport(ERROR, (errmsg("no database info")));

		while(count-- > 0)
		{
			ConnectedInfo *cinfo;
			info.hostname = (char*)pool_getstring(msg);
			info.port = (uint16)pool_getint(msg);
			ereport(PMGRLOG,
					(errmsg("agent %p pid %d begin acquire connect %s:%d",
							agent, agent->pid, info.hostname, info.port),
					 PMGR_BACKTRACE_DETIAL()));

			if ((cinfo = hash_search(agent->connected_node, &info, HASH_FIND, NULL)) != NULL)
			{
				ereport(PMGRLOG,
						(errmsg("agent %p pid %d found connected slot %p owner %p oid %d for %s:%d",
								agent, agent->pid,
								cinfo->slot, cinfo->slot->owner, cinfo->slot->owner ? cinfo->slot->owner->pid:0,
								info.hostname, info.port),
						 PMGR_BACKTRACE_DETIAL()));
				ereport(ERROR, (errmsg("double get node connect for %s:%d", info.hostname, info.port)));
			}

			node_pool = hash_search(agent->db_pool->htab_nodes, &info, HASH_ENTER, &found);
			if (!found)
			{
				PG_TRY();
				{
					node_pool->parent = agent->db_pool;
					old_context = MemoryContextSwitchTo(TopMemoryContext);
					node_pool->hostinfo.hostname = pstrdup(info.hostname);
					node_pool->hostinfo.port = info.port;
					node_pool->connstr = PGXCNodeConnStr(info.hostname,
														 info.port,
														 node_pool->parent->db_info.database,
														 node_pool->parent->db_info.user_name,
														 node_pool->parent->db_info.pgoptions,
														 IsGTMNode() ? "AGTM" : "coordinator");
					MemoryContextSwitchTo(old_context);
				}PG_CATCH();
				{
					node_pool = hash_search(agent->db_pool->htab_nodes, &info, HASH_REMOVE, NULL);
					if (node_pool)
					{
						if (node_pool->connstr)
							pfree(node_pool->connstr);
						if (node_pool->hostinfo.hostname != info.hostname)
							pfree(node_pool->hostinfo.hostname);
					}
					PG_RE_THROW();
				}PG_END_TRY();
				node_pool->last_idle = 0;
				dlist_init(&node_pool->uninit_slot);
				dlist_init(&node_pool->released_slot);
				dlist_init(&node_pool->idle_slot);
				dlist_init(&node_pool->busy_slot);
			}
			Assert(match_host_info(&info, &node_pool->hostinfo, sizeof(info)) == 0);

			/*
			* we append NULL value to list_wait first
			*/
			old_context = MemoryContextSwitchTo(agent->mctx);
			agent->list_wait = lappend(agent->list_wait, NULL);
			MemoryContextSwitchTo(old_context);

			/* first find released by this agent */
			slot = NULL;
			dlist_foreach(iter, &node_pool->released_slot)
			{
				tmp_slot = dlist_container(ADBNodePoolSlot, dnode, iter.cur);
				AssertState(tmp_slot->slot_state == SLOT_STATE_RELEASED);
				if(tmp_slot->last_user_pid == agent->pid)
				{
					AssertState(tmp_slot->owner == agent);
					slot = tmp_slot;
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d got released slot %p", agent, agent->pid, slot),
							 PMGR_BACKTRACE_DETIAL()));
					break;
				}
			}

			/* second find idle slot */
			if(slot == NULL)
			{
				dlist_foreach(iter, &node_pool->idle_slot)
				{
					tmp_slot = dlist_container(ADBNodePoolSlot, dnode, iter.cur);
					AssertState(tmp_slot->slot_state == SLOT_STATE_IDLE);
					if(tmp_slot->owner == NULL)
					{
						slot = tmp_slot;
						slot->last_agtm_port = 0;
						slot->last_user_pid = 0;
						ereport(PMGRLOG,
								(errmsg("agent %p pid %d got idle slot %p", agent, agent->pid, slot),
								 PMGR_BACKTRACE_DETIAL()));
						break;
					}
				}
			}

			/* not found, we use a uninit slot */
			if(slot == NULL)
			{
				dlist_foreach(iter, &node_pool->uninit_slot)
				{
					tmp_slot = dlist_container(ADBNodePoolSlot, dnode, iter.cur);
					AssertState(tmp_slot->slot_state == SLOT_STATE_UNINIT);
					if(tmp_slot->owner == NULL)
					{
						slot = tmp_slot;
						slot->last_agtm_port = 0;
						slot->last_user_pid = 0;
						ereport(PMGRLOG,
								(errmsg("agent %p pid %d got uninit slot %p", agent, agent->pid, slot),
								 PMGR_BACKTRACE_DETIAL()));
						break;
					}
				}
			}

			/* not got any slot, we alloc a new slot */
			if(slot == NULL)
			{
				slot = MemoryContextAllocZero(PoolerMemoryContext, sizeof(*slot));
				slot->parent = node_pool;
				slot->slot_state = SLOT_STATE_UNINIT;
				INIT_SLOT_PARAMS_MAGIC(slot, session_magic);
				INIT_SLOT_PARAMS_MAGIC(slot, local_magic);
				dlist_push_head(&node_pool->uninit_slot, &slot->dnode);
				SET_SLOT_LIST(slot, UNINIT_SLOT);
				ereport(PMGRLOG,
						(errmsg("agent %p pid %d alloc new slot %p", agent, agent->pid, slot),
						 PMGR_BACKTRACE_DETIAL()));
			}

			/* save slot into agent waiting list */
			Assert(slot != NULL);
			llast(agent->list_wait) = slot;

			if(slot->slot_state == SLOT_STATE_UNINIT)
			{
				static PGcustumFuns funs = {NULL, NULL, NULL, pq_custom_msg};
				Assert(node_pool->connstr != NULL);
				slot->conn = PQconnectStart(node_pool->connstr);
				if(slot->conn == NULL)
				{
					ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY)
						,errmsg("out of memory")));
				}else if(PQstatus(slot->conn) == CONNECTION_BAD)
				{
					ereport(ERROR,
						(errmsg("%s", PQerrorMessage(slot->conn))));
				}
				slot->slot_state = SLOT_STATE_CONNECTING;
				slot->poll_state = PGRES_POLLING_WRITING;
				slot->conn->funs = &funs;
				slot->retry = 0;
				slot->last_retry_time = 0;
				ereport(PMGRLOG,
						(errmsg("agent %p pid %d begin slot %p connect \"%s\"",
								agent, agent->pid, slot, node_pool->connstr),
						 PMGR_BACKTRACE_DETIAL()));
			}

			SET_SLOT_OWNER(slot, agent);
			Assert(slot->current_list != NULL_SLOT);
			dlist_delete(&slot->dnode);
			dlist_push_head(&(node_pool->busy_slot), &(slot->dnode));
			SET_SLOT_LIST(slot, BUSY_SLOT);

			connected_info = hash_search(agent->connected_node,
										 &info,
										 HASH_ENTER,
										 NULL);
			ereport(PMGRLOG,
					(errmsg("agent %p pid %d entry slot %p for %s:%d",
							agent, agent->pid, slot, info.hostname, info.port),
					 PMGR_BACKTRACE_DETIAL()));
			connected_info->slot = slot;
			connected_info->info.hostname = MemoryContextStrdup(agent->mctx, connected_info->info.hostname);
		}
	}PG_CATCH();
	{
		ListCell *lc;
		ADBNodePoolSlot *slot;
		foreach(lc,agent->list_wait)
		{
			slot = lfirst(lc);
			if(slot)
			{
				connected_info = hash_search(agent->connected_node,
											 &slot->parent->hostinfo,
											 HASH_REMOVE,
											 NULL);
				if (connected_info != NULL)
				{
					Assert(connected_info->slot == slot);
					ereport(PMGRLOG,
							(errmsg("agent %p pid %d removed slot %p for %s:%d",
									agent, agent->pid, slot, connected_info->info.hostname, connected_info->info.port),
							 PMGR_BACKTRACE_DETIAL()));
					if (connected_info->info.hostname != slot->parent->hostinfo.hostname)
						pfree(connected_info->info.hostname);
					MemSet(connected_info, 0, sizeof(*connected_info));
				}
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(&slot->dnode);
				SET_SLOT_LIST(slot, NULL_SLOT);
				idle_slot(slot, true);
			}
		}
		list_free(agent->list_wait);
		agent->list_wait = NIL;
		PG_RE_THROW();
	}PG_END_TRY();
}

static int agent_session_command(PoolAgent *agent, const char *set_command, PoolCommandType command_type, StringInfo errMsg)
{
	char **ppstr;
	ConnectedInfo *info;
	HASH_SEQ_STATUS hseq;
	int res;
	AssertArg(agent);
	if(command_type == POOL_CMD_TEMP)
	{
		agent->is_temp = true;
		hash_seq_init(&hseq, agent->connected_node);
		while ((info=hash_seq_search(&hseq)) != NULL)
		{
			info->slot->has_temp = true;
		}
		return 0;
	}else if(set_command == NULL)
	{
		return 0;
	}else if(command_type != POOL_CMD_LOCAL_SET
		&& command_type != POOL_CMD_GLOBAL_SET)
	{
		return -1;
	}
	Assert(set_command && (command_type == POOL_CMD_LOCAL_SET || command_type == POOL_CMD_GLOBAL_SET));

	/* params = "params;set_command" */
	if(command_type == POOL_CMD_LOCAL_SET)
	{
		ppstr = &(agent->local_params);
	}else
	{
		ppstr = &(agent->session_params);
	}
	if(*ppstr == NULL)
	{
		*ppstr = MemoryContextStrdup(agent->mctx, set_command);
	}else
	{
		*ppstr = repalloc(*ppstr, strlen(*ppstr) + strlen(set_command) + 2);
		strcat(*ppstr, ";");
		strcat(*ppstr, set_command);
	}
	if(command_type == POOL_CMD_LOCAL_SET)
		UPDATE_PARAMS_MAGIC(agent, local_magic);
	else
		UPDATE_PARAMS_MAGIC(agent, session_magic);

	/*
	 * Launch the new command to all the connections already hold by the agent
	 * It does not matter if command is local or global as this has explicitely been sent
	 * by client. PostgreSQL backend also cannot send to its pooler agent SET LOCAL if current
	 * transaction is not in a transaction block. This has also no effect on local Coordinator
	 * session.
	 */
	res = 0;
	hash_seq_init(&hseq, agent->connected_node);
	while((info=hash_seq_search(&hseq)) != NULL)
	{
		if (pool_exec_set_query(info->slot->conn, set_command, errMsg) == false)
			res = 1;
	}
	return res;
}

static uint32 hash_any_v(const void *key, int keysize, ...)
{
	uint32 h;
	va_list args;

	h = DatumGetUInt32(hash_any((const unsigned char *)key, keysize));

	va_start(args, keysize);
	while((key=va_arg(args, void*)) != NULL)
	{
		keysize = va_arg(args, int);
		h = (h << 1) | (h & 0x80000000 ? 1:0);
		h ^= DatumGetUInt32(hash_any((const unsigned char *)key, keysize));
	}
	va_end(args);

	return h;
}

static uint32 hash_database_info(const void *key, Size keysize)
{
	const DatabaseInfo *info = key;
	AssertArg(info != NULL && keysize == sizeof(*info));

	return hash_any_v(info->database, strlen(info->database),
					  info->user_name, strlen(info->user_name),
					  info->pgoptions, strlen(info->pgoptions),
					  NULL);
}

static int match_database_info(const void *key1, const void *key2, Size keysize)
{
	const DatabaseInfo *l,*r;
	int rval;
	Assert(keysize == sizeof(DatabaseInfo));

	l = key1;
	r = key2;

	if (NULL == l->database || NULL == r->database ||
			NULL == l->user_name || NULL == r->user_name ||
			NULL == l->pgoptions || NULL == r->pgoptions)
		ereport(ERROR, (errmsg("match_database_info some parameters is null")));

	rval = strcmp(l->database, r->database);
	if(rval != 0)
		return rval;

	rval = strcmp(l->user_name, r->user_name);
	if(rval != 0)
		return rval;

	rval = strcmp(l->pgoptions, r->pgoptions);
	if(rval != 0)
		return rval;
	return 0;
}

static uint32 hash_host_info(const void *key, Size keysize)
{
	const HostInfo *info = key;
	Assert(info != NULL && keysize == sizeof(*info));

	return hash_any_v(info->hostname, strlen(info->hostname),
					  &info->port, sizeof(info->port),
					  NULL);
}

static int match_host_info(const void *key1, const void *key2, Size keysize)
{
	const HostInfo *l = key1;
	const HostInfo *r = key2;
	int rval;

	if (l == r)
		return 0;

	if (l->hostname != r->hostname &&
		(rval=strcmp(l->hostname, r->hostname)) != 0)
		return rval;

	if (l->port < r->port)
		return -1;
	else if (l->port > r->port)
		return 1;

	return 0;
}

static void create_htab_database(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(DatabaseInfo);
	hctl.entrysize = sizeof(DatabasePool);
	hctl.hash = hash_database_info;
	hctl.match = match_database_info;
	hctl.hcxt = TopMemoryContext;
	htab_database = hash_create("hash DatabasePool", 97, &hctl
			, HASH_ELEM|HASH_FUNCTION|HASH_COMPARE|HASH_CONTEXT);
}

static void destroy_htab_database(void)
{
	DatabasePool *pool;
	HASH_SEQ_STATUS status;

	if(htab_database == NULL)
		return;

	for(;;)
	{
		hash_seq_init(&status, htab_database);
		pool = hash_seq_search(&status);
		if(pool == NULL)
			break;
		hash_seq_term(&status);
		destroy_database_pool(pool, true);
	}
	hash_destroy(htab_database);
	htab_database = NULL;
}

/*---------------------------------------------------------------------------*/

static void pool_end_flush_msg(PoolPort *port, StringInfo buf)
{
	AssertArg(port && buf);
	pool_putmessage(port, (char)(buf->cursor), buf->data, buf->len);
	pfree(buf->data);
	pool_flush(port);
}

static void pool_sendstring(StringInfo buf, const char *str)
{
	if(str)
	{
		pq_sendbyte(buf, false);
		pq_sendbytes(buf, str, strlen(str)+1);
	}else
	{
		pq_sendbyte(buf, true);
	}
}

static const char *pool_getstring(StringInfo buf)
{
	char *str;
	int slen;
	AssertArg(buf);

	if(pq_getmsgbyte(buf))
		return NULL;
	str = &buf->data[buf->cursor];
	slen = strlen(str);
	if (buf->cursor + slen >= buf->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	buf->cursor += slen + 1;
	return str;
}

static void pool_sendint(StringInfo buf, int ival)
{
	pq_sendbytes(buf, (char*)&ival, 4);
}

static int pool_getint(StringInfo buf)
{
	int ival;
	pq_copymsgbytes(buf, (char*)&ival, 4);
	return ival;
}

static void on_exit_pooler(int code, Datum arg)
{
	if(server_fd != PGINVALID_SOCKET)
	{
		pgsocket new_fd;
		while ((new_fd = accept(server_fd, NULL, NULL)) != PGINVALID_SOCKET)
			closesocket(new_fd);
		closesocket(server_fd);
	}
	while(agentCount)
		agent_destroy(poolAgents[--agentCount]);
	/* destroy agents and MemoryContext*/
	destroy_htab_database();
}

static bool pool_exec_set_query(PGconn *conn, const char *query, StringInfo errMsg)
{
	PGresult *result;
	ExecStatusType status;
	bool res;

	AssertArg(query);
	if(!PQsendQuery(conn, query))
		return false;

	res = true;
	for(;;)
	{
		if(pool_wait_pq(conn) < 0)
		{
			res = false;
			break;
		}
		result = PQgetResult(conn);
		if(result == NULL)
			break;
		status = PQresultStatus(result);
		if(status != PGRES_COMMAND_OK)
		{
			if(errMsg)
			{
				if(errMsg->data == NULL)
					initStringInfo(errMsg);
				if(status == PGRES_FATAL_ERROR)
				{
					if(res)	/* first error */
						appendStringInfoString(errMsg, PQresultErrorMessage(result));
				}else
				{
					if(res)	/* first error */
						appendStringInfo(errMsg,
										 "execute \"%s\" expect %s, but return %s\n",
										 query,
										 PQresStatus(PGRES_COMMAND_OK),
										 PQresStatus(status));
					if (status == PGRES_COPY_BOTH ||
						status == PGRES_COPY_IN)
					{
						PQputCopyEnd(conn, NULL);
					}else if(status == PGRES_COPY_OUT)
					{
						const char *buf;
						PQgetCopyDataBuffer(conn, &buf, true);
					}
				}
			}
			res = false;
		}
		PQclear(result);
	}
	return res;
}

/* return
 * < 0: EOF or timeout
 */
static int pool_wait_pq(PGconn *conn)
{
	time_t finish_time;
	AssertArg(conn);

	if(conn->inEnd > conn->inStart)
		return 1;

	if(PoolRemoteCmdTimeout == 0)
	{
		finish_time = (time_t)-1;
	}else
	{
		Assert(PoolRemoteCmdTimeout > 0);
		finish_time = time(NULL);
		finish_time += PoolRemoteCmdTimeout;
	}
	return pqWaitTimed(1, 0, conn, finish_time);
}

static int pq_custom_msg(PGconn *conn, char id, int msgLength)
{
	switch (id)
	{
		case 'U':
			conn->inCursor += msgLength;
			return 0;
		default:
			break;
	}
	return -1;
}

static void close_idle_connection(void)
{
	time_t cur_time;
	cur_time = time(NULL);
	close_timeout_idle_slots(cur_time + pool_time_out);

	/* to test idle slot, never run in common*/
	if(false)
		check_idle_slot();
}

static void check_idle_slot(void)
{
	HASH_SEQ_STATUS hash_database_stats;
	HASH_SEQ_STATUS hash_nodepool_status;
	DatabasePool *db_pool;
	ADBNodePool *node_pool;
	ADBNodePoolSlot *slot;
	dlist_mutable_iter miter;

	if(htab_database == NULL)
		return;

	hash_seq_init(&hash_database_stats, htab_database);
	while((db_pool = hash_seq_search(&hash_database_stats)) != NULL)
	{
		hash_seq_init(&hash_nodepool_status, db_pool->htab_nodes);
		while((node_pool = hash_seq_search(&hash_nodepool_status)) != NULL)
		{
			dlist_foreach_modify(miter, &node_pool->idle_slot)
			{
				slot = dlist_container(ADBNodePoolSlot, dnode, miter.cur);
				Assert(slot->slot_state == SLOT_STATE_IDLE);
				Assert(slot->current_list != NULL_SLOT);
				dlist_delete(miter.cur);
				SET_SLOT_LIST(slot, NULL_SLOT);
				destroy_slot(slot, false);
			}
		}
	}
}

Datum pool_close_idle_conn(PG_FUNCTION_ARGS)
{
	StringInfoData buf;

	if (!(IS_PGXC_COORDINATOR || IsConnFromCoord()))
		PG_RETURN_BOOL(true);

re_try_:
	if (!poolHandle)
		PoolManagerReconnect();
	Assert(poolHandle != NULL);

	pq_beginmessage(&buf, PM_MSG_CLOSE_IDLE_CONNECT);
	/* send message */
	pool_putmessage(&poolHandle->port, (char)(buf.cursor), buf.data, buf.len);
	if (pool_flush(&poolHandle->port)||
		poolHandle->port.SendPointer != 0)
	{
		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
		pfree(buf.data);
		goto re_try_;
	}

	pfree(buf.data);
	PG_RETURN_BOOL(true);
}
