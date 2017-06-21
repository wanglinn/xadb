#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>

#include "rdc_globals.h"
#include "rdc_exit.h"
#include "rdc_comm.h"
#include "rdc_msg.h"
#include "getopt_long.h"
#include "utils/memutils.h"		/* for MemoryContext */

#include <poll.h>

static const char	   *progname;
ReduceOptions 			MyReduceOpts = NULL;
int						MyReduceId = -1;
#define 				MAXLISTEN	64
pgsocket				MyListenSock[MAXLISTEN];
int						MyListenNum = 0;

pgsocket				MyParentSock = PGINVALID_SOCKET;
pgsocket				MyLogSock = PGINVALID_SOCKET;
int						MyListenPort = 0;

static volatile bool	reduce_group_ready = false;

static void InitReduceOptions(void);
static void FreeReduceOptions(int code, Datum arg);
static void ParseReduceOptions(int argc, char * const argvs[]);
static void Usage(bool exit_success);
static int  ReduceListenInet(void);
static void CloseReduceListenSocket(int code, Datum arg);
static void ResetReduceGroup(void);
static void DropReduceGroup(void);
static void DropPlanGroup(void);
#ifdef DEBUG_ADB
static void DropReduceNodeInfo(void);
#endif
static void ReduceCancelHandler(SIGNAL_ARGS);
static void ReduceDieHandler(SIGNAL_ARGS);
static void SetReduceSignals(void);
static void WaitForReduceGroupReady(void);
static bool IsReduceGroupReady(RdcPort **rdc_nodes, int rdc_num);
static int PrepareConstSocks(struct pollfd *fds, int begin);
static int PrepareSetupGroup(RdcPort **rdc_nodes,	/* IN/OUT */
							 struct pollfd *fds,	/* IN/OUT */
							 RdcPortType expected,	/* IN */
							 int begin,				/* IN */
							 List **rdc_list,		/* IN/OUT */
							 List **poll_list);		/* OUT */
static void SetupReduceGroup(RdcPort *port);
static bool BackendIsAlive(void);
static void ReduceAcceptPlanConn(List **accept_list, List **pln_list);
static int  ReduceLoopRun(void);

static void
InitReduceOptions(void)
{
	if (MyReduceOpts == NULL)
		MyReduceOpts = (ReduceOptions) palloc(sizeof(*MyReduceOpts));

	MemSet(MyReduceOpts, 0, sizeof(*MyReduceOpts));
	MyReduceOpts->lhost = NULL;
	MyReduceOpts->lport = 0;
	MyReduceOpts->work_mem = 1024;
	MyReduceOpts->log_min_messages = WARNING;

	/* don't forget free Reduce options */
	on_rdc_exit(FreeReduceOptions, 0);
}

static void
FreeReduceOptions(int code, Datum arg)
{
	rdc_freeport(MyReduceOpts->parent_watch);
	rdc_freeport(MyReduceOpts->log_watch);
#ifdef DEBUG_ADB
	DropReduceNodeInfo();
#endif
	DropReduceGroup();
	DropPlanGroup();
	safe_pfree(MyReduceOpts->lhost);
	safe_pfree(MyReduceOpts);

	return ;
}

static void
ParseReduceOptions(int argc, char * const argvs[])
{
	int			c;
	int			optindex;
	static struct option long_options[] = {
		{"reduce_id", required_argument, NULL, 'n'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"parent_watch", required_argument, NULL, 'W'},
		{"log_watch", required_argument, NULL, 'L'},
		{"log_min_messages", required_argument, NULL, 'l'},
		{"work_mem", required_argument, NULL, 'S'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};

	while ((c = getopt_long(argc, argvs, "n:h:p:l:S:W:L:?", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'n':
				MyReduceId = atoi(optarg);
				if (MyReduceId < 0)
					elog(ERROR, "Invalid Reduce ID number: %d", MyReduceId);
				break;
			case 'h':
				MyReduceOpts->lhost = pstrdup(optarg);
				break;
			case 'p':
				MyReduceOpts->lport = atoi(optarg);
				if (MyReduceOpts->lport > 65535 || MyReduceOpts->lport < 1024)
					elog(ERROR, "Invalid listen port: %d", MyReduceOpts->lport);
				break;
			case 'S':
				MyReduceOpts->work_mem = atoi(optarg);
				/* should i check it? */
				break;
			case 'l':
				MyReduceOpts->log_min_messages = atoi(optarg);
				/* should i check it? */
				break;
			case 'W':
				MyParentSock = atoi(optarg);
				MyReduceOpts->parent_watch = rdc_newport(MyParentSock);
				MyReduceOpts->parent_watch->type = TYPE_BACKEND;
				break;
			case 'L':
				MyLogSock = atoi(optarg);
				MyReduceOpts->log_watch = rdc_newport(MyLogSock);
				MyReduceOpts->log_watch->type = TYPE_BACKEND;
				break;
			case '?':
				Usage(true);
				break;
			default:
				Usage(false);
				break;
		}
	}

	if (optind < argc)
		Usage(false);

	if (MyReduceId < 0)
		Usage(false);
}

static void
Usage(bool exit_success)
{
	FILE *fd = exit_success ? stdout : stderr;

	fprintf(fd, "%s - Receive and dispatch data among Reduce Cluster.\n\n",
			progname);
	fprintf(fd, "Usage:\n");
	fprintf(fd, "  %s [OPTION]\n", progname);
	fprintf(fd, "\nOptions:\n");
	fprintf(fd, "  -n, --reduce_id=RID              set the reduce ID number, it is requisite\n");
	fprintf(fd, "  -h, --host=HOSTNAME              local server host(default: 0.0.0.0)\n");
	fprintf(fd, "  -p, --port=PORT                  local server port(default: 0)\n");
	fprintf(fd, "  -w, --parent_watch=SOCK          file descriptor from parent process for interprocess communication\n");
	fprintf(fd, "  -l, --log_watch=SOCK             file descriptor from parent process for log record\n");
	fprintf(fd, "  -S, --work_mem=WORKMEM           set amount of memory for tupstore (in kB)\n");
	fprintf(fd, "  -d, --log_min_messages=elevel    set the message levels that are logged\n");
	fprintf(fd, "  -?, --help                       show this help, then exit\n");

	exit(exit_success ? EXIT_SUCCESS: EXIT_FAILURE);
}

static int
ReduceListenInet(void)
{
	int					i;
	int 				fd;
	struct addrinfo		hint;
	char				portNumberStr[32];
	char			   *service;
	struct addrinfo	   *addrs = NULL,
					   *addr;
	int					ret;
	int					err;
	int					listen_index = 0;
#if !defined(WIN32) || defined(IPV6_V6ONLY)
	int					one = 1;
#endif

	/*
	 * Establish input sockets.
	 *
	 * First, mark them all closed, and set up an on_proc_exit function that's
	 * charged with closing the sockets again at exit.
	 */
	for (i = 0; i < MAXLISTEN; i++)
		MyListenSock[i] = PGINVALID_SOCKET;

	/* don't forget close listen socket */
	on_rdc_exit(CloseReduceListenSocket, 0);

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = AF_INET;
	hint.ai_flags = AI_PASSIVE;
	hint.ai_socktype = SOCK_STREAM;

	snprintf(portNumberStr, sizeof(portNumberStr), "%d", MyReduceOpts->lport);
	service = portNumberStr;

	ret = getaddrinfo(MyReduceOpts->lhost, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (MyReduceOpts->lhost)
			ereport(LOG,
					(errmsg("could not translate host name \"%s\", service \"%s\" to address: %s",
							MyReduceOpts->lhost, service, gai_strerror(ret))));
		else
			ereport(LOG,
				 (errmsg("could not translate service \"%s\" to address: %s",
						 service, gai_strerror(ret))));
		if (addrs)
			freeaddrinfo(addrs);
		return STATUS_ERROR;
	}

	for (addr = addrs; addr; addr = addr->ai_next)
	{
		/* See if there is still room to add 1 more socket. */
		for (; listen_index < MAXLISTEN; listen_index++)
		{
			if (MyListenSock[listen_index] == PGINVALID_SOCKET)
				break;
		}
		if (listen_index >= MAXLISTEN)
		{
			ereport(LOG,
					(errmsg("could not bind to all requested addresses: MAXLISTEN (%d) exceeded",
							MAXLISTEN)));
			break;
		}

		if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not create %s socket: %m",
							_("IPv4"))));
			continue;
		}

		if (!pg_set_noblock(fd))
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("set noblocking mode failed: %m")));
			continue;
		}

#ifndef WIN32

		/*
		 * Without the SO_REUSEADDR flag, a new reduce can't be started
		 * right away after a stop or crash, giving "address already in use"
		 * error on TCP ports.
		 *
		 * On win32, however, this behavior only happens if the
		 * SO_EXLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows multiple
		 * servers to listen on the same address, resulting in unpredictable
		 * behavior. With no flags at all, win32 behaves as Unix with
		 * SO_REUSEADDR.
		 */
		if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
						(char *) &one, sizeof(one))) == -1)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("setsockopt(SO_REUSEADDR) failed: %m")));
			closesocket(fd);
			continue;
		}
#endif

		/*
		 * Note: This might fail on some OS's, like Linux older than
		 * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
		 * ipv4 addresses to ipv6.  It will show ::ffff:ipv4 for all ipv4
		 * connections.
		 */
		err = bind(fd, addr->ai_addr, addr->ai_addrlen);
		if (err < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not bind %s socket: %m",
							_("IPv4")),
				  errhint("Is another reduce already running on port %d?"
						  " If not, wait a few seconds and retry.",
						  (int) MyReduceOpts->lport)));
			closesocket(fd);
			continue;
		}

		err = listen(fd, PG_SOMAXCONN);
		if (err < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: %s is IPv4, IPv6, or Unix */
					 errmsg("could not listen on %s socket: %m",
							_("IPv4"))));
			closesocket(fd);
			continue;
		}
		MyListenSock[listen_index] = fd;
		MyListenNum++;
	}

	freeaddrinfo(addrs);

	if (!MyListenNum)
		return STATUS_ERROR;

	return STATUS_OK;
}

static void
CloseReduceListenSocket(int code, Datum arg)
{
	int		i;

	for (i = 0; i < MAXLISTEN; i++)
	{
		if (MyListenSock[i] != PGINVALID_SOCKET)
		{
			elog(LOG, "close listen socket: %d", MyListenSock[i]);
			closesocket(MyListenSock[i]);
			MyListenSock[i] = PGINVALID_SOCKET;
		}
	}
}

static void
ResetReduceGroup(void)
{
	RdcPort		   *port;
	int				i;

	for (i = 0; i < MyReduceOpts->rdc_num; i++)
	{
		if (i == MyReduceId)
			continue;
		port = MyReduceOpts->rdc_nodes[i];
		RdcWaitEvent(port) = WE_SOCKET_READABLE;
		elog(LOG,
			 "reset [%s %d] {%s:%s}",
			 RdcTypeStr(port), RdcID(port),
			 RdcHostStr(port), RdcPortStr(port));
		rdc_resetport(port);
	}
}

static void
DropReduceGroup(void)
{
	int			i;
	RdcPort	   *port;

	for (i = 0; i < MyReduceOpts->rdc_num; i++)
	{
		if (i == MyReduceId ||
			MyReduceOpts->rdc_nodes[i] == NULL)
			continue;

		port = MyReduceOpts->rdc_nodes[i];
		elog(LOG,
			 "free [%s %d] {%s:%s}",
			 RdcTypeStr(port), RdcID(port),
			 RdcHostStr(port), RdcPortStr(port));
		rdc_freeport(port);
	}
	safe_pfree(MyReduceOpts->rdc_nodes);
}

static void
DropPlanGroup(void)
{
	ListCell	   *cell = NULL;
	PlanPort	   *pln_port = NULL;

	foreach (cell, MyReduceOpts->pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(cell);
		elog(LOG,
			 "free [PLAN %d]", pln_port->pln_id);
		plan_freeport(pln_port);
	}
	list_free(MyReduceOpts->pln_nodes);
	MyReduceOpts->pln_nodes = NIL;
}

#ifdef DEBUG_ADB
static void
DropReduceNodeInfo(void)
{
	int				i;
	ReduceInfo	   *ninfo = NULL;

	for (i = 0; i < MyReduceOpts->rdc_num; i++)
	{
		ninfo = &(MyReduceOpts->rdc_infos[i]);
		elog(LOG,
			 "free node info [REDUCE %d] {%s:%d}",
			 ninfo->rnid,
			 ninfo->host,
			 ninfo->port);
		safe_pfree(ninfo->host);
	}
	safe_pfree(MyReduceOpts->rdc_infos);
	MyReduceOpts->rdc_infos = NULL;
}
#endif

int main(int argc, char* const argvs[])
{
	set_pglocale_pgservice(argvs[0], TEXTDOMAIN);

	progname = get_progname(argvs[0]);

	/* Initialize TopMemoryContext and ErrorContext */
	MemoryContextInit();

	/* Initialize Reduce options */
	InitReduceOptions();

	/* parse options */
	ParseReduceOptions(argc, argvs);

	/* start listen */
	if (ReduceListenInet() != STATUS_OK ||
		MyListenSock[0] == PGINVALID_SOCKET)
	{
		ereport(ERROR,
				(errmsg("could not create listen socket for \"%s : %d\"",
						MyReduceOpts->lhost, MyReduceOpts->lport)));
	}

	/* set signals */
	SetReduceSignals();

	/* wait for reduce group be ready */
	WaitForReduceGroupReady();

	/* loop run */
	rdc_exit(ReduceLoopRun());

	return 0;	/* never reach here */
}

/*
 * Handler for SIGINT
 */
static void
ReduceCancelHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of rdc_exit
	 */
	if (!rdc_exit_inprogress)
	{
		InterruptPending = true;
		QueryCancelPending = true;
	}

	errno = save_errno;
}

/*
 * Handler for SIGTERM/SIGQUIT
 */
static void
ReduceDieHandler(SIGNAL_ARGS)
{
	int 		save_errno = errno;

	/* Don't joggle the elbow of rdc_exit */
	if (!rdc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	/*
	 * If we're in single user mode, we want to quit immediately - we can't
	 * rely on latches as they wouldn't work when stdin/stdout is a file.
	 * Rather ugly, but it's unlikely to be worthwhile to invest much more
	 * effort just for the benefit of single user mode.
	 */
	/*if (DoingCommandRead && whereToSendOutput != DestRemote)
		ProcessInterrupts();*/

	errno = save_errno;
}

static void
ReduceGroupHook(SIGNAL_ARGS)
{
	if (MyReduceOpts && !MyReduceOpts->parent_watch)
	{
		RdcPort		*port;
		StringInfoData	buf;
		int			rdc_num = 5;
		int			i;
		uint32		n32;
		const char	*host[] = {"localhost", "localhost", "localhost", "localhost", "localhost",
							   "localhost", "localhost", "localhost", "localhost", "localhost"};
		int			portnum[] = {9000, 9001, 9002, 9003, 9004,
								 9005, 9006, 9007, 9008, 9009};

		rdc_num = sizeof(portnum)/sizeof(portnum[0]);
		port = rdc_newport(PGINVALID_SOCKET);
		MyReduceOpts->parent_watch = port;
		initStringInfo(&buf);
		rdc_beginmessage(&buf, RDC_GROUP_RQT);
		rdc_sendint(&buf, rdc_num, sizeof(rdc_num));
		for (i = 0; i < rdc_num; i++)
		{
			rdc_sendstring(&buf, host[i]);
			rdc_sendint(&buf, portnum[i], sizeof(portnum[i]));
		}
		n32 = htonl((uint32)(buf.len - 1));
		memcpy(&buf.data[1], &n32, sizeof(n32));
		appendBinaryStringInfo(&(port->in_buf), buf.data, buf.len);
		pfree(buf.data);
		buf.data = NULL;
	}
}

static void
SetReduceSignals(void)
{
	pqsignal(SIGINT, ReduceCancelHandler);
	pqsignal(SIGTERM, ReduceDieHandler);
	pqsignal(SIGQUIT, ReduceDieHandler);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGCHLD, SIG_IGN);
	pqsignal(SIGUSR1, ReduceGroupHook);		/* for test */
	pqsignal(SIGUSR2, SIG_IGN);
}

static void
WaitForReduceGroupReady(void)
{
	char		firstchar;
	RdcPort	   *port = MyReduceOpts->parent_watch;

	while (!reduce_group_ready)
	{
		CHECK_FOR_INTERRUPTS();

		port = MyReduceOpts->parent_watch;
		if (!port)
		{
			pg_usleep(1000000);
			continue;
		}

		firstchar = rdc_getmessage(port, 0);
		switch (firstchar)
		{
			case RDC_GROUP_RQT:
				{
					SetupReduceGroup(port);
					reduce_group_ready = true;
				}
				break;
			case EOF:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("fail to set up reduce group"),
						 errhint("%s", RdcError(port))));
				break;
		}
	}
}

static bool
IsReduceGroupReady(RdcPort **rdc_nodes, int rdc_num)
{
	int		i;
	bool	ready = true;
	RdcPort	*port;

	AssertArg(rdc_nodes);
	Assert(rdc_num > 0);

	for (i = 0; i < rdc_num; i++)
	{
		if (i == MyReduceId)
			continue;

		port = rdc_nodes[i];
		if (!port)
		{
			ready = false;
			break;
		}

		if (RdcStatus(port) == CONNECTION_BAD)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("fail to connect or to be "
							"connected [%s %d] {%s:%s}: %s",
							RdcTypeStr(port), RdcID(port),
							RdcHostStr(port), RdcPortStr(port),
							RdcError(port))));
		}
	}

	return ready;
}

static int
PrepareConstSocks(struct pollfd *fds, int begin)
{
	int nfds;

	for (nfds = 0; nfds < MyListenNum; nfds++)
	{
		fds[begin].fd = MyListenSock[nfds];
		fds[begin].events = POLLIN;
		begin++;
	}
	if (MyParentSock != PGINVALID_SOCKET)
	{
		fds[begin].fd = MyParentSock;
		fds[begin].events = POLLIN;
		begin++;
	}

	return begin;
}

static int
PrepareSetupGroup(RdcPort **rdc_nodes,	/* IN/OUT */
				  struct pollfd *fds,	/* IN/OUT */
				  RdcPortType expected,	/* IN */
				  int begin,			/* IN */
				  List **rdc_list,		/* IN/OUT */
				  List **poll_list)		/* OUT */
{
	RdcPort		   *port = NULL;
	ListCell	   *cell = NULL;

	if (rdc_list == NULL)
		return begin;

	for (cell = list_head(*rdc_list); cell != NULL;)
	{
		port = (RdcPort *) lfirst(cell);
		Assert(port);
		cell = lnext(cell);

		/* skip it which is not my concerned */
		if (!(RdcType(port) & expected))
		{
			/* dump it if it is bad connection */
			if (RdcStatus(port) == CONNECTION_BAD)
			{
				*rdc_list = lremove(port, *rdc_list);
				rdc_freeport(port);
			}
			continue;
		}

		if (RdcStatus(port) == CONNECTION_BAD)
			ereport(ERROR,
					(errmsg("fail to connect or to be "
							"connected [%s %d] {%s:%s}: %s",
							RdcTypeStr(port), RdcID(port),
							RdcHostStr(port), RdcPortStr(port),
							RdcError(port))));

		if (RdcStatus(port) == CONNECTION_OK)
		{
			int port_id = RdcID(port);
			Assert(ReducePortIsValid(port));
			if (rdc_nodes[port_id] == NULL)
			{
				rdc_nodes[port_id] = port;
				*rdc_list = lremove(port, *rdc_list);
			}
			continue;
		}
		Assert(RdcSockIsValid(port));

		fds[begin].fd = RdcSocket(port);
		fds[begin].events = 0;
		if (RdcWaitRead(port))
			fds[begin].events |= POLLIN;
		if (RdcWaitWrite(port))
			fds[begin].events |= POLLOUT;
		if (poll_list)
			*poll_list = lappend(*poll_list, port);
		begin++;
	}

	return begin;
}

static void
SetupReduceGroup(RdcPort *port)
{
	int					rdc_num = 0;
	int					i;
	int					nready = 0;
	List			   *accept_list = NIL;
	List			   *connect_list = NIL;
	List			   *poll_list = NIL;
#ifdef DEBUG_ADB
	ReduceInfo		   *rdc_infos = NULL;
#endif
	ListCell		   *cell = NULL;
	RdcPort			  **rdc_nodes = NULL;

	struct pollfd	   *fds;
	int					nfds, max_nfds, cur_nfds;
	int					timeout = 10;		/* 10 milliseconds */

	if (rdc_parse_group(port, &rdc_num,
#ifdef DEBUG_ADB
						&rdc_infos,
#endif
						&connect_list) != STATUS_OK)
	{
		ereport(ERROR,
				(errmsg("fail to set up reduce group"),
				 errhint("%s", RdcError(port))));
	}

	rdc_nodes = (RdcPort **)palloc0(rdc_num * sizeof(RdcPort *));
	MyReduceOpts->rdc_nodes = rdc_nodes;
	MyReduceOpts->rdc_num = rdc_num;
#ifdef DEBUG_ADB
	MyReduceOpts->rdc_infos = rdc_infos;
#endif

	max_nfds = MyListenNum + 1 + rdc_num;
	fds = (struct pollfd *) palloc(max_nfds * sizeof(struct pollfd));

	for (;;)
	{
		list_free(poll_list);
		poll_list = NIL;

		CHECK_FOR_INTERRUPTS();

		/* all Reduce are ready */
		if (IsReduceGroupReady(rdc_nodes, rdc_num))
		{
			ereport(LOG,
					(errmsg("Reduce group network is OK")));
			break;
		}

		cur_nfds = MyListenNum + 1 + list_length(connect_list) +
				   list_length(accept_list);
		if (cur_nfds > max_nfds)
		{
			max_nfds = cur_nfds;
			fds = (struct pollfd *)
				repalloc(fds, max_nfds * sizeof(struct pollfd));
		}

		nfds = PrepareConstSocks(fds, 0);
		nfds = PrepareSetupGroup(rdc_nodes, fds, TYPE_REDUCE, nfds,
							&connect_list, &poll_list);
		nfds = PrepareSetupGroup(rdc_nodes, fds, TYPE_UNDEFINE | TYPE_REDUCE, nfds,
							&accept_list, &poll_list);
_re_poll:
		nready = poll(fds, nfds, timeout);
		CHECK_FOR_INTERRUPTS();
		if (nready < 0)
		{
			if (errno == EINTR)
				goto _re_poll;

			ereport(ERROR,
					(errmsg("fail to poll: %m")));
		} else if (nready == 0)
		{
			/* do nothing if timeout */
		} else
		{
			/* check MyListenSock */
			for (i = 0; i < MyListenNum; i++)
			{
				if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("something wrong with listen socket")));
				/* accept a new connection */
				if (fds[i].revents & POLLIN)
				{
					for (;;)
					{
						RdcPort		*port = NULL;

						port = rdc_accept(MyListenSock[i]);
						if (port == NULL)
							break;

						accept_list = lappend(accept_list, port);
					}
				}
			}

			/* check MyParentSock */
			if (MyParentSock != PGINVALID_SOCKET)
			{
				if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL))
					break;
				if (fds[i].revents & POLLIN && !BackendIsAlive())
					break;
				i++;
			}

			/* check Reduce group nodes */
			foreach (cell, poll_list)
			{
				Assert(lfirst(cell));
				(void) rdc_connect_poll((RdcPort *) lfirst(cell));
			}
		}
	}
	safe_pfree(fds);
	list_free(connect_list);
	list_free(accept_list);
}

static bool
BackendIsAlive(void)
{
	return true;
}

static void
ReduceAcceptPlanConn(List **accept_list, List **pln_list)
{
	RdcPort		   *port;
	ListCell	   *cell;

	Assert(pln_list);
	for (cell = list_head(*accept_list); cell != NULL;)
	{
		port = (RdcPort *) lfirst(cell);
		Assert(port);
		cell = lnext(cell);
		(void) rdc_connect_poll(port);

		/*
		 * Only good connection from Plan Node will be accepted.
		 */
		if (PlanPortIsValid(port) &&
			RdcStatus(port) == CONNECTION_OK)
		{
			*accept_list = lremove(port, *accept_list);
			add_new_plan_port(pln_list, port);
		}

		/*
		 * Connection came from other Reduce will be rejected,
		 * Bad connection will be done the same.
		 */
		if (IsPortForReduce(port) ||
			RdcStatus(port) == CONNECTION_BAD)
		{
			*accept_list = lremove(port, *accept_list);
			rdc_freeport(port);
		}
	}
}

static int
ReduceLoopRun(void)
{
	struct pollfd		   *fds = NULL;
	int						max_num, cur_num;
	nfds_t					nfds;
	int						timeout = -1;
	int						i, ret;
	int						rdc_num;
	PlanPort			   *pln_port;
	RdcPort				   *port;
	RdcPort				  **rdc_nodes = NULL;
	List				   *pln_list = NIL;
	List				   *accept_list = NIL;
	ListCell			   *cell = NULL;
	sigjmp_buf				local_sigjmp_buf;

	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_SIZES);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Drop old plan group, ready to accept new plan */
		DropPlanGroup();

		/* Reset buffer of reduce group and ready to dispatch new plan */
		ResetReduceGroup();

		/*
		 * Now return to normal top-level context and clear ErrorContext
		 * and MessageContext for the next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();
		MemoryContextResetAndDeleteChildren(MessageContext);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	MyReduceOpts->rdc_work_stack = &local_sigjmp_buf;

	Assert(reduce_group_ready);
	Assert(MyReduceOpts->rdc_nodes);
	rdc_nodes = MyReduceOpts->rdc_nodes;
	rdc_num = MyReduceOpts->rdc_num;
	pln_list = MyReduceOpts->pln_nodes;

	/* Check Reduce group again */
	if (!IsReduceGroupReady(rdc_nodes, rdc_num))
		ereport(ERROR,
				(errmsg("Reduce group is crushed")));

	MemoryContextSwitchTo(MessageContext);
	max_num = MyListenNum + 1 + rdc_num +
			  get_plan_port_num(pln_list) +
			  list_length(accept_list);
	fds = (struct pollfd *) palloc(max_num * sizeof(struct pollfd));

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		cur_num = MyListenNum + 1 + rdc_num +
				  get_plan_port_num(pln_list) +
				  list_length(accept_list);
		if (max_num > cur_num)
		{
			max_num = cur_num;
			fds = (struct pollfd *)
				repalloc(fds, max_num * sizeof(struct pollfd));
		}

		nfds = PrepareConstSocks(fds, 0);
		for (i = 0; i < rdc_num; i++)
		{
			if (i == MyReduceId)
				continue;
			port = rdc_nodes[i];
			if (RdcWaitEvent(port) == WE_NONE)
				continue;
			fds[nfds].fd = RdcSocket(port);
			fds[nfds].events = 0;
			if (RdcWaitRead(port))
				fds[nfds].events |= POLLIN;
			if (RdcWaitWrite(port))
				fds[nfds].events |= POLLOUT;
			nfds++;
		}
		foreach(cell, pln_list)
		{
			pln_port = (PlanPort *) lfirst(cell);
			port = pln_port->port;
			while (port != NULL)
			{
				if (RdcWaitEvent(port) == WE_NONE)
					continue;
				fds[nfds].fd = RdcSocket(port);
				fds[nfds].events = 0;
				if (RdcWaitRead(port))
					fds[nfds].events |= POLLIN;
				if (RdcWaitWrite(port))
					fds[nfds].events |= POLLOUT;
				nfds++;
				port = RdcNext(port);
			}
		}
		foreach(cell, accept_list)
		{
			port = (RdcPort *) lfirst(cell);
			Assert(port);
			if (RdcWaitEvent(port) == WE_NONE)
				continue;
			fds[nfds].fd = RdcSocket(port);
			fds[nfds].events = 0;
			if (RdcWaitRead(port))
				fds[nfds].events |= POLLIN;
			if (RdcWaitWrite(port))
				fds[nfds].events |= POLLOUT;
			nfds++;
		}

_re_poll:
		ret = poll(fds, nfds, timeout);
		CHECK_FOR_INTERRUPTS();
		if (ret < 0)
		{
			if (errno == EINTR)
				goto _re_poll;

			ereport(ERROR,
					(errmsg("fail to poll(2): %m")));
		}
		else if (ret == 0)
		{
			/* do nothing if timeout */
		}
		else
		{
			for (i = 0; i < MyListenNum; i++)
			{
				if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL))
					ereport(ERROR,
							(errmsg("something wrong with listen socket: %m")));
				/* accept a new connection */
				if (fds[i].revents & POLLIN)
				{
					for (;;)
					{
						RdcPort		*port = NULL;

						port = rdc_accept(MyListenSock[i]);
						if (port == NULL)
							break;

						accept_list = lappend(accept_list, port);
					}
				}
			}

			if (MyParentSock != PGINVALID_SOCKET)
			{
				if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL))
					break;
				if (fds[i].revents & POLLIN && !BackendIsAlive())
					break;
				i++;
			}

			ReduceAcceptPlanConn(&accept_list, &pln_list);
			rdc_handle_reduce(rdc_nodes, rdc_num, &pln_list);
			rdc_handle_plannode(rdc_nodes, rdc_num, pln_list);
		}
	}
	return STATUS_OK;
}
