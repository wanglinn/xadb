#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "getopt_long.h"
#include "rdc_globals.h"
#include "rdc_exit.h"
#include "rdc_handler.h"
#include "rdc_plan.h"
#include "reduce/rdc_msg.h"
#include "reduce/wait_event.h"
#include "utils/memutils.h"		/* for MemoryContext */

static const char	   *progname;
ReduceOptions 			MyReduceOpts = NULL;
pgsocket				MyListenSock = PGINVALID_SOCKET;
pgsocket				MyParentSock = PGINVALID_SOCKET;
pgsocket				MyLogSock = PGINVALID_SOCKET;
int						MyListenPort = 0;
static volatile bool	reduce_group_ready = false;

static void InitReduceOptions(void);
static void FreeReduceOptions(int code, Datum arg);
static void ParseExtraOptions(char *extra_options);
static void ParseReduceOptions(int argc, char * const argvs[]);
static void Usage(bool exit_success);
static void ReduceListen(void);
static void TransListenPort(void);
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
static void PrepareConnectGroup(RdcPort **rdc_nodes,  /* IN/OUT */
								RdcPortType expected, /* IN */
								List **rdc_list);		/* IN/OUT */
static pgsocket GetRdcPortSocket(void *port);
static uint32 GetRdcPortWaitEvents(void *port);
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
ParseExtraOptions(char *extra_options)
{
	char	   *pname;
	char	   *pval;
	char	   *cp;
	char	   *cp2;

	cp = extra_options;
	while (*cp)
	{
		/* Skip blanks before the parameter name */
		if (isspace((unsigned char) *cp))
		{
			cp++;
			continue;
		}

		/* Get the parameter name */
		pname = cp;
		while (*cp)
		{
			if (*cp == '=')
				break;
			if (isspace((unsigned char) *cp))
			{
				*cp++ = '\0';
				while (*cp)
				{
					if (!isspace((unsigned char) *cp))
						break;
					cp++;
				}
				break;
			}
			cp++;
		}

		/* Check that there is a following '=' */
		if (*cp != '=')
		{
			elog(ERROR,
				 _("missing \"=\" after \"%s\" in extra option string\n"),
				 pname);
		}
		*cp++ = '\0';

		/* Skip blanks after the '=' */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				break;
			cp++;
		}

		/* Get the parameter value */
		pval = cp;

		if (*cp != '\'')
		{
			cp2 = pval;
			while (*cp)
			{
				if (isspace((unsigned char) *cp))
				{
					*cp++ = '\0';
					break;
				}
				if (*cp == '\\')
				{
					cp++;
					if (*cp != '\0')
						*cp2++ = *cp++;
				}
				else
					*cp2++ = *cp++;
			}
			*cp2 = '\0';
		}
		else
		{
			cp2 = pval;
			cp++;
			for (;;)
			{
				if (*cp == '\0')
				{
					elog(ERROR,
						 _("unterminated quoted string in extra option string"));
				}
				if (*cp == '\\')
				{
					cp++;
					if (*cp != '\0')
						*cp2++ = *cp++;
					continue;
				}
				if (*cp == '\'')
				{
					*cp2 = '\0';
					cp++;
					break;
				}
				*cp2++ = *cp++;
			}
		}

		/*
		 * Now that we have the name and the value, store the record.
		 */
		if (strcmp(pname, "log_min_messages") == 0)
			MyReduceOpts->log_min_messages = atoi(pval);
		else if (strcmp(pname, "work_mem") == 0)
			MyReduceOpts->work_mem = atoi(pval);
		else
			elog(ERROR, "invalid extra option \"%s\"", pname);
	}
}

static void
ParseReduceOptions(int argc, char * const argvs[])
{
	int			c;
	int			optindex;
	char	   *extra_options = NULL;
	static struct option long_options[] = {
		{"reduce_id", required_argument, NULL, 'n'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"parent_watch", required_argument, NULL, 'W'},
		{"log_watch", required_argument, NULL, 'L'},
		{"help", no_argument, NULL, '?'},
		{"extra", required_argument, NULL, 'E'},
		{NULL, 0, NULL, 0}
	};

	while ((c = getopt_long(argc, argvs, "n:h:p:E:W:L:?", long_options, &optindex)) != -1)
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
			case 'W':
				MyParentSock = atoi(optarg);
				MyReduceOpts->parent_watch = rdc_newport(MyParentSock, TYPE_BACKEND, InvalidPortId);
				rdc_set_noblock(MyReduceOpts->parent_watch);
				break;
			case 'L':
				MyLogSock = atoi(optarg);
				MyReduceOpts->log_watch = rdc_newport(MyLogSock, TYPE_BACKEND, InvalidPortId);
				break;
			case 'E':
				extra_options = pstrdup(optarg);
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

	if (extra_options)
	{
		ParseExtraOptions(extra_options);
		pfree(extra_options);
	}
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
	fprintf(fd, "  -W, --parent_watch=SOCK          file descriptor from parent process for interprocess communication\n");
	fprintf(fd, "  -L, --log_watch=SOCK             file descriptor from parent process for log record\n");
	fprintf(fd, "  -E, --extra=STRING               extra key-value options, quoted string\n");
	fprintf(fd, "  -?, --help                       show this help, then exit\n");

	fprintf(fd, "\nExtra options:\n");
	fprintf(fd, "  log_min_messages=ELEVEL          set the minimum log level\n");
	fprintf(fd, "  work_mem=WORKMEM                 set amount of memory for tupstore (in kB)\n");

	exit(exit_success ? EXIT_SUCCESS: EXIT_FAILURE);
}

static void
ReduceListen(void)
{
	int					fd;
	int					err;
	struct sockaddr_in	addr_inet;
	socklen_t			addrlen;
	char			   *listen_host = NULL;
#if !defined(WIN32)
	int					one = 1;
#endif

	MyListenSock = PGINVALID_SOCKET;
	MyListenPort = MyReduceOpts->lport;
	/* don't forget close listen socket */
	on_rdc_exit(CloseReduceListenSocket, 0);

	if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == PGINVALID_SOCKET)
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not create socket: %m")));
		goto _error_end;
	}

	if (!pg_set_noblock(fd))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("set noblocking mode failed: %m")));
		goto _error_end;
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
		goto _error_end;
	}
#endif

	/* Create an INET socket address */
	addrlen = sizeof(addr_inet);
	MemSet(&addr_inet, 0, addrlen);
	addr_inet.sin_family = AF_INET;
	addr_inet.sin_port = htons(MyListenPort);
	if (MyReduceOpts->lhost)
		addr_inet.sin_addr.s_addr = inet_addr(MyReduceOpts->lhost);
	else
		addr_inet.sin_addr.s_addr = htonl(INADDR_ANY);

	/*
	 * Note: This might fail on some OS's, like Linux older than
	 * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
	 * ipv4 addresses to ipv6.  It will show ::ffff:ipv4 for all ipv4
	 * connections.
	 */
	err = bind(fd, (struct sockaddr *) &addr_inet, addrlen);
	if (err < 0)
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not bind IPv4 socket: %m"),
				 errhint("Is another reduce already running on port %d?"
						 " If not, wait a few seconds and retry.",
						 MyListenPort)));
		goto _error_end;
	}

	err = listen(fd, PG_SOMAXCONN);
	if (err < 0)
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not listen on IPv4 socket: %m")));
		goto _error_end;
	}

	if (MyListenPort == 0)
	{
		/* Get random listen port */
		MemSet(&addr_inet, 0, addrlen);
		if (getsockname(fd, (struct sockaddr *) &addr_inet, &addrlen) < 0)
		{
			ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("getsockname(2) failed: %m")));
			goto _error_end;
		}
		MyListenPort = ntohs(addr_inet.sin_port);
		listen_host = inet_ntoa(addr_inet.sin_addr);
		elog(LOG, "Listen on {%s:%d}", listen_host, MyListenPort);
	}
	MyListenSock = fd;

	/* OK */
	return ;

_error_end:
	if (fd != PGINVALID_SOCKET)
		closesocket(fd);
	ereport(ERROR,
			(errmsg("could not create listen socket for \"%s : %d\"",
					MyReduceOpts->lhost, MyReduceOpts->lport)));
	rdc_exit(EXIT_FAILURE);
}

static void
TransListenPort(void)
{
	if (MyParentSock != PGINVALID_SOCKET)
	{
		StringInfoData	buf;
		RdcPort		   *parent_watch = MyReduceOpts->parent_watch;

		Assert(MyListenPort > 0);
		Assert(parent_watch);
		rdc_beginmessage(&buf, RDC_LISTEN_PORT);
		rdc_sendint(&buf, MyListenPort, sizeof(MyListenPort));
		rdc_endmessage(parent_watch, &buf);
		rdc_flush(parent_watch);
	}
}

static void
CloseReduceListenSocket(int code, Datum arg)
{
	if (MyListenSock != PGINVALID_SOCKET)
	{
		elog(LOG, "close listen socket: %d", MyListenSock);
		closesocket(MyListenSock);
		MyListenSock = PGINVALID_SOCKET;
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
		RdcWaitEvents(port) = WAIT_SOCKET_READABLE;
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
	ReduceListen();

	/* tell its parent listen port */
	TransListenPort();

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
		port = rdc_newport(PGINVALID_SOCKET, InvalidPortType, InvalidPortId);
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
	if (rdc_send_group_rsp(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to send setup group response:%s",
						RdcError(port))));
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

static void
PrepareConnectGroup(RdcPort **rdc_nodes,  /* IN/OUT */
					RdcPortType expected, /* IN */
					List **rdc_list)		/* IN/OUT */
{
	RdcPort		   *port = NULL;
	ListCell	   *cell = NULL;

	if (rdc_list == NULL)
		return ;

	for (cell = list_head(*rdc_list); cell != NULL;)
	{
		port = (RdcPort *) lfirst(cell);
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
	}
}

static pgsocket
GetRdcPortSocket(void *port)
{
	return RdcSocket(port);
}

static uint32
GetRdcPortWaitEvents(void *port)
{
	return RdcWaitEvents(port);
}

static void
SetupReduceGroup(RdcPort *port)
{
	int					timeout = -1;
	int					rdc_num = 0;
	int					nready = 0;
	List			   *accept_list = NIL;
	List			   *connect_list = NIL;
#ifdef DEBUG_ADB
	ReduceInfo		   *rdc_infos = NULL;
#endif
	RdcPort			  **rdc_nodes = NULL;

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

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		PrepareConnectGroup(rdc_nodes, TYPE_REDUCE, &connect_list);
		PrepareConnectGroup(rdc_nodes, TYPE_UNDEFINE|TYPE_REDUCE, &accept_list);

		/* all Reduce are ready */
		if (IsReduceGroupReady(rdc_nodes, rdc_num))
		{
			ereport(LOG,
					(errmsg("Reduce group network is OK")));
			break;
		}

		PG_TRY();
		{
			begin_wait_events();
			add_wait_events_sock(MyListenSock, WAIT_SOCKET_READABLE);
			add_wait_events_sock(MyParentSock, WAIT_SOCKET_READABLE);
			add_wait_events_list(connect_list, GetRdcPortSocket, GetRdcPortWaitEvents);
			add_wait_events_list(accept_list, GetRdcPortSocket, GetRdcPortWaitEvents);

			nready = exec_wait_events(timeout);
			if (nready < 0)
			{
				ereport(ERROR,
						(errmsg("fail to wait read/write of sockets: %m")));
			} else
			if (nready == 0)
			{
				/* do nothing if timeout */
			} else
			{
				WaitEventElt	*wee = NULL;
				RdcPort			*port = NULL;

				/* listen sock */
				wee = wee_next();
				if (WEEHasError(wee))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("something wrong with listen socket")));
				if (WEECanRead(wee))
				{
					for (;;)
					{
						RdcPort		*port = NULL;
						port = rdc_accept(WEEGetSock(wee));
						if (port == NULL)
							break;
						accept_list = lappend(accept_list, port);
					}
				}
				/* check MyParentSock */
				if (MyParentSock != PGINVALID_SOCKET)
				{
					wee = wee_next();
					if (WEEHasError(wee) ||
						(WEECanRead(wee) && !BackendIsAlive()))
						break;
				}
				/* check reduce group */
				while ((wee = wee_next()) != NULL)
				{
					port = (RdcPort *) WEEGetArg(wee);
					if (WEEHasError(wee))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("something wrong with [%s %d] {%s:%s} socket",
								 RdcTypeStr(port), RdcID(port),
								 RdcHostStr(port), RdcPortStr(port))));
					if (WEECanRead(wee) || WEECanWrite(wee))
						(void) rdc_connect_poll(port);
				}
			}

			end_wait_events();
		} PG_CATCH();
		{
			end_wait_events();
			PG_RE_THROW();
		} PG_END_TRY();
	}
}

static bool
BackendIsAlive(void)
{
	char		c;
	ssize_t		rc;

	rc = recv(MyParentSock, &c, 1, MSG_PEEK);

	/* the peer has performed an orderly shutdown */
	if (rc == 0)
		return false;

	/* receive close message request */
	if (rc > 0 && c == RDC_CLOSE_MSG)
		return false;

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
		if (!IsPortForPlan(port) ||
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
	int						timeout = -1;
	int						nready;
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

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		PG_TRY();
		{
			begin_wait_events();
			add_wait_events_sock(MyListenSock, WAIT_SOCKET_READABLE);
			add_wait_events_sock(MyParentSock, WAIT_SOCKET_READABLE);
			add_wait_events_array((void **)rdc_nodes, rdc_num, GetRdcPortSocket, GetRdcPortWaitEvents);
			add_wait_events_list(accept_list, GetRdcPortSocket, GetRdcPortWaitEvents);
			foreach(cell, pln_list)
			{
				pln_port = (PlanPort *) lfirst(cell);
				port = pln_port->port;
				while (port != NULL)
				{
					add_wait_events_element(port, GetRdcPortSocket, GetRdcPortWaitEvents);
					port = RdcNext(port);
				}
			}

			nready = exec_wait_events(timeout);
			if (nready < 0)
			{
				ereport(ERROR,
						(errmsg("fail to wait read/write of sockets: %m")));
			} else
			if (nready == 0)
			{
				/* do nothing if timeout */
			} else
			{
				WaitEventElt	*wee = NULL;

				/* listen sock */
				wee = wee_next();
				if (WEEHasError(wee))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("something wrong with listen socket")));
				if (WEECanRead(wee))
				{
					for (;;)
					{
						RdcPort		*port = NULL;
						port = rdc_accept(WEEGetSock(wee));
						if (port == NULL)
							break;
						accept_list = lappend(accept_list, port);
					}
				}
				/* check MyParentSock */
				if (MyParentSock != PGINVALID_SOCKET)
				{
					wee = wee_next();
					if (WEEHasError(wee) ||
						(WEECanRead(wee) && !BackendIsAlive()))
						break;
				}

				ReduceAcceptPlanConn(&accept_list, &pln_list);
				rdc_handle_reduce(rdc_nodes, rdc_num, &pln_list);
				rdc_handle_plannode(rdc_nodes, rdc_num, pln_list);
			}
			end_wait_events();
		} PG_CATCH();
		{
			end_wait_events();
			PG_RE_THROW();
		} PG_END_TRY();
	}

	return STATUS_OK;
}
