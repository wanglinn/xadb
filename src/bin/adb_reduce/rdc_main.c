#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include "getopt_long.h"
#include "rdc_globals.h"
#include "rdc_exit.h"
#include "rdc_handler.h"
#include "rdc_plan.h"
#include "reduce/rdc_msg.h"
#include "reduce/wait_event.h"
#include "utils/memutils.h"		/* for MemoryContext */
#include "utils/ps_status.h"	/* for ps status display */

static const char	   *progname = NULL;
static StringInfo		MyPsCmd = NULL;

int						MyProcPid = -1;
int						MyBossPid = -1;
pg_time_t				MyStartTime;
RdcOptions 				MyRdcOpts = NULL;
pgsocket				MyListenSock = PGINVALID_SOCKET;
pgsocket				MyLogSock = PGINVALID_SOCKET;
int						MyListenPort = 0;

static void InitReduceOptions(void);
static void FreeReduceOptions(int code, Datum arg);
static void ParseExtraOptions(char *extra_options);
static void ParseReduceOptions(int argc, char *const argv[]);
static void Usage(bool exit_success);
static void ReduceListen(void);
static void TransListenPort(void);
static void CloseReduceListenSocket(int code, Datum arg);
#ifdef NOT_USED
static void ResetReduceGroup(void);
#endif
static void DropReduceGroup(void);
static void DropPlanGroup(void);
static void ReduceCancelHandler(SIGNAL_ARGS);
static void ReduceDieHandler(SIGNAL_ARGS);
static void SetReduceSignals(void);
static void WaitForReduceGroupReady(void);
static bool IsReduceGroupReady(void);
static pgsocket GetRdcPortSocket(void *port);
static uint32 GetRdcPortWaitEvents(void *port);
static void ConnectReduceHook(void *arg);
static void AcceptReduceHook(void *arg);
static void StartSetupReduceGroup(RdcPort *port);
static void EndSetupReduceGroup(void);
static void HandleAcceptConn(List **acp_nodes, List **pln_nodes);
static void PrePrepareAcceptNodes(WaitEVSet set, List *acp_nodes);
static bool PrePrepareRdcNodes(WaitEVSet set, RdcNode *rdc_nodes, int rdc_num);
static void PrePreparePlanNodes(WaitEVSet set, List *pln_nodes);
static int  ReduceLoopRun(void);

static void
InitReduceOptions(void)
{
	if (MyRdcOpts == NULL)
		MyRdcOpts = (RdcOptions) palloc0(sizeof(*MyRdcOpts));

	MyRdcOpts->lhost = NULL;
	MyRdcOpts->lport = 0;
	MyRdcOpts->work_mem = 1024;
	MyRdcOpts->log_min_messages = WARNING;
	MyRdcOpts->Log_error_verbosity = PGERROR_DEFAULT;
	MyRdcOpts->Log_destination = LOG_DESTINATION_STDERR;
	MyRdcOpts->redirection_done = false;

	/* don't forget free Reduce options */
	on_rdc_exit(FreeReduceOptions, 0);
}

static void
FreeReduceOptions(int code, Datum arg)
{
	rdc_freeport(MyRdcOpts->boss_watch);
	rdc_freeport(MyRdcOpts->log_watch);
	DropReduceGroup();
	DropPlanGroup();
	safe_pfree(MyRdcOpts->lhost);
	safe_pfree(MyRdcOpts);

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
			MyRdcOpts->log_min_messages = atoi(pval);
		else if (strcmp(pname, "work_mem") == 0)
			MyRdcOpts->work_mem = atoi(pval);
		else if (strcmp(pname, "log_error_verbosity") == 0)
			MyRdcOpts->Log_error_verbosity = atoi(pval);
		else if (strcmp(pname, "log_destination") == 0)
			MyRdcOpts->Log_destination = atoi(pval);
		else if (strcmp(pname, "redirection_done") == 0)
			MyRdcOpts->redirection_done = (bool) atoi(pval);
		else
			elog(ERROR, "invalid extra option \"%s\"", pname);
	}
}

static void
ParseReduceOptions(int argc, char *const argv[])
{
	int			c;
	int			optindex;
	char	   *extra_options = NULL;
	static struct option long_options[] = {
		{"reduce_id", required_argument, NULL, 'n'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"boss_watch", required_argument, NULL, 'W'},
		{"log_watch", required_argument, NULL, 'L'},
		{"extra", required_argument, NULL, 'E'},
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	while ((c = getopt_long(argc, argv, "n:h:p:E:W:L:V?", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'n':
				MyReduceId = atoll(optarg);
				if (MyReduceId <= InvalidPortId)
					elog(ERROR, "invalid Reduce ID number: " PORTID_FORMAT, MyReduceId);
				break;
			case 'h':
				MyRdcOpts->lhost = pstrdup(optarg);
				break;
			case 'p':
				MyRdcOpts->lport = atoi(optarg);
				if (MyRdcOpts->lport > 65535 || MyRdcOpts->lport < 1024)
					elog(ERROR, "invalid listen port: %d", MyRdcOpts->lport);
				break;
			case 'W':
				MyBossSock = atoi(optarg);
				MyRdcOpts->boss_watch = rdc_newport(MyBossSock,
													  TYPE_BACKEND, InvalidPortId,
													  TYPE_REDUCE, MyReduceId,
													  MyProcPid, NULL);
				rdc_set_noblock(MyRdcOpts->boss_watch);
				break;
			case 'L':
				MyLogSock = atoi(optarg);
				MyRdcOpts->log_watch = rdc_newport(MyLogSock,
												   TYPE_BACKEND, InvalidPortId,
												   TYPE_REDUCE, MyReduceId,
												   MyProcPid, NULL);
				break;
			case 'E':
				extra_options = pstrdup(optarg);
				break;
			case 'V':
				fprintf(stdout, "%s based on (PG " PG_VERSION ")\n", progname);
				exit(EXIT_SUCCESS);
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

	if (extra_options)
	{
		ParseExtraOptions(extra_options);
		pfree(extra_options);
	}

#if !defined(RDC_TEST)
	if (MyRdcOpts->boss_watch == NULL)
	{
		ereport(ERROR,
				(errmsg("lack of pipe with parent process")));
	}
#endif

	{
		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "reduce process(" PORTID_FORMAT ")", MyReduceId);
		init_ps_display(buf.data, "", "", "");
		pfree(buf.data);
		buf.data = NULL;
	}
}

void
SetRdcPsStatus(const char *format, ...)
{
	va_list			args;
	int				needed;

	AssertArg(MyPsCmd);
	resetStringInfo(MyPsCmd);
	for(;;)
	{
		va_start(args, format);
		needed = appendStringInfoVA(MyPsCmd, format, args);
		va_end(args);
		if(needed == 0)
			break;
		enlargeStringInfo(MyPsCmd, needed);
	}
	set_ps_display(MyPsCmd->data, false);
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
	fprintf(fd, "  -W, --boss_watch=SOCK            file descriptor from parent process for interprocess communication\n");
	fprintf(fd, "  -L, --log_watch=SOCK             file descriptor from parent process for log record\n");
	fprintf(fd, "  -E, --extra=STRING               extra key-value options, quoted string\n");
	fprintf(fd, "  -V, --version                    output version information, then exit\n");
	fprintf(fd, "  -?, --help                       show this help, then exit\n");

	fprintf(fd, "\nExtra options:\n\n");
	fprintf(fd, "  These options only come from backend, do not set if not sure.\n\n");
	fprintf(fd, "  work_mem=WORKMEM                 set amount of memory for tupstore (in kB)\n");
	fprintf(fd, "  log_min_messages=ELEVEL          set the minimum log level\n");
	fprintf(fd, "  log_error_verbosity=VERBOSE      set the verbosity of logged messages\n");
	fprintf(fd, "  log_destination=DEST             set the destination for server log output\n");
	fprintf(fd, "  redirection_done=(T|F)           set true if stderr is redirected done\n");

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
	MyListenPort = MyRdcOpts->lport;
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
	if (MyRdcOpts->lhost)
		addr_inet.sin_addr.s_addr = inet_addr(MyRdcOpts->lhost);
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
	}
	MyListenSock = fd;

	elog(LOG,
		 "[REDUCE " PORTID_FORMAT " PROC %d BOSS %d] listen on {%s:%d}",
		 MyReduceId, MyProcPid, MyBossPid, listen_host, MyListenPort);

	/* OK */
	return ;

_error_end:
	if (fd != PGINVALID_SOCKET)
		closesocket(fd);
	ereport(ERROR,
			(errmsg("could not create listen socket for \"%s : %d\"",
					MyRdcOpts->lhost, MyRdcOpts->lport)));
	rdc_exit(EXIT_FAILURE);
}

static void
TransListenPort(void)
{
	if (MyBossSock != PGINVALID_SOCKET)
	{
		StringInfo		buf;
		RdcPort		   *boss_watch = MyRdcOpts->boss_watch;

		Assert(MyListenPort > 0);
		Assert(boss_watch);
		buf = RdcMsgBuf(boss_watch);

		resetStringInfo(buf);
		rdc_beginmessage(buf, MSG_LISTEN_PORT);
		rdc_sendint(buf, MyListenPort, sizeof(MyListenPort));
		rdc_endmessage(boss_watch, buf);
		rdc_flush(boss_watch);
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

#ifdef NOT_USED
static void
ResetReduceGroup(void)
{
	RdcNode		   *node;
	RdcPort		   *port;
	int				i;

	for (i = 0; i < MyRdcOpts->rdc_num; i++)
	{

		node = &(MyRdcOpts->rdc_nodes[i]);
		port = node->port;
		if (RdcNodeID(node) == MyReduceId ||
			port == NULL)
			continue;
		Assert (RdcPeerID(port) != MyReduceId);
		RdcWaitEvents(port) = WT_SOCK_READABLE;
		elog(LOG,
			 "reset" RDC_PORT_PRINT_FORMAT,
			 RDC_PORT_PRINT_VALUE(port));
		rdc_resetport(port);
		RdcFlags(port) = RDC_FLAG_VALID;
	}
}
#endif

static void
DropReduceGroup(void)
{
	int			i;
	RdcNode	   *node;
	RdcPort	   *port;

	for (i = 0; i < MyRdcOpts->rdc_num; i++)
	{
		node = &(MyRdcOpts->rdc_nodes[i]);
		port = node->port;
		if (RdcNodeID(node) == MyReduceId ||
			port == NULL)
			continue;
		Assert(RdcPeerID(port) != MyReduceId);
		elog(LOG,
			 "free port of" RDC_PORT_PRINT_FORMAT,
			 RDC_PORT_PRINT_VALUE(port));
		rdc_freeport(port);
	}
	safe_pfree(MyRdcOpts->rdc_nodes);
}

static void
DropPlanGroup(void)
{
	ListCell	   *cell = NULL;
	PlanPort	   *pln_port = NULL;

	foreach (cell, MyRdcOpts->pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(cell);
		plan_freeport(pln_port);
	}
	list_free(MyRdcOpts->pln_nodes);
	MyRdcOpts->pln_nodes = NIL;
}

int main(int argc, char** argv)
{
	MyProcPid = getpid();
	MyBossPid = getppid();
	MyStartTime = time(NULL);

	set_pglocale_pgservice(argv[0], TEXTDOMAIN);

	progname = get_progname(argv[0]);

	argv = save_ps_display_args(argc, argv);

	/* Initialize TopMemoryContext and ErrorContext */
	MemoryContextInit();
	MyPsCmd = makeStringInfo();

	/* Initialize Reduce options */
	InitReduceOptions();

	/* parse options */
	ParseReduceOptions(argc, argv);

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

#ifdef RDC_TEST
static void
ReduceGroupHook(SIGNAL_ARGS)
{
	if (MyRdcOpts && !MyRdcOpts->boss_watch)
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
		RdcPortId	portid[] = {9000, 9001, 9002, 9003, 9004,
								9005, 9006, 9007, 9008, 9009};

		rdc_num = sizeof(portnum)/sizeof(portnum[0]);
		port = rdc_newport(PGINVALID_SOCKET,
						   InvalidPortType, InvalidPortId,
						   TYPE_REDUCE, MyReduceId,
						   MyProcPid, NULL);
		MyRdcOpts->boss_watch = port;
		initStringInfo(&buf);
		rdc_beginmessage(&buf, MSG_GROUP_RQT);
		rdc_sendint(&buf, rdc_num, sizeof(rdc_num));
		for (i = 0; i < rdc_num; i++)
		{
			rdc_sendstring(&buf, host[i]);
			rdc_sendint(&buf, portnum[i], sizeof(portnum[i]));
			rdc_sendRdcPortID(&buf, portid[i]);
		}
		n32 = htonl((uint32)(buf.len - 1));
		memcpy(&buf.data[1], &n32, sizeof(n32));
		appendBinaryStringInfo(&(port->in_buf), buf.data, buf.len);
		pfree(buf.data);
		buf.data = NULL;
	}
}
#endif

static void
SetReduceSignals(void)
{
	pqsignal(SIGINT, ReduceCancelHandler);
	pqsignal(SIGTERM, ReduceDieHandler);
	pqsignal(SIGQUIT, ReduceDieHandler);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGCHLD, SIG_IGN);
#ifdef RDC_TEST
	pqsignal(SIGUSR1, ReduceGroupHook);		/* for test */
#else
	pqsignal(SIGUSR1, SIG_IGN);
#endif
	pqsignal(SIGUSR2, SIG_IGN);
}

static void
WaitForReduceGroupReady(void)
{
	bool		quit = false;
	RdcPort	   *port = MyRdcOpts->boss_watch;

	SetRdcPsStatus(" make up reduce group");
	while (!quit)
	{
		CHECK_FOR_INTERRUPTS();

#ifdef RDC_TEST
		port = MyRdcOpts->boss_watch;
		if (!port)
		{
			pg_usleep(1000000);
			continue;
		}
#endif

		if (rdc_getmessage(port, 0) == MSG_GROUP_RQT)
		{
			StartSetupReduceGroup(port);
			EndSetupReduceGroup();
			quit = true;
		} else
		{
			ereport(ERROR,
					(errmsg("fail to set up reduce group"),
					 errdetail("%s", RdcError(port))));
		}
	}
#if !defined(RDC_TEST)
	if (rdc_send_group_rsp(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to send setup group response"),
				 errdetail("%s", RdcError(port))));
#endif
}

static bool
IsReduceGroupReady(void)
{
	int			i;
	bool		ready = true;
	RdcNode	   *node = NULL;

	Assert(MyRdcOpts);
	Assert(MyRdcOpts->rdc_nodes);

	for (i = 0; i < MyRdcOpts->rdc_num; i++)
	{
		node = &(MyRdcOpts->rdc_nodes[i]);
		if (RdcNodeID(node) == MyReduceId)
			continue;

		if (node->port == NULL ||
			RdcStatus(node->port) != RDC_CONNECTION_OK)
		{
			ready = false;
			break;
		}
	}

	return ready;
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
ConnectReduceHook(void *arg)
{
	RdcPort *port = (RdcPort *) arg;

	Assert(port);
	Assert(PortForReduce(port));
	if (RdcStatus(port) == RDC_CONNECTION_BAD)
		ereport(ERROR,
				(errmsg("fail to connect" RDC_PORT_PRINT_FORMAT,
						RDC_PORT_PRINT_VALUE(port)),
				 errdetail("%s", RdcError(port))));

	Assert(RdcStatus(port) == RDC_CONNECTION_OK);
	RdcFlags(port) = RDC_FLAG_VALID;
}

static void
AcceptReduceHook(void *arg)
{
	RdcPort	   *port = (RdcPort *) arg;
	RdcNode	   *node;
	RdcPortId	rpid;
	int i;

	Assert(port);
	if (!(RdcPeerType(port) & (TYPE_REDUCE | TYPE_UNDEFINE)))
	{
		RdcFlags(port) = RDC_FLAG_CLOSED;
		return ;
	}

	if (RdcStatus(port) == RDC_CONNECTION_BAD)
		ereport(ERROR,
				(errmsg("fail to accept connect from" RDC_PORT_PRINT_FORMAT,
						RDC_PORT_PRINT_VALUE(port)),
				 errdetail("%s", RdcError(port))));

	Assert(RdcStatus(port) == RDC_CONNECTION_OK);
	Assert(MyRdcOpts->rdc_nodes);
	rpid = RdcPeerID(port);
	node = NULL;
	for (i = 0; i < MyRdcOpts->rdc_num; i++)
	{
		node = &(MyRdcOpts->rdc_nodes[i]);
		if (node->mask.rdc_rpid == rpid)
			break;
	}
	Assert(node->port == NULL);
	node->port= port;
	RdcFlags(port) = RDC_FLAG_VALID;
}

static void
StartSetupReduceGroup(RdcPort *port)
{
	RdcNode			   *rdc_nodes = NULL;
	int					rdc_num = 0;

	rdc_nodes = rdc_parse_group(port, &rdc_num, ConnectReduceHook);
	if (!rdc_nodes)
	{
		ereport(ERROR,
				(errmsg("fail to set up reduce group"),
				 errdetail("%s", RdcError(port))));
	}
	MyRdcOpts->rdc_nodes = rdc_nodes;
	MyRdcOpts->rdc_num = rdc_num;
}

static void
EndSetupReduceGroup(void)
{
	int					timeout = -1;
	int					nready = 0;
	int					i;
	int					rdc_num = 0;
	RdcNode			   *rdc_nodes = NULL;
	RdcNode			   *rdc_node = NULL;
	RdcPort			   *acpt_port = NULL;
	List			   *acp_nodes = NIL;
	ListCell		   *lc = NULL;
	WaitEVSetData		set;

	Assert(MyRdcOpts->rdc_nodes);
	rdc_nodes = MyRdcOpts->rdc_nodes;
	rdc_num = MyRdcOpts->rdc_num;
	initWaitEVSet(&set);
	PG_TRY();
	{
		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			/* all Reduce are ready */
			if (IsReduceGroupReady())
			{
				ereport(LOG,
						(errmsg("reduce group network is OK")));
				break;
			}

			resetWaitEVSet(&set);
			addWaitEventBySock(&set, MyListenSock, WT_SOCK_READABLE);
			addWaitEventBySock(&set, MyBossSock, WT_SOCK_READABLE);
			for (i = 0; i < rdc_num; i++)
			{
				rdc_node = &(rdc_nodes[i]);
				if (rdc_node->port &&
					RdcStatus(rdc_node->port) != RDC_CONNECTION_OK)
				{
					addWaitEventByArg(&set, rdc_node->port,
									  GetRdcPortSocket,
									  GetRdcPortWaitEvents);
				}
			}
			for (lc = list_head(acp_nodes); lc != NULL;)
			{
				acpt_port = (RdcPort *) lfirst(lc);
				lc = lnext(lc);

				if (PortMustClosed(acpt_port))
				{
					acp_nodes = list_delete_ptr(acp_nodes, acpt_port);
					rdc_freeport(acpt_port);
					continue;
				}

				if (RdcStatus(acpt_port) != RDC_CONNECTION_OK)
					addWaitEventByArg(&set, acpt_port,
									  GetRdcPortSocket,
									  GetRdcPortWaitEvents);
			}
			nready = execWaitEVSet(&set, timeout);
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
				wee = nextWaitEventElt(&set);
				if (WEEHasError(wee))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("something wrong with listen socket")));
				if (WEECanRead(wee))
				{
					for (;;)
					{
						acpt_port = rdc_accept(WEEGetSock(wee),
											   TYPE_REDUCE, MyReduceId,
											   MyProcPid, NULL);
						if (acpt_port == NULL)
							break;
						RdcHook(acpt_port) = AcceptReduceHook;
						acp_nodes = lappend(acp_nodes, acpt_port);
					}
				}
				/* check MyBossSock */
				if (MyBossSock != PGINVALID_SOCKET)
				{
					wee = nextWaitEventElt(&set);
					if (WEEHasError(wee) ||
						(WEECanRead(wee) && BossNowStatus() != BOSS_IS_WORKING))
						break;
				}
				/* check reduce group */
				while ((wee = nextWaitEventElt(&set)) != NULL)
				{
					port = (RdcPort *) WEEGetArg(wee);
					if (WEEHasError(wee))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("something wrong with socket of" RDC_PORT_PRINT_FORMAT,
								 		RDC_PORT_PRINT_VALUE(port))));
					if (WEECanRead(wee) || WEECanWrite(wee))
						(void) rdc_connect_poll(port);
				}
			}
		}
	} PG_CATCH();
	{
		freeWaitEVSet(&set);
		PG_RE_THROW();
	} PG_END_TRY();

	freeWaitEVSet(&set);
}

static void
HandleAcceptConn(List **acp_nodes, List **pln_nodes)
{
	RdcPort		   *port;
	ListCell	   *cell;

	Assert(pln_nodes);
	for (cell = list_head(*acp_nodes); cell != NULL;)
	{
		port = (RdcPort *) lfirst(cell);
		Assert(port);
		cell = lnext(cell);
		(void) rdc_connect_poll(port);

		/*
		 * Only good connection from Plan Node will be accepted.
		 */
		if (PlanTypeIDIsValid(port) &&
			RdcStatus(port) == RDC_CONNECTION_OK)
		{
			*acp_nodes = list_delete_ptr(*acp_nodes, port);
			RdcFlags(port) = RDC_FLAG_VALID;
			AddNewPlanPort(pln_nodes, port);
			continue ;
		}

		/*
		 * Connection came from other Reduce will be rejected,
		 * Bad connection will be done the same.
		 */
		if (!PortForPlan(port) ||
			RdcStatus(port) == RDC_CONNECTION_BAD)
		{
			RdcFlags(port) = RDC_FLAG_CLOSED;
			*acp_nodes = list_delete_ptr(*acp_nodes, port);
			rdc_freeport(port);
		}
	}
}

static void
PrePrepareAcceptNodes(WaitEVSet set, List *acp_nodes)
{
	addWaitEventByList(set, acp_nodes,
					   GetRdcPortSocket,
					   GetRdcPortWaitEvents);
}

static bool
PrePrepareRdcNodes(WaitEVSet set, RdcNode *rdc_nodes, int rdc_num)
{
	RdcNode	   *rdc_node;
	int			rdc_idx;
	bool		quit = true;

	for (rdc_idx = 0; rdc_idx < rdc_num; rdc_idx++)
	{
		rdc_node = &(rdc_nodes[rdc_idx]);
		if (!PortIsValid(rdc_node->port))
			continue;
		quit = false;
		addWaitEventByArg(set, rdc_node->port,
						  GetRdcPortSocket,
						  GetRdcPortWaitEvents);
	}

	return quit;
}

static void
PrePreparePlanNodes(WaitEVSet set, List *pln_nodes)
{
	PlanPort	   *pln_port;
	RdcPort		   *wrk_port;
	ListCell	   *cell;

	foreach(cell, pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(cell);
		wrk_port = pln_port->work_port;
		if (!PlanPortIsValid(pln_port))
			continue;
		while (wrk_port != NULL)
		{
			if (!PortIsValid(wrk_port))
				continue;

			addWaitEventByArg(set, wrk_port,
							  GetRdcPortSocket,
							  GetRdcPortWaitEvents);
			wrk_port = RdcNext(wrk_port);
		}
	}
}

static int
ReduceLoopRun(void)
{
	int						timeout = -1;
	int						nready;
	int						rdc_num;
	RdcNode				   *rdc_nodes = NULL;
	List				  **pln_nodes = NULL;
	List				   *acp_nodes = NIL;
	WaitEVSetData			set;
#ifdef NOT_USED
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
	MyRdcOpts->rdc_work_stack = &local_sigjmp_buf;

	MemoryContextSwitchTo(MessageContext);
#endif
	Assert(MyRdcOpts->rdc_nodes);
	rdc_num = MyRdcOpts->rdc_num;
	rdc_nodes = MyRdcOpts->rdc_nodes;
	pln_nodes = &(MyRdcOpts->pln_nodes);

	initWaitEVSet(&set);
	PG_TRY();
	{
		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			resetWaitEVSet(&set);
			addWaitEventBySock(&set, MyListenSock, WT_SOCK_READABLE);
			addWaitEventBySock(&set, MyBossSock, WT_SOCK_READABLE);
			PrePrepareAcceptNodes(&set, acp_nodes);
			if (PrePrepareRdcNodes(&set, rdc_nodes, rdc_num))
				break;
			PrePreparePlanNodes(&set, *pln_nodes);

			SetRdcPsStatus(" idle");
			nready = execWaitEVSet(&set, timeout);
			SetRdcPsStatus(" running");
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
				wee = nextWaitEventElt(&set);
				if (WEEHasError(wee))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("something wrong with listen socket")));
				if (WEECanRead(wee))
				{
					for (;;)
					{
						RdcPort		*port = NULL;
						port = rdc_accept(WEEGetSock(wee),
										  TYPE_REDUCE, MyReduceId,
										  MyProcPid, NULL);
						if (port == NULL)
							break;
						acp_nodes = lappend(acp_nodes, port);
					}
				}
				/* check MyBossSock */
				if (MyBossSock != PGINVALID_SOCKET)
				{
					wee = nextWaitEventElt(&set);
					if (WEECanRead(wee) || WEEHasError(wee))
					{
						BossStatus status = BossNowStatus();
						if (status != BOSS_IS_WORKING)
						{
							/*
							 * got boss CLOSE message, notify other reduce
							 * to quit and never receive response, just quit.
							 */
							if (status == BOSS_WANT_QUIT)
								BroadcastRdcClose();
							break;
						}
					}
				}

				HandleAcceptConn(&acp_nodes, pln_nodes);
				HandlePlanIO(pln_nodes);
				HandleReduceIO(pln_nodes);
			}
		}
	} PG_CATCH();
	{
		freeWaitEVSet(&set);
		PG_RE_THROW();
	} PG_END_TRY();

	freeWaitEVSet(&set);

	return STATUS_OK;
}
