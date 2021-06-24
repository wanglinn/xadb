#include "agent.h"

#include <unistd.h>
#include <signal.h>
#if defined(HAVE_POLL) && defined(HAVE_POLL_H)
#	include <poll.h>
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/wait.h>

#include "getopt_long.h"
#include "utils/memutils.h"

int			MyProcPid;
const char *agent_argv0;
static const char *progname;
static int listen_port = 0;
static pgsocket listen_sock;
static bool run_as_demon = false;

static void parse_options(int argc, char **argv);
static void show_help(bool exit_succes) __attribute__((noreturn));
static void start_listen(void);
static void begin_service(void);
static void service_run(void) __attribute__((noreturn));
static void agt_sig_die(SIGNAL_ARGS);
static void agt_sig_cancel(SIGNAL_ARGS);
static void apt_sig_child(SIGNAL_ARGS);
sigjmp_buf agent_reset_sigjmp_buf;

int main(int argc, char **argv)
{
	agent_argv0 = argv[0];
	progname = get_progname(argv[0]);
	MyProcPid = getpid();

	parse_options(argc, argv);
	MemoryContextInit();
	MemoryContextSwitchTo(TopMemoryContext);
	if (sigsetjmp(agent_reset_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Forget any pending QueryCancel */
		QueryCancelPending = false; /* second to avoid race condition */

		/* Ensure to close socket listen */
		closesocket(listen_sock);

		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();
		MemoryContextReset(TopMemoryContext);
	}
	MemoryContextSwitchTo(TopMemoryContext);

	start_listen();
	begin_service();
	service_run();

	return EXIT_SUCCESS;
}

static void parse_options(int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"port", required_argument, NULL, 'p'},
		{"background", no_argument, NULL, 'b'}
	};
	int c;
	int	option_index;
	while((c = getopt_long(argc, argv, "bhVP:", long_options, &option_index)) != -1)
	{
		switch(c)
		{
		case 'V':
#ifdef ADB
			puts("mgr_agent (" ADB_VERSION " based on PostgreSQL) " PG_VERSION);
#else
			puts("mgr_agent (PostgreSQL) " PG_VERSION);
#endif
			exit(EXIT_SUCCESS);
			break;
		case 'P':
			listen_port = atoi(optarg);
			if(listen_port < 0 || listen_port > 65535)
			{
				fprintf(stderr, "Invalid port number \"%s\"\n", optarg);
				exit(EXIT_FAILURE);
			}
			break;
		case 'b':
			run_as_demon = true;
			break;
		case '?':
			show_help(true);
			break;
		default:
			show_help(false);
			break;
		}
	}
	if(optind < argc)
		show_help(false);
}

static void show_help(bool exit_succes)
{
	FILE *fd = exit_succes ? stdout : stderr;

	fprintf(fd, _("%s ADB manager command agent\n"), progname);
	fprintf(fd, _("Usage:\n"));
	fprintf(fd, _("  %s [OPTION]\n"), progname);
	fprintf(fd, _("\nOptions:\n"));
	fprintf(fd, _("  -P, --port=PORT-NUMBER    default listen port is random\n"));
	fprintf(fd, _("  -b, --background          go to background after startup.\n"));
	fprintf(fd, _("  -V, --version             output version information, then exit\n"));
	fprintf(fd, _("  -?, --help                show this help, then exit\n"));
	exit(exit_succes ? EXIT_SUCCESS:EXIT_FAILURE);
}

static void start_listen(void)
{
	struct sockaddr_in listen_addr;
	int one = 1;
	int nTry = 0;

	/* give it 10 chances to bind */
	while (true)
	{
		listen_sock = socket(AF_INET, SOCK_STREAM, 0);
		if (listen_sock == PGINVALID_SOCKET)
		{
			fprintf(stderr, _("could not bind %s socket: %m"), _("IPv4"));
			exit(EXIT_FAILURE);
		}

		if ((setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR,
						(char *)&one, sizeof(one))) == -1)
		{
			fprintf(stderr, _("setsockopt(SO_REUSEADDR) failed for %s port \"%d\": %m"), _("IPv4"), listen_port);
			closesocket(listen_sock);
			exit(EXIT_FAILURE);
		}

		memset(&listen_addr, 0, sizeof(listen_addr));
		listen_addr.sin_family = AF_INET;
		listen_addr.sin_port = htons((unsigned short)listen_port);
		listen_addr.sin_addr.s_addr = INADDR_ANY;
	
		if (bind(listen_sock, (struct sockaddr *)&listen_addr, sizeof(listen_addr)) < 0 || listen(listen_sock, PG_SOMAXCONN) < 0)
		{
			nTry++;
			closesocket(listen_sock);
			if(nTry > 10)
			{
				fprintf(stderr, _("could not listen on %s socket: %m"), _("IPv4"));
				exit(EXIT_FAILURE);
			}
			else
			{
				/* pg_usleep() is useless,why? */
				sleep(1);
				continue;
			}
		}
		break;
	}

	if(listen_port == 0)
	{
		socklen_t slen = sizeof(listen_addr);
		memset(&listen_addr, 0, sizeof(listen_addr));
		if(getsockname(listen_sock, (struct sockaddr *)&listen_addr, &slen) < 0)
		{
			fprintf(stderr, _("could not get listen port on socket:%m"));
			closesocket(listen_sock);
			exit(EXIT_FAILURE);
		}
		listen_port = htons(listen_addr.sin_port);
	}
	printf("%d\n", listen_port);
}

static void begin_service(void)
{
	pqsignal(SIGINT, agt_sig_cancel);
	pqsignal(SIGTERM, agt_sig_die);
	pqsignal(SIGCHLD, apt_sig_child);
	pqsignal(SIGHUP, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	if(run_as_demon)
	{
		pid_t pid = fork();
		if(pid == 0)
		{
			/* child */
			FILE *fd = fopen(DEVNULL, "r");
			if(fd != NULL)
			{
				dup2(fileno(fd), 0);
				fclose(fd);
			}
			fd = fopen(DEVNULL, "w");
			if(fd != NULL)
			{
				dup2(fileno(fd), 1);
				dup2(fileno(fd), 2);
				fclose(fd);
			}
			setsid();
			/* umask(0); */
			chdir("/");
			MyProcPid = pid;
		}else if(pid > 0)
		{
			printf("%d\n", (int)pid);
			exit(EXIT_SUCCESS);
		}else
		{
			fprintf(stderr, "could not fork new process:%m\n");
			exit(EXIT_SUCCESS);
			/*printf("%d\n", (int)getpid());*/
		}
	}else
	{
		printf("%d\n", (int)getpid());
	}
}

static void service_run(void)
{
	sigjmp_buf	local_sigjmp_buf;
	pid_t pid;
	pgsocket new_client;
	int rval;
#if defined(HAVE_POLL) && defined(HAVE_POLL_H)
	struct pollfd poll_fd;
	poll_fd.fd = listen_sock;
	poll_fd.events = POLLIN;
#else
	fd_set rfd_set;
#endif

	PG_exception_stack = &local_sigjmp_buf;
	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* nothing to do */
	}

	for(;;)
	{
#if defined(HAVE_POLL) && defined(HAVE_POLL_H)
		rval = poll(&poll_fd, 1, -1);
#else
		FD_ZERO(&rfd_set);
		FD_SET(listen_sock, &rfd_set);
		rval = select(listen_sock + 1, &rfd_set, NULL, NULL, NULL);
#endif
		CHECK_FOR_INTERRUPTS();
		if(rval < 0)
		{
			if(errno == EINTR)
				continue;
			ereport(FATAL, (errcode_for_socket_access()
				,errmsg("can not select liste socket:%m")));
		}
		new_client = accept(listen_sock, NULL, 0);
		if(new_client == PGINVALID_SOCKET)
			continue;
		pid = fork();
		if(pid == 0)
		{
			closesocket(listen_sock);
			agent_backend(new_client);
		}else if(pid > 0)
		{
			closesocket(new_client);
		}else
		{
			/* fork error */
			closesocket(new_client);
		}
	}

	exit(EXIT_FAILURE);
}

static void agt_sig_die(SIGNAL_ARGS)
{
	/*int			save_errno = errno;*/

	/* Don't joggle the elbow of proc_exit */
	/*if (!proc_exit_inprogress)*/
	{
		InterruptPending = true;
		ProcDiePending = true;

		/*
		 * If we're waiting for input or a lock so that it's safe to
		 * interrupt, service the interrupt immediately
		 */
		/*if (ImmediateInterruptOK)
			ProcessInterrupts();*/
	}

	/*errno = save_errno;*/
}

static void agt_sig_cancel(SIGNAL_ARGS)
{
	/*int			save_errno = errno;*/

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	/*if (!proc_exit_inprogress)*/
	{
		InterruptPending = true;
		QueryCancelPending = true;

		/*
		 * If we're waiting for input or a lock so that it's safe to
		 * interrupt, service the interrupt immediately
		 */
		/*if (ImmediateInterruptOK)
			ProcessInterrupts();*/
	}
	/*errno = save_errno;*/
}

void apt_sig_child(SIGNAL_ARGS)
{
	int exit_status;
	int pid;
	while((pid = waitpid(-1, &exit_status, WNOHANG)) > 0)
	{
		/* nothing to do */
	}
}

void agt_ProcessInterrupts(void)
{
	InterruptPending = false;

	if (ProcDiePending)
	{
		ProcDiePending = false;
		QueryCancelPending = false;		/* ProcDie trumps QueryCancel */
		/*ImmediateInterruptOK = false;	*//* not idle anymore */
		ereport(FATAL, (errcode(ERRCODE_ADMIN_SHUTDOWN),
			errmsg("terminating connection due to administrator command")));

	}
	if (QueryCancelPending)
	{
		/*
		 * Don't allow query cancel interrupts while reading input from the
		 * client, because we might lose sync in the FE/BE protocol.  (Die
		 * interrupts are OK, because we won't read any further messages from
		 * the client in that case.)
		 */
		if (QueryCancelHoldoffCount != 0)
		{
			/*
			 * Re-arm InterruptPending so that we process the cancel request
			 * as soon as we're done reading the message.
			 */
			InterruptPending = true;
			return;
		}

		QueryCancelPending = false;
		/*ImmediateInterruptOK = false;*/		/* not idle anymore */
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED),
			errmsg("canceling authentication due to timeout")));
	}
}