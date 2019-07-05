#include "agent.h"

#include <signal.h>
#include <unistd.h>

#include "agt_msg.h"
#include "agt_utility.h"
#include "lib/stringinfo.h"
#include "mgr/mgr_msg_type.h"
#include "utils/memutils.h"

void agent_backend(pgsocket fd)
{
	sigjmp_buf	local_sigjmp_buf;
	StringInfoData input_message;
	StringInfoData echo_message;
	int msg_type;
	pid_t ppid;

	pqsignal(SIGCHLD, SIG_DFL);
	PG_exception_stack = NULL;
	MemoryContextResetAndDeleteChildren(TopMemoryContext);
	MemoryContextSwitchTo(TopMemoryContext);
	agt_msg_init(fd);
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										 "MessageContext",
										 ALLOCSET_DEFAULT_SIZES);
	ErrorContext = AllocSetContextCreate(TopMemoryContext,
										 "ErrorContext",
										 ALLOCSET_DEFAULT_SIZES);
	initStringInfo(&input_message);
	initStringInfo(&echo_message);
	MemoryContextSwitchTo(MessageContext);

	PG_exception_stack = &local_sigjmp_buf;
	if(sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Forget any pending QueryCancel */
		QueryCancelPending = false;		/* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(MessageContext);
		FlushErrorState();
	}
	MemoryContextSwitchTo(MessageContext);
	/* send read for query */
	agt_put_msg(AGT_MSG_IDLE, NULL, 0);

	for(;;)
	{
		MemoryContextResetAndDeleteChildren(MessageContext);
		msg_type = agt_get_msg(&input_message);
		switch(msg_type)
		{
		case AGT_MSG_COMMAND:
			do_agent_command(&input_message);
			break;
		case AGT_MSG_EXIT:
			exit(EXIT_SUCCESS);
			break;
		case AGT_MSG_IDLE:
			/* is father process died? */
			ppid = getppid();
			if(ppid == 1)
			{
				/* Send the message that parent process died. */
				agt_beginmessage_reuse(&echo_message, AGT_MSG_EXIT);
				appendStringInfoString(&echo_message, "My parent AGENT process died!");
				agt_endmessage_reuse(&echo_message);
			}
			/* reponse idle message, do it as heartbeat message. */
			break;
		default:
			ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION)
				, errmsg("invalid message type")));
			break;
		}
		if(msg_type != AGT_MSG_IDLE)
		{
			/* Echo back the message that sended by client. */
			agt_beginmessage_reuse(&echo_message, msg_type);
			appendStringInfoString(&echo_message, input_message.data);
			agt_endmessage_reuse(&echo_message);
		}
		agt_put_msg(AGT_MSG_IDLE, NULL, 0);
		agt_flush();
	}

	exit(EXIT_FAILURE);
}
