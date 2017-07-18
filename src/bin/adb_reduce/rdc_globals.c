#include "rdc_globals.h"
#include "reduce/rdc_comm.h"

volatile bool InterruptPending = false;
volatile bool QueryCancelPending = false;
volatile bool ProcDiePending = false;
volatile bool ClientConnectionLost = false;
volatile uint32 CritSectionCount = 0;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;

volatile uint32 ClientConnectionLostType = 0;
volatile int64 ClientConnectionLostID = -1;

void rdc_ProcessInterrupts(void)
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

	if (ClientConnectionLost)
	{
		QueryCancelPending = false;		/* lost connection trumps QueryCancel */
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("connection with [%s %ld] to client lost",
						rdc_type2string(ClientConnectionLostType),
						ClientConnectionLostID)));
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

