/*-------------------------------------------------------------------------
 *
 * wait_events.c
 *	  Routines for I/O multiplexing encapsulation
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/reduce/wait_events.c
 *
 * NOTES
 *	  The correct way to use these routines is like this:
 *
 *	  First of all, call "begin_wait_events" to initialize elements used
 *	  internally.
 *
 *	  The next, call "add_wait_events_sock" to add wait events for the
 *	  specified socket, also you can call "add_wait_events_list" to add
 *	  wait events for a whole list, of course you should offer functions
 *	  to "GetWaitSocket" and "GetWaitEvents".
 *
 *	  Then call "exec_wait_events" to wait until one or more events occured
 *	  on these sockets. On success, a positive number is returned, and now
 *	  you can call "wee_next" to have a WaitEventElt traversal.
 *
 *	  Finally, do not forget to call "end_wait_events" to reset elements
 *	  used internally, otherwise a assert statement will happen when call
 *	  "begin_wait_events" next time.
 *
 *	  Of course, you can call "exit_wait_events" to reset and free all used
 *	  elements.
 *-------------------------------------------------------------------------
 */
#include "reduce/wait_event.h"

#if defined(RDC_FRONTEND)
#include "rdc_exit.h"
#include "rdc_list.h"
#else
#include "miscadmin.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#endif

#define STEP_SIZE			32
static WaitEventElt		   *WaitElements = NULL;
#if defined(WAIT_USE_POLL)
static struct pollfd	   *WaitFds = NULL;
#elif defined(WAIT_USE_SELECT)
static fd_set				rmask;
static fd_set				wmask;
static fd_set				emask;
#endif
static volatile uint32		max_number = 0;
static volatile uint32		cur_number = 0;
static volatile uint32		idx_number = 0;
static volatile bool		on_exit_set = false;
static volatile bool		wait_begin = false;

static void init_wait_elements(void);
static void enlarge_wait_elements(uint32 add_number);
static void add_wait_event_internal(pgsocket wait_sock,
									uint32 wait_events,
									void *wait_arg);

/*
 * init_wait_elements
 *	  initialize wait event elements used internal
 *
 * Here we keep global static variable "WaitElements" (maybe include
 * "WaitFds" if WAIT_USE_POLL) to avoid malloc and free memory frequently.
 * You can use "exit_wait_events" to free them.
 */
static void
init_wait_elements(void)
{
	Size	sz;
	if (WaitElements == NULL)
	{
		max_number = STEP_SIZE;
		sz = max_number * sizeof(WaitEventElt);
		WaitElements = (WaitEventElt *) palloc0(sz);
#if defined(WAIT_USE_POLL)
		Assert(WaitFds == NULL);
		sz = max_number * sizeof(struct pollfd);
		WaitFds = (struct pollfd *) palloc0(sz);
#endif
	} else
	{
		sz = max_number * sizeof(WaitEventElt);
		MemSet(WaitElements, 0, sz);
#if defined(WAIT_USE_POLL)
		sz = max_number * sizeof(struct pollfd);
		MemSet(WaitFds, 0, sz);
#endif
	}
}

/*
 * enlarge_wait_elements
 *	  enlarge memory of static variable if needed
 */
static void
enlarge_wait_elements(uint32 add_number)
{
	Size	sz;
	if (cur_number + add_number > max_number)
	{
		Assert(wait_begin);
		Assert(WaitElements != NULL);

		while (cur_number + add_number > max_number)
			max_number += STEP_SIZE;

		sz = max_number * sizeof(WaitEventElt);
		WaitElements = (WaitEventElt *) repalloc(WaitElements, sz);
#if defined(WAIT_USE_POLL)
		sz = max_number * sizeof(struct pollfd);
		WaitFds = (struct pollfd *) repalloc(WaitFds, sz);
#endif
	}
}

/*
 * begin_wait_events
 *	  initialize all wait event elements
 */
void
begin_wait_events(void)
{
	Assert(!wait_begin);
	init_wait_elements();
	cur_number = 0;
	idx_number = 0;
	wait_begin = true;
#if defined(WAIT_USE_SELECT)
	FD_ZERO(&rmask);
	FD_ZERO(&wmask);
	FD_ZERO(&emask);
#endif

	if (!on_exit_set)
	{
#if defined(RDC_FRONTEND)
	on_rdc_exit(exit_wait_events, 0);
#else
	on_proc_exit(exit_wait_events, 0);
#endif
	on_exit_set = true;
	}
}

/*
 * add_wait_event_internal
 *	  add wait events for a socket with wait_arg argument
 */
static void
add_wait_event_internal(pgsocket wait_sock,
						uint32 wait_events,
						void *wait_arg)
{
	Assert(wait_begin);
	if (wait_sock != PGINVALID_SOCKET)
	{
		WaitEventElt *wee = NULL;

		enlarge_wait_elements(1);

		wee = &(WaitElements[cur_number++]);
#if defined(WAIT_USE_POLL)
		wee->pfd = NULL;
#elif defined(WAIT_USE_SELECT)
		wee->rmask = &rmask;
		wee->wmask = &wmask;
		wee->emask = &emask;
#endif
		wee->sock = wait_sock;
		wee->wait_events = wait_events;
		wee->arg = wait_arg;
	}
}

/*
 * add_wait_events_sock
 *	  add wait events for a socket.
 */
void
add_wait_events_sock(pgsocket wait_sock, uint32 wait_events)
{
	add_wait_event_internal(wait_sock, wait_events, NULL);
}

/*
 * add_wait_events_element
 *	  add wait events for a element.
 *
 * NOTES
 *	  The wait socket will be got by function "GetWaitSocket"
 *	  The wait events will be got by function "GetWaitEvents"
 */
void
add_wait_events_element(void *wait_arg,
						pgsocket (*GetWaitSocket)(void *arg),
						uint32 (*GetWaitEvents)(void *arg))
{
	if (wait_arg)
	{
		pgsocket	wait_sock;
		uint32		wait_events;

		AssertArg(GetWaitSocket && GetWaitEvents);
		wait_sock = (*GetWaitSocket)(wait_arg);
		wait_events = (*GetWaitEvents)(wait_arg);

		add_wait_event_internal(wait_sock, wait_events, wait_arg);
	}
}

/*
 * add_wait_events_list
 *	  add wait events for a whole list.
 */
void
add_wait_events_list(struct List *wait_list,
					 pgsocket (*GetWaitSocket)(void *arg),
					 uint32 (*GetWaitEvents)(void *arg))
{
	ListCell   *lc = NULL;

	foreach (lc, wait_list)
	{
		add_wait_events_element(lfirst(lc),
								GetWaitSocket,
								GetWaitEvents);
	}
}

/*
 * add_wait_events_array
 *	  add wait events for an array.
 */
void
add_wait_events_array(void **elements, int num,
					  pgsocket (*GetWaitSocket)(void *arg),
					  uint32 (*GetWaitEvents)(void *arg))
{
	int					i;

	for (i = 0; i < num; i++)
	{
		add_wait_events_element(elements[i],
								GetWaitSocket,
								GetWaitEvents);
	}
}

/*
 * exec_wait_events
 *	  execute I/O multiplexing to wait until some events occurred
 */
int
exec_wait_events(int timeout)
{
	WaitEventElt	   *wee = NULL;
	int					nready = 0;

	Assert(wait_begin);
#if defined(WAIT_USE_POLL)
	int					nfds = 0;

	for (nfds = 0; nfds < cur_number; nfds++)
	{
		wee = &(WaitElements[nfds]);
		WaitFds[nfds].fd = wee->sock;
		WaitFds[nfds].events = 0;
		WaitFds[nfds].revents = 0;
		if (WaitRead(WaitEvents(wee)))
			WaitFds[nfds].events |= POLLIN;
		if (WaitWrite(WaitEvents(wee)))
			WaitFds[nfds].events |= POLLOUT;
		wee->pfd = &(WaitFds[nfds]);
	}

_re_poll:
	nready = poll(WaitFds, nfds, timeout);
	CHECK_FOR_INTERRUPTS();
	if (nready < 0)
	{
		if (errno == EINTR)
			goto _re_poll;

		goto _error_end;
	}
	return nready;

#elif defined(WAIT_USE_SELECT)
	int				i;
	struct timeval	tv, *ptv;
	pgsocket		max_sock = PGINVALID_SOCKET;

	if (timeout > 0)
	{
		tv.tv_sec = 0;
		tv.tv_usec = timeout * 1000;
		ptv = &tv;
	} else
		ptv = NULL;

	for (i = 0; i < cur_number; i++)
	{
		wee = &(WaitElements[i]);
		if (wee->sock > max_sock)
			max_sock = wee->sock;
		if (WaitRead(WaitEvents(wee)))
			FD_SET(wee->sock, &rmask);
		if (WaitWrite(WaitEvents(wee)))
			FD_SET(wee->sock, &wmask);
		FD_SET(wee->sock, &emask);
	}

_re_select:
	nready = select(max_sock + 1, &rmask, &wmask, &emask, ptv);
	CHECK_FOR_INTERRUPTS();
	if (nready < 0)
	{
		if (errno == EINTR)
			goto _re_select;

		goto _error_end;
	}

	return nready;
#endif

_error_end:
	end_wait_events();
	return nready;
}

/*
 * wee_next
 *	  iterator for the "WaitElements"
 */
WaitEventElt *
wee_next(void)
{
	Assert(wait_begin);
	if (idx_number < cur_number)
		return &(WaitElements[idx_number++]);

	return NULL;
}

/*
 * end_wait_events
 *	  reset some elements but do not free memory
 */
void
end_wait_events(void)
{
	wait_begin = false;
	cur_number = 0;
	idx_number = 0;
}

/*
 * exit_wait_events
 *	  reset and free all elements
 */
void
exit_wait_events(int code, Datum arg)
{
	end_wait_events();
	safe_pfree(WaitElements);
#if defined(WAIT_USE_POLL)
	safe_pfree(WaitFds);
#endif
}
