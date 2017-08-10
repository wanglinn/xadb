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
 *	  First of all, call "initWaitEVSet" to initialize an empty WaitEVSet.
 *
 *	  The next, call "addWaitEventBySock" to add wait events for the
 *	  specified socket, also you can call "addWaitEventByArg",
 *	  "addWaitEventByList", "addWaitEventByArray" to add wait events
 *	  for a wait argument, of course you should offer functions to
 *	  "GetWaitSocket" and "GetWaitEvents".
 *
 *	  Then call "execWaitEVSet" to wait until one or more events occured
 *	  on these sockets. On success, a positive number is returned, and now
 *	  you can call "nextWaitEventElt" to have a WaitEventElt traversal.
 *
 *	  Finally, do not forget to call "freeWaitEVSet" to free WaitEVSet.
 *
 *	  Call "resetWaitEVSet" to reset a WaitEVSet to use once again, if need.
 *-------------------------------------------------------------------------
 */
#include "reduce/wait_event.h"
#include "utils/memutils.h"
#if defined(RDC_FRONTEND)
#include "rdc_exit.h"
#include "rdc_list.h"
#else
#include "miscadmin.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#endif

#define SET_STEP			32

static void addWaitEventInternal(WaitEVSet set,
								 pgsocket wait_sock,
								 EventType wait_events,
								 void *wait_arg);

/*
 * makeWaitEVSet
 *
 * Create an empty 'WaitEVSetData' & return a pointer to it.
 */
WaitEVSet
makeWaitEVSet(void)
{
	WaitEVSet set;

	set = (WaitEVSet) palloc(sizeof(WaitEVSetData));

	initWaitEVSet(set);

	return set;
}

/*
 * makeWaitEVSetExtend
 *
 * Create an empty 'WaitEVSetData' & return a pointer to it.
 */
WaitEVSet
makeWaitEVSetExtend(int num)
{
	WaitEVSet set;

	set = (WaitEVSet) palloc(sizeof(WaitEVSetData));

	initWaitEVSetExtend(set, num);

	return set;
}

/*
 * initWaitEVSet
 *
 * Initialize a WaitEVSetData struct (with previously undefined contents)
 * to describe SET_STEP wait event elements.
 */
void
initWaitEVSet(WaitEVSet set)
{
	initWaitEVSetExtend(set, SET_STEP);
}

/*
 * initWaitEVSetExtend
 *
 * Like initWaitEVSet, but describe specified "num" wait event elements.
 */
void
initWaitEVSetExtend(WaitEVSet set, int num)
{
	AssertArg(set);

	set->events = (WaitEventElt *) palloc(num * sizeof(WaitEventElt));
#if defined(WAIT_USE_POLL)
	set->pollfds = (struct pollfd *) palloc(num * sizeof(struct pollfd));
#endif
	set->maxno = num;

	resetWaitEVSet(set);
}

/*
 * resetWaitEVSet
 *
 * Reset the WaitEVSet: the "events" remains valid, but its
 * previous content, if any, is cleared.
 */
void
resetWaitEVSet(WaitEVSet set)
{
	Assert(set && set->maxno > 0);
	set->curno = 0;
	set->idxno = 0;
	MemSet(set->events, 0, set->maxno * sizeof(WaitEventElt));
#if defined(WAIT_USE_POLL)
	MemSet(set->pollfds, 0, set->maxno * sizeof(struct pollfd));
#elif defined(WAIT_USE_SELECT)
	FD_ZERO(&(set->rmask));
	FD_ZERO(&(set->wmask));
	FD_ZERO(&(set->emask));
#endif
}

/*
 * freeWaitEVSet
 *
 * Free the WaitEVSet: the "events" remains valid, but its
 * previous content, if any, is cleared.
 */
void
freeWaitEVSet(WaitEVSet set)
{
	if (set)
	{
		safe_pfree(set->events);
#if defined(WAIT_USE_POLL)
		safe_pfree(set->pollfds);
#elif defined(WAIT_USE_SELECT)
		FD_ZERO(&(set->rmask));
		FD_ZERO(&(set->wmask));
		FD_ZERO(&(set->emask));
#endif
		set->curno = 0;
		set->idxno = 0;
		set->maxno = 0;
	}
}

/*
 * enlargeWaitEVSet
 *
 * Make sure there is enough space for 'needed' more WaitEventElt.
 *
 * External callers usually need not concern themselves with this, since
 * all wait_event.c routines do it automatically.
 *
 * NB: because we use repalloc() to enlarge the buffer, the events
 * will remain allocated in the same memory context that was current when
 * initWaitEVSet was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
void
enlargeWaitEVSet(WaitEVSet set, int needed)
{
	Size	sz;
	int		newno;

	if (needed < 0)
		elog(ERROR,
			 "invalid wait event set enlargement request size: %d",
			 needed);

	AssertArg(set && set->maxno > 0);
	needed += set->curno;
	if (needed <= set->maxno)
		return ;

	newno = set->maxno + SET_STEP;
	while (needed > newno)
		newno += SET_STEP;

	sz = newno * sizeof(WaitEventElt);
	set->events = (WaitEventElt *) repalloc(set->events, sz);
#if defined(WAIT_USE_POLL)
	sz = newno * sizeof(struct pollfd);
	set->pollfds = (struct pollfd *) repalloc(set->pollfds, sz);
#endif
	set->maxno = newno;
}


/*
 * addWaitEventInternal
 *
 * add wait events for a socket with wait_arg argument to WaitEVSet.
 */
static void
addWaitEventInternal(WaitEVSet set,
					 pgsocket wait_sock,
					 EventType wait_events,
					 void *wait_arg)
{
	AssertArg(set);
	if (wait_sock != PGINVALID_SOCKET)
	{
		WaitEventElt   *wee = NULL;

		enlargeWaitEVSet(set, 1);
		wee = &(set->events[set->curno++]);
		MemSet(wee, 0, sizeof(*wee));
		wee->wait_sock = wait_sock;
		wee->wait_events = wait_events;
		wee->wait_arg = wait_arg;
#if defined(WAIT_USE_POLL)
		wee->pfd = NULL;
#elif defined(WAIT_USE_SELECT)
		wee->rmask = &(set->rmask);
		wee->wmask = &(set->wmask);
		wee->emask = &(set->emask);
#endif
	}
}

/*
 * addWaitEventBySock
 *
 * add wait events for a socket to WaitEVSet.
 */
void
addWaitEventBySock(WaitEVSet set, pgsocket wait_sock, EventType wait_events)
{
	addWaitEventInternal(set, wait_sock, wait_events, NULL);
}

/*
 * addWaitEventByArg
 *
 * add wait events for a wait_arg argument to WaitEVSet.
 */
void
addWaitEventByArg(WaitEVSet set, void *wait_arg,
				  pgsocket (*GetWaitSocket)(void *),
				  uint32 (*GetWaitEvents)(void *))
{
	if (wait_arg)
	{
		pgsocket	wait_sock;
		EventType	wait_events;

		AssertArg(GetWaitSocket && GetWaitEvents);
		wait_sock = (*GetWaitSocket)(wait_arg);
		wait_events = (*GetWaitEvents)(wait_arg);

		addWaitEventInternal(set, wait_sock, wait_events, wait_arg);
	}
}

/*
 * addWaitEventByList
 *
 * add wait events for a list of wait_arg argument to WaitEVSet.
 */
void
addWaitEventByList(WaitEVSet set, struct List *wait_list,
				   pgsocket (*GetWaitSocket)(void *),
				   uint32 (*GetWaitEvents)(void *))
{
	ListCell   *lc = NULL;

	foreach (lc, wait_list)
	{
		addWaitEventByArg(set, lfirst(lc),
						  GetWaitSocket,
						  GetWaitEvents);
	}
}

/*
 * addWaitEventByArray
 *
 * add wait events for a array of wait_arg argument to WaitEVSet.
 */
void
addWaitEventByArray(WaitEVSet set, void **wait_args, int num,
					pgsocket (*GetWaitSocket)(void *),
					uint32 (*GetWaitEvents)(void *))
{
	int					i;

	for (i = 0; i < num; i++)
	{
		addWaitEventByArg(set, wait_args[i],
						  GetWaitSocket,
						  GetWaitEvents);
	}
}

/*
 * execWaitEVSet
 *
 * execute I/O multiplexing to wait until some events occurred on
 * the sockets of WaitEVSet.
 */
int
execWaitEVSet(WaitEVSet set, int timeout)
{
	WaitEventElt	   *wee = NULL;
	int					nready = 0;

#if defined(WAIT_USE_POLL)
	int					nfds = 0;

	AssertArg(set);
	for (nfds = 0; nfds < set->curno; nfds++)
	{
		wee = &(set->events[nfds]);
		set->pollfds[nfds].fd = wee->wait_sock;
		set->pollfds[nfds].events = 0;
		set->pollfds[nfds].revents = 0;
		if (WEEWaitRead(wee))
			set->pollfds[nfds].events |= POLLIN;
		if (WEEWaitWrite(wee))
			set->pollfds[nfds].events |= POLLOUT;
		wee->pfd = &(set->pollfds[nfds]);
	}

_re_poll:
	nready = poll(set->pollfds, nfds, timeout);
	CHECK_FOR_INTERRUPTS();
	if (nready < 0)
	{
		if (errno == EINTR)
			goto _re_poll;
	}
	return nready;

#elif defined(WAIT_USE_SELECT)
	int				i;
	struct timeval	tv, *ptv;
	pgsocket		max_sock = PGINVALID_SOCKET;

	AssertArg(set);
	if (timeout > 0)
	{
		tv.tv_sec = 0;
		tv.tv_usec = timeout * 1000;
		ptv = &tv;
	} else
		ptv = NULL;

	for (i = 0; i < set->curno; i++)
	{
		wee = &(set->events[i]);
		if (wee->wait_sock > max_sock)
			max_sock = wee->wait_sock;
		if (WEEWaitRead(wee))
			FD_SET(wee->wait_sock, &(set->rmask));
		if (WEEWaitWrite(wee))
			FD_SET(wee->wait_sock, &(set->wmask));
		FD_SET(wee->wait_sock, &(set->emask));
	}

_re_select:
	nready = select(max_sock + 1, &(set->rmask), &(set->wmask), &(set->emask), ptv);
	CHECK_FOR_INTERRUPTS();
	if (nready < 0)
	{
		if (errno == EINTR)
			goto _re_select;
	}

	return nready;
#endif
}

/*
 * nextWaitEventElt
 *
 * get iterator of WaitEVSet.
 */
WaitEventElt *
nextWaitEventElt(WaitEVSet set)
{
	AssertArg(set);
	if (set->idxno < set->curno)
		return &(set->events[set->idxno++]);

	return NULL;
}