#ifndef RDC_WAIT_EVENT_H
#define RDC_WAIT_EVENT_H

#if defined(RDC_FRONTEND)
#include "rdc_globals.h"
#else
#include "postgres.h"
#endif

#if defined(HAVE_POLL_H)
#include <poll.h>
#endif
#if defined(HAVE_SYS_SELECT_H)
#include <sys/select.h>
#endif

#if defined(WAIT_USE_POLL) || defined(WAIT_USE_SELECT)
/* don't overwrite manual choice */
#elif defined(HAVE_POLL)
#define WAIT_USE_POLL
#elif HAVE_SYS_SELECT_H
#define WAIT_USE_SELECT
#else
#error "Can not use poll(2) or select(2), what a pity!"
#endif

typedef enum EventType
{
	WAIT_NONE			=	0,
	WT_SOCK_READABLE	=	(1 << 0),
	WT_SOCK_WRITEABLE	=	(1 << 1),
} EventType;

typedef struct WaitEventElt
{
	pgsocket			wait_sock;
	EventType			wait_events;
	void			   *wait_arg;
#if defined(WAIT_USE_POLL)
	struct pollfd	   *pfd;
#elif defined(WAIT_USE_SELECT)
	fd_set			   *rmask;
	fd_set			   *wmask;
	fd_set			   *emask;
#endif
} WaitEventElt;

#define WaitEvents(wee)		(((WaitEventElt *) wee)->wait_events)
#define WaitRead(events)	(events & WT_SOCK_READABLE)
#define WaitWrite(events)	(events & WT_SOCK_WRITEABLE)
#define WEEWaitRead(wee)	(WaitEvents(wee) & WT_SOCK_READABLE)
#define WEEWaitWrite(wee)	(WaitEvents(wee) & WT_SOCK_WRITEABLE)
#define WEEGetSock(wee)		(((WaitEventElt *) (wee))->wait_sock)
#define WEEGetEvents(wee)	(((WaitEventElt *) (wee))->wait_events)
#define WEEGetArg(wee)		(((WaitEventElt *) (wee))->wait_arg)
#if defined(WAIT_USE_POLL)
#define WEERetEvent(wee)	(((WaitEventElt *) (wee))->pfd->revents)
#define WEEHasError(wee)	(WEERetEvent(wee) & (POLLERR | POLLHUP | POLLNVAL))
#define WEECanRead(wee)		(WEERetEvent(wee) & (POLLIN))
#define WEECanWrite(wee)	(WEERetEvent(wee) & (POLLOUT))
#elif  defined(WAIT_USE_SELECT)
#define WEEHasError(wee)	FD_ISSET(WEEGetSock(wee), ((WaitEventElt *) (wee))->emask)
#define WEECanRead(wee)		FD_ISSET(WEEGetSock(wee), ((WaitEventElt *) (wee))->rmask)
#define WEECanWrite(wee)	FD_ISSET(WEEGetSock(wee), ((WaitEventElt *) (wee))->wmask)
#endif

typedef struct WaitEVSetData
{
	int				curno;		/* number of registered events */
	int				maxno;		/* maximum number of events in this set */
	int				idxno;		/* used for traversal RdcWaitEvent like iterator */

	/*
	 * Array, of maxno length, storing the definition of events this
	 * set is waiting for.
	 */
	WaitEventElt   *events;
#if defined(WAIT_USE_POLL)
	/* poll expects events to be waited on every poll() call, prepare once */
	struct pollfd  *pollfds;
#elif defined(WAIT_USE_SELECT)
	fd_set			rmask;
	fd_set			wmask;
	fd_set			emask;
#endif
} WaitEVSetData;

typedef WaitEVSetData *WaitEVSet;

extern WaitEVSet makeWaitEVSet(void);
extern WaitEVSet makeWaitEVSetExtend(int num);
extern void initWaitEVSet(WaitEVSet set);
extern void initWaitEVSetExtend(WaitEVSet set, int num);
extern void resetWaitEVSet(WaitEVSet set);
extern void freeWaitEVSet(WaitEVSet set);
extern void enlargeWaitEVSet(WaitEVSet set, int needed);
extern void addWaitEventBySock(WaitEVSet set, pgsocket sock,
					EventType wait_events);
extern void addWaitEventByArg(WaitEVSet set, void *wait_arg,
					pgsocket (*GetWaitSocket)(void *),
					uint32 (*GetWaitEvents)(void *));
struct List;
extern void addWaitEventByList(WaitEVSet set, struct List *wait_list,
					pgsocket (*GetWaitSocket)(void *),
					uint32 (*GetWaitEvents)(void *));
extern void addWaitEventByArray(WaitEVSet set, void **wait_args, int num,
					pgsocket (*GetWaitSocket)(void *),
					uint32 (*GetWaitEvents)(void *));
extern int  execWaitEVSet(WaitEVSet set, int timeout);
extern WaitEventElt *nextWaitEventElt(WaitEVSet set);

#endif	/* RDC_WAIT_EVENT_H */
