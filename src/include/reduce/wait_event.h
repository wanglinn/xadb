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

#define WAIT_NONE					0
#define WAIT_SOCKET_READABLE		(1 << 0)
#define WAIT_SOCKET_WRITEABLE		(1 << 1)

typedef struct WaitEventElt
{
#if defined(WAIT_USE_POLL)
	struct pollfd	   *pfd;
#elif defined(WAIT_USE_SELECT)
	fd_set			   *rmask;
	fd_set			   *wmask;
	fd_set			   *emask;
#endif
	pgsocket			sock;
	uint32				wait_events;
	void			   *arg;
} WaitEventElt;

#define WaitEvents(wee)		(((WaitEventElt *) wee)->wait_events)
#define WaitRead(events)	(events & WAIT_SOCKET_READABLE)
#define WaitWrite(events)	(events & WAIT_SOCKET_WRITEABLE)

#define WEEGetSock(wee)		(((WaitEventElt *) (wee))->sock)
#define WEEGetEvents(wee)	(((WaitEventElt *) (wee))->wait_events)
#define WEEGetArg(wee)		(((WaitEventElt *) (wee))->arg)
#if defined(WAIT_USE_POLL)
#define WEEHasError(wee)	((((WaitEventElt *) (wee))->pfd->revents) & (POLLERR | POLLHUP | POLLNVAL))
#define WEECanRead(wee)		((((WaitEventElt *) (wee))->pfd->revents) & (POLLIN))
#define WEECanWrite(wee)	((((WaitEventElt *) (wee))->pfd->revents) & (POLLOUT))
#elif  defined(WAIT_USE_SELECT)
#define WEEHasError(wee)	FD_ISSET(WEEGetSock(wee), ((WaitEventElt *) (wee))->emask)
#define WEECanRead(wee)		FD_ISSET(WEEGetSock(wee), ((WaitEventElt *) (wee))->rmask)
#define WEECanWrite(wee)	FD_ISSET(WEEGetSock(wee), ((WaitEventElt *) (wee))->wmask)
#endif

extern void begin_wait_events(void);
extern void add_wait_events_sock(pgsocket wait_sock, uint32 wait_events);
extern void add_wait_events_element(void *wait_arg,
									pgsocket (*GetWaitSocket)(void *arg),
									uint32 (*GetWaitEvents)(void *arg));
struct List;
extern void add_wait_events_list(struct List *wait_list,
								 pgsocket (*GetWaitSocket)(void *arg),
								 uint32 (*GetWaitEvents)(void *arg));
extern void add_wait_events_array(void **elements, int num,
								  pgsocket (*GetWaitSocket)(void *arg),
								  uint32 (*GetWaitEvents)(void *arg));
extern int  exec_wait_events(int timeout);
extern WaitEventElt *wee_next(void);
extern void end_wait_events(void);
extern void exit_wait_events(int code, Datum arg);

#endif	/* RDC_WAIT_EVENT_H */
