/*-------------------------------------------------------------------------
 *
 * adb_rsocket.h
 *	  rdma-core librspreload.so preload
 *
 * Portions Copyright (c) 2019, AntDB Development Group
 *
 * src/include/rdma/adb_rsocket.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RSOCKET_H_
#define RSOCKET_H_

#include <sys/stat.h>
#include <dlfcn.h>
#include <poll.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

typedef int (*RSOCKET)(int domain, int type, int protocol);
typedef int (*RBIND)(int socket, const struct sockaddr *addr, socklen_t addrlen);
typedef	int (*RLISTEN)(int socket, int backlog);
typedef	int (*RACCEPT)(int socket, struct sockaddr *addr, socklen_t *addrlen);
typedef	int (*RCONNECT)(int socket, const struct sockaddr *addr, socklen_t addrlen);
typedef	ssize_t (*RRECV)(int socket, void *buf, size_t len, int flags);
typedef	ssize_t (*RECVFROM)(int socket, void *buf, size_t len, int flags,
			    struct sockaddr *src_addr, socklen_t *addrlen);
typedef	ssize_t (*RECVMSG)(int socket, struct msghdr *msg, int flags);
typedef	ssize_t (*RREAD)(int socket, void *buf, size_t count);
typedef	ssize_t (*READV)(int socket, const struct iovec *iov, int iovcnt);
typedef	ssize_t (*RSEND)(int socket, const void *buf, size_t len, int flags);
typedef	ssize_t (*SENDTO)(int socket, const void *buf, size_t len, int flags,
			  const struct sockaddr *dest_addr, socklen_t addrlen);
typedef	size_t (*SENDMSG)(int socket, const struct msghdr *msg, int flags);
typedef	ssize_t (*RWRITE)(int socket, const void *buf, size_t count);
typedef	ssize_t (*WRITEV)(int socket, const struct iovec *iov, int iovcnt);
typedef	int (*RPOLL)(struct pollfd *fds, nfds_t nfds, int timeout);
typedef	int (*SHUTDOWN)(int socket, int how);
typedef	int (*RCLOSE)(int socket);
typedef	int (*RGETPEERNAME)(int socket, struct sockaddr *addr, socklen_t *addrlen);
typedef	int (*RGETSOCKNAME)(int socket, struct sockaddr *addr, socklen_t *addrlen);
typedef	int (*RSETSOCKOPT)(int socket, int level, int optname,
			  const void *optval, socklen_t optlen);
typedef	int (*RGETSOCKOPT)(int socket, int level, int optname,
			  void *optval, socklen_t *optlen);
typedef	int (*FCNTL)(int socket, int cmd, ... /* arg */);
typedef	int (*DUP2)(int oldfd, int newfd);
typedef	ssize_t (*SENDFILE)(int out_fd, int in_fd, off_t *offset, size_t count);
typedef	int (*FXSTAT)(int ver, int fd, struct stat *buf);
typedef int (*RSELECT)(int nfds, fd_set *readfds, fd_set *writefds,
	   fd_set *exceptfds, struct timeval *timeout);

extern RSOCKET adb_rsocket;
extern RBIND adb_rbind;
extern RLISTEN adb_rlisten;
extern RACCEPT adb_raccept;
extern RCONNECT adb_rconnect;
extern RPOLL adb_rpoll;
extern RCLOSE adb_rclose;
extern RRECV adb_rrecv;
extern RSEND adb_rsend;
extern RGETSOCKOPT adb_rgetsockopt;
extern RREAD adb_rread;
extern RWRITE adb_rwrite;
extern RGETPEERNAME adb_rgetpeername;
extern RGETSOCKNAME adb_rgetsockname;
extern RSETSOCKOPT adb_rsetsockopt;
extern RSELECT adb_rselect;
extern FCNTL adb_rfcntl;
extern int rsocket_preload_int(void);
extern void rsocket_preload_exit(void);
#endif /* RSOCKET_H_ */