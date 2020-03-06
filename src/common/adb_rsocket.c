#include "postgres.h"

#include "rdma/adb_rsocket.h"
#include <unistd.h>
#include <sys/signalfd.h>

static void* rs_handle = NULL;
RSOCKET adb_rsocket;
RBIND adb_rbind;
RCONNECT adb_rconnect;
RLISTEN adb_rlisten;
RACCEPT adb_raccept;
RSELECT adb_rselect;
RPOLL adb_rpoll;
RCLOSE adb_rclose;
RRECV adb_rrecv;
RSEND adb_rsend;
RGETSOCKOPT adb_rgetsockopt;
RREAD adb_rread;
RWRITE adb_rwrite;
RGETPEERNAME adb_rgetpeername;
RGETSOCKNAME adb_rgetsockname;
RSETSOCKOPT adb_rsetsockopt;
RGETSOCKOPT adb_rgetsockopt;
FCNTL adb_rfcntl;

int rsocket_preload_int(void)
{
    if (!rs_handle)
    {
        //setenv("RDMAV_FORK_SAFE","1",1);
        rs_handle = dlopen("/usr/lib64/rsocket/librspreload.so", RTLD_NOW);
    }
    else
        return 0;

   if(!rs_handle)
        return -1;

    adb_rsocket = (RSOCKET)dlsym(rs_handle, "socket");
    adb_rconnect = (RCONNECT)dlsym(rs_handle, "connect");
    adb_rbind = (RBIND)dlsym(rs_handle, "bind");
    adb_rlisten = (RLISTEN)dlsym(rs_handle, "listen");
    adb_raccept = (RACCEPT)dlsym(rs_handle, "accept");
    adb_rpoll = (RPOLL)dlsym(rs_handle, "poll");
    adb_rclose = (RCLOSE)dlsym(rs_handle, "close");
    adb_rrecv = (RRECV)dlsym(rs_handle, "recv");
    adb_rsend = (RSEND)dlsym(rs_handle, "send");
    adb_rgetsockopt = (RGETSOCKOPT)dlsym(rs_handle, "getsockopt");
    adb_rread = (RREAD)dlsym(rs_handle, "read");
    adb_rwrite = (RWRITE)dlsym(rs_handle, "write");
    adb_rgetpeername = (RGETPEERNAME)dlsym(rs_handle, "getpeername");
    adb_rgetsockname = (RGETSOCKNAME)dlsym(rs_handle, "getsockname");
    adb_rsetsockopt = (RSETSOCKOPT)dlsym(rs_handle, "setsockopt");
    adb_rgetsockopt = (RGETSOCKOPT)dlsym(rs_handle, "getsockopt");
    adb_rfcntl = (FCNTL)dlsym(rs_handle, "fcntl");
    adb_rselect = (RSELECT)dlsym(rs_handle, "select");

    return 0;
}

void rsocket_preload_exit(void)
{
    if (rs_handle)
    {
        dlclose(rs_handle);
        rs_handle = NULL;
    }
}