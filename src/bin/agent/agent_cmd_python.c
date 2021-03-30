#include "agent.h"

#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/*#include "plpython.h"*/

#include "agt_msg.h"
#include "agt_utility.h"
#include "mgr/mgr_msg_type.h"
#include "conf_scan.h"
#include "get_uptime.h"
#include "hba_scan.h"
#include "utils/memutils.h"
#include <sys/statvfs.h>

#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE
#undef HAVE_STRERROR
#undef HAVE_TZNAME

#ifdef USE_REPL_SNPRINTF
#undef snprintf
#undef vsnprintf
#endif

bool get_cpu_info(StringInfo hostinfostring);
bool get_mem_info(StringInfo hostinfostring);
bool get_disk_info(StringInfo hostinfostring);
bool get_net_info(StringInfo hostinfostring);
bool get_host_info(StringInfo hostinfostring);
bool get_disk_iops_info(StringInfo hostinfostring);
bool get_system_info(StringInfo hostinfostring);
bool get_platform_type_info(StringInfo hostinfostring);
bool get_cpu_freq(StringInfo hostinfostring);
bool get_filesystem_info(StringInfo filesystemstring);

bool get_filesystem_infos(StringInfo hostinfostring, char* filesysteminfo);
bool get_cpu_usage_info(int64 *total_values, int64 *use_values);
bool append_timestamp(StringInfo hostinfostring);
bool get_net_io_info(int64 *recv, int64 *sent);

static void monitor_append_str(StringInfo hostinfostring, char *str);
static void monitor_append_int64(StringInfo hostinfostring, int64 i);
static void monitor_append_float(StringInfo hostinfostring, float f);


/*
 * get cpu info: timestamp and cpu usage.
 * timestamp: string type, for example:201606130951
 * cpu usage: the current system-wide CPU utilization as a percentage
 *            float type, for example 3.24 it means 3.24%
 */
bool get_cpu_info(StringInfo hostinfostring)
{
    int64 first_total_values = 0;
    int64 first_use_values = 0;
    int64 last_total_values = 0;
    int64 last_use_values = 0;
    float usage_percent;

    /*cacumlate cpu usage */
    get_cpu_usage_info(&first_total_values, &first_use_values);
    sleep(5);
    get_cpu_usage_info(&last_total_values, &last_use_values); 

    if(last_total_values < first_total_values)
        ereport(ERROR, (errmsg("cacumlate cpu usage fails")));
    
    if(last_use_values < first_use_values)
        last_use_values = first_use_values;
    
    usage_percent = (float)(last_use_values - first_use_values) / (last_total_values - first_total_values);

    append_timestamp(hostinfostring);
	monitor_append_float(hostinfostring, usage_percent);
    get_cpu_freq(hostinfostring);

    return true;
}

/*
 * append_timestamp: timestamp
 * timestamp: string type, for example:201606130951
 */
bool append_timestamp(StringInfo hostinfostring)
{
    char time_Stamp[100];
    struct tm   *p;
    time_t	ntime;

    time(&ntime);
	p = localtime(&ntime);
	sprintf(time_Stamp, "%d-%d-%d %02d:%02d:%02d GMT", 1900 + p->tm_year, 1+ p->tm_mon, p->tm_mday,p->tm_hour, p->tm_min, p->tm_sec);

    monitor_append_str(hostinfostring, time_Stamp);
    return true;
}

bool get_cpu_usage_info(int64 *total_values, int64 *use_values)
{
    FILE *fd;
    char buffs[1024];
    char *info_list[1024];
    int i;
    int num;

    if((fd = fopen("/proc/stat", "r")) == NULL)
        ereport(ERROR, (errmsg("can't open file /proc/stat")));

    while (!feof(fd) && !ferror(fd))
    {
        if (fgets(buffs, sizeof(buffs), fd) == NULL)
            break;
        num = 0;
        info_list[num] = strtok(buffs, " ");
        while(info_list[num])
        {
            num++;
            info_list[num] = strtok(NULL, " ");
        }
        
        if(strcmp(info_list[0],"cpu") == 0 && num > 7)
        {
            for(i = 1 ; i < 8; i++)
            {
                if(i < 4)
                    *use_values += strtol(info_list[i],NULL,10);
                *total_values += strtol(info_list[i],NULL,10);
            }
            break;
        } 
    }

    if(fd != NULL)
        fclose(fd);    

    return true;
}

bool get_filesystem_infos(StringInfo hostinfostring, char* filesysteminfo)
{
    int num = 0;
    char *info_list[1024];
    int state;
    int64 disk_Total,disk_Used, disk_free;
    float percent;
    struct statvfs vfs;

    info_list[num] = strtok(filesysteminfo, " ");
    while(info_list[num])
    {
        num++;
        info_list[num] = strtok(NULL, " ");
    }

    state = statvfs(info_list[1],&vfs);
    if(state < 0)
        ereport(ERROR, (errmsg("read statvfs() system function error !!!")));
    disk_Total = vfs.f_blocks * vfs.f_frsize;
    disk_Used = disk_Total - vfs.f_bfree * vfs.f_frsize;
    disk_free = vfs.f_bavail * vfs.f_frsize;
    percent = (float)disk_Used / disk_Total;

    append_timestamp(hostinfostring);                        /*timestamp*/
    monitor_append_str(hostinfostring, info_list[0]);        /*device*/
    monitor_append_str(hostinfostring, info_list[1]);        /*mountpoint*/
    monitor_append_str(hostinfostring, info_list[2]);        /*fstype*/
    monitor_append_int64(hostinfostring, disk_Total);
    monitor_append_int64(hostinfostring, disk_Used);
    monitor_append_int64(hostinfostring, disk_free);
    monitor_append_float(hostinfostring, percent);
    appendStringInfoCharMacro(hostinfostring, '\n');

    return true;
}

bool get_filesystem_info(StringInfo filesystemstring)
{
  FILE *fd;
    char buffs[1024];
    char *info_list[1024];
    char *disk_fstypes[1024];
    int disk_num = 0;
    int num, i;

    if((fd = fopen("/proc/filesystems", "r")) == NULL)
        ereport(ERROR, (errmsg("can't open file /proc/filesystems")));
    
    while (!feof(fd) && !ferror(fd))
    {
        if (fgets(buffs, sizeof(buffs), fd) == NULL)
            break;
        num = 0;
        info_list[num] = strtok(buffs, "\t\n");
        while(info_list[num])
        {
            num++;
            info_list[num] = strtok(NULL, "\t\n");
        }

        if(strcmp(info_list[0],"nodev") != 0)
        {
            // find non-system disk file info
            disk_fstypes[disk_num] = pstrdup(info_list[0]);
            disk_num++;
        }
    }

    if(fd != NULL)
        fclose(fd);
    
    /*open /proc/self/mounts and get the devie, mountpoint, fstype. */
    if((fd = fopen("/proc/self/mounts", "r")) == NULL)
        ereport(ERROR, (errmsg("can't open file /proc/self/mounts")));
    
    while (!feof(fd) && !ferror(fd))
    {
        if (fgets(buffs, sizeof(buffs), fd) == NULL)
            break;
        num = 0;
        info_list[num] = strtok(buffs, ",");
        while(info_list[num])
        {
            num++;
            info_list[num] = strtok(NULL, ",");
        }

        for(i = 0; i < disk_num; i++)
        {
            if(strstr(info_list[0],disk_fstypes[i]))
            {
                get_filesystem_infos(filesystemstring, info_list[0]);
                continue;
            }
        }
    }

    if(fd != NULL)
        fclose(fd);
    return true;
}

bool get_net_io_info(int64 *recv, int64 *sent)
{
    FILE *fd;
    char buffs[1024];
    char *info_list[1024];
    int num;

    if((fd = fopen("/proc/net/dev", "r")) == NULL)
        ereport(ERROR, (errmsg("can't open file /proc/net/dev")));
    
    while (!feof(fd) && !ferror(fd))
    {
        if (fgets(buffs, sizeof(buffs), fd) == NULL)
            break;
        num = 0;
        info_list[num] = strtok(buffs, " ");
        while(info_list[num])
        {
            num++;
            info_list[num] = strtok(NULL, " ");
        }

        if(strchr(info_list[0],':') != NULL)
        {
            *recv += strtol(info_list[1],NULL,10);

            *sent += strtol(info_list[9],NULL,10);
        }
    }

    if(fd != NULL)
        fclose(fd);
    
    return true;
}

/*
 * get memory info:timestamp, memory Total, memory Used and memory Usage.
 * timestamp:string type, for example:201606130951
 * memory Total: total physical memory available (in Bytes).
 * memory Used: memory used (in Bytes).
 * memory Usage: the percentage usage calculated as (total - available) / total * 100
 *               float type, for example 3.24 it means 3.24%
 */
bool get_mem_info(StringInfo hostinfostring)
{
    FILE *fd;
    char buffs[1024];
    char *info_list[1024];
    int64 total_mem = 0;
    int64 used_mem = 0;
    int64 free_mem = 0;
    int64 buffer_mem = 0;
    int64 cached = 0;
    int64 avail_mem = 0;
    float percent_mem = 0;
    int num;

    if((fd = fopen("/proc/meminfo", "r")) == NULL)
        ereport(ERROR, (errmsg("can't open file /proc/meminfo")));
    
    while (!feof(fd) && !ferror(fd))
    {
        if (fgets(buffs, sizeof(buffs), fd) == NULL)
            break;

        num = 0;
        info_list[num] = strtok(buffs, " ");
        while(info_list[num])
        {
            num++;
            info_list[num] = strtok(NULL, " ");
        }

        if(strcmp(info_list[0], "MemTotal:") == 0)
        {
            total_mem = strtol(info_list[1],NULL,10);
            continue;
        }

       if(strcmp(info_list[0], "MemFree:") == 0)
        {
            free_mem = strtol(info_list[1],NULL,10);
            continue;
        }

        if(strcmp(info_list[0], "Buffers:") == 0)
        {
            buffer_mem = strtol(info_list[1],NULL,10);
            continue;
        }

        if(strcmp(info_list[0], "Cached:") == 0)
        {
            cached = strtol(info_list[1],NULL,10);
            continue;
        }

        if(strcmp(info_list[0], "MemAvailable:") == 0)
        {
            avail_mem = strtol(info_list[1],NULL,10);
            continue;
        }
    }

    used_mem = total_mem - free_mem - cached - buffer_mem;

    if(used_mem < 0)
        used_mem = total_mem - free_mem;

    if(avail_mem < 0)
        avail_mem = 0;
    
    if(avail_mem > total_mem)
        avail_mem = free_mem;
    
    percent_mem =(float)(total_mem - avail_mem) / total_mem;

    if(fd != NULL)
        fclose(fd);
    
    append_timestamp(hostinfostring);
    monitor_append_int64(hostinfostring, total_mem*1024);
    monitor_append_int64(hostinfostring, used_mem*1024);
    monitor_append_float(hostinfostring, percent_mem);
    return true;
}

/*
 * get disk info:timestamp, disk_Read_Bytes, disk_Read_Time,
 *               disk_Write_Bytes, disk_Write_Time,disk_Total,disk_Used.
 * timestamp:string type, for example:201606130951.
 * disk_Read_Bytes: number of reads (in Bytes).
 * disk_Read_Time: time spent reading from disk (in milliseconds).
 * disk_Write_Bytes: number of writes (in Bytes).
 * disk_Write_Time: time spent writing to disk (in milliseconds).
 * disk_Total: total physical disk available (in Bytes).
 * disk_Used: disk used (in Bytes).
 */
bool get_disk_info(StringInfo hostinfostring)
{
    FILE *fd;
    char buffs[1024];
    char *info_list[1024];
    int64   disk_io_read_bytes = 0;
    int64   disk_io_read_time = 0;
    int64   disk_io_write_bytes = 0;
    int64   disk_io_write_time = 0;
    int state;
    struct statvfs vfs;
    int64 disk_Total,disk_Used;
    int num;

    if((fd = fopen("/proc/diskstats", "r")) == NULL)
        ereport(ERROR, (errmsg("can't open file /proc/diskstats")));
    
    while (!feof(fd) && !ferror(fd))
    {
        if (fgets(buffs, sizeof(buffs), fd) == NULL)
            break;

        num = 0;
        info_list[num] = strtok(buffs, " ");
        while(info_list[num])
        {
            num++;
            info_list[num] = strtok(NULL, " ");
        }

        if(num == 15)
        {
            /*Linux 2.4*/
            disk_io_read_bytes += atoi(info_list[2]);
            disk_io_write_bytes += atoi(info_list[7]); 
            disk_io_read_time += atoi(info_list[6]);
            disk_io_write_time += atoi(info_list[10]);

        }
        else if(num == 14 || num > 17)
        {
            /*Linux 2.6+*/
            disk_io_read_bytes += atoi(info_list[3]);
            disk_io_write_bytes += atoi(info_list[7]); 
            disk_io_read_time += atoi(info_list[6]);
            disk_io_write_time += atoi(info_list[10]);
        }
        else if(num == 7)
        {
            /*Linux 2.6+, line referring to a partition*/
            disk_io_read_bytes += atoi(info_list[3]);
            disk_io_write_bytes += atoi(info_list[3]);             
        }
        else
        {
            ereport(ERROR, (errmsg("not sure how to interpret this line")));
        }
    }

    if(fd != NULL)
        fclose(fd);

    state = statvfs("/",&vfs);
    if(state < 0)
        ereport(ERROR, (errmsg("read statvfs() system function error !!!")));
    disk_Total = vfs.f_blocks * vfs.f_frsize;
    disk_Used = disk_Total - vfs.f_bfree * vfs.f_frsize;

    append_timestamp(hostinfostring);
    monitor_append_int64(hostinfostring, disk_io_read_bytes);
    monitor_append_int64(hostinfostring, disk_io_read_time);
    monitor_append_int64(hostinfostring, disk_io_write_bytes);
    monitor_append_int64(hostinfostring, disk_io_write_time);
    monitor_append_int64(hostinfostring, disk_Total);
    monitor_append_int64(hostinfostring, disk_Used);

    return true;
}

/*
 * get network info: system-wide network I/O statistics
 *                   timestamp, sent_speed and recv_speed.
 * timestamp: string type, for example:201606130951
 * sent_Speed: the network to sent data rate (in bytes/s).
 * recv_Speed: the network to recv data rate (in bytes/s).
 */
bool get_net_info(StringInfo hostinfostring)
{
    int64 recv_first = 0;
    int64 sent_first = 0;
    int64 recv_last = 0;
    int64 sent_last = 0;
    int  recv, sent;

    get_net_io_info(&recv_first, &sent_first);
    sleep(3);
    get_net_io_info(&recv_last, &sent_last);
    recv = (recv_last - recv_first)/3;
    sent = (sent_last - sent_first)/3;

    /*print "%s %d %d" % (time_stamp, sent, recv)*/
	append_timestamp(hostinfostring);
    monitor_append_int64(hostinfostring,sent);
    monitor_append_int64(hostinfostring,recv);

    return true;
}

bool get_host_info(StringInfo hostinfostring)
{
    time_t seconds_since_boot;
    int cpu_cores_total, cpu_cores_available;

    cpu_cores_total = sysconf(_SC_NPROCESSORS_CONF);
    cpu_cores_available = sysconf(_SC_NPROCESSORS_ONLN);
    monitor_append_int64(hostinfostring, cpu_cores_total);
    monitor_append_int64(hostinfostring, cpu_cores_available);

    seconds_since_boot = get_uptime();
    monitor_append_int64(hostinfostring, seconds_since_boot);

    return true;
}

bool get_disk_iops_info(StringInfo hostinfostring)
{
    FILE *fstream=NULL;
    char cmd[MAXPGPATH],
         cmd_output[MAXPGPATH];

    memset(cmd,0,sizeof(cmd));
    snprintf(cmd,sizeof(cmd),"iostat  -x -d | grep -v -i -E \"linux|device|^$\"|awk '{sum += $4+$5 } END {print sum}'");
    if(NULL == (fstream=popen(cmd,"r")))
    {
        ereport(ERROR, (errmsg("execute command failed: %m")));
        return false;
    }
    if(NULL != fgets(cmd_output, sizeof(cmd_output), fstream))
    {
         monitor_append_float(hostinfostring,(float)atof(cmd_output));
    }
    else
    {
	monitor_append_float(hostinfostring, 0);
        pclose(fstream);
        return false;
    }
    pclose(fstream);
    return true;
}

bool get_system_info(StringInfo hostinfostring)
{
    FILE *fstream=NULL;
    char cmd[MAXPGPATH],
         cmd_output[MAXPGPATH];

    memset(cmd, 0, sizeof(cmd));
    snprintf(cmd, sizeof(cmd), "lsb_release -d | awk 'BEGIN { FS=\":\"} { print $2}' | sed 's/^[ \t]*//g'");
    if(NULL == (fstream=popen(cmd, "r")))
    {
        ereport(ERROR, (errmsg("execute command failed: %m")));
        return false;
    }
    if(NULL != fgets(cmd_output, sizeof(cmd_output), fstream))
    {
        cmd_output[strlen(cmd_output) - 1] = 0;
        monitor_append_str(hostinfostring, cmd_output);
    }
    else
    {
	monitor_append_str(hostinfostring, "unknown operating system version");
        pclose(fstream);
        return false;
    }
    pclose(fstream);
    return true;
}

bool get_platform_type_info(StringInfo hostinfostring)
{
    FILE *fstream=NULL;
    char cmd[MAXPGPATH],
         cmd_output[MAXPGPATH];

    memset(cmd, 0, sizeof(cmd));
    snprintf(cmd, sizeof(cmd), "uname -m");
    if(NULL == (fstream=popen(cmd, "r")))
    {
        ereport(ERROR, (errmsg("execute command failed: %m")));
        return false;
    }
    if(NULL != fgets(cmd_output, sizeof(cmd_output), fstream))
    {
        cmd_output[strlen(cmd_output) - 1] = 0;
        monitor_append_str(hostinfostring, cmd_output);
    }
    else
    {
        monitor_append_str(hostinfostring, "nnknown operating system architecture");
        pclose(fstream);
        return false;
    }
    pclose(fstream);
    return true;
}

bool get_cpu_freq(StringInfo hostinfostring)
{
    FILE *fstream=NULL;
    char cmd[MAXPGPATH],
         cmd_output[MAXPGPATH];

    memset(cmd, 0, sizeof(cmd));
    snprintf(cmd, sizeof(cmd), "cat /proc/cpuinfo | grep GHz | uniq -c | awk ' BEGIN { FS=\"@\"} { print $2}' | sed 's/^[ \t]*//g'");
    if(NULL == (fstream=popen(cmd, "r")))
    {
        ereport(ERROR, (errmsg("execute command failed: %m")));
        return false;
    }
    if(NULL != fgets(cmd_output, sizeof(cmd_output), fstream))
    {
        cmd_output[strlen(cmd_output) - 1] = 0;
        monitor_append_str(hostinfostring, cmd_output);
    }
    else
    {
        monitor_append_str(hostinfostring, "0GHz");
        pclose(fstream);
        return false;
    }
    pclose(fstream);
    return true;
}

static void monitor_append_str(StringInfo hostinfostring, char *str)
{
    Assert(str != NULL && &(hostinfostring->data) != NULL);
    appendStringInfoString(hostinfostring, str);
    appendStringInfoCharMacro(hostinfostring, '\0');
}

static void monitor_append_int64(StringInfo hostinfostring, int64 i)
{
    appendStringInfo(hostinfostring, INT64_FORMAT, i);
    appendStringInfoCharMacro(hostinfostring, '\0');
}

static void monitor_append_float(StringInfo hostinfostring, float f)
{
    appendStringInfo(hostinfostring, "%0.2f", f);
    appendStringInfoCharMacro(hostinfostring, '\0');
}
