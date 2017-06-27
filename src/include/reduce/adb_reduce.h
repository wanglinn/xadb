/*-------------------------------------------------------------------------
 *
 * adb_reduce.h
 *	  header file for integrated adb reduce daemon for backend called.
 *
 * Portions Copyright (c) 2010-2017 ADB Development Group
 *
 * IDENTIFICATION
 * 			src/include/reduce/adb_reduce.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADB_REDUCE_H
#define ADB_REDUCE_H

#include "reduce/rdc_comm.h"

extern int StartAdbReduceLauncher(int rid);

extern int StartAdbReduceGroup(const char *hosts[], int ports[], int num);

extern int SendTupleToReduce(RdcPort *port, const Oid nodeIds[], int num,
							 const char *data, int len);

extern void* GetTupleFromReduce(RdcPort *port);

#endif /* ADB_BROKER_H */
