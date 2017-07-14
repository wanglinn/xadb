/*-------------------------------------------------------------------------
 *
 * rdc_handler.h
 *	  interface for handling messages
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/bin/adb_reduce/rdc_handler.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RDC_HANDLE_H
#define RDC_HANDLE_H

#include "rdc_list.h"
#include "reduce/rdc_comm.h"

extern void HandlePlanIO(List **pln_nodes);
extern void HandleReduceIO(List **pln_nodes);

#endif	/* RDC_HANDLE_H */
