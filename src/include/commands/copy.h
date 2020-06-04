/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"

/* CopyStateData is private in commands/copy.c */
typedef struct CopyStateData *CopyState;
typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread);

extern void DoCopy(ParseState *state, const CopyStmt *stmt,
				   int stmt_location, int stmt_len,
				   uint64 *processed ADB_ONLY_COMMA_ARG(bool cluster_safe));

extern void ProcessCopyOptions(ParseState *pstate, CopyState cstate, bool is_from, List *options);
extern CopyState BeginCopyFrom(ParseState *pstate, Relation rel, const char *filename,
							   bool is_program, copy_data_source_cb data_source_cb, List *attnamelist, List *options);
extern void EndCopyFrom(CopyState cstate);
extern bool NextCopyFrom(CopyState cstate, ExprContext *econtext,
						 Datum *values, bool *nulls);
extern bool NextCopyFromRawFields(CopyState cstate,
								  char ***fields, int *nfields);
extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyState cstate);

extern DestReceiver *CreateCopyDestReceiver(void);

#ifdef ADB

typedef struct TupleTableSlot *(*CustomNextRowFunction)(CopyState cstate, ExprContext *econtext, void *data);
typedef int (*SimpleCopyDataFunction)(void *context, const char *data, int len);

extern void DoClusterCopy(CopyStmt *stmt, struct StringInfoData *mem_toc);
extern void ClusterCopyFromReduce(Relation rel, Expr *reduce, List *remote_oids, int id, bool freeze, CustomNextRowFunction fun, void *data);
extern void ClusterDummyCopyFromReduce(List *target, Expr *reduce, List *remote_oids, int id, CustomNextRowFunction fun, void *data);
extern int SimpleNextCopyFromNewFE(SimpleCopyDataFunction fun, void *context);
extern bool GetCopyDataFromNewFE(StringInfo buf, bool flush_write, bool copy_done_ok);

typedef struct AuxiliaryRelCopy
{
	char	   *schemaname;		/* the schema name */
	char	   *relname;		/* the relation/sequence name */
	List	   *targetList;		/* main rel result of Exprs */
	Expr	   *reduce;			/* reduce expr */
	int			id;				/* id for reduce plan id */
} AuxiliaryRelCopy;

#define AUX_REL_COPY_INFO	0x1
#define AUX_REL_MAIN_NODES	0x2

extern AuxiliaryRelCopy *MakeAuxRelCopyInfoFromMaster(Relation masterrel, Relation auxrel, int auxid);
extern List* MakeAuxRelCopyInfo(Relation rel);

extern void SerializeAuxRelCopyInfo(StringInfo buf, List *list);
extern List* RestoreAuxRelCopyInfo(StringInfo buf);

extern void DoPaddingDataForAuxRel(Relation master,
								   Relation auxrel,
								   List *rnodes,
								   AuxiliaryRelCopy *auxcopy);
extern TupleTableSlot *
NextRowForPadding(CopyState cstate, ExprContext *context, void *data);

#endif /* ADB */

#endif							/* COPY_H */
