/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
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
#include "tcop/dest.h"

/* CopyStateData is private in commands/copy.c */
typedef struct CopyStateData *CopyState;

extern Oid DoCopy(const CopyStmt *stmt, const char *queryString,
	   uint64 *processed ADB_ONLY_COMMA_ARG(bool cluster_safe));

extern void ProcessCopyOptions(CopyState cstate, bool is_from, List *options);
extern CopyState BeginCopyFrom(Relation rel, const char *filename,
			  bool is_program, List *attnamelist, List *options);
extern void EndCopyFrom(CopyState cstate);
extern bool NextCopyFrom(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls, Oid *tupleOid);
extern bool NextCopyFromRawFields(CopyState cstate,
					  char ***fields, int *nfields);
extern void CopyFromErrorCallback(void *arg);

extern DestReceiver *CreateCopyDestReceiver(void);

#ifdef ADB

typedef struct TupleTableSlot *(*CustomNextRowFunction)(CopyState cstate, ExprContext *econtext, void *data);

extern CopyState pgxcMatViewBeginCopyTo(Relation mvrel);
extern int64 pgxcDoCopyTo(CopyState cstate);
extern void DoClusterCopy(CopyStmt *stmt, struct StringInfoData *mem_toc);
extern void ClusterCopyFromReduce(Relation rel, Expr *reduce, List *remote_oids, int id, bool freeze, CustomNextRowFunction fun, void *data);
extern void ClusterDummyCopyFromReduce(List *target, Expr *reduce, List *remote_oids, int id, CustomNextRowFunction fun, void *data);

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
extern void SerializeAuxRelCopyInfo(StringInfo buf, List *list);
extern List* RestoreAuxRelCopyInfo(StringInfo buf);

extern void DoPaddingDataForAuxRel(Relation master,
								   Relation auxrel,
								   List *rnodes,
								   AuxiliaryRelCopy *auxcopy);
#endif /* ADB */

#endif   /* COPY_H */
