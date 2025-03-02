/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
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

/*
 * A struct to hold COPY options, in a parsed form. All of these are related
 * to formatting, except for 'freeze', which doesn't really belong here, but
 * it's expedient to parse it along with all the other options.
 */
typedef struct CopyFormatOptions
{
	/* parameters from the COPY command */
	int			file_encoding;	/* file or remote side's character encoding,
								 * -1 if not specified */
	bool		binary;			/* binary format? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	bool		header_line;	/* CSV header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select; /* list of column names (can be NIL) */
} CopyFormatOptions;

/* These are private in commands/copy[from|to].c */
typedef struct CopyFromStateData *CopyFromState;
typedef struct CopyToStateData *CopyToState;

typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread);

extern void DoCopy(ParseState *state, const CopyStmt *stmt,
				   int stmt_location, int stmt_len,
				   uint64 *processed ADB_ONLY_COMMA_ARG(bool cluster_safe));

extern void ProcessCopyOptions(ParseState *pstate, CopyFormatOptions *ops_out, bool is_from, List *options);
extern CopyFromState BeginCopyFrom(ParseState *pstate, Relation rel, Node *whereClause,
								   const char *filename,
								   bool is_program, copy_data_source_cb data_source_cb, List *attnamelist, List *options);
extern void EndCopyFrom(CopyFromState cstate);
extern bool NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
						 Datum *values, bool *nulls);
extern bool NextCopyFromRawFields(CopyFromState cstate,
								  char ***fields, int *nfields);
extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyFromState cstate);

extern DestReceiver *CreateCopyDestReceiver(void);


/*
 * internal prototypes
 */
extern CopyToState BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *query,
							   Oid queryRelId, const char *filename, bool is_program,
							   List *attnamelist, List *options ADB_ONLY_COMMA_ARG(bool cluster_safe));
extern void EndCopyTo(CopyToState cstate);
extern uint64 DoCopyTo(CopyToState cstate);
extern List *CopyGetAttnums(TupleDesc tupDesc, Relation rel,
							List *attnamelist);

#ifdef ADB

typedef struct TupleTableSlot *(*CustomNextRowFunction)(CopyFromState cstate, ExprContext *econtext, void *data);
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
NextRowForPadding(CopyFromState cstate, ExprContext *context, void *data);

#endif /* ADB */

#endif							/* COPY_H */
