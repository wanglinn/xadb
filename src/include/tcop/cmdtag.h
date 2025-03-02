/*-------------------------------------------------------------------------
 *
 * cmdtag.h
 *	  Declarations for commandtag names and enumeration.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/cmdtag.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CMDTAG_H
#define CMDTAG_H


#define PG_CMDTAG(tag, name, evtrgok, rwrok, rowcnt) \
	tag,

typedef enum CommandTag
{
#include "tcop/cmdtaglist.h"
	COMMAND_TAG_NEXTTAG
} CommandTag;
#ifdef ADB
#define AGTM_START_CMD_TAG CMDTAG_AGTM_GET_GXID
#endif /* ADB */

#undef PG_CMDTAG

typedef struct QueryCompletion
{
	CommandTag	commandTag;
#ifdef ADB
	union
	{
		uint64	nprocessed;
		Datum	datum;
	};
#else
	uint64		nprocessed;
#endif
} QueryCompletion;


static inline void
SetQueryCompletion(QueryCompletion *qc, CommandTag commandTag,
				   uint64 nprocessed)
{
	qc->commandTag = commandTag;
	qc->nprocessed = nprocessed;
}

static inline void
CopyQueryCompletion(QueryCompletion *dst, const QueryCompletion *src)
{
	dst->commandTag = src->commandTag;
	dst->nprocessed = src->nprocessed;
}


extern void InitializeQueryCompletion(QueryCompletion *qc);
extern const char *GetCommandTagName(CommandTag commandTag);
extern bool command_tag_display_rowcount(CommandTag commandTag);
extern bool command_tag_event_trigger_ok(CommandTag commandTag);
extern bool command_tag_table_rewrite_ok(CommandTag commandTag);
extern CommandTag GetCommandTagEnum(const char *tagname);
#ifdef ADB
extern bool command_tag_display_format(const QueryCompletion *qc, char *tag);
#endif

#endif							/* CMDTAG_H */
