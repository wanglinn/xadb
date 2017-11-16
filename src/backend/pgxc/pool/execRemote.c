/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execRemote.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>

#include "postgres.h"
#include "miscadmin.h"

#include "access/relscan.h"
#include "access/twophase.h"
#include "access/transam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/xc_maintenance_mode.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#ifdef ADB
#include "access/rxact_mgr.h"
#include "executor/clusterReceiver.h"
#include "intercomm/inter-comm.h"
#endif

/* Enforce the use of two-phase commit when temporary objects are used */
bool EnforceTwoPhaseCommit = true;

/*
 * non-FQS UPDATE & DELETE to a replicated table without any primary key or
 * unique key should be prohibited (true) or allowed (false)
 */
bool RequirePKeyForRepTab = true;

/*
 * Max to begin flushing data to datanodes, max to stop flushing data to datanodes.
 */
#define MAX_SIZE_TO_FORCE_FLUSH	(2^10 * 64 * 2)
#define MAX_SIZE_TO_STOP_FLUSH	(2^10 * 64)

#define END_QUERY_TIMEOUT	20
#define ROLLBACK_RESP_LEN	9

typedef enum RemoteXactNodeStatus
{
	RXACT_NODE_NONE,				/* Initial state */
	RXACT_NODE_PREPARE_SENT_FAILED,	/* PREPARE request is sent failed */
	RXACT_NODE_PREPARE_SENT,		/* PREPARE request is sent successfully */
	RXACT_NODE_PREPARE_FAILED,		/* PREPARE request is failed on the node */
	RXACT_NODE_PREPARED,			/* PREPARE request is successfully executed on the node */
	RXACT_NODE_COMMIT_SENT,			/* COMMIT request is sent successfully */
	RXACT_NODE_COMMIT_FAILED,		/* COMMIT request is failed on the node */
	RXACT_NODE_COMMITTED,			/* COMMIT request is successfully executed on the node */
	RXACT_NODE_ABORT_SENT,			/* ABORT request is sent successfully */
	RXACT_NODE_ABORT_FAILED,		/* ABORT request is failed on the node */
	RXACT_NODE_ABORTED				/* ABORT request is successfully executed on the node */
} RemoteXactNodeStatus;

typedef enum RemoteXactStatus
{
	RXACT_NONE,						/* Initial state */
	RXACT_PREPARE_FAILED,			/* PREPARE request is failed */
	RXACT_PREPARED,					/* PREPARE request is successful on all nodes */
	RXACT_COMMIT_FAILED,			/* COMMIT request is failed on all the nodes */
	RXACT_PART_COMMITTED,			/* COMMIT request is failed on some and successful on the other nodes */
	RXACT_COMMITTED,				/* COMMIT request is successful on all the nodes */
	RXACT_ABORT_FAILED,				/* ABORT request is failed on all the nodes */
	RXACT_PART_ABORTED,				/* ABORT request is failed on some and successful on the other nodes */
	RXACT_ABORTED					/* ABORT request is successful on all the nodes */
} RemoteXactStatus;

typedef struct
{
	xact_callback function;
	void *fparams;
} abort_callback_type;

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024
#define PGXC_NODE_DATA_ERROR() do\
	{									\
		set_ps_display(PG_FUNCNAME_MACRO, true);	\
		for(;;)							\
			sleep(1);				\
	}while(0)

#define ADB_CONN_STATE_ERROR(b)						\
	do{												\
		if(!(b))									\
		{											\
			char *msg = psprintf("%d:%s:%s:%s"		\
				,__LINE__, PG_FUNCNAME_MACRO		\
				, #b, __FILE__);					\
			set_ps_display(msg, true);				\
			for(;;)									\
			{										\
				pg_usleep(1000*1000);				\
				CHECK_FOR_INTERRUPTS();				\
			}										\
		}											\
	}while(false)
#undef PGXC_NODE_DATA_ERROR
#define PGXC_NODE_DATA_ERROR()						\
	do{												\
		set_ps_display(PG_FUNCNAME_MACRO, true);	\
		for(;;)										\
		{											\
			pg_usleep(1000*1000);					\
			CHECK_FOR_INTERRUPTS();					\
		}											\
	}while(false)

/*
 * List of PGXCNodeHandle to track readers and writers involved in the
 * current transaction
 */
static abort_callback_type dbcleanup_info = { NULL, NULL };

static void close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor);

static TupleTableSlot * RemoteQueryNext(ScanState *node);
static bool RemoteQueryRecheck(RemoteQueryState *node, TupleTableSlot *slot);

static void pgxc_node_report_error(RemoteQueryState *combiner);
static bool IsReturningDMLOnReplicatedTable(RemoteQuery *rq);
static void SetDataRowForIntParams(JunkFilter *junkfilter,
					   TupleTableSlot *sourceSlot, TupleTableSlot *newSlot,
					   RemoteQueryState *rq_state);
static void pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype);
static void pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno, Oid valtype, StringInfo buf);
static void pgxc_rq_fire_bstriggers(RemoteQueryState *node);
static void pgxc_rq_fire_astriggers(RemoteQueryState *node);

/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
RemoteQueryState *
CreateResponseCombiner(int node_count, CombineType combine_type)
{
	RemoteQueryState *combiner;

	/* ResponseComber is a typedef for pointer to ResponseCombinerData */
	combiner = makeNode(RemoteQueryState);
	combiner->cur_handles = NIL;
	combiner->all_handles = NIL;
	combiner->node_count = node_count;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->combine_type = combine_type;
	combiner->command_complete_count = 0;
	combiner->command_error_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	initStringInfo(&(combiner->errorMessage));
	combiner->errorDetail = NULL;
	combiner->errorNodeName = NULL;
	combiner->query_Done = false;
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->remoteCopyType = REMOTE_COPY_NONE;
	combiner->copy_file = NULL;
	combiner->rqs_cmd_id = FirstCommandId;
	combiner->rqs_processed = 0;

	return combiner;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		}
		else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
static TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
	TupleDesc 	result;
	int 		i, nattr;
	uint16		n16;

	/* get number of attributes */
	memcpy(&n16, msg_body, 2);
	nattr = ntohs(n16);
	msg_body += 2;

	result = CreateTemplateTupleDesc(nattr, false);

	/* decode attributes */
	for (i = 1; i <= nattr; i++)
	{
		AttrNumber	attnum;
		char		*attname;
		char		*typname;
		Oid 		oidtypeid;
		int32 		typemode, typmod;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* type name */
		typname = msg_body;
		msg_body += strlen(typname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type OID, ignored */
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&typemode, msg_body, 4);
		typmod = ntohl(typemode);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		/* Get the OID type and mode type from typename */
		parseTypeString(typname, &oidtypeid, NULL, false);

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a Datanode connection
 */
static void
HandleCopyOutComplete(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		PGXC_NODE_DATA_ERROR();
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'c' message, "
				 		"current request type %d", combiner->request_type)));
	}
	/* Just do nothing, close message is managed by the Coordinator */
	combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void
HandleCommandComplete(RemoteQueryState *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{
	int 			digits = 0;
	bool			non_fqs_dml;

	/* Is this a DML query that is not FQSed ? */
	non_fqs_dml = (combiner->ss.ps.plan &&
					((RemoteQuery*)combiner->ss.ps.plan)->rq_params_internal);
	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;

	/* Extract rowcount */
	if (combiner->combine_type != COMBINE_TYPE_NONE)
	{
		uint64	rowcount;
		digits = parse_row_count(msg_body, len, &rowcount);
		if (digits > 0)
		{
			/*
			 * PGXC TODO: Need to completely remove the dependency on whether
			 * it's an FQS or non-FQS DML query. For this, command_complete_count
			 * needs to be better handled. Currently this field is being updated
			 * for each iteration of FetchTuple by re-using the same combiner
			 * for each iteration, whereas it seems it should be updated only
			 * for each node execution, not for each tuple fetched.
			 */

			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->command_complete_count)
				{
					/* For FQS, check if there is a consistency issue with replicated table. */
					if (rowcount != combiner->rqs_processed && !non_fqs_dml)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned"
										" different results from the Datanodes")));
				}
				/* Always update the row count. We have initialized it to 0 */
				combiner->rqs_processed = rowcount;
			}
			else
				combiner->rqs_processed += rowcount;

			/*
			 * This rowcount will be used to increment estate->es_processed
			 * either in ExecInsert/Update/Delete for non-FQS query, or will
			 * used in RemoteQueryNext() for FQS query.
			 */
		}
		else
			combiner->combine_type = COMBINE_TYPE_NONE;
	}

	/* If response checking is enable only then do further processing */
	if (conn->ck_resp_rollback == RESP_ROLLBACK_CHECK)
	{
		conn->ck_resp_rollback = RESP_ROLLBACK_NOT_RECEIVED;
		if (len == ROLLBACK_RESP_LEN)	/* No need to do string comparison otherwise */
		{
			if (strcmp(msg_body, "ROLLBACK") == 0)
				conn->ck_resp_rollback = RESP_ROLLBACK_RECEIVED;
		}
	}

	combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool
HandleRowDescription(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		PGXC_NODE_DATA_ERROR();
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'T' message,"
				 		"current request type %d", combiner->request_type)));
	}
	/* Increment counter and check if it was first */
	if (combiner->description_count++ == 0)
	{
		combiner->tuple_desc = create_tuple_desc(msg_body, len);
		return true;
	}
	return false;
}


#ifdef NOT_USED
/*
 * Handle ParameterStatus ('S') message from a Datanode connection (SET command)
 */
static void
HandleParameterStatus(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		PGXC_NODE_DATA_ERROR();
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'S' message, "
				 		"current request type %d", combiner->request_type)));
	}
	/* Proxy last */
	if (++combiner->description_count == combiner->node_count)
	{
		pq_putmessage('S', msg_body, len);
	}
}
#endif

/*
 * Handle CopyInResponse ('G') message from a Datanode connection
 */
static void
HandleCopyIn(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_IN;

	if (combiner->request_type != REQUEST_TYPE_COPY_IN)
	{
		PGXC_NODE_DATA_ERROR();
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'G' message, "
				 		"current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an G message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a Datanode connection
 */
static void
HandleCopyOut(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		PGXC_NODE_DATA_ERROR();
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'H' message, "
				 		"current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an H message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a Datanode connection
 */
static void
HandleCopyDataRow(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	/* Inconsistent responses */
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		PGXC_NODE_DATA_ERROR();
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'd' message, "
				 		"current request type %d", combiner->request_type)));
	}

	/* count the row */
	combiner->processed++;

	/* Output remote COPY operation to correct location */
	switch (combiner->remoteCopyType)
	{
		case REMOTE_COPY_FILE:
			/* Write data directly to file */
			fwrite(msg_body, 1, len, combiner->copy_file);
			break;
		case REMOTE_COPY_STDOUT:
			/* Send back data to client */
			pq_putmessage('d', msg_body, len);
			break;
		case REMOTE_COPY_TUPLESTORE:
			{
				Datum  *values;
				bool   *nulls;
				TupleDesc   tupdesc = combiner->tuple_desc;
				int i, dropped;
				Form_pg_attribute *attr = tupdesc->attrs;
				FmgrInfo *in_functions;
				Oid *typioparams;
				char **fields;

				values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
				nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
				in_functions = (FmgrInfo *) palloc(tupdesc->natts * sizeof(FmgrInfo));
				typioparams = (Oid *) palloc(tupdesc->natts * sizeof(Oid));

				/* Calculate the Oids of input functions */
				for (i = 0; i < tupdesc->natts; i++)
				{
					Oid         in_func_oid;

					/* Do not need any information for dropped attributes */
					if (attr[i]->attisdropped)
						continue;

					getTypeInputInfo(attr[i]->atttypid,
									 &in_func_oid, &typioparams[i]);
					fmgr_info(in_func_oid, &in_functions[i]);
				}

				/*
				 * Convert message into an array of fields.
				 * Last \n is not included in converted message.
				 */
				fields = CopyOps_RawDataToArrayField(tupdesc, msg_body, len - 1);

				/* Fill in the array values */
				dropped = 0;
				for (i = 0; i < tupdesc->natts; i++)
				{
					char	*string = fields[i - dropped];
					/* Do not need any information for dropped attributes */
					if (attr[i]->attisdropped)
					{
						dropped++;
						nulls[i] = true; /* Consider dropped parameter as NULL */
						continue;
					}

					/* Find value */
					values[i] = InputFunctionCall(&in_functions[i],
												  string,
												  typioparams[i],
												  attr[i]->atttypmod);
					/* Setup value with NULL flag if necessary */
					if (string == NULL)
						nulls[i] = true;
					else
						nulls[i] = false;
				}

				/* Then insert the values into tuplestore */
				tuplestore_putvalues(combiner->tuplestorestate,
									 combiner->tuple_desc,
									 values,
									 nulls);

				/* Clean up everything */
				if (*fields)
					pfree(*fields);
				pfree(fields);
				pfree(values);
				pfree(nulls);
				pfree(in_functions);
				pfree(typioparams);
			}
			break;
		case REMOTE_COPY_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if buffer can accept more data rows.
 * Caller must stop reading if function returns false
 */
static void
HandleDataRow(RemoteQueryState *combiner, char *msg_body, size_t len, Oid nodeoid)
{
	/* We expect previous message is consumed */
	Assert(combiner->currentRow.msg == NULL);
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		PGXC_NODE_DATA_ERROR();
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'D' message, "
				 		"current request type %d", combiner->request_type)));
	}

	/*
	 * If we got an error already ignore incoming data rows from other nodes
	 * Still we want to continue reading until get CommandComplete
	 */
	if (combiner->errorMessage.len > 0)
		return;

	/*
	 * We are copying message because it points into connection buffer, and
	 * will be overwritten on next socket read
	 */
	combiner->currentRow.msg = (char *) palloc(len);
	memcpy(combiner->currentRow.msg, msg_body, len);
	combiner->currentRow.msglen = len;
	combiner->currentRow.msgnode = nodeoid;
}

/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static char *
HandleError(const char *from, RemoteQueryState *combiner, char *msg_body, size_t len)
{
	/* parse error message */
	char *code = NULL;
	char *message = NULL;
	char *detail = NULL;
	int   offset = 0;

	/*
	 * Scan until point to terminating \0
	 */
	while (offset + 1 < len)
	{
		/* pointer to the field message */
		char *str = msg_body + offset + 1;

		switch (msg_body[offset])
		{
			case 'C':	/* code */
				code = str;
				break;
			case 'M':	/* message */
				message = str;
				break;
			case 'D':	/* details */
				detail = str;
				break;

			/* Fields not yet in use */
			case 'S':	/* severity */
			case 'R':	/* routine */
			case 'H':	/* hint */
			case 'P':	/* position string */
			case 'p':	/* position int */
			case 'q':	/* int query */
			case 'W':	/* where */
			case 'F':	/* file */
			case 'L':	/* line */
			default:
				break;
		}

		/* code, message and \0 */
		offset += strlen(str) + 2;
	}

	/*
	 * We may have special handling for some errors, default handling is to
	 * throw out error with the same message. We can not ereport immediately
	 * because we should read from this and other connections until
	 * ReadyForQuery is received, so we just store the error message.
	 * If multiple connections return errors only first one is reported.
	 */
	if (combiner->errorMessage.len == 0)
	{
		appendStringInfo(&(combiner->errorMessage), "%s", message);
		/* Error Code is exactly 5 significant bytes */
		if (code)
			memcpy(combiner->errorCode, code, 5);
	}

	if (!combiner->errorDetail && detail != NULL)
	{
		combiner->errorDetail = pstrdup(detail);
	}

	if(combiner->errorNodeName == NULL && from != NULL)
		combiner->errorNodeName = pstrdup(from);
	/*
	 * If Datanode have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_error_count++;

	return message;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters:
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine,
						const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;

	if (msg_body == NULL)
		return;

	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;

		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}

}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void
HandleDatanodeCommandId(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	uint32		n32;
	CommandId	cid;

	Assert(msg_body != NULL);
	Assert(len >= 2);

	/* Get the command Id */
	memcpy(&n32, &msg_body[0], 4);
	cid = ntohl(n32);

	/* If received command Id is higher than current one, set it to a new value */
	if (cid > GetReceivedCommandId())
		SetReceivedCommandId(cid);
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
static bool
validate_combiner(RemoteQueryState *combiner)
{
	/* There was error message while combining */
	if (combiner->errorMessage.len > 0)
		return false;

	/* Check if state is defined */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		return false;

	/* Check all nodes completed */
	if ((combiner->request_type == REQUEST_TYPE_COMMAND ||
		 combiner->request_type == REQUEST_TYPE_QUERY) &&
		(combiner->command_error_count + combiner->command_complete_count != combiner->node_count))
		return false;

	/* Check count of description responses */
	if (combiner->request_type == REQUEST_TYPE_QUERY &&
		combiner->description_count != combiner->node_count)
		return false;

	/* Check count of copy-in responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_IN &&
		combiner->copy_in_count != combiner->node_count)
		return false;

	/* Check count of copy-out responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_OUT &&
		combiner->copy_out_count != combiner->node_count)
		return false;

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
void
CloseCombiner(RemoteQueryState *combiner)
{
	if (combiner)
	{
		list_free(combiner->cur_handles);
		list_free(combiner->all_handles);
		combiner->cur_handles = NIL;
		combiner->all_handles = NIL;
		if (combiner->connections)
			pfree(combiner->connections);
		if (combiner->tuple_desc)
		{
			/*
			 * In the case of a remote COPY with tuplestore, combiner is not
			 * responsible from freeing the tuple store. This is done at an upper
			 * level once data redistribution is completed.
			 */
			if (combiner->remoteCopyType != REMOTE_COPY_TUPLESTORE)
				FreeTupleDesc(combiner->tuple_desc);
		}
		pfree(combiner->errorMessage.data);
		if (combiner->errorDetail)
			pfree(combiner->errorDetail);
		if(combiner->errorNodeName)
			pfree(combiner->errorNodeName);
		if (combiner->cursor_connections)
			pfree(combiner->cursor_connections);
		if (combiner->tapenodes)
			pfree(combiner->tapenodes);
		pfree(combiner);
	}
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
static bool
ValidateAndCloseCombiner(RemoteQueryState *combiner)
{
	bool		valid = validate_combiner(combiner);

	CloseCombiner(combiner);

	return valid;
}

/*
 * It is possible if multiple steps share the same Datanode connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all Datanode responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn)
{
	RemoteQueryState *combiner = conn->combiner;
	MemoryContext oldcontext;

	if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
		return;

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ss_ScanTupleSlot->tts_mcxt);

	/* Verify the connection is in use by the combiner */
	combiner->current_conn = 0;
	while (combiner->current_conn < combiner->conn_count)
	{
		if (combiner->connections[combiner->current_conn] == conn)
			break;
		combiner->current_conn++;
	}
	Assert(combiner->current_conn <= combiner->conn_count);

	/*
	 * Buffer data rows until Datanode return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (conn->state == DN_CONNECTION_STATE_QUERY)
	{
		int res;

		/* Move to buffer currentRow (received from the Datanode) */
		if (combiner->currentRow.msg)
		{
			RemoteDataRow dataRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData));
			*dataRow = combiner->currentRow;
			combiner->currentRow.msg = NULL;
			combiner->currentRow.msglen = 0;
			combiner->currentRow.msgnode = 0;
			combiner->rowBuffer = lappend(combiner->rowBuffer, dataRow);
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */
		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
			{
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "Failed to fetch from Datanode %s", NameStr(conn->name));
			}
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/* Remove current connection, move last in-place, adjust current_conn */
			if (combiner->conn_count > 0 && combiner->current_conn < --combiner->conn_count)
				combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
			else
				combiner->current_conn = 0;
		}
		/*
		 * Before output RESPONSE_COMPLETE or PORTAL_SUSPENDED handle_response()
		 * changes connection state to DN_CONNECTION_STATE_IDLE, breaking the
		 * loop. We do not need to do anything specific in case of
		 * PORTAL_SUSPENDED so skiping "else if" block for that case
		 */
	}
	MemoryContextSwitchTo(oldcontext);
	conn->combiner = NULL;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
static void
CopyDataRowTupleToSlot(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	char 		*msg;
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
	msg = (char *)palloc(combiner->currentRow.msglen);
	memcpy(msg, combiner->currentRow.msg, combiner->currentRow.msglen);
	ExecStoreDataRowTuple(msg, combiner->currentRow.msglen,
							combiner->currentRow.msgnode, slot, true);
	pfree(combiner->currentRow.msg);
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Read next message from the connection and update the combiner accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_COMPLETE - done with the connection
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response(PGXCNodeHandle * conn, RemoteQueryState *combiner)
{
	char	   *msg;
	int			msg_len;
	char		msg_type;
	bool		suspended = false;

	for (;;)
	{
		Assert(conn->state != DN_CONNECTION_STATE_IDLE);

		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return RESPONSE_COMPLETE;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		Assert(conn->combiner == combiner || conn->combiner == NULL);

		/* TODO handle other possible responses */
		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				return RESPONSE_EOF;
			case 'c':			/* CopyToCommandComplete */
				HandleCopyOutComplete(combiner);
				break;
			case 'C':			/* CommandComplete */
				HandleCommandComplete(combiner, msg, msg_len, conn);
				break;
			case 'T':			/* RowDescription */
#ifdef DN_CONNECTION_DEBUG
				Assert(!conn->have_row_desc);
				conn->have_row_desc = true;
#endif
				if (HandleRowDescription(combiner, msg, msg_len))
					return RESPONSE_TUPDESC;
				break;
			case 'D':			/* DataRow */
#ifdef DN_CONNECTION_DEBUG
				Assert(conn->have_row_desc);
#endif
				HandleDataRow(combiner, msg, msg_len, conn->nodeoid);
				return RESPONSE_DATAROW;
			case 's':			/* PortalSuspended */
				suspended = true;
				break;
			case '1': /* ParseComplete */
			case '2': /* BindComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				combiner->request_type = REQUEST_TYPE_QUERY;
				/* simple notifications, continue reading */
				break;
			case 'G': /* CopyInResponse */
				conn->state = DN_CONNECTION_STATE_COPY_IN;
				HandleCopyIn(combiner);
				/* Done, return to caller to let it know the data can be passed in */
				return RESPONSE_COPY;
			case 'H': /* CopyOutResponse */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyOut(combiner);
				return RESPONSE_COPY;
			case 'd': /* CopyOutDataRow */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyDataRow(combiner, msg, msg_len);
				break;
			case 'E':			/* ErrorResponse */
				{
					char *errmsg;
					errmsg = HandleError(NameStr(conn->name), combiner, msg, msg_len);
					add_error_message(conn, "%s", errmsg);
					/*
					 * Do not return with an error, we still need to consume Z,
					 * ready-for-query
					 */
				}
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
			case 'S':			/* SetCommandComplete */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
			{
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED Coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				int result = suspended ? RESPONSE_SUSPENDED : RESPONSE_COMPLETE;
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				return result;
			}
			case 'M':			/* Command Id */
				HandleDatanodeCommandId(combiner, msg, msg_len);
				break;
			case 'b':
				conn->state = DN_CONNECTION_STATE_IDLE;
				return RESPONSE_BARRIER_OK;
			case 'I':			/* EmptyQuery */
			default:
				/* sync lost? */
				elog(WARNING, "Received unsupported message type: %c", msg_type);
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				/* stop reading */
				return RESPONSE_COMPLETE;
		}
	}
	/* never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}

RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;
	TupleDesc			scan_type;

	/* RemoteQuery node is the leaf node in the plan tree, just like seqscan */
	Assert(innerPlan(node) == NULL);
	Assert(outerPlan(node) == NULL);

	remotestate = CreateResponseCombiner(0, node->combine_type);
	remotestate->ss.ps.plan = (Plan *) node;
	remotestate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialisation
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &remotestate->ss.ps);

	/* Initialise child expressions */
	remotestate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) remotestate);
	remotestate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) remotestate);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK)));

	/* Extract the eflags bits that are relevant for tuplestorestate */
	remotestate->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD));

	/*
	 * We anyways have to support BACKWARD for cache tuples.
	 */
	remotestate->eflags |= (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD);

	/*
	 * tuplestorestate of RemoteQueryState is for two purposes,
	 * one is rescan (see ExecRemoteQueryReScan), the other is cache
	 * (see HandleCache)
	 */
	remotestate->tuplestorestate = tuplestore_begin_remoteheap(false, false, work_mem);
	tuplestore_set_eflags(remotestate->tuplestorestate, remotestate->eflags);

	remotestate->eof_underlying = false;

	ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
	ExecInitScanTupleSlot(estate, &remotestate->ss);
	scan_type = ExecTypeFromTL(node->base_tlist, false);
	ExecAssignScanType(&remotestate->ss, scan_type);
	remotestate->nextSlot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(remotestate->nextSlot, scan_type);
	remotestate->convertSlot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(remotestate->convertSlot, scan_type);

	/*
	 * convert will be set while the tuple description
	 * is set correctly.
	 */
	remotestate->recvState = (ClusterRecvState *) palloc0(sizeof(ClusterRecvState));
	remotestate->recvState->ps = &remotestate->ss.ps;

	remotestate->ss.ps.ps_TupFromTlist = false;

	/*
	 * If there are parameters supplied, get them into a form to be sent to the
	 * Datanodes with bind message. We should not have had done this before.
	 */
	SetDataRowForExtParams(estate->es_param_list_info, remotestate);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&remotestate->ss.ps);
	ExecAssignScanProjectionInfo(&remotestate->ss);

	if (node->rq_save_command_id)
	{
		/* Save command id to be used in some special cases */
		remotestate->rqs_cmd_id = GetCurrentCommandId(false);
	}

	return remotestate;
}

/*
 * IsReturningDMLOnReplicatedTable
 *
 * This function returns true if the passed RemoteQuery
 * 1. Operates on a table that is replicated
 * 2. Represents a DML
 * 3. Has a RETURNING clause in it
 *
 * If the passed RemoteQuery has a non null base_tlist
 * means that DML has a RETURNING clause.
 */

static bool
IsReturningDMLOnReplicatedTable(RemoteQuery *rq)
{
	if (IsExecNodesReplicated(rq->exec_nodes) &&
		rq->base_tlist != NULL &&	/* Means DML has RETURNING */
		(rq->exec_nodes->accesstype == RELATION_ACCESS_UPDATE ||
		rq->exec_nodes->accesstype == RELATION_ACCESS_INSERT))
		return true;

	return false;
}

/*
 * ExecRemoteQuery
 * Wrapper around the main RemoteQueryNext() function. This
 * wrapper provides materialization of the result returned by
 * RemoteQueryNext
 */

TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	return ExecScan(&(node->ss),
					(ExecScanAccessMtd) RemoteQueryNext,
					(ExecScanRecheckMtd) RemoteQueryRecheck);
}

/*
 * RemoteQueryRecheck -- remote query routine to recheck a tuple in EvalPlanQual
 */
static bool
RemoteQueryRecheck(RemoteQueryState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, RemoteQueryScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}
/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the Datanodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
static TupleTableSlot *
RemoteQueryNext(ScanState *scan_node)
{
	RemoteQueryState   *node = (RemoteQueryState *)scan_node;
	TupleTableSlot	   *scanslot = scan_node->ss_ScanTupleSlot;
	RemoteQuery		   *rq = (RemoteQuery*) node->ss.ps.plan;
	EState			   *estate = node->ss.ps.state;

	/*
	 * Initialize tuples processed to 0, to make sure we don't re-use the
	 * values from the earlier iteration of RemoteQueryNext(). For an FQS'ed
	 * DML returning query, it may not get updated for subsequent calls.
	 * because there won't be a HandleCommandComplete() call to update this
	 * field.
	 */
	node->rqs_processed = 0;

	if (!node->query_Done)
	{
		/* Fire BEFORE STATEMENT triggers just before the query execution */
		pgxc_rq_fire_bstriggers(node);
		scanslot = StartRemoteQuery(node, scanslot);
		node->query_Done = true;
	} else
		ExecClearTuple(scanslot);

	if (node->update_cursor)
	{
#ifdef ADB
		ereport(ERROR,
				(errmsg("The new version of ADB communication has not yet covered this use case.")));
#endif
#if 0
		PGXCNodeAllHandles *all_dn_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
		close_node_cursors(all_dn_handles->datanode_handles,
						   all_dn_handles->dn_conn_count,
						   node->update_cursor);
		pfree(node->update_cursor);
		node->update_cursor = NULL;
		pfree_pgxc_all_handles(all_dn_handles);
#endif
	} else
	if (TupIsNull(scanslot))
	{
		scanslot = FetchRemoteQuery(node, scanslot);
		node->eof_underlying = TupIsNull(scanslot);
	}

	/* report error if any */
	pgxc_node_report_error(node);

	/*
	 * Now we know the query is successful. Fire AFTER STATEMENT triggers. Make
	 * sure this is the last iteration of the query. If an FQS query has
	 * RETURNING clause, this function can be called multiple times until we
	 * return NULL.
	 */
	if (TupIsNull(scanslot))
		pgxc_rq_fire_astriggers(node);

	/*
	 * If it's an FQSed DML query for which command tag is to be set,
	 * then update estate->es_processed. For other queries, the standard
	 * executer takes care of it; namely, in ExecModifyTable for DML queries
	 * and ExecutePlan for SELECT queries.
	 */
	if (rq->remote_query &&
		rq->remote_query->canSetTag &&
		!rq->rq_params_internal &&
		(rq->remote_query->commandType == CMD_INSERT ||
		 rq->remote_query->commandType == CMD_UPDATE ||
		 rq->remote_query->commandType == CMD_DELETE))
		estate->es_processed += node->rqs_processed;

	return scanslot;
}

/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ListCell *lc;

	/* clean up the buffer */
	foreach(lc, node->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(node->rowBuffer);

	node->current_conn = 0;
	while (node->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = node->connections[node->current_conn];

		/* throw away message */
		if (node->currentRow.msg)
		{
			pfree(node->currentRow.msg);
			node->currentRow.msg = NULL;
		}

		if (conn == NULL)
		{
			node->conn_count--;
			continue;
		}

		/* no data is expected */
		if (conn->state == DN_CONNECTION_STATE_IDLE ||
				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			if (node->current_conn < --node->conn_count)
				node->connections[node->current_conn] = node->connections[node->conn_count];
			continue;
		}
		res = handle_response(conn, node);
		if (res == RESPONSE_EOF)
		{
			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT;
			timeout.tv_usec = 0;

			if (pgxc_node_receive(1, &conn, &timeout))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from Datanodes when ending query")));
		}
	}

	if (node->tuplestorestate != NULL)
		ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecClearTuple(node->nextSlot);

	freeClusterRecvState(node->recvState);

	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * If there are active cursors close them
	 */
	if (node->cursor || node->update_cursor)
	{
#if 0
		PGXCNodeAllHandles *all_handles = NULL;
		PGXCNodeHandle    **cur_handles;
		bool bFree = false;
		int nCount;
		int i;

		cur_handles = node->cursor_connections;
		nCount = node->cursor_count;

		for(i=0;i<node->cursor_count;i++)
		{
			if (node->cursor_connections == NULL || node->cursor_connections[i]->sock == -1)
			{
				bFree = true;
				all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
				cur_handles = all_handles->datanode_handles;
				nCount = all_handles->dn_conn_count;
				break;
			}
		}

		if (node->cursor)
		{
			close_node_cursors(cur_handles, nCount, node->cursor);
			pfree(node->cursor);
			node->cursor = NULL;
		}

		if (node->update_cursor)
		{
			close_node_cursors(cur_handles, nCount, node->update_cursor);
			pfree(node->update_cursor);
			node->update_cursor = NULL;
		}

		if (bFree)
			pfree_pgxc_all_handles(all_handles);
#endif
	}

	/*
	 * Clean up parameters if they were set
	 */
	if (node->paramval_data)
	{
		pfree(node->paramval_data);
		node->paramval_data = NULL;
		node->paramval_len = 0;
	}

	/* Free the param types if they are newly allocated */
	if (node->rqs_param_types &&
		node->rqs_param_types != ((RemoteQuery*)node->ss.ps.plan)->rq_param_types)
	{
		pfree(node->rqs_param_types);
		node->rqs_param_types = NULL;
		node->rqs_num_params = 0;
	}

	if (node->ss.ss_currentRelation)
		ExecCloseScanRelation(node->ss.ss_currentRelation);

	HandleListResetOwner(node->all_handles);

	CloseCombiner(node);
}

static void
close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor)
{
	int i;
	RemoteQueryState *combiner;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], false, cursor) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		if (pgxc_node_send_sync(connections[i]) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		/*
		 * No matter any connection has crash down, we still need to deal with
		 * other connections.
		 */
		pgxc_node_receive(conn_count, connections, NULL);
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				// Unexpected response, ignore?
				if (connections[i]->error == NULL)
				{
					add_error_message(connections[i],
						"Unexpected response from node %s", NameStr(connections[i]->name));
				}
				if (combiner->errorMessage.len == 0)
				{
					appendStringInfo(&(combiner->errorMessage),
						"Unexpected response from node %s", NameStr(connections[i]->name));
				}
				/* Stop tracking and move last connection in place */
				conn_count--;
				if (i < conn_count)
					connections[i] = connections[conn_count];
			}
		}
	}

	pgxc_node_report_error(combiner);

	ValidateAndCloseCombiner(combiner);
}


/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = 0;
	RemoteQuery *node = (RemoteQuery*) rq_state->ss.ps.plan;

	/* If there are no parameters, there is no data to BIND. */
	if (!paraminfo)
		return;

	/*
	 * If this query has been generated internally as a part of two-step DML
	 * statement, it uses only the internal parameters for input values taken
	 * from the source data, and it never uses external parameters. So even if
	 * parameters were being set externally, they won't be present in this
	 * statement (they might be present in the source data query). In such
	 * case where parameters refer to the values returned by SELECT query, the
	 * parameter data and parameter types would be set in SetDataRowForIntParams().
	 */
	if (node->rq_params_internal)
		return;

	Assert(!rq_state->paramval_data);

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	for (i = 0; i < paraminfo->numParams; i++)
	{
		ParamExternData *param;

		param = &paraminfo->params[i];

		if (!OidIsValid(param->ptype) && paraminfo->paramFetch != NULL)
			(*paraminfo->paramFetch) (paraminfo, i + 1);

		/*
		 * This is the last parameter found as useful, so we need
		 * to include all the previous ones to keep silent the remote
		 * nodes. All the parameters prior to the last usable having no
		 * type available will be considered as NULL entries.
		 */
		if (OidIsValid(param->ptype))
			real_num_params = i + 1;
	}

	/*
	 * If there are no parameters available, simply leave.
	 * This is possible in the case of a query called through SPI
	 * and using no parameters.
	 */
	if (real_num_params == 0)
	{
		rq_state->paramval_data = NULL;
		rq_state->paramval_len = 0;
		return;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < real_num_params; i++)
	{
		ParamExternData *param = &paraminfo->params[i];
		uint32 n32;

		/*
		 * Parameters with no types are considered as NULL and treated as integer
		 * The same trick is used for dropped columns for remote DML generation.
		 */
		if (param->isnull || !OidIsValid(param->ptype))
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;

			/* Get info needed to output the value */
			getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
		}
	}


	/*
	 * If parameter types are not already set, infer them from
	 * the paraminfo.
	 */
	if (node->rq_num_params > 0)
	{
		/*
		 * Use the already known param types for BIND. Parameter types
		 * can be already known when the same plan is executed multiple
		 * times.
		 */
		if (node->rq_num_params != real_num_params)
			elog(ERROR, "Number of user-supplied parameters do not match "
						"the number of remote parameters");
		rq_state->rqs_num_params = node->rq_num_params;
		rq_state->rqs_param_types = node->rq_param_types;
	}
	else
	{
		rq_state->rqs_num_params = real_num_params;
		rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
		for (i = 0; i < real_num_params; i++)
			rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;
}


/* ----------------------------------------------------------------
 *		ExecRemoteQueryReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt)
{
	/*
	 * If the materialized store is not empty, just rewind the stored output.
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (!node->tuplestorestate)
		return;

	tuplestore_rescan(node->tuplestorestate);
}

/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Clean up prepared transactions before releasing connections */
	DropAllPreparedStatements();

	/* Disconnect with rxact manager process */
	DisconnectRemoteXact();

	/*
	 * Make sure the old NodeHandle will never keep anything about the
	 * last SQL.
	 */
	ResetNodeExecutor();

	/* Make sure the old PGconn will dump the trash data */
	PQNReleaseAllConnect();

	/* Disconnect from Pooler */
	PoolManagerDisconnect();

	/* Close connection with AGTM */
	agtm_Close();
}

void
ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle	  **connections;
	RemoteQueryState   *combiner;
	int					conn_count;
	int 				i;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

	/* get needed Datanode connections */
	all_handles = get_handles(nodelist, NIL, false);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
		{
			/*
			 * statements are not affected by statement end, so consider
			 * unclosed statement on the Datanode as a fatal issue and
			 * force connection is discarded
			 */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statemrnt")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		/*
		 * No matter any connection has crash down, we still need to deal with
		 * other connections.
		 */
		pgxc_node_receive(conn_count, connections, NULL);
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
				if (connections[i]->error == NULL)
				{
					add_error_message(connections[i],
						"Failed to close Datanode %s statement", NameStr(connections[i]->name));
				}
				if (combiner->errorMessage.len == 0)
				{
					appendStringInfo(&(combiner->errorMessage),
						"Failed to close Datanode %s statement", NameStr(connections[i]->name));
				}
				/* Stop tracking and move last connection in place */
				conn_count--;
				if (i < conn_count)
					connections[i] = connections[conn_count];
			}
		}
	}

	pgxc_node_report_error(combiner);

	ValidateAndCloseCombiner(combiner);
	pfree_pgxc_all_handles(all_handles);
}

/*
 * ExecProcNodeDMLInXC
 *
 * This function is used by ExecInsert/Update/Delete to execute the
 * Insert/Update/Delete on the datanode using RemoteQuery plan.
 *
 * In XC, a non-FQSed UPDATE/DELETE is planned as a two step process
 * The first step selects the ctid & node id of the row to be modified and the
 * second step creates a parameterized query that is supposed to take the data
 * row returned by the lower plan node as the parameters to modify the affected
 * row. In case of an INSERT however the first step is used to get the new
 * column values to be inserted in the target table and the second step uses
 * those values as parameters of the INSERT query.
 *
 * We use extended query protocol to avoid repeated planning of the query and
 * pass the column values(in case of an INSERT) and ctid & xc_node_id
 * (in case of UPDATE/DELETE) as parameters while executing the query.
 *
 * Parameters:
 * resultRemoteRel:  The RemoteQueryState containing DML statement to be
 *					 executed
 * sourceDataSlot: The tuple returned by the first step (described above)
 *					 to be used as parameters in the second step.
 * newDataSlot: This has all the junk attributes stripped off from
 *				sourceDataSlot, plus BEFORE triggers may have modified the
 *				originally fetched data values. In other words, this has
 *				the final values that are to be sent to datanode through BIND.
 *
 * Returns the result of RETURNING clause if any
 */
TupleTableSlot *
ExecProcNodeDMLInXC(EState *estate,
					TupleTableSlot *sourceDataSlot,
					TupleTableSlot *newDataSlot)
{
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
	RemoteQueryState *resultRemoteRel = (RemoteQueryState *) estate->es_result_remoterel;
	ExprContext	*econtext = resultRemoteRel->ss.ps.ps_ExprContext;
	TupleTableSlot	*returningResultSlot = NULL;	/* RETURNING clause result */
	TupleTableSlot	*temp_slot;
	bool			dml_returning_on_replicated = false;
	RemoteQuery		*step = (RemoteQuery *) resultRemoteRel->ss.ps.plan;
	uint32			save_rqs_processed = 0;

	/*
	 * If the tuple returned by the previous step was null,
	 * simply return null tuple, no need to execute the DML
	 */
	if (TupIsNull(sourceDataSlot))
		return NULL;

	/*
	 * The current implementation of DMLs with RETURNING when run on replicated
	 * tables returns row from one of the datanodes. In order to achieve this
	 * ExecProcNode is repeatedly called saving one tuple and rejecting the rest.
	 * Do we have a DML on replicated table with RETURNING?
	 */
	dml_returning_on_replicated = IsReturningDMLOnReplicatedTable(step);

	/*
	 * Use data row returned by the previous step as parameter for
	 * the DML to be executed in this step.
	 */
	SetDataRowForIntParams(resultRelInfo->ri_junkFilter,
						   sourceDataSlot, newDataSlot, resultRemoteRel);

	/*
	 * do_query calls get_exec_connections to determine target nodes
	 * at execution time. The function get_exec_connections can decide
	 * to evaluate en_expr to determine the target nodes. To evaluate en_expr,
	 * ExecEvalVar is called which picks up values from ecxt_scantuple if Var
	 * does not refer either OUTER or INNER varno. Hence we should copy the
	 * tuple returned by previous step in ecxt_scantuple if econtext is set.
	 * The econtext is set only when en_expr is set for execution time
	 * determination of the target nodes.
	 */

	if (econtext)
		econtext->ecxt_scantuple = newDataSlot;


	/*
	 * This loop would be required to reject tuples received from datanodes
	 * when a DML with RETURNING is run on a replicated table otherwise it
	 * would run once.
	 * PGXC_TODO: This approach is error prone if the DML statement constructed
	 * by the planner is such that it updates more than one row (even in case of
	 * non-replicated data). Fix it.
	 */
	do
	{
		temp_slot = ExecProcNode((PlanState *)resultRemoteRel);
		if (!TupIsNull(temp_slot))
		{
			/* Have we already copied the returned tuple? */
			if (returningResultSlot == NULL)
			{
				/* Copy the received tuple to be returned later */
				returningResultSlot = MakeSingleTupleTableSlot(temp_slot->tts_tupleDescriptor);
				returningResultSlot = ExecCopySlot(returningResultSlot, temp_slot);
			}
			/* Clear the received tuple, the copy required has already been saved */
			ExecClearTuple(temp_slot);
		}
		else
		{
			/* Null tuple received, so break the loop */
			ExecClearTuple(temp_slot);
			break;
		}

		/*
		 * If we don't save rqs_processed, it will be assigned to 0 when enter
		 * RemoteQueryNext(see more details) again, and when break the loop,
		 * the caller will nerver get correct rqs_processed. See ExecInsert.
		 *
		 * eg.
		 *		create table t1(id int, value int) distribute by replication.
		 *		insert into t1 values(1,1),(2,2),(3,3) returning id;
		 */
		if (dml_returning_on_replicated)
			save_rqs_processed = resultRemoteRel->rqs_processed;

	} while (dml_returning_on_replicated);

	if (dml_returning_on_replicated)
		resultRemoteRel->rqs_processed = save_rqs_processed;

	/*
	 * A DML can impact more than one row, e.g. an update without any where
	 * clause on a table with more than one row. We need to make sure that
	 * RemoteQueryNext calls do_query for each affected row, hence we reset
	 * the flag here and finish the DML being executed only when we return
	 * NULL from ExecModifyTable
	 */
	resultRemoteRel->query_Done = false;

	return returningResultSlot;
}

/*
 * pgxc_node_report_error
 * Throw error from Datanode if any.
 */
static void
pgxc_node_report_error(RemoteQueryState *combiner)
{
	/* If no combiner, nothing to do */
	if (!combiner)
		return;
	if (combiner->errorMessage.len > 0)
	{
		char *code = combiner->errorCode;
		int i;
		for (i = 0; i < combiner->conn_count; i++)
			pgxc_node_flush_read(combiner->connections[i]);

		ereport(ERROR,
				(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
				errmsg("%s", combiner->errorMessage.data),
				combiner->errorDetail ? errdetail("%s", combiner->errorDetail) : 0,
				combiner->errorNodeName ? errnode(combiner->errorNodeName) : 0));

	}
}

/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size)
{
	void *fparams;

	fparams = MemoryContextAlloc(TopMemoryContext, paraminfo_size);
	memcpy(fparams, paraminfo, paraminfo_size);

	dbcleanup_info.function = function;
	dbcleanup_info.fparams = fparams;
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void AtEOXact_DBCleanup(bool isCommit)
{
	if (dbcleanup_info.function)
		(*dbcleanup_info.function)(isCommit, dbcleanup_info.fparams);

	/*
	 * Just reset the callbackinfo. We anyway don't want this to be called again,
	 * until explicitly set.
	 */
	dbcleanup_info.function = NULL;
	if (dbcleanup_info.fparams)
	{
		pfree(dbcleanup_info.fparams);
		dbcleanup_info.fparams = NULL;
	}
}


/*
 * SetDataRowForIntParams: Form a BIND data row for internal parameters.
 * This function is called when the data for the parameters of remote
 * statement resides in some plan slot of an internally generated remote
 * statement rather than from some extern params supplied by the caller of the
 * query. Currently DML is the only case where we generate a query with
 * internal parameters.
 * The parameter data is constructed from the slot data, and stored in
 * RemoteQueryState.paramval_data.
 * At the same time, remote parameter types are inferred from the slot
 * tuple descriptor, and stored in RemoteQueryState.rqs_param_types.
 * On subsequent calls, these param types are re-used.
 * The data to be BOUND consists of table column data to be inserted/updated
 * and the ctid/nodeid values to be supplied for the WHERE clause of the
 * query. The data values are present in dataSlot whereas the ctid/nodeid
 * are available in sourceSlot as junk attributes.
 * sourceSlot is used only to retrieve ctid/nodeid, so it does not get
 * used for INSERTs, although it will never be NULL.
 * The slots themselves are undisturbed.
 */
static void
SetDataRowForIntParams(JunkFilter *junkfilter,
					   TupleTableSlot *sourceSlot, TupleTableSlot *dataSlot,
					   RemoteQueryState *rq_state)
{
	StringInfoData	buf;
	uint16			numparams = 0;
	RemoteQuery		*step = (RemoteQuery *) rq_state->ss.ps.plan;

	Assert(sourceSlot);

	/* Calculate the total number of parameters */
	if (step->rq_max_param_num > 0)
		numparams = step->rq_max_param_num;
	else if (dataSlot)
		numparams = dataSlot->tts_tupleDescriptor->natts;
	/* Add number of junk attributes */
	if (junkfilter)
	{
		if (junkfilter->jf_junkAttNo)
			numparams++;
		if (junkfilter->jf_xc_node_id)
			numparams++;
	}

	/*
	 * Infer param types from the slot tupledesc and junk attributes. But we
	 * have to do it only the first time: the interal parameters remain the same
	 * while processing all the source data rows because the data slot tupdesc
	 * never changes. Even though we can determine the internal param types
	 * during planning, we want to do it here: we don't want to set the param
	 * types and param data at two different places. Doing them together here
	 * helps us to make sure that the param types are in sync with the param
	 * data.
	 */

	/*
	 * We know the numparams, now initialize the param types if not already
	 * done. Once set, this will be re-used for each source data row.
	 */
	if (rq_state->rqs_num_params == 0)
	{
		int	attindex = 0;

		rq_state->rqs_num_params = numparams;
		rq_state->rqs_param_types =
			(Oid *) palloc(sizeof(Oid) * rq_state->rqs_num_params);

		if (dataSlot) /* We have table attributes to bind */
		{
			TupleDesc tdesc = dataSlot->tts_tupleDescriptor;
			int numatts = tdesc->natts;

			if (step->rq_max_param_num > 0)
				numatts = step->rq_max_param_num;

			for (attindex = 0; attindex < numatts; attindex++)
			{
				rq_state->rqs_param_types[attindex] =
					tdesc->attrs[attindex]->atttypid;
			}
		}
		if (junkfilter) /* Param types for specific junk attributes if present */
		{
			/* jf_junkAttNo always contains ctid */
			if (AttributeNumberIsValid(junkfilter->jf_junkAttNo))
				rq_state->rqs_param_types[attindex] = TIDOID;

			if (AttributeNumberIsValid(junkfilter->jf_xc_node_id))
				rq_state->rqs_param_types[attindex + 1] = INT4OID;
		}
	}
	else
	{
		Assert(rq_state->rqs_num_params == numparams);
	}

	/*
	 * If we already have the data row, just copy that, and we are done. One
	 * scenario where we can have the data row is for INSERT ... SELECT.
	 * Effectively, in this case, we just re-use the data row from SELECT as-is
	 * for BIND row of INSERT. But just make sure all of the data required to
	 * bind is available in the slot. If there are junk attributes to be added
	 * in the BIND row, we cannot re-use the data row as-is.
	 */
	if (!junkfilter && dataSlot && dataSlot->tts_dataRow)
	{
		rq_state->paramval_data = (char *)palloc(dataSlot->tts_dataLen);
		memcpy(rq_state->paramval_data, dataSlot->tts_dataRow, dataSlot->tts_dataLen);
		rq_state->paramval_len = dataSlot->tts_dataLen;
		return;
	}

	initStringInfo(&buf);

	{
		uint16 params_nbo = htons(numparams); /* Network byte order */
		appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
	}

	if (dataSlot)
	{
		TupleDesc	 	tdesc = dataSlot->tts_tupleDescriptor;
		int				attindex;
		int				numatts = tdesc->natts;

		/* Append the data attributes */

		if (step->rq_max_param_num > 0)
			numatts = step->rq_max_param_num;

		/* ensure we have all values */
		slot_getallattrs(dataSlot);
		for (attindex = 0; attindex < numatts; attindex++)
		{
			uint32 n32;
			Assert(attindex < numparams);

			if (dataSlot->tts_isnull[attindex])
			{
				n32 = htonl(-1);
				appendBinaryStringInfo(&buf, (char *) &n32, 4);
			}
			else
				pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], tdesc->attrs[attindex]->atttypid);

		}
	}

	/*
	 * From the source data, fetch the junk attribute values to be appended in
	 * the end of the data buffer. The junk attribute vals like ctid and
	 * xc_node_id are used in the WHERE clause parameters.
	 * These attributes would not be present for INSERT.
	 */
	if (junkfilter)
	{
		/* First one - jf_junkAttNo - always reprsents ctid */
		pgxc_append_param_junkval(sourceSlot, junkfilter->jf_junkAttNo,
								  TIDOID, &buf);
		pgxc_append_param_junkval(sourceSlot, junkfilter->jf_xc_node_id,
								  INT4OID, &buf);
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;

}


/*
 * pgxc_append_param_junkval:
 * Append into the data row the parameter whose value cooresponds to the junk
 * attributes in the source slot, namely ctid or node_id.
 */
static void
pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno,
						  Oid valtype, StringInfo buf)
{
	bool isNull;

	if (slot && attno != InvalidAttrNumber)
	{
		/* Junk attribute positions are saved by ExecFindJunkAttribute() */
		Datum val = ExecGetJunkAttribute(slot, attno, &isNull);
		/* shouldn't ever get a null result... */
		if (isNull)
			elog(ERROR, "NULL junk attribute");

		pgxc_append_param_val(buf, val, valtype);
	}
}

/*
 * pgxc_append_param_val:
 * Append the parameter value for the SET clauses of the UPDATE statement.
 * These values are the table attribute values from the dataSlot.
 */
static void
pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype)
{
	/* Convert Datum to string */
	char *pstring;
	int len;
	uint32 n32;
	Oid		typOutput;
	bool	typIsVarlena;

	/* Get info needed to output the value */
	getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
	/*
	 * If we have a toasted datum, forcibly detoast it here to avoid
	 * memory leakage inside the type's output routine.
	 */
	if (typIsVarlena)
		val = PointerGetDatum(PG_DETOAST_DATUM(val));

	pstring = OidOutputFunctionCall(typOutput, val);

	/* copy data to the buffer */
	len = strlen(pstring);
	n32 = htonl(len);
	appendBinaryStringInfo(buf, (char *) &n32, 4);
	appendBinaryStringInfo(buf, pstring, len);
}

/*
 * pgxc_rq_fire_bstriggers:
 * BEFORE STATEMENT triggers to be fired for a user-supplied DML query.
 * For non-FQS query, we internally generate remote DML query to be executed
 * for each row to be processed. But we do not want to explicitly fire triggers
 * for such a query; ExecModifyTable does that for us. It is the FQS DML query
 * where we need to explicitly fire statement triggers on coordinator. We
 * cannot run stmt triggers on datanode. While we can fire stmt trigger on
 * datanode versus coordinator based on the function shippability, we cannot
 * do the same for FQS query. The datanode has no knowledge that the trigger
 * being fired is due to a non-FQS query or an FQS query. Even though it can
 * find that all the triggers are shippable, it won't know whether the stmt
 * itself has been FQSed. Even though all triggers were shippable, the stmt
 * might have been planned on coordinator due to some other non-shippable
 * clauses. So the idea here is to *always* fire stmt triggers on coordinator.
 * Note that this does not prevent the query itself from being FQSed. This is
 * because we separately fire stmt triggers on coordinator.
 */
static void
pgxc_rq_fire_bstriggers(RemoteQueryState *node)
{
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/* If it's not an internally generated query, fire BS triggers */
	if (!rq->rq_params_internal && estate->es_result_relations)
	{
		Assert(rq->remote_query);
		switch (rq->remote_query->commandType)
		{
			case CMD_INSERT:
				ExecBSInsertTriggers(estate, estate->es_result_relations);
				break;
			case CMD_UPDATE:
				ExecBSUpdateTriggers(estate, estate->es_result_relations);
				break;
			case CMD_DELETE:
				ExecBSDeleteTriggers(estate, estate->es_result_relations);
				break;
			default:
				break;
		}
	}
}

/*
 * pgxc_rq_fire_astriggers:
 * AFTER STATEMENT triggers to be fired for a user-supplied DML query.
 * See comments in pgxc_rq_fire_astriggers()
 */
static void
pgxc_rq_fire_astriggers(RemoteQueryState *node)
{
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/* If it's not an internally generated query, fire AS triggers */
	if (!rq->rq_params_internal && estate->es_result_relations)
	{
		Assert(rq->remote_query);
		switch (rq->remote_query->commandType)
		{
			case CMD_INSERT:
				ExecASInsertTriggers(estate, estate->es_result_relations);
				break;
			case CMD_UPDATE:
				ExecASUpdateTriggers(estate, estate->es_result_relations);
				break;
			case CMD_DELETE:
				ExecASDeleteTriggers(estate, estate->es_result_relations);
				break;
			default:
				break;
		}
	}
}
