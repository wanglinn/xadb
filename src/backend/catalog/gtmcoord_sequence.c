#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/printtup.h"
#include "catalog/pg_sequence.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "storage/lock.h"
#include "agtm/agtm.h"
#include "agtm/agtm_utils.h"
#include "commands/sequence.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"
#include "libpq/pqformat.h"

static Oid SequenceSystemClassOid(char* schema, char* sequencename);

static	void parse_seqFullName_to_details(StringInfo message, char ** dbName, 
							char ** schemaName, char ** sequenceName)
{
	int	 sequenceSize = 0;
	int  dbNameSize = 0;
	int  schemaSize = 0;

	sequenceSize = pq_getmsgint(message, sizeof(sequenceSize));
	*sequenceName = pnstrdup(pq_getmsgbytes(message, sequenceSize), sequenceSize);
	if(sequenceSize == 0 || *sequenceName == NULL)
		ereport(ERROR,
			(errmsg("sequence name is null")));

	dbNameSize = pq_getmsgint(message, sizeof(dbNameSize));
	*dbName = pnstrdup(pq_getmsgbytes(message, dbNameSize), dbNameSize);
	if(dbNameSize == 0 ||  *dbName == NULL)
		ereport(ERROR,
			(errmsg("sequence database name is null")));

	schemaSize = pq_getmsgint(message, sizeof(schemaSize));
	*schemaName = pnstrdup(pq_getmsgbytes(message, schemaSize), schemaSize);
	if(schemaSize == 0 ||  *schemaName == NULL)
		ereport(ERROR,
			(errmsg("sequence schemaName name is null")));
}

static void
RespondSeqToClient(int64 seq_val, AGTM_ResultType type, StringInfo output)
{
	pq_sendint(output, type, 4);
	pq_sendbytes(output, (char *)&seq_val, sizeof(seq_val));
}

static TupleTableSlot* get_agtm_command_slot(void)
{
	MemoryContext oldcontext;
	static TupleTableSlot *slot = NULL;
	static TupleDesc desc = NULL;

	if(desc == NULL)
	{
		TupleDesc volatile temp = NULL;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		PG_TRY();
		{
			temp = CreateTemplateTupleDesc(1);
			TupleDescInitEntry((TupleDesc)temp, 1, "result", BYTEAOID, -1, 0);
		}PG_CATCH();
		{
			if(temp)
				FreeTupleDesc((TupleDesc)temp);
			PG_RE_THROW();
		}PG_END_TRY();
		desc = (TupleDesc)temp;
		MemoryContextSwitchTo(oldcontext);
	}
	if(slot == NULL)
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual);
		MemoryContextSwitchTo(oldcontext);
	}
	return slot;
}

static Datum
prase_to_agtm_sequence_name(StringInfo message)
{
	char* dbName = NULL;
	char* schemaName = NULL;
	char* sequenceName = NULL;	
	StringInfoData	buf;
	Oid			lineOid;

	initStringInfo(&buf);
	parse_seqFullName_to_details(message, &dbName, &schemaName, &sequenceName);
	lineOid = SequenceSystemClassOid(schemaName, sequenceName);

	pfree(sequenceName);
	pfree(dbName);
	pfree(schemaName);

	return ObjectIdGetDatum(lineOid);
}

static StringInfo
ProcessNextSeqCommand(StringInfo message, StringInfo output)
{
	Datum	seq_name_to_oid;
	int64	seq_val;
	int64	min;
	int64	max;
	int64	cache;
	int64	cached;
	int64	inc;
	bool	cycle;

	seq_name_to_oid = prase_to_agtm_sequence_name(message);
	pq_copymsgbytes(message, (char*)&min, sizeof(min));
	pq_copymsgbytes(message, (char*)&max, sizeof(max));
	pq_copymsgbytes(message, (char*)&cache, sizeof(cache));
	pq_copymsgbytes(message, (char*)&inc, sizeof(inc));
	cycle = (bool)pq_getmsgbyte(message);
	pq_getmsgend(message);

	seq_val = agtm_seq_next_value(DatumGetObjectId(seq_name_to_oid),
								  min, max, cache, inc, cycle,
								  &cached);

	/* Respond to the client */
	RespondSeqToClient(seq_val, AGTM_SEQUENCE_GET_NEXT_RESULT, output);
	pq_sendbytes(output, (char*)&cached, sizeof(cached));

	return output;
}

static StringInfo
ProcessSetSeqCommand(StringInfo message, StringInfo output)
{
	int64 seq_nextval;
	bool  iscalled;
	int64 seq_val;
	Datum seq_val_datum;
	Datum seq_name_to_oid;

	seq_name_to_oid= prase_to_agtm_sequence_name(message);
	memcpy(&seq_nextval,pq_getmsgbytes(message, sizeof(seq_nextval)),
		sizeof (seq_nextval));	
	iscalled = pq_getmsgbyte(message);
	pq_getmsgend(message);

	seq_val_datum = DirectFunctionCall3(setval3_oid,
		seq_name_to_oid, seq_nextval, iscalled);

	seq_val = DatumGetInt64(seq_val_datum);

	/* Respond to the client */
	RespondSeqToClient(seq_val,AGTM_SEQUENCE_SET_VAL_RESULT, output);

	return output;
}

static Oid SequenceSystemClassOid(char* schema, char* sequencename)
{
	RangeVar   *sequence;
	Oid			relid;

	sequence = makeRangeVar(schema, sequencename, -1);
	relid = RangeVarGetRelid(sequence, NoLock, false);

	return relid;
}

void
ProcessAGtmCommand(StringInfo input_message, CommandDest dest)
{
	DestReceiver *receiver;
	const char *msg_name;
	StringInfo output;
	AGTM_MessageType mtype;
	StringInfoData buf;

	mtype = pq_getmsgint(input_message, sizeof (AGTM_MessageType));
	msg_name = gtm_util_message_name(mtype);
	set_ps_display(msg_name, true);
	BeginCommand(msg_name, dest);
	ereport(DEBUG1,
		(errmsg("[ pid=%d] Process Command mtype = %s (%d).",
		MyProcPid, msg_name, (int)mtype)));

	
	initStringInfo(&buf);
	buf.len = VARDATA(buf.data) - buf.data;
	switch (mtype)
	{
		case AGTM_MSG_SEQUENCE_SET_VAL:
			PG_TRY();
			{
				output = ProcessSetSeqCommand(input_message, &buf);
			} PG_CATCH();
			{
			/*	CommitTransactionCommand();*/
				PG_RE_THROW();
			} PG_END_TRY();
			break;

		case AGTM_MSG_SEQUENCE_GET_NEXT:
			PG_TRY();
			{
				output = ProcessNextSeqCommand(input_message, &buf);
			} PG_CATCH();
			{
			//CommitTransactionCommand();
				PG_RE_THROW();
			} PG_END_TRY();
			break;

		default:
			ereport(FATAL,
					(EPROTO,
					 errmsg("[ pid=%d] invalid frontend message type %d",
					 MyProcPid, mtype)));
			break;
	}

	if(output != NULL)
	{
		TupleTableSlot *slot;
		static int16 format = 1;

		Assert(output->len >= VARHDRSZ);
		SET_VARSIZE(output->data, output->len);

		slot = ExecClearTuple(get_agtm_command_slot());
		slot->tts_values[0] = PointerGetDatum(output->data);
		slot->tts_isnull[0] = false;
		ExecStoreVirtualTuple(slot);

		receiver = CreateDestReceiver(dest);
		if (dest == DestRemote)
			StartupRemoteDestReceiver(receiver, slot->tts_tupleDescriptor, &format);
		(*receiver->receiveSlot)(slot, receiver);
		(*receiver->rShutdown)(receiver);
		(*receiver->rDestroy)(receiver);
	}

	EndCommand(msg_name, dest);

	pfree(buf.data);
}
