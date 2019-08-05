#include "postgres.h"

#include "executor/executor.h"
#include "pgxc/pgxc.h"

#include "utils/dynamicreduce.h"

void DynamicReduceInitFetch(DynamicReduceIOBuffer *io, dsm_segment *seg, TupleDesc desc,
							void *send_addr, Size send_size, void *recv_addr, Size recv_size)
{
	shm_mq	   *mq;
	
	mq = shm_mq_create(send_addr, send_size);
	shm_mq_set_sender(mq, MyProc);
	io->mqh_sender = shm_mq_attach(mq, seg, NULL);

	mq = shm_mq_create(recv_addr, recv_size);
	shm_mq_set_receiver(mq, MyProc);
	io->mqh_receiver = shm_mq_attach(mq, seg, NULL);

	initStringInfo(&io->send_buf);
	initStringInfo(&io->recv_buf);
	initOidBuffer(&io->tmp_buf);

	io->slot_remote = MakeSingleTupleTableSlot(desc);

	io->eof_local = io->eof_remote = false;
}

void DynamicReduceClearFetch(DynamicReduceIOBuffer *io)
{
	if (io == NULL)
		return;

	ExecDropSingleTupleTableSlot(io->slot_remote);
	io->slot_remote = NULL;

	pfree(io->tmp_buf.oids);
	io->tmp_buf.oids = NULL;
	pfree(io->recv_buf.data);
	io->recv_buf.data = NULL;
	pfree(io->send_buf.data);
	io->send_buf.data = NULL;

	shm_mq_detach(io->mqh_receiver);
	io->mqh_receiver = NULL;
	shm_mq_detach(io->mqh_sender);
	io->mqh_sender = NULL;
}

TupleTableSlot* DynamicReduceFetchSlot(DynamicReduceIOBuffer *io)
{
	TupleTableSlot	   *slot;
	int					dr_flags;

	while (io->eof_local == false ||
		   io->eof_remote == false ||
		   io->send_buf.len > 0)
	{
		if (io->eof_remote == false &&
			DynamicReduceRecvTuple(io->mqh_receiver,
								   io->slot_remote,
								   &io->recv_buf,
								   NULL,
								   io->eof_local ? false:true))
		{
			if (TupIsNull(io->slot_remote))
				io->eof_remote = true;
			else
				return io->slot_remote;
		}

		if (io->send_buf.len == 0 &&
			io->eof_local == false)
		{
			slot = DynamicReduceFetchLocal(io);

			if (io->send_buf.len > 0 &&
				DynamicReduceSendMessage(io->mqh_sender,
										 io->send_buf.len,
										 io->send_buf.data,
										 io->eof_local ? false:true))
			{
				io->send_buf.len = 0;
			}

			if (!TupIsNull(slot))
				return slot;
			if (io->send_buf.len == 0)
				continue;
		}

		if (io->eof_local &&
			io->eof_remote &&
			io->send_buf.len == 0)
		{
			break;
		}

		Assert(io->send_buf.len > 0);
		slot = ExecClearTuple(io->slot_remote);
		dr_flags = DynamicReduceSendOrRecvTuple(io->mqh_sender,
												io->mqh_receiver,
												&io->send_buf,
												slot,
												&io->recv_buf);
		if (dr_flags & DR_MSG_SEND)
			io->send_buf.len = 0;
		if (dr_flags & DR_MSG_RECV)
		{
			if (TupIsNull(slot))
				io->eof_remote = true;
			else
				return slot;
		}
	}

	return NULL;
}

TupleTableSlot* DynamicReduceFetchLocal(DynamicReduceIOBuffer *io)
{
	ExprContext	   *econtext = io->econtext;
	Datum			datum;
	ExprDoneCond	done;
	Oid				nodeoid;
	TupleTableSlot *slot;
	TupleTableSlot *result;
	bool			isNull;
	Assert(io->eof_local == false);
	Assert(io->send_buf.len == 0);

	result = NULL;
	slot = (*io->FetchLocal)(io->user_data, econtext);
	if (TupIsNull(slot))
	{
		SerializeEndOfPlanMessage(&io->send_buf);
		io->eof_local = true;
	}else
	{
		io->tmp_buf.len = 0;
		for(;;)
		{
			datum = ExecEvalReduceExpr(io->expr_state, econtext, &isNull, &done);
			if (done == ExprEndResult)
			{
				break;
			}else if (isNull)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("ReduceExpr return a null value")));
			}else
			{
				nodeoid = DatumGetObjectId(datum);
				if (nodeoid == PGXCNodeOid)
					result = slot;
				else
					appendOidBufferUniqueOid(&io->tmp_buf, nodeoid);
				if (done == ExprSingleResult)
					break;
			}
		}
		if (io->tmp_buf.len > 0)
		{
			SerializeDynamicReduceSlot(&io->send_buf,
									   slot,
									   &io->tmp_buf);
		}
	}

	return result;
}
