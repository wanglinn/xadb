#include "postgres.h"

#include "executor/executor.h"
#include "pgxc/pgxc.h"
#include "utils/sharedtuplestore.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

void DynamicReduceInitFetch(DynamicReduceIOBuffer *io, dsm_segment *seg, TupleDesc desc,
							void *send_addr, Size send_size, void *recv_addr, Size recv_size)
{
	shm_mq	   *mq;

	if (send_size == 0)
		mq = (shm_mq*)send_addr;
	else
		mq = shm_mq_create(send_addr, send_size);
	shm_mq_set_sender(mq, MyProc);
	io->mqh_sender = shm_mq_attach(mq, seg, NULL);

	if (recv_size == 0)
		mq = (shm_mq*)recv_addr;
	else
		mq = shm_mq_create(recv_addr, recv_size);
	shm_mq_set_receiver(mq, MyProc);
	io->mqh_receiver = shm_mq_attach(mq, seg, NULL);

	io->shared_file = NULL;
	io->sts = NULL;
	io->sts_dsa_ptr = InvalidDsaPointer;
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

	if (io->shared_file)
	{
		BufFileClose(io->shared_file);
		io->shared_file = NULL;
	}

	if (io->slot_remote)
	{
		ExecDropSingleTupleTableSlot(io->slot_remote);
		io->slot_remote = NULL;
	}

	if (io->tmp_buf.oids)
	{
		pfree(io->tmp_buf.oids);
		io->tmp_buf.oids = NULL;
	}
	if (io->recv_buf.data)
	{
		pfree(io->recv_buf.data);
		io->recv_buf.data = NULL;
	}
	if (io->send_buf.data)
	{
		pfree(io->send_buf.data);
		io->send_buf.data = NULL;
	}

	if (io->mqh_receiver)
	{
		shm_mq_detach(io->mqh_receiver);
		io->mqh_receiver = NULL;
	}
	if (io->mqh_sender)
	{
		shm_mq_detach(io->mqh_sender);
		io->mqh_sender = NULL;
	}
	if (io->sts)
	{
		Assert(io->sts_dsa_ptr != InvalidDsaPointer);
		DynamicReduceCloseSharedTuplestore(io->sts, io->sts_dsa_ptr);
		io->sts = NULL;
		io->sts_dsa_ptr = InvalidDsaPointer;
	}
}

static inline void DRFetchOpenSharedFile(DynamicReduceIOBuffer *io, uint32 id)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(io->mqh_sender));
	char name[MAXPGPATH];
	Assert(io->shared_file == NULL);
	io->shared_file = BufFileOpenShared(DynamicReduceGetSharedFileSet(),
										DynamicReduceSharedFileName(name, id));
	io->shared_file_no = id;
	MemoryContextSwitchTo(oldcontext);
}

static inline void DRFetchCloseSharedFile(DynamicReduceIOBuffer *io)
{
	char name[MAXPGPATH];

	BufFileClose(io->shared_file);
	io->shared_file = NULL;
	/* shared file is last message */
	io->eof_remote = true;

	/* delete cache file */
	BufFileDeleteShared(DynamicReduceGetSharedFileSet(),
						DynamicReduceSharedFileName(name, io->shared_file_no));
}

static inline void DRFetchOpenSharedTuplestore(DynamicReduceIOBuffer *io, dsa_pointer dp)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(io->mqh_sender));
	Assert(io->sts_dsa_ptr == InvalidDsaPointer);
	Assert(io->sts == NULL);
	io->sts_dsa_ptr = dp;
	io->sts = DynamicReduceOpenSharedTuplestore(dp);
	sts_begin_parallel_scan(io->sts);
	MemoryContextSwitchTo(oldcontext);
}

static inline void DRFetchCloseSharedTuplestore(DynamicReduceIOBuffer *io)
{
	DynamicReduceCloseSharedTuplestore(io->sts, io->sts_dsa_ptr);
	io->sts = NULL;
	io->sts_dsa_ptr = InvalidDsaPointer;

	/* shared tuplestore is last message */
	io->eof_remote = true;
}

TupleTableSlot* DynamicReduceFetchSlot(DynamicReduceIOBuffer *io)
{
	TupleTableSlot		   *slot;
	int						dr_flags;
	DynamicReduceRecvInfo	info;

	while (io->eof_local == false ||
		   io->eof_remote == false ||
		   io->send_buf.len > 0)
	{
		if (io->send_buf.len > 0 &&
			DynamicReduceSendMessage(io->mqh_sender,
									 io->send_buf.len,
									 io->send_buf.data,
									 io->eof_remote ? false:true))
		{
			io->send_buf.len = 0;
		}

		if (io->eof_remote == false)
		{
			if (io->shared_file)
			{
				resetStringInfo(&io->recv_buf);
				slot = DynamicReduceReadSFSTuple(io->slot_remote, io->shared_file, &io->recv_buf);
				if (TupIsNull(slot))
				{
					DRFetchCloseSharedFile(io);
					continue;
				}
				return slot;
			}
			if (io->sts)
			{
				MinimalTuple mtup = sts_parallel_scan_next(io->sts, NULL);
				if (mtup == NULL)
				{
					DRFetchCloseSharedTuplestore(io);
					continue;
				}
				return ExecStoreMinimalTuple(mtup, io->slot_remote, false);
			}

			dr_flags = DynamicReduceRecvTuple(io->mqh_receiver,
											  io->slot_remote,
											  &io->recv_buf,
											  &info,
											  io->eof_local ? false:true);
			if (dr_flags == DR_MSG_RECV)
			{
				if (TupIsNull(io->slot_remote))
					io->eof_remote = true;
				else
					return io->slot_remote;
			}else if (dr_flags == DR_MSG_RECV_SF)
			{
				DRFetchOpenSharedFile(io, info.u32);
				Assert(io->shared_file != NULL);
				continue;
			}else if (dr_flags == DR_MSG_RECV_STS)
			{
				DRFetchOpenSharedTuplestore(io, info.dp);
				Assert(io->sts != NULL);
				continue;
			}else if (dr_flags != 0)
			{
				elog(ERROR, "unknown result type %u from dynamic reduce recv tuple", (unsigned int)dr_flags);
			}
		}

		if (io->send_buf.len == 0 &&
			io->eof_local == false)
		{
			slot = DynamicReduceFetchLocal(io);

			if (io->send_buf.len > 0 &&
				DynamicReduceSendMessage(io->mqh_sender,
										 io->send_buf.len,
										 io->send_buf.data,
										 io->eof_remote ? false:true))
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
												&io->recv_buf,
												&info);
		if (dr_flags & DR_MSG_SEND)
			io->send_buf.len = 0;
		if (dr_flags & DR_MSG_RECV)
		{
			if (TupIsNull(slot))
				io->eof_remote = true;
			else
				return slot;
		}
		if (dr_flags & DR_MSG_RECV_SF)
		{
			DRFetchOpenSharedFile(io, info.u32);
			Assert(io->shared_file != NULL);
		}
		if (dr_flags & DR_MSG_RECV_STS)
		{
			DRFetchOpenSharedTuplestore(io, info.dp);
			Assert(io->sts != NULL);
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

struct SharedTuplestoreAccessor* DynamicReduceOpenSharedTuplestore(dsa_pointer ptr)
{
	DynamicReduceSharedTuplestore	*sts_mem;
	SharedTuplestoreAccessor		*sts_accessor;

	sts_mem = dsa_get_address(dr_dsa, ptr);
	Assert(pg_atomic_read_u32(&sts_mem->attached) > 0);
	sts_accessor = sts_attach_read_only((SharedTuplestore*)sts_mem->sts, dr_shared_fs);

	return sts_accessor;
}

void DynamicReduceCloseSharedTuplestore(struct SharedTuplestoreAccessor *stsa, dsa_pointer ptr)
{
	DynamicReduceSharedTuplestore	*sts_mem;

	sts_mem = dsa_get_address(dr_dsa, ptr);
	Assert(pg_atomic_read_u32(&sts_mem->attached) > 0);
	sts_detach(stsa);
	if (pg_atomic_sub_fetch_u32(&sts_mem->attached, 1) == 0)
	{
		sts_delete_shared_files((SharedTuplestore*)sts_mem->sts, dr_shared_fs);
		dsa_free(dr_dsa, ptr);
	}
}
