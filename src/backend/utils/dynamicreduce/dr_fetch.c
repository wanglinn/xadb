#include "postgres.h"

#include "access/tuptypeconvert.h"
#include "executor/executor.h"
#include "pgxc/pgxc.h"
#include "utils/sharedtuplestore.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

void DynamicReduceInitFetch(DynamicReduceIOBuffer *io, dsm_segment *seg, TupleDesc desc, uint32 flags,
							void *send_addr, Size send_size, void *recv_addr, Size recv_size)
{
	shm_mq	   *mq;

	io->mqh_sender = io->mqh_receiver = NULL;

	if (flags & DR_MQ_INIT_SEND)
		shm_mq_create(send_addr, send_size);
	if (flags & DR_MQ_ATTACH_SEND)
	{
		shm_mq_set_sender((shm_mq*)send_addr, MyProc);
		io->mqh_sender = shm_mq_attach((shm_mq*)send_addr, seg, NULL);
	}

	if (flags & DR_MQ_INIT_RECV)
		shm_mq_create(recv_addr, recv_size);
	if (flags & DR_MQ_ATTACH_RECV)
	{
		shm_mq_set_receiver((shm_mq*)recv_addr, MyProc);
		io->mqh_receiver = shm_mq_attach((shm_mq*)recv_addr, seg, NULL);
	}

	io->shared_file = NULL;
	io->sts = NULL;
	io->sts_dsa_ptr = InvalidDsaPointer;
	initStringInfo(&io->send_buf);
	initStringInfo(&io->recv_buf);
	initOidBuffer(&io->tmp_buf);

	io->convert = create_type_convert(desc, true, true);
	if (io->convert != NULL)
		io->slot_remote = MakeSingleTupleTableSlot(io->convert->out_desc);
	io->slot_local = MakeSingleTupleTableSlot(desc);

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

	if (io->slot_local)
	{
		ExecDropSingleTupleTableSlot(io->slot_local);
		io->slot_local = NULL;
	}

	if (io->convert)
	{
		free_type_convert(io->convert);
		io->convert = NULL;
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
				slot = DynamicReduceFetchBufFile(io, io->shared_file);
				if (TupIsNull(slot))
				{
					DRFetchCloseSharedFile(io);
					continue;
				}
				return slot;
			}
			if (io->sts)
			{
				slot = DynamicReduceFetchSTS(io, io->sts);
				if (TupIsNull(slot))
				{
					DRFetchCloseSharedTuplestore(io);
					continue;
				}
				return slot;
			}

			slot = io->convert ? io->slot_remote : io->slot_local;
			dr_flags = DynamicReduceRecvTuple(io->mqh_receiver,
											  slot,
											  &io->recv_buf,
											  &info,
											  io->eof_local ? false:true);
			if (dr_flags == DR_MSG_RECV)
			{
				if (TupIsNull(slot))
					io->eof_remote = true;
				else if(io->convert)
					return do_type_convert_slot_in(io->convert, slot, io->slot_local, false);
				else
					return slot;
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
		slot = ExecClearTuple(io->convert ? io->slot_remote:io->slot_local);
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
			else if (io->convert)
				return do_type_convert_slot_in(io->convert, slot, io->slot_local, false);
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

TupleTableSlot* DynamicReduceFetchBufFile(DynamicReduceIOBuffer *io, BufFile *buffile)
{
	TupleTableSlot *slot = io->convert ? io->slot_remote : io->slot_local;
	resetStringInfo(&io->recv_buf);
	DynamicReduceReadSFSTuple(slot, buffile, &io->recv_buf);
	if (!TupIsNull(slot))
	{
		if (io->convert)
			slot = do_type_convert_slot_in(io->convert,
										   slot,
										   io->slot_local,
										   false);
		return slot;
	}
	return ExecClearTuple(io->slot_local);
}

TupleTableSlot* DynamicReduceFetchSTS(DynamicReduceIOBuffer *io, SharedTuplestoreAccessor *sts)
{
	MinimalTuple	mtup = sts_parallel_scan_next(sts, NULL);
	if (mtup == NULL)
		return ExecClearTuple(io->slot_local);
	if (io->convert)
	{
		return do_type_convert_slot_in(io->convert,
									   ExecStoreMinimalTuple(mtup, io->slot_remote, false),
									   io->slot_local,
									   false);
	}
	return ExecStoreMinimalTuple(mtup, io->slot_local, false);
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
			if (io->convert)
				slot = do_type_convert_slot_out(io->convert, slot, io->slot_remote, false);
			SerializeDynamicReduceSlot(&io->send_buf,
									   slot,
									   &io->tmp_buf);
		}
	}

	return result;
}

void DRFetchSaveNothing(TupleTableSlot *slot, void *context)
{
}

void DRFetchSaveSTS(TupleTableSlot *slot, void *context)
{
	sts_puttuple(context, NULL, ExecFetchSlotMinimalTuple(slot));
}

void DynamicReduceFetchAllLocalAndSend(DynamicReduceIOBuffer *io, const void *context, FetchSaveFunc func)
{
	bool			send_result PG_USED_FOR_ASSERTS_ONLY;
	TupleTableSlot *slot;

	while(io->eof_local == false)
	{
		CHECK_FOR_INTERRUPTS();
		slot = DynamicReduceFetchLocal(io);
		if (io->send_buf.len > 0)
		{
			send_result = DynamicReduceSendMessage(io->mqh_sender,
												   io->send_buf.len,
												   io->send_buf.data,
												   false);
			Assert(send_result);
			io->send_buf.len = 0;
		}
		if (!TupIsNull(slot))
			(*func)(slot, (void*)context);
	}
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

	if (dr_dsa == NULL)
	{
		/*
		 * maybe dynamic reduce shared memory already detached,
		 * in AtEOXact_Parallel function call DestroyParallelContext
		 */
		if (dr_mem_seg != NULL)	/* should not happen */
			ereport(WARNING,
					(errmsg("maybe should not delete shared file for shared tuplestore")));
		return;
	}
	sts_mem = dsa_get_address(dr_dsa, ptr);
	Assert(pg_atomic_read_u32(&sts_mem->attached) > 0);
	sts_detach(stsa);
	if (pg_atomic_sub_fetch_u32(&sts_mem->attached, 1) == 0)
	{
		sts_delete_shared_files((SharedTuplestore*)sts_mem->sts, dr_shared_fs);
		dsa_free(dr_dsa, ptr);
	}
}

void DynamicReduceAttachPallel(DynamicReduceIOBuffer *io)
{
	uint8 remote;
	DynamicReduceRecvInfo info;
	if (io->called_attach)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("DynamicReduceIOBuffer already attached")));

	io->eof_local = DynamicReduceNotifyAttach(io->mqh_sender,
											  io->mqh_receiver,
											  &remote,
											  &info);
	switch(remote)
	{
	case ADB_DR_MSG_ATTACH_PLAN:
		break;
	case ADB_DR_MSG_END_OF_PLAN:
		io->eof_remote = true;
		break;
	case ADB_DR_MSG_SHARED_FILE_NUMBER:
		DRFetchOpenSharedFile(io, info.u32);
		break;
	case ADB_DR_MSG_SHARED_TUPLE_STORE:
		DRFetchOpenSharedTuplestore(io, info.dp);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknow result dynamic reduce remote state %u for attach pallel plan", remote)));
		break;
	}
	io->called_attach = true;
}
