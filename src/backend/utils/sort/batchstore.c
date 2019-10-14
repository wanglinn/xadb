#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "commands/tablespace.h"
#include "executor/nodeHash.h"
#include "port/atomics.h"
#include "storage/buffile.h"
#include "utils/batchstore.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"

#define InvalidBatch UINT32_MAX

typedef enum BatchMethod
{
	BSM_HASH = 1,
	BSM_PARALLEL_HASH
}BatchMethod;

typedef struct BatchStoreParallelHashData
{
	SharedFileSet		fileset;
	pg_atomic_uint32	cur_batches;
	uint32				num_batches;
	uint32				num_participants;
}BatchStoreParallelHashData;

typedef struct BatchStoreData
{
	BatchStoreFuncs	func;
	BatchMethod	method;
	uint32		num_batches;
	void	  **all_batches;
	void	   *cur_batch_ptr;
	uint32		cur_batch_num;
	union
	{
		/* for hash */
		struct
		{
			StringInfoData	hash_read_buf;
		};
		/* for parallel hash */
		struct
		{
			dsm_segment		   *dsm_seg;
			MemoryContext		accessor_mcontext;
			Bitmapset		   *our_batches;		/* we got batches(for rescan) */
			pg_atomic_uint32   *shm_ph_batch_num;	/* in shared memory, parallel hash batch number */
			bool				ended_parallel;		/* parallel batches loop end? for rescan. */
			bool				parallel_batch;		/* each worker read part of all batches? */
		};
	};
}BatchStoreData;

static void bs_write_normal_hash(BatchStore bs, MinimalTuple mtup, uint32 hash);
static MinimalTuple bs_read_normal_hash(BatchStore bs, uint32 *hash);

static void bs_write_parallel_hash(BatchStore bs, MinimalTuple mtup, uint32 hash);
static MinimalTuple bs_read_parallel_hash(BatchStore bs, uint32 *hash);

static inline BatchStore make_empty_batch_store(uint32 num_batches)
{
	BatchStore bs;

	if (num_batches > BATCH_STORE_MAX_BATCH)
		ereport(ERROR,
				(errmsg("too many batches")));

	bs = palloc0(MAXALIGN(sizeof(BatchStoreData)) +
					sizeof(void*) * num_batches);
	bs->all_batches = (void**)(((char*)bs) + MAXALIGN(sizeof(*bs)));
	bs->num_batches = num_batches;
	bs->cur_batch_num = InvalidBatch;

	return bs;
}

BatchStore bs_begin_hash(uint32 num_batches)
{
	BatchStore bs = make_empty_batch_store(num_batches);
	bs->method = BSM_HASH;

	initStringInfo(&bs->hash_read_buf);
	enlargeStringInfo(&bs->hash_read_buf, MINIMAL_TUPLE_DATA_OFFSET);
	MemSet(bs->hash_read_buf.data, 0, MINIMAL_TUPLE_DATA_OFFSET);

	PrepareTempTablespaces();

	bs->func.hash_write = bs_write_normal_hash;
	bs->func.hash_read = bs_read_normal_hash;
	return bs;
}

size_t bs_parallel_hash_estimate(uint32 num_batches, uint32 nparticipants)
{
	return MAXALIGN(sizeof(struct BatchStoreParallelHashData)) + 
				MAXALIGN(sts_estimate(nparticipants)) * num_batches;
}

static BatchStore bs_begin_parallel_hash(BatchStoreParallelHash bsph,
										 uint32 my_participant_num, bool init,
										 dsm_segment *dsm_seg)
{
	uint32			i;
	MemoryContext	oldcontext;
	char		   *addr;
	char			name[24];
	Size			sts_size = MAXALIGN(sts_estimate(bsph->num_participants));
	BatchStore		bs = make_empty_batch_store(bsph->num_batches);

	bs->method = BSM_PARALLEL_HASH;
	bs->shm_ph_batch_num = &bsph->cur_batches;

	bs->accessor_mcontext = AllocSetContextCreate(CurrentMemoryContext,
												  "batch parallel hash",
												  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(bs->accessor_mcontext);
	addr = ((char*)bsph) + MAXALIGN(sizeof(*bsph));
	for (i=bsph->num_batches;i>0;)
	{
		--i;
		if (init)
		{
			sprintf(name, "bsph%u", i);
			bs->all_batches[i] = sts_initialize((SharedTuplestore*)addr,
												bsph->num_participants,
												my_participant_num,
												sizeof(uint32),
												0,
												&bsph->fileset,
												name);
		}else
		{
			bs->all_batches[i] = sts_attach((SharedTuplestore*)addr,
											my_participant_num,
											&bsph->fileset);
		}
		addr += sts_size;
	}
	MemoryContextSwitchTo(oldcontext);

	bs->dsm_seg = dsm_seg;
	bs->func.hash_read = bs_read_parallel_hash;
	bs->func.hash_write = bs_write_parallel_hash;

	return bs;
}

BatchStore bs_init_parallel_hash(uint32 num_batches,
								 uint32 nparticipants, uint32 my_participant_num,
								 BatchStoreParallelHash bsph, dsm_segment *dsm_seg)
{
	bsph->num_batches = num_batches;
	bsph->num_participants = nparticipants;
	pg_atomic_init_u32(&bsph->cur_batches, InvalidBatch);
	SharedFileSetInit(&bsph->fileset, dsm_seg);

	return bs_begin_parallel_hash(bsph, my_participant_num, true, dsm_seg);
}

BatchStore bs_attach_parallel_hash(BatchStoreParallelHash bsph, dsm_segment *dsm_seg,
								   uint32 my_participant_num)
{
	SharedFileSetAttach(&bsph->fileset, dsm_seg);

	return bs_begin_parallel_hash(bsph, my_participant_num, false, dsm_seg);
}

void bs_destory(BatchStore bs)
{
	uint32	i;
	if (bs == NULL)
		return;

	switch(bs->method)
	{
	case BSM_HASH:
		for(i=0;i<bs->num_batches;++i)
		{
			if (bs->all_batches[i])
				BufFileClose(bs->all_batches[i]);
		}
		pfree(bs->hash_read_buf.data);
		break;
	case BSM_PARALLEL_HASH:
		{
			BatchStoreParallelHash bsph = (BatchStoreParallelHash)(((char*)bs->shm_ph_batch_num) -
											offsetof(BatchStoreParallelHashData, cur_batches));
			SharedFileSetDetach(&bsph->fileset, bs->dsm_seg);
			MemoryContextDelete(bs->accessor_mcontext);
		}
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}

	pfree(bs);
}

static void bs_write_normal_hash(BatchStore bs, MinimalTuple mtup, uint32 hash)
{
	uint32 batch = hash % bs->num_batches;
	uint32 data_len = mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET;
	BufFile *buffile = bs->all_batches[batch];

	if (buffile == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(bs));
		buffile = BufFileCreateTemp(false);
		bs->all_batches[batch] = buffile;
		MemoryContextSwitchTo(oldcontext);
	}

	if (BufFileWrite(buffile, &hash, sizeof(hash)) != sizeof(hash) ||
		BufFileWrite(buffile, &mtup->t_len, sizeof(mtup->t_len)) != sizeof(mtup->t_len) ||
		BufFileWrite(buffile, ((char*)mtup) + MINIMAL_TUPLE_DATA_OFFSET, data_len) != data_len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to batch store temporary file: %m")));
	}
}

static MinimalTuple bs_read_normal_hash(BatchStore bs, uint32 *hash)
{
	MinimalTuple	mtup;
	size_t			nread;
	uint32			head[2];
	uint32			data_len;

	nread = BufFileRead(bs->cur_batch_ptr, head, sizeof(head));
	if (nread == 0)
		return NULL;

	if (nread != sizeof(head))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from batch store temporary file: %m")));
	*hash = head[0];
	enlargeStringInfo(&bs->hash_read_buf, head[1]);
	mtup = (MinimalTuple)bs->hash_read_buf.data;
	mtup->t_len = head[1];
	data_len = head[1] - MINIMAL_TUPLE_DATA_OFFSET;
	if (BufFileRead(bs->cur_batch_ptr, ((char*)mtup) + MINIMAL_TUPLE_DATA_OFFSET, data_len) != data_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from batch store temporary file: %m")));

	return mtup;
}

void bs_end_write(BatchStore bs)
{
	uint32 i;
	switch(bs->method)
	{
	case BSM_HASH:
		/* nothing to do */
		break;
	case BSM_PARALLEL_HASH:
		for (i=bs->num_batches;i>0;)
			sts_end_write(bs->all_batches[--i]);
		bs->cur_batch_ptr = NULL;
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
}

static void bs_write_parallel_hash(BatchStore bs, MinimalTuple mtup, uint32 hash)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(bs->accessor_mcontext);
	sts_puttuple(bs->all_batches[hash%bs->num_batches],
				 &hash,
				 mtup);
	MemoryContextSwitchTo(oldcontext);
}

static MinimalTuple bs_read_parallel_hash(BatchStore bs, uint32 *hash)
{
	return sts_scan_next(bs->cur_batch_ptr, hash);
}

bool bs_next_batch(BatchStore bs, bool no_parallel)
{
	uint32 batch;
	switch(bs->method)
	{
	case BSM_HASH:

		batch = bs->cur_batch_num;
		++batch;

		for (;batch < bs->num_batches;++batch)
		{
			if (bs->all_batches[batch])
			{
				bs->cur_batch_ptr = bs->all_batches[batch];
				bs->cur_batch_num = batch;
				if (BufFileSeek(bs->cur_batch_ptr, 0, 0, SEEK_SET) != 0)
				{
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("can not seek batch store file to head")));
				}
				return true;
			}
		}
		break;
	case BSM_PARALLEL_HASH:
		if (no_parallel)
		{
			batch = bs->cur_batch_num;
			++batch;
		}else
		{
			batch = pg_atomic_add_fetch_u32(bs->shm_ph_batch_num, 1);
		}

		if (batch < bs->num_batches)
		{
			bs->cur_batch_num = batch;
			bs->cur_batch_ptr = bs->all_batches[batch];
			sts_begin_scan(bs->cur_batch_ptr);
			return true;
		}
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}

	return false;
}

void bs_rescan(BatchStore bs)
{
	switch(bs->method)
	{
	case BSM_HASH:
		bs->cur_batch_ptr = NULL;
		bs->cur_batch_num = InvalidBatch;
		break;
	case BSM_PARALLEL_HASH:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel batch store not support rescan yet")));
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
}

void bs_end_cur_batch(BatchStore bs)
{
	switch(bs->method)
	{
	case BSM_HASH:
		bs->cur_batch_ptr = NULL;
		break;
	case BSM_PARALLEL_HASH:
		sts_end_scan(bs->cur_batch_ptr);
		bs->cur_batch_ptr = NULL;
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown batch store method %u", bs->method)));
		break;
	}
}

uint32 bs_choose_batch_size(double rows, int width)
{
	size_t	space_allowed;
	int		numbuckets;
	int		numbatches;
	int		num_skew_mcvs;

	ExecChooseHashTableSize(rows, width,
							false, false, 0,
							&space_allowed,
							&numbuckets,
							&numbatches,
							&num_skew_mcvs);
	
	if (numbatches > BATCH_STORE_MAX_BATCH)
		numbatches = BATCH_STORE_MAX_BATCH;
	if (numbatches < BATCH_STORE_MIN_BATCH)
		numbatches = BATCH_STORE_MIN_BATCH;

	return (uint32)numbatches;
}