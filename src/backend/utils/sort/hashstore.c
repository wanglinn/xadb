/*-------------------------------------------------------------------------
 *
 * hashstore.h
 *	  Generalized routines for temporary hash tuple storage.
 *
 * IDENTIFICATION
 *   src/backend/utils/sort/hashstore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "storage/buf_internals.h"
#include "utils/hashstore.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


#define READER_SIZE_STEP	4
#define HASH_VALUE_OFFSET	(MINIMAL_TUPLE_DATA_OFFSET - sizeof(uint32))

/*
 * when space is not enough for save tuple in one PAGE, we set MultiData mark
 */
#define ItemIdMarkMultiData	ItemIdMarkDead
#define ItemIdIsMultiData	ItemIdIsDead

typedef struct HashBucketHead
{
	BlockNumber	first;
	BlockNumber	last;
}HashBucketHead;

typedef struct HashStoreBufferDesc
{
	BlockNumber	tag;			/* ID of page contained in buffer */
	int			buf_id;			/* buffer's index number (from 0) */

	/* state of the tag, containing flags, refcount and usagecount */
	uint32		state;
}HashStoreBufferDesc;

/* entry for buffer lookup hashtable */
typedef struct HashStoreBufferLookupEntry
{
	BlockNumber	key;			/* Tag of a disk page */
	int			id;				/* Associated local buffer's index */
} HashStoreBufferLookupEntry;

typedef struct HashReader
{
	MinimalTuple	cache_tuple;
	BlockNumber		next_block;
	BlockNumber		cur_block;
	OffsetNumber	cur_offset;
	OffsetNumber	max_offset;
	bool			is_idle;
	bool			is_seq;		/* is sequence read? */
	uint32			hashvalue;
	StringInfoData	buf;
}HashReader;

struct Hashstorestate
{
	BufFile				   *myfile;
	HashReader			   *reader;
	char				   *cache_page;
	HashStoreBufferDesc	   *cache_desc;
	HTAB				   *hash_page;		/* hash BlockNumber to buffer id */
	int						next_free_buf_id;
	BlockNumber				next_block;		/* next of file BlockNumber */
	int						nbucket;
	int						max_reader;
	int						max_cache;
	HashBucketHead			header[FLEXIBLE_ARRAY_MEMBER];	/* must at last */
};

extern int work_mem;

#define HashStoreBufferIdGetPage(store, buf_id) (&(store->cache_page[buf_id*BLCKSZ]))
#define HashStoreBufferIdGetDesc(store, buf_id) (&(store->cache_desc[buf_id]))

static Hashstorestate *new_hashstore(int nbuckets);
static void hashstore_put_mintuple(Hashstorestate *state, MinimalTuple tuple, uint32 hashvalue);
static void hashstore_put_data(Hashstorestate *state, HashStoreBufferDesc *desc, char *data, int len, uint32 hashvalue);
static void hashstore_get_data(Hashstorestate *state, HashReader *reader, StringInfo buf);
static HashStoreBufferDesc *hashstore_get_store_page(Hashstorestate *state, uint32 hashvalue);
static HashStoreBufferDesc *hashstore_read_page(Hashstorestate *state, BlockNumber blockno);
static HashStoreBufferDesc *hashstore_alloc_page(Hashstorestate *state);
static void hashstore_init_page(Hashstorestate *state, Page page);
static void hashstore_page_set_next_block(Page page, BlockNumber next);
static BlockNumber hashstore_page_get_next_block(Page page);
static void hashstore_save_page(BufFile *buffile, BlockNumber blockno, Page page);
static void hashstore_load_page(BufFile *buffile, BlockNumber blockno, Page page);

#ifdef HSDEBUG
#define HS_DEBUG_HASH_INSERT(tag, id)	fprintf(stderr, "hash_search((%u,%d), HASH_ENTER) line:%d\n", tag, id, __LINE__)
#define HS_DEBUG_HASH_REMOVE(tag)		fprintf(stderr, "hash_search(%u, HASH_REMOVE) line:%d\n", tag, __LINE__)
#define HS_DEBUG_SET_NEXT_BLOCK(b1, b2, buf_id)	fprintf(stderr, "hashstore_page_set_next_block(%u, %u, %d) line:%d\n", b1, b2, buf_id, __LINE__)
#else /* HSDEBUG */
#define HS_DEBUG_HASH_INSERT(tag, id)	((void)0)
#define HS_DEBUG_HASH_REMOVE(tag)		((void)0)
#define HS_DEBUG_SET_NEXT_BLOCK(b1, b2, buf_id)	((void)0)
#endif /* HSDEBUG */

Hashstorestate *hashstore_begin_heap(bool interXact, int nbuckets)
{
	Hashstorestate *store = new_hashstore(nbuckets);

	store->myfile = BufFileCreateTemp(interXact);

	return store;
}

void hashstore_put_tupleslot(Hashstorestate *state, TupleTableSlot *slot, uint32 hashvalue)
{
	hashstore_put_mintuple(state, ExecFetchSlotMinimalTuple(slot), hashvalue);
}

static int hashstore_alloc_reader(Hashstorestate *state)
{
	int ptr;
	int i;

	ptr = 0;
	for (i=0;i<state->max_reader;++i)
	{
		if (state->reader[i].is_idle)
		{
			ptr = i+1;
			break;
		}
	}

	if (ptr == 0)
	{
		MemoryContext old_context = MemoryContextSwitchTo(GetMemoryChunkContext(state));
		int old_max = state->max_reader;
		if (old_max == 0)
		{
			state->reader = palloc(sizeof(state->reader[0])*READER_SIZE_STEP);
		}else
		{
			state->reader = repalloc(state->reader, sizeof(state->reader[0])*(old_max+READER_SIZE_STEP));
		}
		state->max_reader += READER_SIZE_STEP;
		for (i=old_max;i<state->max_reader;++i)
		{
			state->reader[i].is_idle = true;
			initStringInfo(&state->reader[i].buf);
		}
		ptr = old_max+1;
		MemoryContextSwitchTo(old_context);
	}

	return ptr;
}

int hashstore_begin_read(Hashstorestate *state, uint32 hashvalue)
{
	HashReader *reader;
	HashBucketHead *head;
	int ptr;

	ptr = hashstore_alloc_reader(state);
	Assert(ptr > 0);
	reader = &(state->reader[ptr-1]);
	Assert(reader->is_idle);

	head = &(state->header[hashvalue%state->nbucket]);
	if(head->first == InvalidBlockNumber)
	{
		reader->next_block = reader->cur_block = InvalidBlockNumber;
	}else
	{
		reader->next_block = head->first;
		reader->cur_block = InvalidBlockNumber;
	}
	reader->cur_offset = MaxOffsetNumber;
	reader->max_offset = FirstOffsetNumber;
	reader->hashvalue = hashvalue;
	reader->is_idle = false;
	reader->is_seq = false;

	return ptr;
}

int hashstore_begin_seq_read(Hashstorestate *state)
{
	HashReader *reader;
	int ptr;

	ptr = hashstore_alloc_reader(state);
	Assert(ptr > 0);
	reader = &(state->reader[ptr-1]);
	Assert(reader->is_idle);

	reader->next_block = 0;
	reader->cur_block = InvalidBlockNumber;
	reader->cur_offset = MaxOffsetNumber;
	reader->max_offset = FirstOffsetNumber;
	reader->hashvalue = 0;
	reader->is_idle = false;
	reader->is_seq = true;

	return ptr;
}

void hashstore_end_read(Hashstorestate *state, int reader)
{
	HashReader *hr;
	AssertArg(state && reader < state->max_reader);

	hr = &(state->reader[reader-1]);
	AssertArg(hr->is_idle == false);
	hr->is_idle = true;
}

TupleTableSlot* hashstore_next_slot(Hashstorestate *state, TupleTableSlot *slot, int reader, bool copy)
{
	HashStoreBufferDesc *desc;
	HashReader *hr;
	Page page;
	uint32 *phashvalue;
	AssertArg(state && reader < state->max_reader);

	hr = &(state->reader[reader-1]);
	AssertArg(hr->is_idle == false);

re_get_:
	if (hr->cur_offset > hr->max_offset)
	{
		if (hr->next_block == InvalidBlockNumber ||
			hr->next_block >= state->next_block)
			return ExecClearTuple(slot);
		hr->cur_block = hr->next_block;
		desc = hashstore_read_page(state, hr->cur_block);
		Assert(desc->tag == hr->cur_block);
		page = HashStoreBufferIdGetPage(state, desc->buf_id);
		if (hr->is_seq)
			++(hr->next_block);
		else
			hr->next_block = hashstore_page_get_next_block(page);
		hr->cur_offset = FirstOffsetNumber;
		hr->max_offset = PageGetMaxOffsetNumber(page);
	}

	/* data is hashvalue + MinimalTuple's data */
	resetStringInfo(&hr->buf);
	hr->buf.len = HASH_VALUE_OFFSET;
	hashstore_get_data(state, hr, &hr->buf);
	phashvalue = (uint32*)(&hr->buf.data[HASH_VALUE_OFFSET]);
	if (hr->is_seq)
		hr->hashvalue = *phashvalue;
	else if (*phashvalue != hr->hashvalue)
		goto re_get_;

	hr->cache_tuple = (MinimalTuple)(hr->buf.data);
	MemSet(hr->cache_tuple, 0, MINIMAL_TUPLE_DATA_OFFSET);
	hr->cache_tuple->t_len = hr->buf.len;

	if (copy)
		return ExecStoreMinimalTuple(heap_copy_minimal_tuple(hr->cache_tuple), slot, true);
	return ExecStoreMinimalTuple(hr->cache_tuple, slot, false);
}

uint32 hashstore_get_reader_hashvalue(Hashstorestate *state, int reader)
{
	HashReader *hr;
	AssertArg(state && reader < state->max_reader);

	hr = &(state->reader[reader-1]);
	AssertArg(hr->is_idle == false);

	return hr->hashvalue;
}

void hashstore_clear(Hashstorestate *state)
{
	HashStoreBufferLookupEntry *entry;
	HashStoreBufferDesc *desc;
	HASH_SEQ_STATUS seq_status;
	int i;
	for (i=0;i<state->max_reader;++i)
	{
		HashReader *reader = &state->reader[i];
		reader->next_block = reader->cur_block = InvalidBlockNumber;
		reader->cur_offset = MaxOffsetNumber;
		reader->max_offset = FirstOffsetNumber;
	}

	hash_seq_init(&seq_status, state->hash_page);
	while ((entry=hash_seq_search(&seq_status)) != NULL)
		hash_search(state->hash_page, &entry->key, HASH_REMOVE, NULL);

	for (i=0;i<state->max_cache;++i)
	{
		desc = &(state->cache_desc[i]);
		desc->tag = InvalidBlockNumber;
		desc->state = 0;
	}
	MemSet(state->cache_page, 0, state->max_cache * BLCKSZ);

	state->next_block = 0;
	for (i=0;i<state->nbucket;++i)
		state->header[i].first = state->header[i].last = InvalidBlockNumber;
}

void hashstore_end(Hashstorestate *state)
{
	int i;
	if (state)
	{
		BufFileClose(state->myfile);
		if (state->reader)
		{
			for (i=0;i<state->max_reader;++i)
				pfree(state->reader[i].buf.data);
			pfree(state->reader);
		}
		pfree(state->cache_page);
		pfree(state->cache_desc);
		hash_destroy(state->hash_page);
		pfree(state);
	}
}

static Hashstorestate *new_hashstore(int nbuckets)
{
	Hashstorestate *store;
	HASHCTL info;
	int nbufs = work_mem/(BLCKSZ/1024);
	int i;
	AssertArg(nbuckets > 0);

	store = palloc(offsetof(Hashstorestate, header) + sizeof(store->header[0])*nbuckets);
	store->nbucket = nbuckets;

	/* init pages and desc */
	if (nbufs < 8)
		nbufs = 8;
	store->cache_page = palloc0(nbufs * BLCKSZ);
	store->cache_desc = palloc0(nbufs * sizeof(HashStoreBufferDesc));
	store->max_cache = nbufs;
	for (i=0;i<nbufs;++i)
		store->cache_desc[i].buf_id = i;

	/* init hash table for BlockNumber to buffer id */
	MemSet(&info, 0, sizeof(info));
	info.hcxt = CurrentMemoryContext;
	info.keysize = sizeof(BlockNumber);
	info.entrysize = sizeof(HashStoreBufferLookupEntry);
	store->hash_page = hash_create("Hash Store Buffer Lookup Table",
								   nbufs,
								   &info,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* init buckets */
	for(i=0;i<nbuckets;++i)
		store->header[i].first = store->header[i].last = InvalidBlockNumber;

	store->next_block = 0;
	store->reader = NULL;
	store->max_reader = 0;
	store->myfile = NULL;
	store->next_free_buf_id = 0;

	return store;
}

static void hashstore_put_mintuple(Hashstorestate *state, MinimalTuple tuple, uint32 hashvalue)
{
	HashStoreBufferDesc *desc = hashstore_get_store_page(state, hashvalue);
	char *tupdata = ((char*)tuple) + HASH_VALUE_OFFSET;
	int bodylen = tuple->t_len - HASH_VALUE_OFFSET;
	uint32 save_mem;

	save_mem = *(uint32*)tupdata;
	*(uint32*)tupdata = hashvalue;
	hashstore_put_data(state, desc, tupdata, bodylen, hashvalue);
	*(uint32*)tupdata = save_mem;
}

static void hashstore_put_data(Hashstorestate *state, HashStoreBufferDesc *desc, char *data, int len, uint32 hashvalue)
{
	Page page;
	ItemId itemid;
	int free_size;
	OffsetNumber offset;

	while (len > 0)
	{
		page = HashStoreBufferIdGetPage(state, desc->buf_id);
		free_size = PageGetFreeSpace(page);
		Assert(free_size >= 0);
		if (free_size > MAXIMUM_ALIGNOF)
		{
			free_size -= (free_size % MAXIMUM_ALIGNOF);

			offset = PageAddItemExtended(page, (Item)data, Min(free_size, len), InvalidOffsetNumber, 0);
			if(offset == InvalidOffsetNumber)
				ereport(ERROR, (errmsg("failed to add tuple to hash store page")));
			desc->state |= BM_DIRTY;

			if (free_size >= len)
				return;

			itemid = PageGetItemId(page, offset);
			Assert(ItemIdIsNormal(itemid));
			ItemIdMarkMultiData(itemid);

			data += free_size;
			len -= free_size;
		}
		desc = hashstore_get_store_page(state, hashvalue);
	}
}

static void hashstore_get_data(Hashstorestate *state, HashReader *reader, StringInfo buf)
{
	HashStoreBufferDesc *desc;
	Page page;
	ItemId item;
	AssertArg(reader->cur_offset <= reader->max_offset);
	AssertArg(reader->cur_block != InvalidBlockNumber);

	desc = hashstore_read_page(state, reader->cur_block);
	Assert(desc->tag == reader->cur_block);
	page = HashStoreBufferIdGetPage(state, desc->buf_id);
	for(;;)
	{
		item = PageGetItemId(page, reader->cur_offset);
		appendBinaryStringInfo(buf, PageGetItem(page, item), ItemIdGetLength(item));
		++reader->cur_offset;

		if (!ItemIdIsMultiData(item))
			break;

		Assert(reader->cur_offset > reader->max_offset);
		if (reader->next_block == InvalidBlockNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Invalid buffer data of hash store")));
		}
		reader->cur_block = reader->next_block;
		desc = hashstore_read_page(state, reader->cur_block);
		Assert(desc->tag == reader->cur_block);
		page = HashStoreBufferIdGetPage(state, desc->buf_id);
		reader->next_block = hashstore_page_get_next_block(page);
		reader->max_offset = PageGetMaxOffsetNumber(page);
		reader->cur_offset = FirstOffsetNumber;
	}
}

static HashStoreBufferDesc *hashstore_get_store_page(Hashstorestate *state, uint32 hashvalue)
{
	HashStoreBufferDesc *desc;
	HashBucketHead *head = &(state->header[hashvalue%state->nbucket]);
	Page page;
	if (head->first == InvalidBlockNumber)
	{
		Assert(head->last == InvalidBlockNumber);
		desc = hashstore_read_page(state, P_NEW);
		head->first = head->last = desc->tag;
		hashstore_page_set_next_block(HashStoreBufferIdGetPage(state, desc->buf_id), InvalidBlockNumber);
		HS_DEBUG_SET_NEXT_BLOCK(desc->tag, InvalidBlockNumber, desc->buf_id);
		desc->state |= BM_DIRTY;
		return desc;
	}
	Assert(head->last != InvalidBlockNumber);
	desc = hashstore_read_page(state, head->last);
	Assert(desc->tag == head->last);
	page = HashStoreBufferIdGetPage(state, desc->buf_id);
	Assert (hashstore_page_get_next_block(page) == InvalidBlockNumber);
	if (PageGetFreeSpace(page) <= MAXIMUM_ALIGNOF) /* min size must > MAXIMUM_ALIGNOF */
	{
		HashStoreBufferDesc *old_desc = desc;
		uint32 old_desc_state = old_desc->state;

		old_desc->state |= BM_LOCKED;

		desc = hashstore_read_page(state, P_NEW);
		Assert(desc != old_desc);
		hashstore_page_set_next_block(page, desc->tag);
		HS_DEBUG_SET_NEXT_BLOCK(old_desc->tag, desc->tag, old_desc->buf_id);
		head->last = desc->tag;
		old_desc_state |= BM_DIRTY;
		old_desc->state = old_desc_state;
	}
	return desc;
}

static HashStoreBufferDesc *hashstore_read_page(Hashstorestate *state, BlockNumber blockno)
{
	HashStoreBufferDesc *desc;
	HashStoreBufferLookupEntry *entry;
	Page page;
	bool found;

	if (blockno == P_NEW)
	{
		blockno = state->next_block;

		desc = hashstore_alloc_page(state);
		entry = hash_search(state->hash_page,
							&blockno,
							HASH_ENTER,
							&found);
		if (found)					/* shouldn't happen */
			elog(ERROR, "hash store buffer hash table corrupted");
		desc->tag = blockno;
		HS_DEBUG_HASH_INSERT(desc->tag, desc->buf_id);
		entry->id = desc->buf_id;
		entry->key = blockno;
		desc->tag = blockno;

		++(state->next_block);
		page = HashStoreBufferIdGetPage(state, desc->buf_id);
		hashstore_init_page(state, page);
		hashstore_save_page(state->myfile, desc->tag, page);
		desc->state = ((BM_TAG_VALID|BM_VALID) + BUF_USAGECOUNT_ONE);
		return desc;
	}

	entry = hash_search(state->hash_page,
						&blockno,
						HASH_FIND,
						&found);
	if (found)
	{
		desc = HashStoreBufferIdGetDesc(state, entry->id);
		Assert(desc->buf_id == entry->id);
		if (BUF_STATE_GET_USAGECOUNT(desc->state) < BM_MAX_USAGE_COUNT)
			desc->state += BUF_USAGECOUNT_ONE;
		return desc;
	}

	desc = hashstore_alloc_page(state);
	page = HashStoreBufferIdGetPage(state, desc->buf_id);
	hashstore_load_page(state->myfile, blockno, page);
	entry = hash_search(state->hash_page,
						&blockno,
						HASH_ENTER,
						&found);
	if (found)					/* shouldn't happen */
		elog(ERROR, "hash store buffer hash table corrupted");
	desc->tag = blockno;
	HS_DEBUG_HASH_INSERT(desc->tag, desc->buf_id);
	entry->key = blockno;
	entry->id = desc->buf_id;
	desc->state = ((BM_TAG_VALID|BM_VALID) + BUF_USAGECOUNT_ONE);
	return desc;
}

static HashStoreBufferDesc *hashstore_alloc_page(Hashstorestate *state)
{
	HashStoreBufferDesc *desc;
	int b;
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		b = state->next_free_buf_id;

		if (++(state->next_free_buf_id) >= state->max_cache)
			state->next_free_buf_id = 0;

		desc = HashStoreBufferIdGetDesc(state, b);
		if (desc->state & BM_LOCKED)
			continue;

		if (BUF_STATE_GET_USAGECOUNT(desc->state) > 0)
		{
			desc->state -= BUF_USAGECOUNT_ONE;
		}else
		{
			break;
		}
	}

	if (desc->state & BM_DIRTY)
	{
		hashstore_save_page(state->myfile, desc->tag, HashStoreBufferIdGetPage(state, desc->buf_id));
		desc->state &= ~BM_DIRTY;
	}

	if (desc->state & BM_TAG_VALID)
	{
		bool found;
		hash_search(state->hash_page, &desc->tag, HASH_REMOVE, &found);
		if (found == false)			/* shouldn't happen */
			elog(ERROR, "hash store buffer hash table corrupted");
		HS_DEBUG_HASH_REMOVE(desc->tag);
		desc->state &= ~(BM_VALID|BM_TAG_VALID);
	}

	return desc;
}

static void hashstore_init_page(Hashstorestate *state, Page page)
{
	/* specialSize is size of next BlockNumber */
	PageInit(page, BLCKSZ, sizeof(BlockNumber));
	hashstore_page_set_next_block(page, InvalidBlockNumber);
}

static void hashstore_page_set_next_block(Page page, BlockNumber next)
{
	Assert(PageGetSpecialSize(page) >= sizeof(BlockNumber));
	Assert(PageValidateSpecialPointer(page));
	*((BlockNumber*)PageGetSpecialPointer(page)) = next;
}

static BlockNumber hashstore_page_get_next_block(Page page)
{
	Assert(PageGetSpecialSize(page) >= sizeof(BlockNumber));
	Assert(PageValidateSpecialPointer(page));
	return *((BlockNumber*)PageGetSpecialPointer(page));
}

static void hashstore_save_page(BufFile *buffile, BlockNumber blockno, Page page)
{
	if (BufFileSeekBlock(buffile, blockno) != 0)
	{
		ereport(ERROR,
				(errcode(errcode_for_file_access()),
				 errmsg("could not seek temporary file: %m")));
	}

	if (BufFileWrite(buffile, page, BLCKSZ) != BLCKSZ)
	{
		/*
		 * the other errors in Read/WriteTempFileBlock shouldn't happen, but
		 * an error at write can easily happen if you run out of disk space.
		 */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write block %u of temporary file: %m",
						blockno)));
	}
}

static void hashstore_load_page(BufFile *buffile, BlockNumber blockno, Page page)
{
	if (BufFileSeekBlock(buffile, blockno) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek temporary file: %m")));
	}
	if (BufFileRead(buffile, page, BLCKSZ) != BLCKSZ)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read temporary file: %m")));
	}
}
