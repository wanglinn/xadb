#include "postgres.h"

#include "storage/mem_toc.h"

typedef struct mem_toc_entry
{
	uint32	key;
	uint32	length;	/* data length */
	char	data[FLEXIBLE_ARRAY_MEMBER];
}mem_toc_entry;

void begin_mem_toc_insert(StringInfo buf, uint32 key)
{
	mem_toc_entry *entry;

	AssertArg(buf && key);
	Assert(buf->len % MAXIMUM_ALIGNOF == 0);

	enlargeStringInfo(buf, offsetof(mem_toc_entry,data));
	entry = (mem_toc_entry*)(buf->data + buf->len);
	entry->key = key;
	entry->length = 0;

	buf->cursor = buf->len;
	buf->len += offsetof(mem_toc_entry, data);
}

void end_mem_toc_insert(StringInfo buf, uint32 key)
{
	mem_toc_entry *entry;
	int space;

	AssertArg(buf && key);
	Assert(buf->len > buf->cursor);

	space = buf->len % MAXIMUM_ALIGNOF;
	if(space)
	{
		space = MAXIMUM_ALIGNOF - space;
		enlargeStringInfo(buf, space);
		buf->len += space;
	}

	entry = (mem_toc_entry*)(buf->data + buf->cursor);
	Assert(entry->key == key);
	entry->length = (buf->len - buf->cursor - offsetof(mem_toc_entry, data));
	Assert(entry->length > 0);
}

void *mem_toc_lookup(StringInfo buf, uint32 key, int *len)
{
	mem_toc_entry *entry;
	AssertArg(buf);

	entry = (mem_toc_entry*)(buf->data);
	while((char*)entry < (buf->data+buf->len))
	{
		if(entry->key == key)
		{
			if(len)
				*len = (int)(entry->length);
			return entry->data;
		}
		entry = (mem_toc_entry*)&(entry->data[entry->length]);
	}
	return NULL;
}
