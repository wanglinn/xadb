#ifndef OID_BUFFER_H
#define OID_BUFFER_H

#include "postgres.h"

#define OID_BUF_DEF_SIZE	512

typedef struct OidBufferData
{
	uint32		maxlen;
	uint32		step;
	uint32		len;
	uint32		cursor;
	Oid		   *oids;
}OidBufferData, *OidBuffer;

extern OidBuffer makeOidBuffer(uint32 size);

#define initOidBuffer(buf) initOidBufferContext(buf, OID_BUF_DEF_SIZE, CurrentMemoryContext)

extern OidBuffer initOidBufferEx(OidBuffer buf, uint32 size, MemoryContext context);

static inline void
freeOidBuffer(OidBuffer buf, bool free_self)
{
	if (buf->oids)
		pfree(buf->oids);
	if (free_self)
		pfree(buf);
}

static inline void
resetOidBuffer(OidBuffer buf)
{
	buf->cursor = buf->len = 0;
}

static inline void
enlargeOidBuffer(OidBuffer buf, uint32 need)
{
	if (buf->maxlen < need)
	{
		if (need % buf->step != 0)
			need += (need % buf->step);
		buf->oids = (Oid*)repalloc(buf->oids, sizeof(Oid)*need);
		buf->maxlen = need;
	}
}

static inline void
appendOidBufferArray(OidBuffer buf, const Oid *oids, uint32 len)
{
	enlargeOidBuffer(buf, len+buf->len);
	memmove(&buf->oids[buf->len],
			oids,
			sizeof(Oid) * len);
	buf->len += len;
}

static inline void
appendOidBufferOid(OidBuffer buf, Oid oid)
{
	enlargeOidBuffer(buf, buf->len+1);
	buf->oids[buf->len++] = oid;
}

extern OidBuffer appendOidBufferUniqueOid(OidBuffer buf, Oid oid);

extern bool oidBufferMember(OidBuffer buf, Oid oid, uint32 *idx);

#endif							/* OID_BUFFER_H */
