#include "postgres.h"

#include "lib/oidbuffer.h"

OidBuffer makeOidBuffer(uint32 size)
{
	return initOidBufferEx(palloc(sizeof(OidBufferData)), size, CurrentMemoryContext);
}

OidBuffer initOidBufferEx(OidBuffer buf, uint32 size, MemoryContext context)
{
	buf->oids = MemoryContextAlloc(context, size);
	buf->step = buf->maxlen = size;
	resetOidBuffer(buf);

	return buf;
}

OidBuffer appendOidBufferUniqueOid(OidBuffer buf, Oid oid)
{
	uint32	i,
			len = buf->len;
	Oid	   *oids = buf->oids;

	for (i=0;i<len;++i)
	{
		if (oids[i] == oid)
			return buf;
	}

	appendOidBufferOid(buf, oid);
	return buf;
}

bool
oidBufferMember(OidBuffer buf, Oid oid, uint32 *idx)
{
	uint32 i,len;
	Oid *oids = buf->oids;

	for (i=0,len=buf->len;i<len;++i)
	{
		if (oids[i] == oid)
		{
			if (idx)
				*idx = i;
			return true;
		}
	}

	return false;
}
