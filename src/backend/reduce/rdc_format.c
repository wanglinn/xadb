/*-------------------------------------------------------------------------
 *
 * rdc_format.c
 *		Routines for formatting and parsing reduce/backend messages
 *
 *	src/bin/adb_reduce/rdc_format.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 * Message assembly and output:
 *		rdc_beginmessage - initialize StringInfo buffer
 *		rdc_sendbyte		- append a raw byte to a StringInfo buffer
 *		rdc_sendint		- append a binary integer to a StringInfo buffer
 *		rdc_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *		rdc_sendfloat4	- append a float4 to a StringInfo buffer
 *		rdc_sendfloat8	- append a float8 to a StringInfo buffer
 *		rdc_sendbytes	- append raw data to a StringInfo buffer
 *		rdc_sendstring	- append a null-terminated text string (with conversion)
 *		rdc_endmessage	- send the completed message to the frontend
 * Note: it is also possible to append data to the StringInfo buffer using
 * the regular StringInfo routines, but this is discouraged since required
 * character set conversion may not occur.
 *
 * Message parsing after input:
 *		rdc_getmsgbyte	- get a raw byte from a message buffer
 *		rdc_getmsgint	- get a binary integer from a message buffer
 *		rdc_getmsgint64	- get a binary 8-byte int from a message buffer
 *		rdc_getmsgfloat4 - get a float4 from a message buffer
 *		rdc_getmsgfloat8 - get a float8 from a message buffer
 *		rdc_getmsgbytes	- get raw data from a message buffer
 *		rdc_copymsgbytes - copy raw data from a message buffer
 *		rdc_getmsgstring - get a null-terminated text string - NO conversion
 *		rdc_getmsgend	- verify message fully consumed
 */
#if defined(RDC_FRONTEND)
#include "rdc_globals.h"
#endif

#include "reduce/rdc_comm.h"

static void rdc_endmessage_internal(RdcPort *port, StringInfo buf, bool iserror);

/* --------------------------------
 *		rdc_beginmessage		- initialize for sending a message
 * --------------------------------
 */
void
rdc_beginmessage(StringInfo buf, char msgtype)
{
	initStringInfo(buf);

	appendStringInfoChar(buf, msgtype);		/* data[0] is message type */
	appendStringInfoSpaces(buf, 4);			/* keep data[1]~data[4] as placeholder for message length */
}

/* --------------------------------
 *		rdc_sendbyte		- append a raw byte to a StringInfo buffer
 * --------------------------------
 */
void
rdc_sendbyte(StringInfo buf, int byt)
{
	appendStringInfoCharMacro(buf, byt);
}

/* --------------------------------
 *		rdc_sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void
rdc_sendbytes(StringInfo buf, const char *data, int datalen)
{
	appendBinaryStringInfo(buf, data, datalen);
}

/* --------------------------------
 *		rdc_sendstring	- append a null-terminated text string (with conversion)
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
rdc_sendstring(StringInfo buf, const char *str)
{
	int			slen = strlen(str);

	appendBinaryStringInfo(buf, str, slen + 1);
}

/* --------------------------------
 *		rdc_sendint		- append a binary integer to a StringInfo buffer
 * --------------------------------
 */
void
rdc_sendint(StringInfo buf, int i, int b)
{
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			n8 = (unsigned char) i;
			appendBinaryStringInfo(buf, (char *) &n8, 1);
			break;
		case 2:
			n16 = htons((uint16) i);
			appendBinaryStringInfo(buf, (char *) &n16, 2);
			break;
		case 4:
			n32 = htonl((uint32) i);
			appendBinaryStringInfo(buf, (char *) &n32, 4);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			break;
	}
}

/* --------------------------------
 *		rdc_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *
 * It is tempting to merge this with rdc_sendint, but we'd have to make the
 * argument int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
void
rdc_sendint64(StringInfo buf, int64 i)
{
	uint32		n32;

	/* High order half first, since we're doing MSB-first */
	n32 = (uint32) (i >> 32);
	n32 = htonl(n32);
	appendBinaryStringInfo(buf, (char *) &n32, 4);

	/* Now the low order half */
	n32 = (uint32) i;
	n32 = htonl(n32);
	appendBinaryStringInfo(buf, (char *) &n32, 4);
}

/* --------------------------------
 *		rdc_sendfloat4	- append a float4 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float4, which is a component of several datatypes.
 *
 * We currently assume that float4 should be byte-swapped in the same way
 * as int4.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
rdc_sendfloat4(StringInfo buf, float4 f)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.f = f;
	swap.i = htonl(swap.i);

	appendBinaryStringInfo(buf, (char *) &swap.i, 4);
}

/* --------------------------------
 *		rdc_sendfloat8	- append a float8 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float8, which is a component of several datatypes.
 *
 * We currently assume that float8 should be byte-swapped in the same way
 * as int8.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
rdc_sendfloat8(StringInfo buf, float8 f)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.f = f;
	rdc_sendint64(buf, swap.i);
}

void
rdc_sendlength(StringInfo buf)
{
	uint32	n32;

	/* Fill in the placeholder for the message length */
	Assert(buf->len >= 5);
	n32 = htonl((uint32)(buf->len - 1));
	memcpy(&buf->data[1], &n32, sizeof(n32));
}

/*
 * rdc_endmessage_internal - send the completed message to the frontend
 *
 * The data buffer is pfree()d, but if the StringInfo was allocated with
 * makeStringInfo then the caller must still pfree it.
 */
static void
rdc_endmessage_internal(RdcPort *port, StringInfo buf, bool iserror)
{
	AssertArg(port);

	rdc_sendlength(buf);

	/* send buf.data with length buf.len */
	if (iserror)
		(void) rdc_puterror_binary(port, buf->data, buf->len);
	else
		(void) rdc_putmessage(port, buf->data, buf->len);

	/* no need to complain about any failure, since pqcomm.c already did */
	pfree(buf->data);
	buf->data = NULL;
}

void
rdc_endmessage(RdcPort *port, StringInfo buf)
{
	rdc_endmessage_internal(port, buf, false);
}

void
rdc_enderror(RdcPort *port, StringInfo buf)
{
	rdc_endmessage_internal(port, buf, true);
}

/* --------------------------------
 *		rdc_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
rdc_getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("no data left in message")));
	return (unsigned char) msg->data[msg->cursor++];
}

/* --------------------------------
 *		rdc_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
rdc_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			rdc_copymsgbytes(msg, (char *) &n8, 1);
			result = n8;
			break;
		case 2:
			rdc_copymsgbytes(msg, (char *) &n16, 2);
			result = ntohs(n16);
			break;
		case 4:
			rdc_copymsgbytes(msg, (char *) &n32, 4);
			result = ntohl(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* --------------------------------
 *		rdc_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with rdc_getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64
rdc_getmsgint64(StringInfo msg)
{
	int64		result;
	uint32		h32;
	uint32		l32;

	rdc_copymsgbytes(msg, (char *) &h32, 4);
	rdc_copymsgbytes(msg, (char *) &l32, 4);
	h32 = ntohl(h32);
	l32 = ntohl(l32);

	result = h32;
	result <<= 32;
	result |= l32;

	return result;
}

/* --------------------------------
 *		rdc_getmsgfloat4 - get a float4 from a message buffer
 *
 * See notes for rdc_sendfloat4.
 * --------------------------------
 */
float4
rdc_getmsgfloat4(StringInfo msg)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.i = rdc_getmsgint(msg, 4);
	return swap.f;
}

/* --------------------------------
 *		rdc_getmsgfloat8 - get a float8 from a message buffer
 *
 * See notes for rdc_sendfloat8.
 * --------------------------------
 */
float8
rdc_getmsgfloat8(StringInfo msg)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.i = rdc_getmsgint64(msg);
	return swap.f;
}

/* --------------------------------
 *		rdc_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
rdc_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* --------------------------------
 *		rdc_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
rdc_copymsgbytes(StringInfo msg, char *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		rdc_getmsgstring - get a null-terminated text string - NO conversion
 *
 *		Returns a pointer directly into the message buffer.
 * --------------------------------
 */
const char *
rdc_getmsgstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	msg->cursor += slen + 1;

	return str;
}

/* --------------------------------
 *		rdc_getmsgend	- verify message fully consumed
 * --------------------------------
 */
void
rdc_getmsgend(StringInfo msg)
{
	if (msg->cursor > msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format")));
}

