#include "rdc_tupstore.h"

#include "postgres_fe.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

int main(int argc, char **argv)
{
	RSstate * state = NULL;
	char *tup1 = "abcdefg1";
	char *tup2 = "abcdefg2";
	char *tup3 = "abcdefg3";
	char *tup4 = "abcdefg4";
	char *tup5 = "abcdefg5";
	char *tup6 = "abcdefg6";
	RSdata  *input;

	StringInfo buf;
	bool	hasData = false;
	int     len = strlen(tup1);
	MemoryContextInit();

	buf = makeStringInfo();
	initStringInfo(buf);

	input = (RSdata*)palloc0(sizeof(RSdata));

	if (state == NULL)
		state = rdcstore_begin(1,"test",2);

	rdcstore_puttuple(state, tup1, len);	
	rdcstore_gettuple(state,buf,&hasData);

	resetStringInfo(buf);
	rdcstore_gettuple(state,buf,&hasData);

	rdcstore_puttuple(state, tup2, len);

	rdcstore_puttuple(state, tup3, len);

	resetStringInfo(buf);
	rdcstore_gettuple(state,buf,&hasData);
	resetStringInfo(buf);
	rdcstore_gettuple(state,buf,&hasData);

/* case 1 
 * #define BLCKSZ 10
 * #define MAX_PHYSICAL_FILESIZE 12
 */
/*
	readptr			put		get			get			put			get
	current			0		0			0			0			
	file			0		0			0			0			
	offset			0		0			12			12			
	eof				0		0			1			0			
write 	
	curfile			0		0			0			0			
	offset			0		12			12			12			
status				WF		RF			RF			WF			RF
writepos_file		0		0			0			0			0
writepos_offset		0		12			12			12			24
myfile	
	nbytes			12	12->0->12		0			12			0
	pos				12	0->12			0			12			0
	curOffset		0	0->12->0		12			12			0
	curFile			0		0			0			0			1
	offset			0		12			12			12			12
*/
/*
	rdcstore_puttuple(state, tup1);
	res = rdcstore_gettuple(state);
	res = rdcstore_gettuple(state);
	rdcstore_puttuple(state, tup2);
	res = rdcstore_gettuple(state);
	rdcstore_puttuple(state, tup6);
	res = rdcstore_gettuple(state);
*/

/*case 2  memory
 * rdcstore_begin(10000);
 */
/*
	if (state == NULL)
		state = rdcstore_begin(18);
	rdcstore_puttuple(state, tup1);
	rdcstore_puttuple(state, tup2);
	res = rdcstore_gettuple(state);
	res = rdcstore_gettuple(state);
	res = rdcstore_gettuple(state);
	rdcstore_trim(state);
	rdcstore_puttuple(state, tup3);
	res = rdcstore_gettuple(state);
*/
	rdcstore_end(state);	
}

