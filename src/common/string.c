/*-------------------------------------------------------------------------
 *
 * string.c
 *		string handling helpers
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/string.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/string.h"


#define DELIMITER_TAB		'\t'
#define DELIMITER_ENTER		'\n'
#define DELIMITER_BLANK		' '


static void TrimTabSpaceLeft(char *lpInput);
static void TrimTabSpaceRight(char *lpInput);

/*
 * Returns whether the string `str' has the postfix `end'.
 */
bool
pg_str_endswith(const char *str, const char *end)
{
	size_t		slen = strlen(str);
	size_t		elen = strlen(end);

	/* can't be a postfix if longer */
	if (elen > slen)
		return false;

	/* compare the end of the strings */
	str += slen - elen;
	return strcmp(str, end) == 0;
}


/*
 * strtoint --- just like strtol, but returns int not long
 */
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
	long		val;

	val = strtol(str, endptr, base);
	if (val != (int) val)
		errno = ERANGE;
	return (int) val;
}

void TrimTabSpace(char *lpInput)
{
    TrimTabSpaceRight(lpInput);
    TrimTabSpaceLeft(lpInput);
}
static void TrimTabSpaceLeft(char *lpInput)
{
    int i;
    
	if (lpInput == NULL)
		return;
		
	if (*lpInput == '\0')
	{
		return;
	}
	
	for (i = 0;;i++)
	{
		if (lpInput[i] != DELIMITER_TAB &&
			lpInput[i] != DELIMITER_ENTER &&
			lpInput[i] != DELIMITER_BLANK)
		{
			break;
		}
	}
	if (i != 0)
	{
		int cur = i;
		for (;;i++)
		{
			lpInput[i - cur] = lpInput[i];
			if (lpInput[i] == '\0')
				break;
		}
	}
}

static void TrimTabSpaceRight(char *lpInput)
{
    int i;
	
    if (lpInput == NULL)
		return;
		
	if ('\0' == lpInput[0])
	{
		return;
	}
	
	for (i = 0;;i++)
	{
		if (lpInput[i] == '\0')
			break;
	}
	for (;;)
	{
		--i;
		if (lpInput[i] != DELIMITER_TAB &&
			lpInput[i] != DELIMITER_ENTER &&
			lpInput[i] != DELIMITER_BLANK)
			break;
	}
	lpInput[++i] = '\0';
	return ;
}


