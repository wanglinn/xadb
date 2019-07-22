/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */

#include "adb_doctor_utils.h"
#include "utils/palloc.h"

AdbDoctorBounceNum *newAdbDoctorBounceNum(int max)
{
	AdbDoctorBounceNum *obj;
	Assert(max > 0);
	obj = palloc(sizeof(AdbDoctorBounceNum));
	resetAdbDoctorBounceNum(obj);
	obj->max = max;
	return obj;
}

void pfreeAdbDoctorBounceNum(AdbDoctorBounceNum *src)
{
	if (src)
	{
		pfree(src);
		src = NULL;
	}
}

void resetAdbDoctorBounceNum(AdbDoctorBounceNum *src)
{
	src->num = 0;
	src->increase = true;
}

void nextAdbDoctorBounceNum(AdbDoctorBounceNum *src)
{
	Assert(src->max > 0);
	if (src->increase)
	{
		if (src->num >= src->max)
		{
			/* hit the maximum, reverse. */
			src->num--;
			src->increase = false;
		}
		else
		{
			src->num++;
		}
	}
	else
	{
		if (src->num <= 0)
		{
			/* hit the minimum, reverse. */
			src->num++;
			src->increase = true;
		}
		else
		{
			src->num--;
		}
	}
	/* negative value is not allowed. */
	if (src->num < 0)
	{
		src->num = 0;
	}
}

AdbDoctorErrorRecorder *newAdbDoctorErrorRecorder(int max)
{
	AdbDoctorErrorRecorder *obj;
	Assert(max > 0);
	obj = palloc(sizeof(AdbDoctorErrorRecorder));
	obj->errors = NULL;
	obj->nerrors = 0;
	obj->max = max;
	obj->firstErrorTime = 0;
	return obj;
}

void pfreeAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *src)
{
	if (src)
	{
		if (src->errors)
		{
			pfree(src->errors);
			src->errors = NULL;
		}
		pfree(src);
		src = NULL;
	}
}

void resetAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *recorder)
{
	if (recorder)
	{
		if (recorder->errors)
		{
			pfree(recorder->errors);
			recorder->errors = NULL;
		}
		recorder->nerrors = 0;
		recorder->firstErrorTime = 0;
	}
}

void appendAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *recorder,
								  int errorno)
{
	AdbDoctorError *newErrors;
	int oldErrorNum;
	int newErrorNum;
	int oldMemSize;
	int newMemSize;

	oldErrorNum = recorder->nerrors;
	if (oldErrorNum >= recorder->max)
	{
		/* It is not necessary to record more errors. do not a waste of memory.
		 * Remove the first error, move other errors to the left to make room,
		 * and then append the new error to the tail of that array.
		 */
		memmove(recorder->errors, recorder->errors + 1,
				sizeof(AdbDoctorError) * (oldErrorNum - 1));
		recorder->errors[oldErrorNum - 1].errorno = errorno;
		recorder->errors[oldErrorNum - 1].time = GetCurrentTimestamp();
		return;
	}
	if (oldErrorNum > 0)
	{
		/* if there have errors occurred already, append this new occurred error.  */
		newErrorNum = oldErrorNum + 1;
		oldMemSize = sizeof(AdbDoctorError) * oldErrorNum;
		newMemSize = sizeof(AdbDoctorError) * newErrorNum;
		newErrors = palloc(newMemSize);
		memcpy(newErrors, recorder->errors, oldMemSize);
		newErrors[oldErrorNum].errorno = errorno;
		newErrors[oldErrorNum].time = GetCurrentTimestamp();
		pfree(recorder->errors);
		recorder->errors = newErrors;
		recorder->nerrors = newErrorNum;
	}
	else
	{
		recorder->errors = palloc(sizeof(AdbDoctorError));
		recorder->errors[0].errorno = errorno;
		recorder->errors[0].time = GetCurrentTimestamp();
		recorder->nerrors = 1;
		recorder->firstErrorTime = recorder->errors[0].time;
	}
}

int countAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *recorder,
								int *errornos, int nErrornos)
{
	int i, j, count;
	if (recorder->nerrors <= 0)
	{
		return false;
	}
	count = 0;
	for (i = 0; i < recorder->nerrors; i++)
	{
		for (j = 0; j < nErrornos; j++)
		{
			if (recorder->errors[i].errorno == errornos[j])
			{
				count++;
			}
			else
			{
				continue;
			}
		}
	}
	return count;
}

AdbDoctorError *getFirstAdbDoctorError(AdbDoctorErrorRecorder *recorder)
{
	if (recorder->errors)
	{
		return &recorder->errors[0];
	}
	else
	{
		return NULL;
	}
}

AdbDoctorError *getLastAdbDoctorError(AdbDoctorErrorRecorder *recorder)
{
	if (recorder->errors)
	{
		return &recorder->errors[recorder->nerrors - 1];
	}
	else
	{
		return NULL;
	}
}

AdbDoctorError *findFirstAdbDoctorError(AdbDoctorErrorRecorder *recorder,
										int *errornos, int nErrornos)
{
	int i, j;
	if (recorder->errors)
	{
		for (i = 0; i < recorder->nerrors; i++)
		{
			for (j = 0; j < nErrornos; j++)
			{
				if (recorder->errors[i].errorno == errornos[j])
				{
					return &recorder->errors[i];
				}
				else
				{
					continue;
				}
			}
		}
		return NULL;
	}
	else
	{
		return NULL;
	}
}
AdbDoctorError *findLastAdbDoctorError(AdbDoctorErrorRecorder *recorder,
									   int *errornos, int nErrornos)
{
	int i, j;
	if (recorder->errors)
	{
		for (i = recorder->nerrors - 1; i >= 0; i--)
		{
			for (j = 0; j < nErrornos; j++)
			{
				if (recorder->errors[i].errorno == errornos[j])
				{
					return &recorder->errors[i];
				}
				else
				{
					continue;
				}
			}
		}
		return NULL;
	}
	else
	{
		return NULL;
	}
}