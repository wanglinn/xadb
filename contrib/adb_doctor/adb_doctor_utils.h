/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_UTILS_H
#define ADB_DOCTOR_UTILS_H

#include "postgres.h"
#include "utils/timestamp.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

/* Limit the value to the range between the minimum and maximum. */
#define LIMIT_VALUE_RANGE(min, max, val) Min(max, Max(min, val))

#define SPI_CONNECT_TRANSACTIONAL_START(res, complain)               \
	do                                                               \
	{                                                                \
		SetCurrentStatementStartTimestamp();                         \
		StartTransactionCommand();                                   \
		PushActiveSnapshot(GetTransactionSnapshot());                \
		res = SPI_connect();                                         \
		if (res != SPI_OK_CONNECT)                                   \
		{                                                            \
			ereport(complain ? ERROR : WARNING,                      \
					(errmsg("SPI_connect failed, connect return:%d", \
							res)));                                  \
		}                                                            \
	} while (0)

#define SPI_FINISH_TRANSACTIONAL_COMMIT() \
	do                                    \
	{                                     \
		SPI_finish();                     \
		PopActiveSnapshot();              \
		CommitTransactionCommand();       \
	} while (0)

#define SPI_FINISH_TRANSACTIONAL_ABORT() \
	do                                   \
	{                                    \
		SPI_finish();                    \
		PopActiveSnapshot();             \
		AbortCurrentTransaction();       \
	} while (0)

typedef struct AdbDoctorBounceNum
{
	int num;
	int min;
	int max;
	bool increase;
} AdbDoctorBounceNum;

typedef struct AdbDoctorError
{
	int errorno;
	TimestampTz time;
} AdbDoctorError;

typedef struct AdbDoctorErrorRecorder
{
	AdbDoctorError *errors;
	int nerrors;
	int max;
	TimestampTz firstErrorTime;
} AdbDoctorErrorRecorder;

extern AdbDoctorBounceNum *newAdbDoctorBounceNum(int min, int max);
extern void pfreeAdbDoctorBounceNum(AdbDoctorBounceNum *src);
extern void resetAdbDoctorBounceNum(AdbDoctorBounceNum *src);
extern void nextAdbDoctorBounceNum(AdbDoctorBounceNum *src);

extern AdbDoctorErrorRecorder *newAdbDoctorErrorRecorder(int max);
extern void pfreeAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *src);
extern void resetAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *recorder);
extern void appendAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *recorder,
										 int errorno);
extern int countAdbDoctorErrorRecorder(AdbDoctorErrorRecorder *recorder,
									   int *errornos, int nErrornos);
extern AdbDoctorError *getFirstAdbDoctorError(AdbDoctorErrorRecorder *recorder);
extern AdbDoctorError *getLastAdbDoctorError(AdbDoctorErrorRecorder *recorder);
extern AdbDoctorError *findFirstAdbDoctorError(AdbDoctorErrorRecorder *recorder,
											   int *errornos, int nErrornos);
extern AdbDoctorError *findLastAdbDoctorError(AdbDoctorErrorRecorder *recorder,
											  int *errornos, int nErrornos);

#endif