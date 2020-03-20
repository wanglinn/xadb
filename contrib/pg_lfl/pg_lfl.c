#include "postgres.h"

#include "access/hash.h"
#include "lib/dshash.h"
#include "libpq/auth.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

#define DEFAULT_SHM_SIZE	(512*1024)
typedef struct LoginFailedInfo
{
	NameData	user;
	time_t		first_failed;
	uint32		failed_count;
}LoginFailedInfo;

typedef struct LoginFailedShmInfo
{
	dshash_table_handle	ds_hash;
	int hash_tranche_id;
	int area_tranche_id;
	/* force align to Size */
	Size mem[FLEXIBLE_ARRAY_MEMBER];
}LoginFailedShmInfo;

static LoginFailedShmInfo *lfl_shm = NULL;
static dshash_table *lfl_htable = NULL;
static const char *lfl_name = "login_failed_lock";
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ClientAuthentication_hook_type prev_ClientAuthentication_hook = NULL;
static int lfl_count = 3;
static int lfl_time = 60;

void _PG_init(void);
void _PG_fini(void);

static int lfl_namecmp(const void *a, const void *b, size_t size, void *arg)
{
	return strcmp(a, b);
}

static dshash_hash lfl_namehash(const void *v, size_t size, void *arg)
{
	return hash_any(v, strlen(v));
}

static void lfl_client_auth_hook(Port *port, int state)
{
	LoginFailedInfo*info;
	time_t			now;
	int				diff;
	bool			found;

	if (prev_ClientAuthentication_hook)
		(*prev_ClientAuthentication_hook)(port, state);

	now = time(NULL);
	if (state == STATUS_OK)
	{
		if ((info = dshash_find(lfl_htable, port->user_name, true)) == NULL)
			return;
	}else if (state == STATUS_ERROR)
	{
		info = dshash_find_or_insert(lfl_htable, port->user_name, &found);
		if (found == false ||
			info->first_failed + lfl_time < now)
		{
			info->first_failed = now;
			info->failed_count = 1;
		}else
		{
			info->failed_count++;
		}
	}else
	{
		return;
	}

	if (info->failed_count > lfl_count)
	{
		diff = (int)(info->first_failed + lfl_time - now);
		if (diff > 0)
		{
			dshash_release_lock(lfl_htable, info);
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("user is locked, please try again later"),
					 errhint("unlock after %d second(s)", diff)));
		}
	}
	if (state == STATUS_OK)
		dshash_delete_entry(lfl_htable, info);
	else
		dshash_release_lock(lfl_htable, info);
}

static void lfl_shmem_startup(void)
{
	dshash_parameters	param;
	dsa_area		   *dsa;
	bool				found;
	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook)();

	MemSet(&param, 0, sizeof(param));
	param.key_size = sizeof(NameData),
	param.entry_size = sizeof(LoginFailedInfo),
	param.compare_function = lfl_namecmp;
	param.hash_function = lfl_namehash;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	lfl_shm = ShmemInitStruct(lfl_name, DEFAULT_SHM_SIZE, &found);
	if (found)
	{
		param.tranche_id = lfl_shm->hash_tranche_id;
		dsa = dsa_attach_in_place(lfl_shm->mem, NULL);
		lfl_htable = dshash_attach(dsa, &param, lfl_shm->ds_hash, NULL);
	}else
	{
		lfl_shm->area_tranche_id = LWLockNewTrancheId();
		lfl_shm->hash_tranche_id = param.tranche_id = LWLockNewTrancheId();
		dsa = dsa_create_in_place(lfl_shm->mem,
								  DEFAULT_SHM_SIZE - offsetof(LoginFailedShmInfo, mem),
								  lfl_shm->area_tranche_id,
								  NULL);
		dsa_pin(dsa);
		lfl_htable = dshash_create(dsa, &param, NULL);
		lfl_shm->ds_hash = dshash_get_hash_table_handle(lfl_htable);
	}
	LWLockRelease(AddinShmemInitLock);
	LWLockRegisterTranche(lfl_shm->area_tranche_id, lfl_name);
	LWLockRegisterTranche(lfl_shm->hash_tranche_id, lfl_name);
	dsa_pin_mapping(dsa);
}

void _PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RequestAddinShmemSpace(DEFAULT_SHM_SIZE);
	RequestNamedLWLockTranche(lfl_name, 2);
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lfl_shmem_startup;
	prev_ClientAuthentication_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = lfl_client_auth_hook;

	DefineCustomIntVariable("pg_lfl.seconds",
							"Set lock time when user login failed",
							NULL,
							&lfl_time,
							60, 1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);
	DefineCustomIntVariable("pg_lfl.count",
							"Set lock user when user login failed count",
							NULL,
							&lfl_count,
							3, 1, INT_MAX/2,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);
}

void _PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}
