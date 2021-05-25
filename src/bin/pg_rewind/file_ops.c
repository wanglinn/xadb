/*-------------------------------------------------------------------------
 *
 * file_ops.c
 *	  Helper functions for operating on files.
 *
 * Most of the functions in this file are helper functions for writing to
 * the target data directory. The functions check the --dry-run flag, and
 * do nothing if it's enabled. You should avoid accessing the target files
 * directly but if you do, make sure you honor the --dry-run mode!
 *
 * Portions Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "file_ops.h"
#include "filemap.h"
#include "pg_rewind.h"

/*
 * Currently open target file.
 */
static int	dstfd = -1;
static char dstpath[MAXPGPATH] = "";

static void create_target_dir(const char *path);
static void remove_target_dir(const char *path);
static void create_target_symlink(const char *path, const char *link);
static void remove_target_symlink(const char *path);

#ifdef ADB
extern const char *target_nodename;
extern const char *source_nodename;

FILE *rewind_file = NULL;

#define ADB_REWIND_TMP_DIR				"_rewind"
#define ADB_REWIND_FILE					"rewind.sh"

char	datadir_rewind[MAXPGPATH] = {0};

static void create_dir_sequence(char *old_path);
#endif
/*
 * Open a target file for writing. If 'trunc' is true and the file already
 * exists, it will be truncated.
 */
void
#ifdef ADB
open_target_file(const char *adbpath, bool trunc)
#else
open_target_file(const char *path, bool trunc)
#endif
{
	int			mode;
#ifdef ADB
	char		path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s", adbpath);
	/* Replace the tablespce directory name to avoid target (local) open failure. */
	replace_tblspc_directory_name(path, source_tblspc_directory, target_tblspc_directory);
#endif
	if (dry_run)
		return;

	if (dstfd != -1 && !trunc &&
		strcmp(path, &dstpath[strlen(datadir_target) + 1]) == 0)
		return;					/* already open */

	close_target_file();

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	mode = O_WRONLY | O_CREAT | PG_BINARY;
	if (trunc)
		mode |= O_TRUNC;
	dstfd = open(dstpath, mode, pg_file_create_mode);
	if (dstfd < 0)
		pg_fatal("could not open target file \"%s\": %m",
				 dstpath);
}

/*
 * Close target file, if it's open.
 */
void
close_target_file(void)
{
	if (dstfd == -1)
		return;

	if (close(dstfd) != 0)
		pg_fatal("could not close target file \"%s\": %m",
				 dstpath);

	dstfd = -1;
}

void
write_target_range(char *buf, off_t begin, size_t size)
{
	int			writeleft;
	char	   *p;

	/* update progress report */
	fetch_done += size;
	progress_report(false);

	if (dry_run)
		return;

	if (lseek(dstfd, begin, SEEK_SET) == -1)
		pg_fatal("could not seek in target file \"%s\": %m",
				 dstpath);

	writeleft = size;
	p = buf;
	while (writeleft > 0)
	{
		int			writelen;

		errno = 0;
		writelen = write(dstfd, p, writeleft);
		if (writelen < 0)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("could not write file \"%s\": %m",
					 dstpath);
		}

		p += writelen;
		writeleft -= writelen;
	}

	/* keep the file open, in case we need to copy more blocks in it */
}


void
remove_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_REMOVE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			remove_target_dir(entry->path);
			break;

		case FILE_TYPE_REGULAR:
			remove_target_file(entry->path, false);
			break;

		case FILE_TYPE_SYMLINK:
			remove_target_symlink(entry->path);
			break;
	}
}

void
create_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_CREATE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			create_target_dir(entry->path);
			break;

		case FILE_TYPE_SYMLINK:
			create_target_symlink(entry->path, entry->link_target);
			break;

		case FILE_TYPE_REGULAR:
			/* can't happen. Regular files are created with open_target_file. */
			pg_fatal("invalid action (CREATE) for regular file");
			break;
	}
}

/*
 * Remove a file from target data directory.  If missing_ok is true, it
 * is fine for the target file to not exist.
 */
void
remove_target_file(const char *path, bool missing_ok)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
	{
		if (errno == ENOENT && missing_ok)
			return;

		pg_fatal("could not remove file \"%s\": %m",
				 dstpath);
	}
}

void
truncate_target_file(const char *path, off_t newsize)
{
	char		dstpath[MAXPGPATH];
	int			fd;

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	fd = open(dstpath, O_WRONLY, pg_file_create_mode);
	if (fd < 0)
		pg_fatal("could not open file \"%s\" for truncation: %m",
				 dstpath);

	if (ftruncate(fd, newsize) != 0)
		pg_fatal("could not truncate file \"%s\" to %u: %m",
				 dstpath, (unsigned int) newsize);

	close(fd);
}

static void
create_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (mkdir(dstpath, pg_dir_create_mode) != 0)
		pg_fatal("could not create directory \"%s\": %m",
				 dstpath);
}

static void
remove_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (rmdir(dstpath) != 0)
		pg_fatal("could not remove directory \"%s\": %m",
				 dstpath);
}

static void
create_target_symlink(const char *path, const char *link)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (symlink(link, dstpath) != 0)
		pg_fatal("could not create symbolic link at \"%s\": %m",
				 dstpath);
}

static void
remove_target_symlink(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
		pg_fatal("could not remove symbolic link \"%s\": %m",
				 dstpath);
}


/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 * This function is used to implement the fetchFile function in the "fetch"
 * interface (see fetch.c), but is also called directly.
 */
char *
slurpFile(const char *datadir, const char *path, size_t *filesize)
{
	int			fd;
	char	   *buffer;
	struct stat statbuf;
	char		fullpath[MAXPGPATH];
	int			len;
	int			r;

	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	if (fstat(fd, &statbuf) < 0)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	len = statbuf.st_size;

	buffer = pg_malloc(len + 1);

	r = read(fd, buffer, len);
	if (r != len)
	{
		if (r < 0)
			pg_fatal("could not read file \"%s\": %m",
					 fullpath);
		else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 fullpath, r, (Size) len);
	}
	close(fd);

	/* Zero-terminate the buffer. */
	buffer[len] = '\0';

	if (filesize)
		*filesize = len;
	return buffer;
}


#ifdef ADB
void 
record_operator_create(file_entry_t *entry)
{
	char restore_cmd[MAXPGPATH] = {0};
	Assert(entry->action == FILE_ACTION_CREATE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			snprintf(restore_cmd, sizeof(restore_cmd), "rm -rf %s/%s;\n", datadir_target, entry->path);
			if (fputs(restore_cmd, rewind_file) == EOF)
				pg_fatal("fputs \"%s\" to rewind.sh fail.", restore_cmd);
			break;

		case FILE_TYPE_SYMLINK:
			snprintf(restore_cmd, sizeof(restore_cmd), "unlink %s/%s;\n", datadir_target, entry->path);
			if (fputs(restore_cmd, rewind_file) == EOF)
				pg_fatal("fputs \"%s\" to rewind.sh fail.", restore_cmd);
			break;

		case FILE_TYPE_REGULAR:
			/* can't happen. Regular files are created with open_target_file. */
			pg_fatal("invalid action (CREATE) for regular file");
			break;
	}
	if(fflush(rewind_file) != 0)
		pg_fatal("fflush failed in the process of record_operator_create");
}

void
record_operator_copy(char *old_file)
{
	char old_path[MAXPGPATH] = {0};
	char new_path[MAXPGPATH] = {0};
	char backup_cmd[MAXPGPATH] = {0};
	char restore_cmd[MAXPGPATH*2] = {0};
	bool exist = false;

	Assert(old_file);
	create_dir_sequence(old_file);

    /* backup old_file */
	snprintf(old_path, MAXPGPATH, "%s/%s", datadir_target, old_file);	
	if (access(old_path, F_OK) != -1)
		exist = true;

	if (exist)
	{
		snprintf(new_path, MAXPGPATH, "%s/%s", datadir_rewind, old_file);
		snprintf(backup_cmd, sizeof(backup_cmd), "cp %s %s", old_path, new_path);
		if (system(backup_cmd) == -1)
			pg_fatal("system exec cmd \"%s\" fail.", backup_cmd);
	}

	/* record restore cmd */
	if (exist)
		snprintf(restore_cmd, sizeof(restore_cmd), 
				"rm -rf %s; cp %s %s;\n",
				old_path, new_path, old_path);
	else
		snprintf(restore_cmd, sizeof(restore_cmd), 
				"rm -rf %s;\n",
				old_path);
	if (fputs(restore_cmd, rewind_file) == EOF)
		pg_fatal("fputs \"%s\" to rewind.sh fail.", restore_cmd);

	if(fflush(rewind_file) != 0)
		pg_fatal("fflush failed in the process of record_operator_copy");

	return;
}

void 
record_operator_truncate(char *old_file)
{
	record_operator_copy(old_file);
	return;
}

void 
record_operator_copytail(char *old_file)
{
	record_operator_copy(old_file);
	return;
}

void 
record_operator_remove(file_entry_t *entry)
{
	char old_path[MAXPGPATH] = {0};
	char new_path[MAXPGPATH] = {0};
	char backup_cmd[MAXPGPATH] = {0};
	char restore_cmd[MAXPGPATH*2] = {0};

	Assert(entry->action == FILE_ACTION_REMOVE);
	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			snprintf(restore_cmd, sizeof(restore_cmd), "mkdir %s/%s;\n", datadir_target, entry->path);
			if (fputs(restore_cmd, rewind_file) == EOF)
				pg_fatal("fputs \"%s\" to rewind.sh fail.", restore_cmd);
			break;

		case FILE_TYPE_REGULAR:
		case FILE_TYPE_SYMLINK:
			snprintf(old_path, sizeof(old_path), "%s/%s", datadir_target, entry->path);
			if (access(old_path, F_OK) == 0)
			{
				create_dir_sequence(entry->path);
				snprintf(new_path, sizeof(new_path), "%s/%s", datadir_rewind, entry->path);
				snprintf(backup_cmd, sizeof(backup_cmd), "cp %s %s;", old_path, new_path);
				if (system(backup_cmd) == -1)
					pg_fatal("system exec cmd \"%s\" fail.", backup_cmd);

				/* record restore cmd */
				snprintf(restore_cmd, sizeof(restore_cmd), 
						"rm -rf %s; cp %s  %s\n",
						old_path, new_path, old_path);
				if (fputs(restore_cmd, rewind_file) == EOF)
					pg_fatal("fputs \"%s\" to rewind.sh fail.", restore_cmd);
			}
			break;
	}

	if(fflush(rewind_file) != 0)
		pg_fatal("fflush failed in the process of record_operator_remove");
	
	return;
}

void 
open_rewind_file()
{
	char path[MAXPGPATH] = {0};
	snprintf(datadir_rewind, MAXPGPATH, 
			"%s%s",
			datadir_target, ADB_REWIND_TMP_DIR);
	snprintf(path, MAXPGPATH, "%s/%s", datadir_rewind, ADB_REWIND_FILE);
	rewind_file = fopen(path, "wb+");
	if (rewind_file == NULL)
		pg_fatal("could not open file \"%s\": %m",
				path);
}

void 
close_rewind_file()
{
	if (rewind_file != NULL)
	{
		fclose(rewind_file);
		rewind_file = NULL;
	}
}

/* create the sub dir of node_rewind */
static void create_dir_sequence(char *old_path)
{
	char *begin = old_path;
	char *end = NULL;
	char tmp_path[MAXPGPATH] = {0};
	char dir_path[MAXPGPATH] = {0};
		
	while(begin != NULL)
	{
		end = strstr(begin, "/");
		if (end == NULL)
			return;

		memset(dir_path, 0, sizeof(dir_path));
		memset(tmp_path, 0, sizeof(tmp_path));
		strncpy(dir_path, old_path, end-old_path);
		snprintf(tmp_path, sizeof(tmp_path), "%s/%s", datadir_rewind, dir_path);
		if (access(tmp_path, F_OK) == -1)
		{
			if (mkdir(tmp_path, PG_DIR_MODE_OWNER) != 0)
				pg_fatal("could not create directory \"%s\": %m",
						tmp_path);
		}
		begin =  end + 1;
	}
}

#endif
