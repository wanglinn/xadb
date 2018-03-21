/*-------------------------------------------------------------------------
 *
 * pg_extendpage.c - Extend file with one or more zero pages.
 *
 * Copyright (c) 2014-2017, ADB Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_extendpage/pg_extendpage.c
 *-------------------------------------------------------------------------
 */
#define FRONTEND 1
#include "postgres.h"
	
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>

#include "common/fe_memutils.h"
#include "getopt_long.h"

static const char *progname;

static void fatal_error(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * Big red button to push when things go horribly wrong.
 */
static void
fatal_error(const char *fmt,...)
{
	va_list		args;

	fflush(stdout);

	fprintf(stderr, "%s: FATAL:  ", progname);
	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);
	fputc('\n', stderr);

	fprintf(stderr,
		"\nTry \"%s --help\" for more information.\n",
		progname);

	exit(EXIT_FAILURE);
}

static void
extend_zero_pages(const char *path, int pageno, size_t pagesz)
{
	char	*buf = NULL;
	int		fd;
	off_t	endpos;
	int		pgnumber = pageno;

	Assert(pageno >= 0);

	fd = open(path, O_RDWR | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd < 0)
	{
		fatal_error("Fail to open or create \"%s\": %m", path);
	}

	if (pgnumber == 0)
	{
		close(fd);
		return ;
	}

	buf = pg_malloc_extended(pagesz, MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
	if (buf == NULL)
	{
		close(fd);
		fatal_error("Fail to malloc %zu bytes of page", pagesz);
	}

	if ((endpos = lseek(fd, 0, SEEK_END)) < 0)
	{
		close(fd);
		fatal_error("Fail to lseek the end of file \"%s\": %m.", path);
	}

	while (pgnumber--)
	{
		if (write(fd, buf, pagesz) != pagesz)
		{
			close(fd);
			fatal_error("Fail to write the %d zero page for \"%s\": %m.",
				pgnumber + 1, path);
		}
	}
	pg_free(buf);
	close(fd);

	if (pageno > 0)
		fprintf(stdout, "Successful to extend %d zero pages, each page with %zu bytes\n",
			pageno, pagesz);
	else
		fprintf(stdout, "Successful to create file \"%s\"\n", path);
}

static void
usage(void)
{
	printf("%s - Extend file with one or more zero pages.\n\n",
		   progname);
	printf("Usage:\n");
	printf("  %s [OPTION]\n", progname);
	printf("\nOptions:\n");
	printf("  -f, --file=FILE        file which will be extend\n");
	printf("  -n, --limit=N          number of pages to extend\n"
		   "                         (default 0, it means create FILE if not exist)\n");
	printf("  -p, --page=SIZE        one page size, page unit: bytes\n"
		   "                         (default BLCKSZ(%d) bytes)\n", BLCKSZ);
	printf("  -V, --version          output version information, then exit\n");
	printf("  -?, --help             show this help, then exit\n");
}

int main(int argc, char * const argv[])
{
	static struct option long_options[] = {
			{"help", no_argument, NULL, '?'},
			{"file", required_argument, NULL, 'f'},
			{"limit", required_argument, NULL, 'n'},
			{"page", required_argument, NULL, 'p'},
			{"version", no_argument, NULL, 'V'},
			{NULL, 0, NULL, 0}
		};
	
	int 		option;
	int 		optindex = 0;
	char	   *filepath = NULL;
	int			pageno = 0;
	int			pagesz = BLCKSZ;

	progname = get_progname(argv[0]);

	if (argc <= 1)
		fatal_error("no arguments specified");

	while ((option = getopt_long(argc, argv, "f:n:p:V?",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case '?':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'f':
				filepath = pg_strdup(optarg);
				break;
			case 'n':
				if (sscanf(optarg, "%d", &pageno) != 1)
				{
					fatal_error("could not parse limit \"%s\"",
						optarg);
				}
				break;
			case 'p':
				if (sscanf(optarg, "%d", &pagesz) != 1)
				{
					fatal_error("could not parse page size \"%s\"",
						optarg);
				}
				if (pagesz <= 0)
					fatal_error("invalid page size: %d", pagesz);
				break;
			case 'V':
				fprintf(stdout, "%s (PostgreSQL) %s\n", progname, PG_VERSION);
				exit(EXIT_SUCCESS);
				break;
			default:
				fatal_error("Unknown argument(%c)", option);
				break;
		}
	}

	if ((optind + 2) < argc)
	{
		fatal_error("too many command-line arguments (first is \"%s\")",
			argv[optind + 2]);
	}

	if (filepath == NULL)
	{
		fatal_error("a file which will be extended is needed");
	}

	if (pageno >= 0)
		extend_zero_pages(filepath, pageno, pagesz);

	exit(EXIT_SUCCESS);
}