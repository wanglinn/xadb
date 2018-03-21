#ifndef MEM_TOC_H
#define MEM_TOC_H

#include "lib/stringinfo.h"

extern void begin_mem_toc_insert(StringInfo buf, uint32 key);
extern void end_mem_toc_insert(StringInfo buf, uint32 key);
extern void *mem_toc_lookup(StringInfo buf, uint32 key, int *len);

#endif /* MEM_TOC_H */
