
#ifndef __PMEM_COMMON_H__
#define __PMEM_COMMON_H__


#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>                                                                      
#include <sys/time.h> //for struct timeval, gettimeofday()
#include <string.h>
#include <stdint.h> //for uint64_t
#include <math.h> //for log()
#include <assert.h>
#include <wchar.h>
#include <unistd.h> //for access()

#include "os0file.h"
//#include "pmem_log.h"
#include <libpmemobj.h>
//cc -std=gnu99 ... -lpmemobj -lpmem
//
#define PMEM_MAX_FILES 1000
#define PMEM_MAX_FILE_NAME_LENGTH 10000

//error handler
#define PMEM_SUCCESS 0
#define PMEM_ERROR -1

#define TOID_ARRAY(x) TOID(x)

enum {
	PMEM_READ = 1,
	PMEM_WRITE = 2
};

static inline int file_exists(char const *file);

/*
 *  * file_exists -- checks if file exists
 *   */
static inline int file_exists(char const *file)
{
	    return access(file, F_OK);
}

#endif
