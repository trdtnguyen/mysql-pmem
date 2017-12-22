
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
#define PMEM_HASH_MASK 1653893711

//random number for checking AIO
#define PMEM_AIO_CHECK 7988
//error handler
#define PMEM_SUCCESS 0
#define PMEM_ERROR -1

#define TOID_ARRAY(x) TOID(x)

#define PMEMOBJ_FILE_NAME "pmemobjfile"
//OS_FILE_LOG_BLOCK_SIZE =512 is defined in os0file.h
static const size_t PMEM_MB = 1024 * 1024;
static const size_t PMEM_MAX_LOG_BUF_SIZE = 1 * 1024 * PMEM_MB;
static const size_t PMEM_PAGE_SIZE = 16*1024; //16KB
static const size_t PMEM_MAX_DBW_PAGES= 128; // 2 * extent_size

#define PMEM_N_BUCKETS 128 
#define PMEM_MAX_LISTS_PER_BUCKET 4 
#define PMEM_BUF_THRESHOLD 0.8


enum {
	PMEM_READ = 1,
	PMEM_WRITE = 2
};
enum PMEM_OBJ_TYPES {
	UNKNOWN_TYPE,
	LOG_BUF_TYPE,
	DBW_TYPE,
	BUF_TYPE,
	META_DATA_TYPE
};
enum PMEM_BLOCK_STATE {
    PMEM_FREE_BLOCK = 1,
    PMEM_IN_USED_BLOCK = 2,
    PMEM_IN_FLUSH_BLOCK=3
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
