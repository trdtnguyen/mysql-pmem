/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
 * */


#ifndef __PMEMOBJ_H__
#define __PMEMOBJ_H__


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
#include <libpmemobj.h>
//cc -std=gnu99 ... -lpmemobj -lpmem
//
#define PMEM_MAX_FILES 1000
#define PMEM_MAX_FILE_NAME_LENGTH 1000
#define PMEM_MAX_LOG_FILE_SIZE 256 * 1024 * 1024  //256MB

#define TOID_ARRAY(x) TOID(x)



//error handler
#define PMEM_SUCCESS 0
#define PMEM_ERROR -1
#define PMEM_FILE "/mnt/pmem01/mapfile"



struct __pmem_log_file;
typedef struct __pmem_log_file PMEM_LOG_FILE;
struct __pmem_wrapper;
typedef struct __pmem_wrapper PMEM_WRAPPER;

enum PMEM_OBJ_TYPES {
	UNKNOWN_TYPE,
	LOG_BUF_TYPE,
	LOG_FILE_TYPE,
	DWRB_TYPE,
	META_DATA_TYPE
};

enum {
	PMEM_READ = 1,
	PMEM_WRITE = 2
};

struct __pmem_log_file {
	char name[PMEM_MAX_FILE_NAME_LENGTH]; //file name
	int fd; //corresponding fd
	size_t size;
	PMEM_OBJ_TYPES type;	
	PMEMoid  data; //log data
};

/*The global wrapper*/
struct __pmem_wrapper {
	char name[PMEM_MAX_FILE_NAME_LENGTH];
	PMEMobjpool* pop;
	size_t logfile_size;
};

POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LOG_FILE);
POBJ_LAYOUT_END(my_pmemobj);


/* FUNCTIONS*/

static inline int file_exists(char const *file);

static inline PMEM_WRAPPER* pm_create_PMEMwrapper(const char* path);
static inline void pm_free_PMEMwrapper(PMEM_WRAPPER* pmemw);

static inline PMEMobjpool* pm_create_PMEMobjpool(const char* path);
static inline void pm_free_PMEMobjpool(PMEMobjpool* pop);

static inline TOID(PMEM_LOG_FILE) pm_find_log_file_by_name(PMEMobjpool* pop, const char *name);
static inline TOID(PMEM_LOG_FILE) pm_find_log_file_by_fd(PMEMobjpool* pop, const int fd);
static inline PMEMoid alloc_bytes(PMEMobjpool* pop, size_t size);
static inline int pm_log_file_alloc(PMEMobjpool* pop, const char* name, const int fd,  const size_t size);

static inline ssize_t  pm_log_io(PMEMobjpool* pop, 
							const int type, 
							const int fd, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n);





/*
 *  * file_exists -- checks if file exists
 *   */
static inline int file_exists(char const *file)
{
	    return access(file, F_OK);
}

static inline PMEM_WRAPPER* pm_create_PMEMwrapper(const char* path){
	PMEM_WRAPPER* pmemw =  (PMEM_WRAPPER*) malloc(sizeof(PMEM_WRAPPER));
	if (!pmemw)
		goto err;

	pmemw->pop = pm_create_PMEMobjpool(path);
	if(!pmemw->pop)
		goto err;

	strncpy(pmemw->name, path, PMEM_MAX_FILE_NAME_LENGTH);
	pmemw->name[PMEM_MAX_FILE_NAME_LENGTH - 1] = '\0';

	pmemw->logfile_size = PMEM_MAX_LOG_FILE_SIZE;

	return pmemw;

err:
	printf("PMEMOBJ_ERROR: error in pm_create_PMEMwrapper");
	if(pmemw)
		pm_free_PMEMwrapper(pmemw);
	return NULL;
}

static inline void pm_free_PMEMwrapper(PMEM_WRAPPER* pmemw){
	if(pmemw->pop)
		pm_free_PMEMobjpool(pmemw->pop);
	free(pmemw);

}

static inline PMEMobjpool* pm_create_PMEMobjpool(const char* path) {
	PMEMobjpool* pop = NULL;

	if (file_exists(path) != 0) {
		if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(my_pmemobj),
						PMEMOBJ_MIN_POOL, S_IWRITE | S_IREAD)) == NULL) {
			printf("[PMEM_ERROR] failed to create pool\n");
			return NULL;
		}
	} else {
		if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(my_pmemobj)))
				== NULL) {
			printf("failed to open pool\n");
			return NULL;
		}
	}
	return pop;
}	

static inline void pm_free_PMEMobjpool(PMEMobjpool* pop){
	TOID(PMEM_LOG_FILE) f;
	TOID(char) data;

	POBJ_FOREACH_TYPE(pop, f) {
		switch ( D_RO(f)->type){
			case LOG_FILE_TYPE: 
				TOID_ASSIGN(data, D_RW(f)->data);
				POBJ_FREE(&data);	
			break;
			case LOG_BUF_TYPE:
			case DWRB_TYPE:
			case META_DATA_TYPE:
			case UNKNOWN_TYPE:
			break;
		}	
		POBJ_FREE(&f);	
	}
	pmemobj_close(pop);
}

static inline TOID(PMEM_LOG_FILE) pm_find_log_file_by_name(PMEMobjpool* pop, const char *name){
	TOID(PMEM_LOG_FILE) f;
	POBJ_FOREACH_TYPE(pop, f) {
		if (strncmp(D_RO(f)->name, name, PMEM_MAX_FILE_NAME_LENGTH) == 0)
			return f;
	}
	return TOID_NULL(PMEM_LOG_FILE);
}

static inline TOID(PMEM_LOG_FILE) pm_find_log_file_by_fd(PMEMobjpool* pop, const int fd){
	TOID(PMEM_LOG_FILE) f;
	POBJ_FOREACH_TYPE(pop, f) {
		if ( D_RO(f)->fd == fd )
			return f;
	}
	return TOID_NULL(PMEM_LOG_FILE);
}

static inline PMEMoid alloc_bytes(PMEMobjpool* pop, size_t size) {
	TOID(char) array;

	POBJ_ALLOC(pop, &array, char, sizeof(char) * size,	NULL, NULL);

	if (TOID_IS_NULL(array)) {
		fprintf(stderr, "POBJ_ALLOC\n");
		return OID_NULL;
	}

	pmemobj_persist(pop, D_RW(array), size * sizeof(*D_RW(array)));
	return array.oid;
}

static inline int pm_log_file_alloc(PMEMobjpool* pop, const char* name, const int fd,  const size_t size) {

	TOID(PMEM_LOG_FILE) logfile = pm_find_log_file_by_name(pop, name);

	if (!TOID_IS_NULL(logfile)){
		//file existed
		//	POBJ_FREE(logfile);
		return PMEM_SUCCESS;
	}

	POBJ_ZNEW(pop, &logfile, PMEM_LOG_FILE);

	PMEM_LOG_FILE *plogfile = D_RW(logfile);
	strncpy(plogfile->name, name, PMEM_MAX_FILE_NAME_LENGTH);
	plogfile->name[PMEM_MAX_FILE_NAME_LENGTH - 1] = '\0';
	plogfile->size = size;
	plogfile->type = LOG_FILE_TYPE;
	plogfile->fd = fd;

	plogfile->data = alloc_bytes(pop, size);
	if (OID_IS_NULL(plogfile->data)){
		//assert(0);
		return PMEM_ERROR;
	}

	pmemobj_persist(pop, plogfile, sizeof(*plogfile));

	return PMEM_SUCCESS;
} 

/*Perform both AIO and normal IO
 *Input:
	type: PMEM_READ or PMEM_WRITE
	fd: file handler
	buf: buffer contains data to read to or write from
	offset: offset in file from 0
	n: number of bytes IO
   Return:
   Number of bytes has read/written
 * */
static inline ssize_t  pm_log_io(PMEMobjpool* pop, 
							const int type, 
							const int fd, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n){
	ssize_t ret_bytes = 0;

	//TOID(PMEM_LOG_FILE) logfile = pm_find_log_file_by_name(name);
	TOID(PMEM_LOG_FILE) logfile = pm_find_log_file_by_fd(pop, fd);

#ifdef UNIV_NVM_LOG_DEBUG
			printf("[PMEM_INFO] pfc_pmem_io() type: %d fd: %d n_bytes: %lu \n", type, fd, n);
#endif
	if (TOID_IS_NULL(logfile)) {
		printf("[PMEM_ERROR] there is no file with fd=%d to do IO\n", fd);
		return 0;
	}			
	else {
		char *p = (char*) pmemobj_direct(D_RW(logfile)->data);

		if (type == PMEM_WRITE){
			pmemobj_memcpy_persist(pop, p + offset, buf, (size_t) n);	
			ret_bytes = n;
		}
		else if (type == PMEM_READ) {
			memcpy(buf, p + offset, n);
			ret_bytes = n;
		}
		return ret_bytes;
	}
}

#endif /*__PMEMOBJ_H__ */
