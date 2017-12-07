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
//#include "pmem_log.h"
#include <libpmemobj.h>
#include "my_pmem_common.h"
//cc -std=gnu99 ... -lpmemobj -lpmem

//OS_FILE_LOG_BLOCK_SIZE =512 is defined in os0file.h
static const size_t PMEM_MB = 1024 * 1024;
static const size_t PMEM_MAX_LOG_BUF_SIZE = 1 * 1024 * PMEM_MB;
static const size_t PMEM_MAX_LOG_DBWR_SIZE = 1 * 1024 * PMEM_MB;

struct __pmem_log_buf;
typedef struct __pmem_log_buf PMEM_LOG_BUF;
struct __pmem_wrapper;
typedef struct __pmem_wrapper PMEM_WRAPPER;

enum PMEM_OBJ_TYPES {
	UNKNOWN_TYPE,
	LOG_BUF_TYPE,
	DWRB_TYPE,
	META_DATA_TYPE
};


struct __pmem_log_buf {
	size_t size;
	PMEM_OBJ_TYPES type;	
	PMEMoid  data; //log data
    uint64_t lsn; 	
	uint64_t buf_free; /* first free offset within the log buffer */
};

/*The global wrapper*/
struct __pmem_wrapper {
	char name[PMEM_MAX_FILE_NAME_LENGTH];
	PMEMobjpool* pop;
	PMEM_LOG_BUF* plogbuf;
	bool is_new;
};

POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LOG_BUF);
POBJ_LAYOUT_END(my_pmemobj);


/* FUNCTIONS*/

static inline PMEM_WRAPPER* pm_wrapper_create(const char* path);
static inline void pm_wrapper_free(PMEM_WRAPPER* pmw);
static inline int pm_wrapper_logbuf_alloc(PMEM_WRAPPER* pmw, const size_t size);
static inline ssize_t  pm_wrapper_logbuf_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);
static inline void* pm_wrapper_logbuf_get_logdata(PMEM_WRAPPER* pmw);

static inline void pm_pop_free(PMEMobjpool* pop);
static inline PMEM_LOG_BUF* pm_pop_get_logbuf(PMEMobjpool* pop);
static inline PMEM_LOG_BUF* pm_pop_logbuf_alloc(PMEMobjpool* pop, const size_t size);
static inline PMEMoid pm_pop_alloc_bytes(PMEMobjpool* pop, size_t size);

///////////////////////////////////////////////////////////////////////////


static inline PMEM_WRAPPER* pm_wrapper_create(const char* path){
	PMEM_WRAPPER* pmw;
	PMEMobjpool* pop = NULL;
	size_t size;

	pmw =  (PMEM_WRAPPER*) malloc(sizeof(PMEM_WRAPPER));
	if (!pmw)
		goto err;
	pmw->is_new = true;

	/*create new or open existed PMEMobjpool*/
	size =  PMEM_MAX_LOG_BUF_SIZE +
				   PMEM_MAX_LOG_DBWR_SIZE;

	if (file_exists(path) != 0) {
		if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(my_pmemobj),
						size, S_IWRITE | S_IREAD)) == NULL) {
			printf("[PMEMOBJ_ERROR] failed to create pool\n");
			goto err;
		}
	} else {
		pmw->is_new = false;

		if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(my_pmemobj)))
				== NULL) {
			printf("[PMEMOBJ_ERROR] failed to open pool\n");
			goto err;
		}
	}
	pmw->pop = pop;

//	pmemw->pop = pm_create_PMEMobjpool(path);
	/*name */
	strncpy(pmw->name, path, PMEM_MAX_FILE_NAME_LENGTH);
	pmw->name[PMEM_MAX_FILE_NAME_LENGTH - 1] = '\0';

	/* log buffer */
	pmw->plogbuf = NULL;

	if(!pmw->is_new) {
		//Try to get the pmem log buffer from pop
		pmw->plogbuf = pm_pop_get_logbuf(pop);
		if(!pmw->plogbuf){
			printf("[PMEMOBJ_INFO] the pmem log buffer is empty. The server've shutdown normally\n");
		}
	}	
	return pmw;

err:
	printf("PMEMOBJ_ERROR: error in pm_create_PMEMwrapper");
	if(pmw)
		pm_wrapper_free(pmw);
	return NULL;
}

static inline void pm_wrapper_free(PMEM_WRAPPER* pmw){
	if(pmw->pop)
		pm_pop_free(pmw->pop);
	pmw->plogbuf = NULL;
	pmw->pop = NULL;
	printf("PMEMOBJ_INFO: free PMEM_WRAPPER from heap allocated\n");
	free(pmw);

}


static inline void pm_pop_free(PMEMobjpool* pop){
	TOID(PMEM_LOG_BUF) logbuf;
	TOID(char) data;
	
	/* Free log buffer*/
	POBJ_FOREACH_TYPE(pop, logbuf) {
		TOID_ASSIGN(data, D_RW(logbuf)->data);
		POBJ_FREE(&data);	

		D_RW(logbuf)->lsn = 0;
		D_RW(logbuf)->buf_free = 0;

		POBJ_FREE(&logbuf);	
	}
	printf("PMEMOBJ_INFO: free PMEMobjpool from pmem\n");
	/*Free DBWR*/

	pmemobj_close(pop);
}


static inline PMEMoid pm_pop_alloc_bytes(PMEMobjpool* pop, size_t size){
	TOID(char) array;

	POBJ_ALLOC(pop, &array, char, sizeof(char) * size,	NULL, NULL);

	if (TOID_IS_NULL(array)) {
		fprintf(stderr, "POBJ_ALLOC\n");
		return OID_NULL;
	}

	pmemobj_persist(pop, D_RW(array), size * sizeof(*D_RW(array)));
	printf("PMEMOBJ_INFO: allocate PMEMobjpool from pmem with size %zu MB\n", (size/1024));

	//Check
	char* p = (char*) pmemobj_direct(array.oid);
	if(!p){
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
	}
	return array.oid;
}

static inline int pm_wrapper_logbuf_alloc(PMEM_WRAPPER* pmw, const size_t size) {
	assert(pmw);

	pmw->plogbuf = pm_pop_logbuf_alloc(pmw->pop, size);
	if (!pmw->plogbuf)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;
}
/*
 * Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * */
static inline PMEM_LOG_BUF* pm_pop_logbuf_alloc(PMEMobjpool* pop, const size_t size) {

	TOID(PMEM_LOG_BUF) logbuf; 

	POBJ_ZNEW(pop, &logbuf, PMEM_LOG_BUF);

	PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
	plogbuf->size = size;
	plogbuf->type = LOG_BUF_TYPE;
	//we will update lsn, buf_free later
	plogbuf->lsn = 0;
	plogbuf->buf_free = 0;
	plogbuf->data = pm_pop_alloc_bytes(pop, size);
	if (OID_IS_NULL(plogbuf->data)){
		//assert(0);
		return NULL;
	}

	pmemobj_persist(pop, plogbuf, sizeof(*plogbuf));
	return plogbuf;
} 

static inline PMEM_LOG_BUF* pm_pop_get_logbuf(PMEMobjpool* pop) {
	TOID(PMEM_LOG_BUF) logbuf;
	//get the first object in pmem has type PMEM_LOG_BUF
	logbuf = POBJ_FIRST(pop, PMEM_LOG_BUF);

	if (TOID_IS_NULL(logbuf)) {
		return NULL;
	}			
	else {
		PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
		if(!plogbuf) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return NULL;
		}
		return plogbuf;
	}
}
/*Perform both AIO and normal IO
 *Input:
	type: PMEM_READ or PMEM_WRITE
	buf: buffer contains data to read to or write from
	offset: offset in file from 0
	n: number of bytes IO
Condition: The plogbuf in the wrapper must be allocated first
   Return:
   Number of bytes has read/written
 * */
static inline ssize_t  pm_wrapper_logbuf_io(PMEM_WRAPPER* pmw, 
							const int type, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n){
	unsigned long int ret_bytes;
		assert(pmw);
		if(!pmw->plogbuf){
			printf("PMEMOBJ_ERROR pmw->plogbuf has not allocated \n");
			return 0;
		}
		char *p = (char*) pmemobj_direct(pmw->plogbuf->data);
		if (type == PMEM_WRITE){
			pmemobj_memcpy_persist(pmw->pop, p + offset, buf, (size_t) n);	
			ret_bytes = n;
		}
		else if (type == PMEM_READ) {
			memcpy(buf, p + offset, n);
			ret_bytes = n;
		}
		return ret_bytes;
}

static inline void* pm_wrapper_logbuf_get_logdata(PMEM_WRAPPER* pmw){
	assert(pmw);
	assert(pmw->plogbuf);
	return pmemobj_direct(pmw->plogbuf->data);
}
#endif /*__PMEMOBJ_H__ */
