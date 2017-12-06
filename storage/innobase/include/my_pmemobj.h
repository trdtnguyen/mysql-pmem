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
};

/*The global wrapper*/
struct __pmem_wrapper {
	char name[PMEM_MAX_FILE_NAME_LENGTH];
	PMEMobjpool* pop;
};

POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LOG_BUF);
POBJ_LAYOUT_END(my_pmemobj);


/* FUNCTIONS*/


static inline PMEM_WRAPPER* pm_create_PMEMwrapper(const char* path);
static inline void pm_free_PMEMwrapper(PMEM_WRAPPER* pmemw);

static inline PMEMobjpool* pm_create_PMEMobjpool(const char* path);
static inline void pm_free_PMEMobjpool(PMEMobjpool* pop);

static inline PMEMoid alloc_bytes(PMEMobjpool* pop, size_t size);
static inline int pm_log_buf_alloc(PMEMobjpool* pop, const size_t size);

static inline ssize_t  pm_log_buf_io(PMEMobjpool* pop, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);




static inline PMEM_WRAPPER* pm_create_PMEMwrapper(const char* path){
	PMEM_WRAPPER* pmemw =  (PMEM_WRAPPER*) malloc(sizeof(PMEM_WRAPPER));
	if (!pmemw)
		goto err;

	pmemw->pop = pm_create_PMEMobjpool(path);
	if(!pmemw->pop)
		goto err;

	strncpy(pmemw->name, path, PMEM_MAX_FILE_NAME_LENGTH);
	pmemw->name[PMEM_MAX_FILE_NAME_LENGTH - 1] = '\0';

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
	size_t size =  PMEM_MAX_LOG_BUF_SIZE +
				   PMEM_MAX_LOG_DBWR_SIZE;

	if (file_exists(path) != 0) {
		if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(my_pmemobj),
						size, S_IWRITE | S_IREAD)) == NULL) {
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
	TOID(PMEM_LOG_BUF) f;
	TOID(char) data;

	POBJ_FOREACH_TYPE(pop, f) {
		switch ( D_RO(f)->type){
			case LOG_BUF_TYPE: 
				TOID_ASSIGN(data, D_RW(f)->data);
				POBJ_FREE(&data);	
			break;
			case DWRB_TYPE:
			case META_DATA_TYPE:
			case UNKNOWN_TYPE:
			break;
		}	
		POBJ_FREE(&f);	
	}
	pmemobj_close(pop);
}


static inline PMEMoid alloc_bytes(PMEMobjpool* pop, size_t size) {
	TOID(char) array;

	POBJ_ALLOC(pop, &array, char, sizeof(char) * size,	NULL, NULL);

	if (TOID_IS_NULL(array)) {
		fprintf(stderr, "POBJ_ALLOC\n");
		return OID_NULL;
	}

	pmemobj_persist(pop, D_RW(array), size * sizeof(*D_RW(array)));
	//Check
	char* p = (char*) pmemobj_direct(array.oid);
	if(!p){
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
	}
	return array.oid;
}

static inline int pm_log_buf_alloc(PMEMobjpool* pop, const size_t size) {

	TOID(PMEM_LOG_BUF) logbuf; 

	POBJ_ZNEW(pop, &logbuf, PMEM_LOG_BUF);

	PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
	plogbuf->size = size;
	plogbuf->type = LOG_BUF_TYPE;

	plogbuf->data = alloc_bytes(pop, size);
	if (OID_IS_NULL(plogbuf->data)){
		//assert(0);
		return PMEM_ERROR;
	}

	pmemobj_persist(pop, plogbuf, sizeof(*plogbuf));
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
static inline ssize_t  pm_log_buf_io(PMEMobjpool* pop, 
							const int type, 
							void* buf, 
							const uint64_t offset,
							unsigned long int n){
	ssize_t ret_bytes = 0;
	char* pret;

	TOID(PMEM_LOG_BUF) logbuf;
	//get the first object in pmem has type PMEM_LOG_BUF
	logbuf = POBJ_FIRST(pop, PMEM_LOG_BUF);

	if (TOID_IS_NULL(logbuf)) {
		printf("[PMEM_ERROR] cannot get PMEM_LOG_BUF");
		return 0;
	}			
	else {
		PMEM_LOG_BUF *plogbuf = D_RW(logbuf);
		if(!plogbuf) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return 0;
		}

		char *p = (char*) pmemobj_direct(plogbuf->data);
		if (type == PMEM_WRITE){
			pret =(char*)  pmemobj_memcpy_persist(pop, p + offset, buf, (size_t) n);	
			ret_bytes = n;
		}
		else if (type == PMEM_READ) {
			pret = (char*) memcpy(buf, p + offset, n);
			ret_bytes = n;
		}
		return ret_bytes;
	}
}

#endif /*__PMEMOBJ_H__ */
