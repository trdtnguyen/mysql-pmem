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

#include "univ.i"
#include "ut0byte.h"
#include "ut0rbt.h"
//#include "hash0hash.h" //for hashtable
#include "buf0buf.h" //for page_id_t
#include "page0types.h"
#include "ut0dbg.h"
#include "ut0new.h"

//#include "pmem_log.h"
#include <libpmemobj.h>
#include "my_pmem_common.h"
//#include "pmem0buf.h"
//cc -std=gnu99 ... -lpmemobj -lpmem
struct __pmem_buf_block_t;
typedef struct __pmem_buf_block_t PMEM_BUF_BLOCK;

struct __pmem_buf_block_list_t;
typedef struct __pmem_buf_block_list_t PMEM_BUF_BLOCK_LIST;

struct __pmem_buf_free_pool;
typedef struct __pmem_buf_free_pool PMEM_BUF_FREE_POOL;

struct __pmem_dbw;
typedef struct __pmem_dbw PMEM_DBW;

struct __pmem_log_buf;
typedef struct __pmem_log_buf PMEM_LOG_BUF;

struct __pmem_buf;
typedef struct __pmem_buf PMEM_BUF;

struct __pmem_wrapper;
typedef struct __pmem_wrapper PMEM_WRAPPER;

struct __pmem_list_cleaner_slot;
typedef struct __pmem_list_cleaner_slot PMEM_LIST_CLEANER_SLOT;

struct __pmem_list_cleaner;
typedef struct __pmem_list_cleaner PMEM_LIST_CLEANER;



POBJ_LAYOUT_BEGIN(my_pmemobj);
POBJ_LAYOUT_TOID(my_pmemobj, char);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_LOG_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_DBW);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_FREE_POOL);
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_BLOCK_LIST);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_BUF_BLOCK_LIST));
POBJ_LAYOUT_TOID(my_pmemobj, PMEM_BUF_BLOCK);
POBJ_LAYOUT_TOID(my_pmemobj, TOID(PMEM_BUF_BLOCK));
POBJ_LAYOUT_END(my_pmemobj);


////////////////////////// THE WRAPPER ////////////////////////
/*The global wrapper*/
struct __pmem_wrapper {
	char name[PMEM_MAX_FILE_NAME_LENGTH];
	PMEMobjpool* pop;
	PMEM_LOG_BUF* plogbuf;
	PMEM_DBW* pdbw;
	PMEM_BUF* pbuf;
	bool is_new;
};



/* FUNCTIONS*/

PMEM_WRAPPER* pm_wrapper_create(const char* path, const size_t pool_size);
void pm_wrapper_free(PMEM_WRAPPER* pmw);


PMEMoid pm_pop_alloc_bytes(PMEMobjpool* pop, size_t size);
void pm_pop_free(PMEMobjpool* pop);


////////////////////// LOG BUFFER /////////////////////////////

struct __pmem_log_buf {
	size_t				size;
	PMEM_OBJ_TYPES		type;	
	PMEMoid				data; //log data
    uint64_t			lsn; 	
	uint64_t			buf_free; /* first free offset within the log buffer */
	bool				need_recv; /*need recovery, it is set to false when init and when the server shutdown
					  normally. Whenever a log record is copy to log buffer, this flag is set to true
	*/
	uint64_t			last_tsec_buf_free; /*the buf_free updated in previous t seconds, update this value in srv_sync_log_buffer_in_background() */
};

void* pm_wrapper_logbuf_get_logdata(PMEM_WRAPPER* pmw);
int pm_wrapper_logbuf_alloc(PMEM_WRAPPER* pmw, const size_t size);
int pm_wrapper_logbuf_realloc(PMEM_WRAPPER* pmw, const size_t size);
PMEM_LOG_BUF* pm_pop_get_logbuf(PMEMobjpool* pop);
PMEM_LOG_BUF* pm_pop_logbuf_alloc(PMEMobjpool* pop, const size_t size);
PMEM_LOG_BUF* pm_pop_logbuf_realloc(PMEMobjpool* pop, const size_t size);
ssize_t  pm_wrapper_logbuf_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);

///////////// DOUBLE WRITE BUFFER //////////////////////////


struct __pmem_dbw {
	size_t size;
	PMEM_OBJ_TYPES type;	
	PMEMoid  data; //dbw data
	uint64_t s_first_free;
	uint64_t b_first_free;
	bool is_new;
};
void* pm_wrapper_dbw_get_dbwdata(PMEM_WRAPPER* pmw);
int pm_wrapper_dbw_alloc(PMEM_WRAPPER* pmw, const size_t size);
PMEM_DBW* pm_pop_get_dbw(PMEMobjpool* pop);
PMEM_DBW* pm_pop_dbw_alloc(PMEMobjpool* pop, const size_t size);
ssize_t  pm_wrapper_dbw_io(PMEM_WRAPPER* pmw, 
							const int type,
							void* buf, 
							const uint64_t offset,
							unsigned long int n);


/////// PMEM BUF  //////////////////////

//This struct is used only for POBJ_LIST_INSERT_NEW_HEAD
//modify this struct according to struct __pmem_buf_block_t
struct list_constr_args{
//	uint64_t		id;
	page_id_t					id;
//	size_t			size;
	page_size_t					size;
	int							check;
	//buf_page_t*		bpage;
	PMEM_BLOCK_STATE			state;
	TOID(PMEM_BUF_BLOCK_LIST)	list;
	uint64_t					pmemaddr;
};

/*
 *A unit page in pmem
 It wrap buf_page_t and an address in pmem
 * */
struct __pmem_buf_block_t{
	PMEMrwlock					lock;
	//POBJ_LIST_ENTRY(PMEM_BUF_BLOCK) entries;
//	uint64_t		id;
	page_id_t					id;
//	size_t			size;
	page_size_t					size;
	int							check;
	//buf_page_t*		bpage;
	bool	sync;
	PMEM_BLOCK_STATE			state;
	TOID(PMEM_BUF_BLOCK_LIST)	list;
	//reference to the flush thread (slot)
	PMEM_LIST_CLEANER_SLOT*	pslot;
	uint64_t		pmemaddr; /*
						  the offset of the page in pmem
						  note that the size of page can be got from page
						*/
};

struct __pmem_buf_block_list_t {
	PMEMrwlock				lock;
	uint64_t				list_id;
	TOID_ARRAY(TOID(PMEM_BUF_BLOCK))	arr;
	//POBJ_LIST_HEAD(block_list, PMEM_BUF_BLOCK) head;
	TOID(PMEM_BUF_BLOCK_LIST) next_list;
	TOID(PMEM_BUF_BLOCK_LIST) prev_list;
	TOID(PMEM_BUF_BLOCK) next_free_block;

	POBJ_LIST_ENTRY(PMEM_BUF_BLOCK_LIST) list_entries;

	size_t				max_pages; //max number of pages
	size_t				cur_pages; // current buffered pages
	bool				is_flush;
	size_t				n_aio_pending; //number of pending flush
	size_t				n_flush; //number of flush
	int					check;
	ulint				last_time;
};

struct __pmem_buf_free_pool {
	PMEMrwlock			lock;
	POBJ_LIST_HEAD(list_list, PMEM_BUF_BLOCK_LIST) head;
	size_t				cur_lists;
};

struct __pmem_buf {
	size_t size;
	size_t page_size;
	PMEM_OBJ_TYPES type;	
	PMEMoid  data; //pmem data
	char* p_align; //align 
	bool is_new;
	TOID(PMEM_BUF_FREE_POOL) free_pool;
	TOID_ARRAY(TOID(PMEM_BUF_BLOCK_LIST)) buckets;
	FILE* deb_file;
};

int pm_wrapper_buf_alloc(PMEM_WRAPPER* pmw, const size_t size, const size_t page_size);
PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop);
PMEM_BUF* pm_pop_buf_alloc(PMEMobjpool* pop, const size_t size, const size_t page_size);
int 
pm_buf_block_init(PMEMobjpool *pop, void *ptr, void *arg);

void
pm_buf_list_init(PMEMobjpool* pop, PMEM_BUF* buf, const size_t size, const size_t page_size);

int
//pm_buf_write(PMEMobjpool* pop, PMEM_BUF* buf, buf_page_t* bpage, void* data, bool sync);
pm_buf_write(PMEMobjpool* pop, PMEM_BUF* buf, page_id_t page_id, page_size_t size, void* src_data, bool sync);

size_t
pm_buf_read(PMEMobjpool* pop, PMEM_BUF* buf, const page_id_t page_id, const page_size_t size, void* data);

void
//pm_buf_flush_list(PMEMobjpool* pop, PMEM_BUF* buf, PMEM_BUF_BLOCK_LIST* plist);
pm_buf_flush_list(PMEMobjpool* pop, PMEM_BUF* buf, PMEM_LIST_CLEANER_SLOT* slot);

void
pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK)* ptoid_block);
//pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK) toid_block);
//pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, PMEMoid* poid);

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop);
//DEBUG functions

void pm_buf_print_lists_info(PMEM_BUF* buf);

///////// THREAD handler///////////////////////////
//This struct follow the design of page_cleaner_t
//Some functions are implemented in buf0flu.cc
//


struct __pmem_list_cleaner_slot {
	pm_list_cleaner_state		state;
	ulint						id;
	//ulint						check;
	ulint						n_pages_requested;
					/*!< number of requested pages
					for the slot */
	ulint						n_flushed_list;
					/*!< number of flushed pages
					by flush_list flushing */
	bool						succeeded_list;
					/*!< true if flush_list flushing
					succeeded. */
	ulint						flush_pass;
					/*!< count to attempt flush_list
					flushing */
	ulint						last_time;

	TOID(PMEM_BUF_BLOCK_LIST)		flush_list;
};

struct __pmem_list_cleaner {
	ib_mutex_t			mutex;
	os_event_t			is_requested;
	os_event_t			is_finished;
	volatile ulint		n_workers;
	bool				requested;/*!< true if requested pages to flush */

	//total slots and number of each slot state
	ulint				n_slots;
	ulint				n_slots_requested;
	ulint				n_slots_flushing;
	ulint				n_slots_finished;
	ulint				flush_time;
	ulint				flush_pass;
	bool				is_running;

	PMEM_LIST_CLEANER_SLOT*	slots;

	//this may not neccessary
	lsn_t				lsn_limit;


#ifdef UNIV_DEBUG
	ulint				n_disabled_debug;
#endif

};


void
pm_list_cleaner_init(void);

void
pm_list_cleaner_close(void);

void
pm_lc_request(
	TOID(PMEM_BUF_BLOCK_LIST) flush_list);
//void
//pm_lc_request(
//	ulint		min_n,
//	lsn_t		lsn_limit);

ulint
pm_lc_resume(
	TOID(PMEM_BUF_BLOCK_LIST) flush_list);

void
pm_lc_flush_slot(void);

void
//pm_handle_finished_slot(PMEM_LIST_CLEANER_SLOT* slot);
pm_handle_finished_block(PMEM_BUF_BLOCK* pblock);

bool
pm_lc_wait_finished(
	ulint*	n_flushed_list);

ulint
pm_lc_sleep_if_needed(
	ulint		next_loop_time,
	int64_t		sig_count);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_buf_flush_list_cleaner_coordinator)(
		void* arg);

extern "C"
os_thread_ret_t
DECLARE_THREAD(pm_buf_flush_list_cleaner_worker)(
		void* arg);

#ifdef UNIV_DEBUG

void
pm_buf_flush_list_cleaner_disabled_loop(void);
#endif


#define PMEM_BUF_LIST_INSERT(pop, list, entries, type, func, args) do {\
	POBJ_LIST_INSERT_NEW_HEAD(pop, &list.head, entries, sizeof(type), func, &args); \
	list.cur_size++;\
}while (0)

#define PMEM_HASH_KEY(hashed, key, n) do {\
	hashed = key ^ PMEM_HASH_MASK;\
	hashed = hashed % n;\
}while(0)


#endif /*__PMEMOBJ_H__ */
