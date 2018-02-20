/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
 * */

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

#include "my_pmem_common.h"
#include "my_pmemobj.h"

#include "os0file.h"
#include "buf0dblwr.h"

#if defined (UNIV_PMEMOBJ_BUF)
//GLOBAL variables
static uint64_t PMEM_N_BUCKETS;
static uint64_t PMEM_BUCKET_SIZE;
static double PMEM_BUF_FLUSH_PCT;

static uint64_t PMEM_N_FLUSH_THREADS;
//set this to large number to eliminate 
//static uint64_t PMEM_PAGE_PER_BUCKET_BITS=32;

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
//256 buckets => 8 bits, max 32 spaces => 5 bits => need 3 = 8 - 5 bits
static uint64_t PMEM_N_BUCKET_BITS = 8;
static uint64_t PMEM_N_SPACE_BITS = 5;
static uint64_t PMEM_PAGE_PER_BUCKET_BITS=10;

static FILE* debug_file = fopen("part_debug.txt","w");

/*
 * LESS_BUCKET partition
 * space_no and page_no are 32-bits value
 * the hashed value is B-bits value where B is the number of bits to present the number of buckets 
 * One space_no in a bucket has maximum N pages where log2(N) is page_per_bucket_bits
 * @space_no [in]: space number
 * @page_no	[in]: page number
 * @ n_buckets [in]: number of buckets
 * @ page_per_bucket_bits [in]: number of bits present the maximum number of pages each space in a hash list can have
 * 
 * */
ulint 
hash_f1(
		uint32_t		space_no,
	   	uint32_t		page_no,
	   	uint64_t		n_buckets,
		uint64_t		page_per_bucket_bits)
{	
	ulint hashed;

	uint32_t mask1 = 0xffffffff >> (32 - PMEM_N_SPACE_BITS);
	uint32_t mask2 = 0xffffffff >> 
		(32 - page_per_bucket_bits - (PMEM_N_BUCKET_BITS - PMEM_N_SPACE_BITS)) ;

	ulint p;
	ulint s;

	s = (space_no & mask1) << (PMEM_N_BUCKET_BITS - PMEM_N_SPACE_BITS);
	p = (page_no & mask2) >> page_per_bucket_bits;

	hashed = (p + s) % n_buckets;

	//printf("space %zu (0x%08x) page %zu (0x%08x)  p 0x%016x s 0x%016x hashed %zu (0x%016x) \n",
	//		space_no, space_no, page_no, page_no, p, s, hashed, hashed);

	return hashed;
}
#endif //UNIV_PMEMOBJ_BUF_PARTITION
void
pm_wrapper_buf_alloc_or_open(
		PMEM_WRAPPER*		pmw,
		const size_t		buf_size,
		const size_t		page_size)
{

	uint64_t i;
	char sbuf[256];

	PMEM_N_BUCKETS = srv_pmem_buf_n_buckets;
	PMEM_BUCKET_SIZE = srv_pmem_buf_bucket_size;
	PMEM_BUF_FLUSH_PCT = srv_pmem_buf_flush_pct;
#if defined (UNIV_PMEMOBJ_BUF_FLUSHER)
	PMEM_N_FLUSH_THREADS= srv_pmem_n_flush_threads;
#endif

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_N_BUCKET_BITS = log2(srv_pmem_buf_n_buckets);
	PMEM_N_SPACE_BITS = srv_pmem_n_space_bits;
	PMEM_PAGE_PER_BUCKET_BITS = srv_pmem_page_per_bucket_bits;
	printf("======> >> > >PMEM PARTITION: n_bucket_bits %zu n_space_bits %zu page_per_bucket_bits %zu\n",
			PMEM_N_BUCKET_BITS, PMEM_N_SPACE_BITS, PMEM_PAGE_PER_BUCKET_BITS);
#endif 
	if (!pmw->pbuf) {
		//Case 1: Alocate new buffer in PMEM
			printf("PMEMOBJ_INFO: allocate %zd MB of buffer in pmem\n", buf_size);

		if ( pm_wrapper_buf_alloc(pmw, buf_size, page_size) == PMEM_ERROR ) {
			printf("PMEMOBJ_ERROR: error when allocate buffer in buf_dblwr_init()\n");
		}
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the buffer is persist, in pmem: size = %zd free_pool has = %zd free lists\n", 
				pmw->pbuf->size, D_RW(pmw->pbuf->free_pool)->cur_lists);
		//Check the page_size of the previous run with current run
		if (pmw->pbuf->page_size != page_size) {
			printf("PMEM_ERROR: the pmem buffer size = %zu is different with UNIV_PAGE_SIZE = %zu, you must use the same page_size!!!\n ",
					pmw->pbuf->page_size, page_size);
			assert(0);
		}
			
		//We need to re-align the p_align
		byte* p;
		p = static_cast<byte*> (pmemobj_direct(pmw->pbuf->data));
		assert(p);
		pmw->pbuf->p_align = static_cast<byte*> (ut_align(p, page_size));
	}
	//In any case (new allocation or resued, we should allocate the flush_events for buckets in DRAM
	pmw->pbuf->flush_events = (os_event_t*) calloc(PMEM_N_BUCKETS, sizeof(os_event_t));

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		sprintf(sbuf,"pm_flush_bucket%zu", i);
		pmw->pbuf->flush_events[i] = os_event_create(sbuf);
	}
		pmw->pbuf->free_pool_event = os_event_create("pm_free_pool_event");


	//Alocate the param array
	PMEM_BUF_BLOCK_LIST* plist;
	pmw->pbuf->params_arr = static_cast<PMEM_AIO_PARAM**> (
		calloc(PMEM_N_BUCKETS, sizeof(PMEM_AIO_PARAM*)));	
	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		plist = D_RW(D_RW(pmw->pbuf->buckets)[i]);

		pmw->pbuf->params_arr[i] = static_cast<PMEM_AIO_PARAM*> (
				calloc(plist->max_pages, sizeof(PMEM_AIO_PARAM)));
	}
}

/*
 * CLose/deallocate resource in DRAM
 * */
void pm_wrapper_buf_close(PMEM_WRAPPER* pmw) {
	uint64_t i;

#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	//pm_filemap_print(pmw->pbuf, pmw->pbuf->deb_file);
	pm_filemap_print(pmw->pbuf, debug_file);
	pm_filemap_close(pmw->pbuf);
#endif

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		os_event_destroy(pmw->pbuf->flush_events[i]); 
	}
	os_event_destroy(pmw->pbuf->free_pool_event);
	free(pmw->pbuf->flush_events);

	//Free the param array
	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		free(pmw->pbuf->params_arr[i]);
	}
	free(pmw->pbuf->params_arr);
#if defined (UNIV_PMEMOBJ_BUF_FLUSHER)
	//Free the flusher
	pm_buf_flusher_close(pmw->pbuf);
#endif 

	fclose(pmw->pbuf->deb_file);
}

int
pm_wrapper_buf_alloc(
		PMEM_WRAPPER*		pmw,
	    const size_t		size,
		const size_t		page_size)
{
	assert(pmw);

	pmw->pbuf = pm_pop_buf_alloc(pmw->pop, size, page_size);
	if (!pmw->pbuf)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;
}
/*
 * Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * */
PMEM_BUF* 
pm_pop_buf_alloc(
		PMEMobjpool*		pop,
		const size_t		size,
		const size_t		page_size)
{
	char* p;
	size_t align_size;

	TOID(PMEM_BUF) buf; 

	POBJ_ZNEW(pop, &buf, PMEM_BUF);

	PMEM_BUF *pbuf = D_RW(buf);
	//align sizes to a pow of 2
	assert(ut_is_2pow(page_size));
	align_size = ut_uint64_align_up(size, page_size);

	pbuf->deb_file = fopen("part_debug.txt","w");

	pbuf->size = align_size;
	pbuf->page_size = page_size;
	pbuf->type = BUF_TYPE;
	pbuf->is_new = true;

	pbuf->is_async_only = false;

	pbuf->data = pm_pop_alloc_bytes(pop, align_size);
	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(pbuf->data));
	assert(p);
	//pbuf->p_align = static_cast<char*> (ut_align(p, page_size));
	pbuf->p_align = static_cast<byte*> (ut_align(p, page_size));
	pmemobj_persist(pop, pbuf->p_align, sizeof(*pbuf->p_align));

	if (OID_IS_NULL(pbuf->data)){
		//assert(0);
		return NULL;
	}

	pm_buf_list_init(pop, pbuf, align_size, page_size);
#if defined( UNIV_PMEMOBJ_BUF_STAT)
	pm_buf_bucket_stat_init(pbuf);
#endif	
#if defined(UNIV_PMEMOBJ_BUF_FLUSHER)
	//init threads for handle flushing, implement in buf0flu.cc
	pm_flusher_init(pbuf, PMEM_N_FLUSH_THREADS);
#endif 
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pm_filemap_init(pbuf);
#endif
	pmemobj_persist(pop, pbuf, sizeof(*pbuf));
	return pbuf;
} 

void 
pm_buf_list_init(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf, 
		const size_t	total_size,
	   	const size_t	page_size)
{
	uint64_t i;
	uint64_t j;
	uint64_t cur_bucket;
	size_t offset;
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* pfreelist;
	PMEM_BUF_BLOCK_LIST* plist;
	page_size_t page_size_obj(page_size, page_size, false);


	size_t n_pages = (total_size / page_size);


	//size_t bucket_size = total_size / (PMEM_N_BUCKETS + n_lists_in_free_pool);
	size_t bucket_size = PMEM_BUCKET_SIZE * page_size;
	//size_t n_pages_per_bucket = bucket_size / page_size;
	size_t n_pages_per_bucket = PMEM_BUCKET_SIZE;
	//size_t n_lists_in_free_pool = static_cast <size_t> (n_pages - PMEM_BUCKET_SIZE * PMEM_N_BUCKETS) / PMEM_BUCKET_SIZE;
	size_t n_lists_in_free_pool = n_pages / PMEM_BUCKET_SIZE - PMEM_N_BUCKETS;


	printf("\n\n=======> PMEM_INFO: n_pages = %zu bucket size = %f MB (%zu %zu-KB pages) n_lists in free_pool %zu\n", n_pages, bucket_size*1.0 / (1024*1024), n_pages_per_bucket, (page_size/1024), n_lists_in_free_pool);

	//Don't reset those variables during the init
	offset = 0;
	cur_bucket = 0;

	//init the temp args struct
	struct list_constr_args* args = (struct list_constr_args*) malloc(sizeof(struct list_constr_args)); 
	args->size.copy_from(page_size_obj);
	args->check = PMEM_AIO_CHECK;
	args->state = PMEM_FREE_BLOCK;
	TOID_ASSIGN(args->list, OID_NULL);

	//The buckets
	//The sub-buf lists
	POBJ_ALLOC(pop, &buf->buckets, TOID(PMEM_BUF_BLOCK_LIST),
			sizeof(TOID(PMEM_BUF_BLOCK_LIST)) * PMEM_N_BUCKETS, NULL, NULL);
	if (TOID_IS_NULL(buf->buckets) ){
		fprintf(stderr, "POBJ_ALLOC\n");
	}

	for(i = 0; i < PMEM_N_BUCKETS; i++) {
		//init the bucket 
		POBJ_ZNEW(pop, &D_RW(buf->buckets)[i], PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(D_RW(buf->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		plist = D_RW(D_RW(buf->buckets)[i]);

		pmemobj_rwlock_wrlock(pop, &plist->lock);

		plist->cur_pages = 0;
		plist->is_flush = false;
		plist->n_aio_pending = 0;
		plist->n_sio_pending = 0;
		plist->max_pages = bucket_size / page_size;
		plist->list_id = cur_bucket;
		plist->hashed_id = cur_bucket;
		//plist->flush_worker_id = PMEM_ID_NONE;
		cur_bucket++;
		plist->hashed_id = i;
		plist->check = PMEM_AIO_CHECK;
		TOID_ASSIGN(plist->next_list, OID_NULL);
		TOID_ASSIGN(plist->prev_list, OID_NULL);
		//plist->pext_list = NULL;
		//TOID_ASSIGN(plist->pext_list, OID_NULL);
	
		//init pages in bucket
		POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
				sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		for (j = 0; j < n_pages_per_bucket; j++) {
			args->pmemaddr = offset;
			TOID_ASSIGN(args->list, (D_RW(buf->buckets)[i]).oid);
			offset += page_size;	

			//POBJ_LIST_INSERT_NEW_HEAD(pop, &plist->head, entries, sizeof(PMEM_BUF_BLOCK), pm_buf_block_init, args); 
			//plist->cur_pages++;
			POBJ_NEW(pop, &D_RW(plist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		}
		//TOID_ASSIGN(plist->next_free_block, POBJ_LIST_FIRST(&plist->head).oid);

		pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		//pmemobj_persist(pop, &plist->pext_list, sizeof(plist->pext_list));
		pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));
		pmemobj_persist(pop, plist, sizeof(*plist));

		pmemobj_rwlock_unlock(pop, &plist->lock);
	}

	//The free pool
	POBJ_ZNEW(pop, &buf->free_pool, PMEM_BUF_FREE_POOL);	
	if(TOID_IS_NULL(buf->free_pool)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pfreepool = D_RW(buf->free_pool);
	pfreepool->max_lists = n_lists_in_free_pool;
	pfreepool->cur_lists = 0;
	
	for(i = 0; i < n_lists_in_free_pool; i++) {
		//init the bucket 
		TOID(PMEM_BUF_BLOCK_LIST) freelist;
		POBJ_ZNEW(pop, &freelist, PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(freelist)) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		pfreelist = D_RW(freelist);

		pmemobj_rwlock_wrlock(pop, &pfreelist->lock);

		pfreelist->cur_pages = 0;
		pfreelist->max_pages = bucket_size / page_size;
		pfreelist->is_flush = false;
		pfreelist->n_aio_pending = 0;
		pfreelist->n_sio_pending = 0;
		pfreelist->list_id = cur_bucket;
		pfreelist->hashed_id = PMEM_ID_NONE;
		//pfreelist->flush_worker_id = PMEM_ID_NONE;
		cur_bucket++;
		pfreelist->check = PMEM_AIO_CHECK;
		TOID_ASSIGN(pfreelist->next_list, OID_NULL);
		TOID_ASSIGN(pfreelist->prev_list, OID_NULL);
	
		//init pages in bucket
		POBJ_ALLOC(pop, &pfreelist->arr, TOID(PMEM_BUF_BLOCK),
				sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		for (j = 0; j < n_pages_per_bucket; j++) {
			args->pmemaddr = offset;
			offset += page_size;	
			TOID_ASSIGN(args->list, freelist.oid);

			//POBJ_LIST_INSERT_NEW_HEAD(pop, &pfreelist->head, entries, sizeof(PMEM_BUF_BLOCK), pm_buf_block_init, args); 
			//pfreelist->cur_pages++;
			POBJ_NEW(pop, &D_RW(pfreelist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		}
		//TOID_ASSIGN(pfreelist->next_free_block, POBJ_LIST_FIRST(&pfreelist->head).oid);

		pmemobj_persist(pop, &pfreelist->cur_pages, sizeof(pfreelist->cur_pages));
		pmemobj_persist(pop, &pfreelist->max_pages, sizeof(pfreelist->max_pages));
		pmemobj_persist(pop, &pfreelist->is_flush, sizeof(pfreelist->is_flush));
		pmemobj_persist(pop, &pfreelist->next_list, sizeof(pfreelist->next_list));
		pmemobj_persist(pop, &pfreelist->prev_list, sizeof(pfreelist->prev_list));
		//pmemobj_persist(pop, &pfreelist->pext_list, sizeof(pfreelist->pext_list));
		pmemobj_persist(pop, &pfreelist->n_aio_pending, sizeof(pfreelist->n_aio_pending));
		pmemobj_persist(pop, &pfreelist->n_sio_pending, sizeof(pfreelist->n_sio_pending));
		pmemobj_persist(pop, &pfreelist->check, sizeof(pfreelist->check));
		pmemobj_persist(pop, &pfreelist->list_id, sizeof(pfreelist->list_id));
		pmemobj_persist(pop, &pfreelist->next_free_block, sizeof(pfreelist->next_free_block));
		pmemobj_persist(pop, pfreelist, sizeof(*plist));

		pmemobj_rwlock_unlock(pop, &pfreelist->lock);

		//insert this list in the freepool
		POBJ_LIST_INSERT_HEAD(pop, &pfreepool->head, freelist, list_entries);
		pfreepool->cur_lists++;
		//loop: init next buckes
	} //end init the freepool
	pmemobj_persist(pop, &buf->free_pool, sizeof(buf->free_pool));

}

/*This function is called as the func pointer in POBJ_LIST_INSERT_NEW_HEAD()*/
int
pm_buf_block_init(
		PMEMobjpool*	pop,
	   	void*			ptr,
	   	void*			arg)
{
	struct list_constr_args *args = (struct list_constr_args *) arg;
	PMEM_BUF_BLOCK* block = (PMEM_BUF_BLOCK*) ptr;
//	block->id = args->id;
	block->id.copy_from(args->id);
//	block->size = args->size;
	block->size.copy_from(args->size);
	block->check = args->check;
	block->state = args->state;
	block->sync = false;
	//block->bpage = args->bpage;
	TOID_ASSIGN(block->list, (args->list).oid);
	block->pmemaddr = args->pmemaddr;

	pmemobj_persist(pop, &block->id, sizeof(block->id));
	pmemobj_persist(pop, &block->size, sizeof(block->size));
	pmemobj_persist(pop, &block->check, sizeof(block->check));
	//pmemobj_persist(pop, &block->bpage, sizeof(block->bpage));
	pmemobj_persist(pop, &block->state, sizeof(block->state));
	pmemobj_persist(pop, &block->list, sizeof(block->list));
	pmemobj_persist(pop, &block->pmemaddr, sizeof(block->pmemaddr));
	return 0;
}

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop) {
	TOID(PMEM_BUF) buf;
	//get the first object in pmem has type PMEM_BUF
	buf = POBJ_FIRST(pop, PMEM_BUF);

	if (TOID_IS_NULL(buf)) {
		return NULL;
	}			
	else {
		PMEM_BUF *pbuf = D_RW(buf);
		if(!pbuf) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return NULL;
		}
		return pbuf;
	}
}

/*
 *Write a page to pmem buffer
 Note that this function may called by concurrency threads
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] src_data	data contains the bytes to write
@param[in] page_size
 * */
int
pm_buf_write(
		PMEMobjpool*	pop,
	   	PMEM_BUF*		buf,
	   	page_id_t		page_id,
	   	page_size_t		size,
	   	byte*			src_data,
	   	bool			sync) 
{

	//bool is_lock_free_block = false;
	//bool is_lock_free_list = false;
	bool is_safe_check = false;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	//Does some checks 
	if (buf->is_async_only)
		assert(!sync);

	assert(buf);
	assert(src_data);
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	assert (pm_check_io(src_data, page_id) );
#endif 
	page_size = size.physical();
	//UNIV_MEM_ASSERT_RW(src_data, page_size);

	//PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif
	//hashed = hash_f1(page_id.space(), 
	//		page_id.page_no(), PMEM_N_BUCKETS, PMEM_PAGE_PER_BUCKET_BITS);

retry:
	//the safe check
	if (is_safe_check){
		if (D_RO(D_RO(buf->buckets)[hashed])->is_flush) {
			os_event_wait(buf->flush_events[hashed]);
			goto retry;
		}
	}
	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);

	//double check
	if (phashlist->is_flush) {
		//When I was blocked (due to mutex) this list is non-flush. When I acquire the lock, it becomes flushing

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

		goto retry;
	}
	//Now the list is non-flush and I have acquired the lock. Let's do my work
	pdata = buf->p_align;
	//(1) search in the hashed list for a first FREE block to write on 
	
	for (i = 0; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);

		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			//if(is_lock_free_block)
			//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) {
			if (pfree_block->id.equals_to(page_id)) {
				//overwrite the old page
				//if(is_lock_free_block)
				//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
				if (pfree_block->sync != sync) {
					if (sync == false) {
						++phashlist->n_aio_pending;
						--phashlist->n_sio_pending;
					}
					else {
						--phashlist->n_aio_pending;
						++phashlist->n_sio_pending;
					}
				}
				pfree_block->sync = sync;
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
#if defined (UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_overwrites;
#endif
				//if(is_lock_free_block)
				//pmemobj_rwlock_unlock(pop, &pfree_block->lock);
				pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, OVERWRITTEN page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	
				return PMEM_SUCCESS;
			}
		}
		//next block
	}

	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
		goto retry;
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, the write page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	

	// (2) At this point, we get the free and un-blocked block, write data to this block

	pfree_block->sync = sync;

	pfree_block->id.copy_from(page_id);
	
	assert(pfree_block->size.equals_to(size));
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);
	
	//phashlist->n_aio_pending = phashlist->cur_pages;
	//we only pending aio when flush list
	if (sync == false)
		++(phashlist->n_aio_pending);		
	else
		++(phashlist->n_sio_pending);		

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {
		//(3) The hashlist is (nearly) full, flush it and assign a free list 
		phashlist->is_flush = true;
		//block upcomming writes into this bucket
		os_event_reset(buf->flush_events[hashed]);
		
		// Crazy test	
		pmemobj_rwlock_unlock(pop, &phashlist->lock);

		//this assert inform that all write is async
		if (buf->is_async_only){ 
			if (phashlist->n_aio_pending != phashlist->cur_pages) {
				printf("!!!! ====> PMEM_ERROR: n_aio_pending=%zu != cur_pages = %zu. They should be equal\n", phashlist->n_aio_pending, phashlist->cur_pages);
				assert (phashlist->n_aio_pending == phashlist->cur_pages);
			}
		}
#if defined (UNIV_PMEMOBJ_BUF_STAT)
		++buf->bucket_stats[hashed].n_flushed_lists;
#endif


		//[flush position 1 ]
		pm_buf_flush_list(pop, buf, phashlist);
		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

get_free_list:
		//Get a free list from the free pool
		pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

		TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
		if (D_RW(buf->free_pool)->cur_lists == 0 ||
				TOID_IS_NULL(first_list) ) {
			pthread_t tid;
		 	tid = pthread_self();
			printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
			pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
			//pmemobj_rwlock_unlock(pop, &phashlist->lock);
			//os_thread_sleep(PMEM_WAIT_FOR_WRITE);
			//os_thread_sleep(PMEM_WAIT_FOR_FREE_LIST);
			os_event_wait(buf->free_pool_event);

			goto get_free_list;
		}

		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;
		//The free_pool may empty now, wait in necessary
		os_event_reset(buf->free_pool_event);
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

		assert(!TOID_IS_NULL(first_list));
		//This ref hashed_id used for batch AIO
		//Hashed id of old list is kept until its batch AIO is completed
		D_RW(first_list)->hashed_id = hashed;
		
		//if(is_lock_free_list)
		//pmemobj_rwlock_wrlock(pop, &D_RW(first_list)->lock);

		//Asign linked-list refs 
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
		TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
		TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);
		

#if defined (UNIV_PMEMOBJ_BUF_STAT)
		if ( !TOID_IS_NULL( D_RW(hash_list)->next_list ))
		++buf->bucket_stats[hashed].max_linked_lists;
#endif 
		//unblock the upcomming writes on this bucket
		os_event_set(buf->flush_events[hashed]);

		//if(is_lock_free_list)
		//pmemobj_rwlock_unlock(pop, &D_RW(first_list)->lock);

		//[flush position 2 ]
		//pm_buf_flush_list(pop, buf, phashlist);
		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}

	return PMEM_SUCCESS;
}

/*
 * VERSION 2
 *Write a page to pmem buffer without using free_pool
 If a list is full, the call thread is blocked until the list completly flush to SSD
 Note that this function may called by concurrency threads
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] src_data	data contains the bytes to write
@param[in] page_size
 * */
int
pm_buf_write_no_free_pool(
		PMEMobjpool*	pop,
	   	PMEM_BUF*		buf,
	   	page_id_t		page_id,
	   	page_size_t		size,
	   	byte*			src_data,
	   	bool			sync) 
{

	//bool is_lock_free_block = false;
	//bool is_lock_free_list = false;
	bool is_safe_check = false;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	//Does some checks 
	if (buf->is_async_only)
		assert(!sync);

	assert(buf);
	assert(src_data);
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	assert (pm_check_io(src_data, page_id) );
#endif 
	page_size = size.physical();
	//UNIV_MEM_ASSERT_RW(src_data, page_size);

	//PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif
//	hashed = hash_f1(page_id.space(),
//			page_id.page_no(), PMEM_N_BUCKETS, PMEM_PAGE_PER_BUCKET_BITS);


retry:
	//the safe check
	if (is_safe_check){
		if (D_RO(D_RO(buf->buckets)[hashed])->is_flush) {
			os_event_wait(buf->flush_events[hashed]);
			goto retry;
		}
	}
	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);

	//double check
	if (phashlist->is_flush) {
		//When I was blocked (due to mutex) this list is non-flush. When I acquire the lock, it becomes flushing

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

		goto retry;
	}
	//Now the list is non-flush and I have acquired the lock. Let's do my work
	pdata = buf->p_align;
	//(1) search in the hashed list for a first FREE block to write on 
	
	for (i = 0; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);

		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			//if(is_lock_free_block)
			//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) {
			if (pfree_block->id.equals_to(page_id)) {
				//overwrite the old page
				//if(is_lock_free_block)
				//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
				if (pfree_block->sync != sync) {
					if (sync == false) {
						++phashlist->n_aio_pending;
						--phashlist->n_sio_pending;
					}
					else {
						--phashlist->n_aio_pending;
						++phashlist->n_sio_pending;
					}
				}
				pfree_block->sync = sync;
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
#if defined (UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_overwrites;
#endif
				//if(is_lock_free_block)
				//pmemobj_rwlock_unlock(pop, &pfree_block->lock);
				pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, OVERWRITTEN page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	
				return PMEM_SUCCESS;
			}
		}
		//next block
	}

	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);
		//os_thread_sleep(PMEM_WAIT_FOR_WRITE);

	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
		goto retry;
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, the write page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	

	// (2) At this point, we get the free and un-blocked block, write data to this block

	//D_RW(free_block)->bpage = bpage;
	pfree_block->sync = sync;

	pfree_block->id.copy_from(page_id);
	
	assert(pfree_block->size.equals_to(size));
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 

	//if(is_lock_free_block)
	//pmemobj_rwlock_unlock(pop, &pfree_block->lock);

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);
	
	//phashlist->n_aio_pending = phashlist->cur_pages;
	//we only pending aio when flush list
	if (sync == false)
		++(phashlist->n_aio_pending);		
	else
		++(phashlist->n_sio_pending);		

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {

		phashlist->is_flush = true;
		phashlist->hashed_id = hashed;

		//block upcomming writes into this bucket
		os_event_reset(buf->flush_events[hashed]);
		
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		pm_buf_flush_list(pop, buf, phashlist);

	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}

	return PMEM_SUCCESS;
}


/*
 * VERSION 3
 * *Write a page to pmem buffer using "instance swap"
 * This function is called by innodb cleaner thread
 * When a list is full, the cleaner thread does:
 * (1) find the free list from free_pool and swap with current full list
 * (2) add current full list to waiting-list of the flusher and notify the flusher about new added list then return (to reduce latency)
 * (3) The flusher looking for a idle worker to handle full list
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] src_data	data contains the bytes to write
@param[in] page_size
 * */
int
pm_buf_write_with_flusher(
			PMEMobjpool*	pop,
		   	PMEM_BUF*		buf,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data,
		   	bool			sync) 
{

	//bool is_lock_free_block = false;
	//bool is_lock_free_list = false;
	bool is_safe_check = false;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	//Does some checks 
	if (buf->is_async_only)
		assert(!sync);

	assert(buf);
	assert(src_data);
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	assert (pm_check_io(src_data, page_id) );
#endif 
	page_size = size.physical();
	//UNIV_MEM_ASSERT_RW(src_data, page_size);

	//PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif
//	hashed = hash_f1(page_id.space(),
//			page_id.page_no(), PMEM_N_BUCKETS, PMEM_PAGE_PER_BUCKET_BITS);

retry:
	//the safe check
	if (is_safe_check){
		if (D_RO(D_RO(buf->buckets)[hashed])->is_flush) {
			os_event_wait(buf->flush_events[hashed]);
			goto retry;
		}
	}
	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);

	//double check
	if (phashlist->is_flush) {
		//When I was blocked (due to mutex) this list is non-flush. When I acquire the lock, it becomes flushing

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

		goto retry;
	}
	//Now the list is non-flush and I have acquired the lock. Let's do my work
	pdata = buf->p_align;
	//(1) search in the hashed list for a first FREE block to write on 
	
	for (i = 0; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);

		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			//if(is_lock_free_block)
			//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) {
			if (pfree_block->id.equals_to(page_id)) {
				//overwrite the old page
				//if(is_lock_free_block)
				//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
				if (pfree_block->sync != sync) {
					if (sync == false) {
						++phashlist->n_aio_pending;
						--phashlist->n_sio_pending;
					}
					else {
						--phashlist->n_aio_pending;
						++phashlist->n_sio_pending;
					}
				}
				pfree_block->sync = sync;
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
#if defined (UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_overwrites;
#endif
				//if(is_lock_free_block)
				//pmemobj_rwlock_unlock(pop, &pfree_block->lock);
				pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
				printf("========   PMEM_DEBUG: in pmem_buf_write, OVERWRITTEN page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	
				return PMEM_SUCCESS;
			}
		}
		//next block
	}

	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);
		//os_thread_sleep(PMEM_WAIT_FOR_WRITE);

	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
		goto retry;
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, the write page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	

	// (2) At this point, we get the free and un-blocked block, write data to this block
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pmemobj_rwlock_wrlock(pop, &buf->filemap->lock);
	pm_filemap_update_items(buf, page_id, hashed, PMEM_BUCKET_SIZE);
	pmemobj_rwlock_unlock(pop, &buf->filemap->lock);
#endif 
	//D_RW(free_block)->bpage = bpage;
	pfree_block->sync = sync;

	pfree_block->id.copy_from(page_id);
	
	assert(pfree_block->size.equals_to(size));
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 

	//if(is_lock_free_block)
	//pmemobj_rwlock_unlock(pop, &pfree_block->lock);

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);
	
	//phashlist->n_aio_pending = phashlist->cur_pages;
	//we only pending aio when flush list
	if (sync == false)
		++(phashlist->n_aio_pending);		
	else
		++(phashlist->n_sio_pending);		

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {
		//(3) The hashlist is (nearly) full, flush it and assign a free list 
		phashlist->hashed_id = hashed;
		phashlist->is_flush = true;
		//block upcomming writes into this bucket
		os_event_reset(buf->flush_events[hashed]);
		
		pmemobj_rwlock_unlock(pop, &phashlist->lock);

		PMEM_FLUSHER* flusher = buf->flusher;
assign_worker:
		mutex_enter(&flusher->mutex);
		//pm_buf_flush_list(pop, buf, phashlist);
		if (flusher->n_requested == flusher->size) {
			//all requested slot is full)
			printf("PMEM_INFO: all reqs are booked, sleep and wait \n");
			mutex_exit(&flusher->mutex);
			os_event_wait(flusher->is_req_full);	
			goto assign_worker;	
		}
			
		//find an idle thread to assign flushing task
		ulint n_try = flusher->size;
		while (n_try > 0) {
			if (flusher->flush_list_arr[flusher->tail] == NULL) {
				//found
				//phashlist->flush_worker_id = PMEM_ID_NONE;
				flusher->flush_list_arr[flusher->tail] = phashlist;
				//printf("before request hashed_id = %d list_id = %zu\n", phashlist->hashed_id, phashlist->list_id);
				++flusher->n_requested;
				//delay calling flush up to a threshold
				//printf("trigger worker...\n");
				if (flusher->n_requested == flusher->size - 2) {
				os_event_set(flusher->is_req_not_empty);
				}

				if (flusher->n_requested >= flusher->size) {
					os_event_reset(flusher->is_req_full);
				}
				//for the next 
				flusher->tail = (flusher->tail + 1) % flusher->size;
				break;
			}
			//circled increase
			flusher->tail = (flusher->tail + 1) % flusher->size;
			n_try--;
		} //end while 

		//check
		if (n_try == 0) {
			/*This imply an logical error 
			 * */
			printf("PMEM_ERROR requested/size = %zu /%zu / %zu\n", flusher->n_requested, flusher->size);
			//mutex_exit(&flusher->mutex);
			//os_event_wait(flusher->is_flush_full);
			//goto assign_worker;
			assert (n_try);
		}

		mutex_exit(&flusher->mutex);
//end FLUSHER handling

		//this assert inform that all write is async
		if (buf->is_async_only){ 
			if (phashlist->n_aio_pending != phashlist->cur_pages) {
				printf("!!!! ====> PMEM_ERROR: n_aio_pending=%zu != cur_pages = %zu. They should be equal\n", phashlist->n_aio_pending, phashlist->cur_pages);
				assert (phashlist->n_aio_pending == phashlist->cur_pages);
			}
		}
#if defined (UNIV_PMEMOBJ_BUF_STAT)
		++buf->bucket_stats[hashed].n_flushed_lists;
#endif

get_free_list:
		//Get a free list from the free pool
		pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

		TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
		if (D_RW(buf->free_pool)->cur_lists == 0 ||
				TOID_IS_NULL(first_list) ) {
			pthread_t tid;
		 	tid = pthread_self();
			printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
			pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
			//pmemobj_rwlock_unlock(pop, &phashlist->lock);
			//os_thread_sleep(PMEM_WAIT_FOR_WRITE);
			//os_thread_sleep(PMEM_WAIT_FOR_FREE_LIST);
			os_event_wait(buf->free_pool_event);

			goto get_free_list;
		}

		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;
		//The free_pool may empty now, wait in necessary
		os_event_reset(buf->free_pool_event);
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

		assert(!TOID_IS_NULL(first_list));
		//This ref hashed_id used for batch AIO
		//Hashed id of old list is kept until its batch AIO is completed
		D_RW(first_list)->hashed_id = hashed;
		
		//if(is_lock_free_list)
		//pmemobj_rwlock_wrlock(pop, &D_RW(first_list)->lock);

		//Asign linked-list refs 
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
		TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
		TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);
		

#if defined (UNIV_PMEMOBJ_BUF_STAT)
		if ( !TOID_IS_NULL( D_RW(hash_list)->next_list ))
		++buf->bucket_stats[hashed].max_linked_lists;
#endif 
		//unblock the upcomming writes on this bucket
		os_event_set(buf->flush_events[hashed]);

		//if(is_lock_free_list)
		//pmemobj_rwlock_unlock(pop, &D_RW(first_list)->lock);
		//pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//[flush position 2 ]
		//pm_buf_flush_list(pop, buf, phashlist);
		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}

	return PMEM_SUCCESS;
}


/////////////////////////// pm_buf_flush_list versions///
/*  VERSION 0 (BATCH)
 * Async write pages from the list to datafile
 * The caller thread need to lock/unlock the plist
 * See buf_dblwr_write_block_to_datafile
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] plist	pointer to the list that will be flushed 
 * */
void
pm_buf_flush_list(
		PMEMobjpool*			pop,
	   	PMEM_BUF*				buf,
	   	PMEM_BUF_BLOCK_LIST*	plist) {

	assert(pop);
	assert(buf);

#if defined (UNIV_PMEMOBJ_BUF)
		ulint type = IORequest::PM_WRITE;
#else
		ulint type = IORequest::WRITE;
#endif
		IORequest request(type);

		dberr_t err = pm_fil_io_batch(request, pop, buf, plist);
		
}
/*  VERSION 1 (DIRECTLY CALL)
 * Async write pages from the list to datafile
 * The caller thread need to lock/unlock the plist
 * See buf_dblwr_write_block_to_datafile
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] plist	pointer to the list that will be flushed 
 * */
void
pm_buf_flush_list_v1(
		PMEMobjpool*			pop,
	   	PMEM_BUF*				buf,
	   	PMEM_BUF_BLOCK_LIST*	plist) 
{

	ulint i;
	//ulint count;

	TOID(PMEM_BUF_BLOCK) flush_block;
	PMEM_BUF_BLOCK* pblock;
	//PMEM_BUF_BLOCK_LIST* plist;

	//char* pdata;
	byte* pdata;
	assert(pop);
	assert(buf);

	bool is_lock_list = false;
	bool is_lock_block = false;
	
	pdata = buf->p_align;
	
	if(is_lock_list)
		pmemobj_rwlock_wrlock(pop, &plist->lock);
	//count = 0;
	plist->n_flush = 0;

	for (i = 0; i < plist->max_pages; ++i) {
		pblock = D_RW(D_RW(plist->arr)[i]);
		//Becareful with this assert
		//assert (pblock->state == PMEM_IN_USED_BLOCK);
		
		if(is_lock_block)
			pmemobj_rwlock_wrlock(pop, &pblock->lock);

		if (pblock->state == PMEM_FREE_BLOCK ||
				pblock->state == PMEM_IN_FLUSH_BLOCK) {
			//printf("!!!!!PMEM_WARNING: in list %zu don't write a FREE BLOCK, skip to next block\n", plist->list_id);
			continue;
		}
		//plist->n_flush++;
		
		assert( pblock->pmemaddr < buf->size);
		UNIV_MEM_ASSERT_RW(pdata + pblock->pmemaddr, page_size);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	//	printf("PMEM_DEBUG: aio request page_id %zu space %zu pmemaddr %zu flush_list id=%zu\n", pblock->id.page_no(), pblock->id.space(), pblock->pmemaddr, plist->list_id);
#endif
		assert(pblock->state == PMEM_IN_USED_BLOCK);	
		pblock->state = PMEM_IN_FLUSH_BLOCK;	

		//save the reference
#if defined (UNIV_PMEMOBJ_BUF)
		ulint type = IORequest::PM_WRITE;
#else
		ulint type = IORequest::WRITE;
#endif
		//count++;
		++plist->n_flush;

		IORequest request(type);

		dberr_t err = fil_io(request, 
			false, pblock->id, pblock->size, 0, pblock->size.physical(),
			pdata + pblock->pmemaddr, D_RW(D_RW(plist->arr)[i]));

		
		if(is_lock_block)
			pmemobj_rwlock_unlock(pop, &pblock->lock);

		if (err != DB_SUCCESS){
			printf("PMEM_ERROR: fil_io() in pm_buf_list_write_to_datafile() space_id = %"PRIu32" page_id = %"PRIu32" size = %zu \n ", pblock->id.space(), pblock->id.page_no(), pblock->size.physical());
			assert(0);
		}
	}

	//printf("request flush list id %zu: n_flush =%zu, n_aio_pending = %zu, n_sio_pending = %zu cur_pages=%zu\n", plist->list_id, plist->n_flush, plist->n_aio_pending, plist->n_sio_pending, plist->cur_pages);

	if (buf->is_async_only) {
		if (plist->n_flush != plist->cur_pages || plist->n_flush != plist->n_aio_pending || plist->n_aio_pending != plist->cur_pages) {
			printf("list_id: %zu n_flush =%zu, n_aio_pending = %zu, cur_pages=%zu\n", plist->list_id,  plist->n_flush, plist->n_aio_pending, plist->cur_pages);
			assert(0);
		}
	}
	else {
		if (plist->n_flush != plist->cur_pages ||
				plist->n_aio_pending + plist->n_sio_pending != plist->cur_pages) {
			printf("error: list_id %zu n_flush =%zu, n_aio_pending = %zu, n_sio_pending = %zu cur_pages=%zu\n", plist->list_id, plist->n_flush, plist->n_aio_pending, plist->n_sio_pending, plist->cur_pages);
			assert(0);
		}
	}

	if(is_lock_list)
		pmemobj_rwlock_unlock(pop, &plist->lock);
}


/*
 *This function is called from aio complete (fil_aio_wait)
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	PMEM_BUF_BLOCK*		pblock)
{

	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);
	
	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	//printf("PMEM_DEBUG aio FINISHED slot_id = %zu n_page_requested = %zu flush_list id = %zu n_pending = %zu/%zu page_id %zu space_id %zu \n",
	//if (pflush_list->n_aio_pending == 0) {
	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
		pm_buf_flush_spaces_in_list(pop, buf, pflush_list);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->is_flush = false;
		pflush_list->hashed_id = PMEM_ID_NONE;
		
		// (2) Remove this list from the doubled-linked list
		
		//assert( !TOID_IS_NULL(pflush_list->prev_list) );
		if( !TOID_IS_NULL(pflush_list->prev_list) ) {

			//if (is_lock_prev_list)
			//pmemobj_rwlock_wrlock(pop, &D_RW(pflush_list->prev_list)->lock);
			TOID_ASSIGN( D_RW(pflush_list->prev_list)->next_list, pflush_list->next_list.oid);
			//if (is_lock_prev_list)
			//pmemobj_rwlock_unlock(pop, &D_RW(pflush_list->prev_list)->lock);
		}

		if (!TOID_IS_NULL(pflush_list->next_list) ) {
			//pmemobj_rwlock_wrlock(pop, &D_RW(pflush_list->next_list)->lock);

			TOID_ASSIGN(D_RW(pflush_list->next_list)->prev_list, pflush_list->prev_list.oid);

			//pmemobj_rwlock_unlock(pop, &D_RW(pflush_list->next_list)->lock);
		}
		
		TOID_ASSIGN(pflush_list->next_list, OID_NULL);
		TOID_ASSIGN(pflush_list->prev_list, OID_NULL);

		// (3) we return this list to the free_pool
		PMEM_BUF_FREE_POOL* pfree_pool;
		pfree_pool = D_RW(buf->free_pool);

		//printf("PMEM_DEBUG: in fil_aio_wait(), try to lock free_pool list id: %zd, cur_lists in free_pool= %zd \n", pflush_list->list_id, pfree_pool->cur_lists);
		pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

		POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, flush_list, list_entries);
		pfree_pool->cur_lists++;
		//wakeup who is waitting for free_pool available
		os_event_set(buf->free_pool_event);

		pmemobj_rwlock_unlock(pop, &pfree_pool->lock);

	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}

/*
 *This function is called from aio complete (fil_aio_wait)
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block_no_free_pool(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	PMEM_BUF_BLOCK*		pblock)
{

	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);
	
	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	//printf("PMEM_DEBUG aio FINISHED slot_id = %zu n_page_requested = %zu flush_list id = %zu n_pending = %zu/%zu page_id %zu space_id %zu \n",
	//if (pflush_list->n_aio_pending == 0) {
	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
		pm_buf_flush_spaces_in_list(pop, buf, pflush_list);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->is_flush = false;

		os_event_set(buf->flush_events[pflush_list->hashed_id]);

		//pflush_list->hashed_id = PMEM_ID_NONE;

	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}
/*
 *	Read a page from pmem buffer using the page_id as key
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *  @param[in] page_id	read key
 *  @param[out] data	read data if the page_id exist in the buffer
 *  @return:  size of read page
 * */
const PMEM_BUF_BLOCK* 
pm_buf_read(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	const page_id_t		page_id,
	   	const page_size_t	size,
	   	byte*				data, 
		bool				sync) 
{
	
	bool is_lock_on_read = true;	
	ulint hashed;
	ulint i;

#if defined(UNIV_PMEMOBJ_BUF_STAT)
	ulint cur_level = 0;
#endif
	//int found;

	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	//PMEM_BUF_BLOCK_LIST* plist;
	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	//char* pdata;
	byte* pdata;
	//size_t bytes_read;
	
	if (buf == NULL){
		printf("PMEM_ERROR, param buf is null in pm_buf_read\n");
		assert(0);
	}

	if (data == NULL){
		printf("PMEM_ERROR, param data is null in pm_buf_read\n");
		assert(0);
	}
	//bytes_read = 0;

	//PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif
//	hashed = hash_f1(page_id.space(),
//			page_id.page_no(), PMEM_N_BUCKETS, PMEM_PAGE_PER_BUCKET_BITS);
	TOID_ASSIGN(cur_list, (D_RO(buf->buckets)[hashed]).oid);
	if ( TOID_IS_NULL(cur_list)) {
		//assert(!TOID_IS_NULL(cur_list));
		printf("PMEM_ERROR error in get hashded list, but return NULL, check again! \n");
		return NULL;
	}
	//plist = D_RO(D_RO(buf->buckets)[hashed]);
	//assert(plist);

	//pblock = NULL;
	//found = -1;
	
	while ( !TOID_IS_NULL(cur_list) ) {
		//plist = D_RW(cur_list);
		//pmemobj_rwlock_rdlock(pop, &plist->lock);
		//Scan in this list
		//for (i = 0; i < plist->max_pages; i++) {
		for (i = 0; i < D_RO(cur_list)->max_pages; i++) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK
			//if ( D_RO(D_RO(plist->arr)[i])->state != PMEM_FREE_BLOCK &&
			if ( D_RO(D_RO(D_RO(cur_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					//D_RO(D_RO(plist->arr)[i])->id.equals_to(page_id)) {
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.equals_to(page_id)) {
				pblock = D_RW(D_RW(D_RW(cur_list)->arr)[i]);
				if(is_lock_on_read)
				pmemobj_rwlock_rdlock(pop, &pblock->lock);
				
				//printf("====> PMEM_DEBUG found page_id[space %zu,page_no %zu] pmemaddr=%zu\n ", D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.space(), D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.page_no(), D_RO(D_RO(D_RO(cur_list)->arr)[i])->pmemaddr);
				//if (!D_RO(D_RO(D_RO(cur_list)->arr)[i])->size.equals_to(size)) {
				if (!pblock->size.equals_to(size)) {
					printf("PMEM_ERROR size not equal!!!\n");
					assert(0);
				}
				//found = i;
				pdata = buf->p_align;

				UNIV_MEM_ASSERT_RW(data, pblock->size.physical());
				//pmemobj_memcpy_persist(pop, data, pdata + pblock->pmemaddr, pblock->size.physical()); 
				memcpy(data, pdata + pblock->pmemaddr, pblock->size.physical()); 
				//bytes_read = pblock->size.physical();
#if defined (UNIV_PMEMOBJ_DEBUG)
				assert( pm_check_io(pdata + pblock->pmemaddr, pblock->id) ) ;
#endif
#if defined(UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_reads;
				if (D_RO(cur_list)->is_flush)
					++buf->bucket_stats[hashed].n_reads_flushing;
#endif
				if(is_lock_on_read)
				pmemobj_rwlock_unlock(pop, &pblock->lock);

				//return bytes_read;
				return pblock;
			}
		}//end for

		//next list
		if ( TOID_IS_NULL(D_RO(cur_list)->next_list))
			break;
		TOID_ASSIGN(cur_list, (D_RO(cur_list)->next_list).oid);
#if defined(UNIV_PMEMOBJ_BUF_STAT)
		cur_level++;
		if (buf->bucket_stats[hashed].max_linked_lists < cur_level)
			buf->bucket_stats[hashed].max_linked_lists = cur_level;
#endif
		
	} //end while
	
	//if (found < 0) {
		//return 0;
		return NULL;
//	}
}

///////////////////////// PARTITION ///////////////////////////
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
void 
pm_filemap_init(
		PMEM_BUF*		buf){
	
	ulint i;
	PMEM_FILE_MAP* fm;

	fm = static_cast<PMEM_FILE_MAP*> (malloc(sizeof(PMEM_FILE_MAP)));
	fm->max_size = 1024*1024;
	fm->items = static_cast<PMEM_FILE_MAP_ITEM**> (
		calloc(fm->max_size, sizeof(PMEM_FILE_MAP_ITEM*)));	
	//for (i = 0; i < fm->max_size; i++) {
	//	fm->items[i].count = 0
	//	fm->item[i].hashed_ids = static_cast<int*> (
	//			calloc(PMEM_BUCKET_SIZE, sizeof(int)));
	//}

	fm->size = 0;

	buf->filemap = fm;
}


#endif //UNIV_PMEMOBJ_PARTITION

//						END OF PARTITION//////////////////////


//////////////////////// STATISTICS FUNCTS/////////////////////

#if defined (UNIV_PMEMOBJ_BUF_STAT)

#define PMEM_BUF_BUCKET_STAT_PRINT(pb, index) do {\
	assert (0 <= index && index <= PMEM_N_BUCKETS);\
	PMEM_BUCKET_STAT* p = &pb->bucket_stats[index];\
	printf("bucket %d [n_writes %zu,\t n_overwrites %zu,\t n_reads %zu, n_reads_flushing %zu \tmax_linked_lists %zu, \tn_flushed_lists %zu] \n ",index,  p->n_writes, p->n_overwrites, p->n_reads, p->n_reads_flushing, p->max_linked_lists, p->n_flushed_lists); \
}while (0)


void pm_buf_bucket_stat_init(PMEM_BUF* pbuf) {
	int i;

	PMEM_BUCKET_STAT* arr = 
		(PMEM_BUCKET_STAT*) calloc(PMEM_N_BUCKETS, sizeof(PMEM_BUCKET_STAT));
	
	for (i = 0; i < PMEM_N_BUCKETS; i++) {
		arr[i].n_writes = arr[i].n_overwrites = 
			arr[i].n_reads = arr[i].n_reads_flushing = arr[i].max_linked_lists =
			arr[i].n_flushed_lists = 0;
	}
	pbuf->bucket_stats = arr;
}

void pm_buf_stat_print_all(PMEM_BUF* pbuf) {
	ulint i;
	PMEM_BUCKET_STAT* arr = pbuf->bucket_stats;

	for (i = 0; i < PMEM_N_BUCKETS; i++) {
		PMEM_BUF_BUCKET_STAT_PRINT(pbuf, i);
	}
}

#endif //UNIV_PMEMOBJ_STAT
///////////////////// DEBUG Funcitons /////////////////////////

/*
 *Read the header from frame that contains page_no and space. Then check whether they matched with the page_id.page_no() and page_id.space()
 * */
bool pm_check_io(byte* frame, page_id_t page_id) {
	//Does some checking
	ulint   read_page_no;
	ulint   read_space_id;

	read_page_no = mach_read_from_4(frame + FIL_PAGE_OFFSET);
	read_space_id = mach_read_from_4(frame + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
	if ((page_id.space() != 0
				&& page_id.space() != read_space_id)
			|| page_id.page_no() != read_page_no) {                                      
		/* We did not compare space_id to read_space_id
		 *             if bpage->space == 0, because the field on the
		 *                         page may contain garbage in MySQL < 4.1.1,                                        
		 *                                     which only supported bpage->space == 0. */

		ib::error() << "PMEM_ERROR: Space id and page no stored in "
			"the page, read in are "
			<< page_id_t(read_space_id, read_page_no)
			<< ", should be " << page_id;
		return 0;
	}   
	return 1;
}


void pm_buf_print_lists_info(PMEM_BUF* buf){
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* plist;
	uint64_t i;

	printf("PMEM_DEBUG ==================\n");

	pfreepool = D_RW(buf->free_pool);
	printf("The free pool: curlists=%zd \n", pfreepool->cur_lists);

	printf("The buckets: \n");
	
	for (i = 0; i < PMEM_N_BUCKETS; i++){

		plist = D_RW( D_RW(buf->buckets)[i] );
		printf("\tBucket %zu: list_id=%zu cur_pages=%zd ", i, plist->list_id, plist->cur_pages);
		printf("\n");
	}		
}
//////////////////////// THREAD HANDLER /////////////
#endif //UNIV_PMEMOBJ_BUF
