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
// 1 < this_value < flusher->size
static uint64_t PMEM_FLUSHER_WAKE_THRESHOLD=5;

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

/*
  There are two types of structure need to be allocated: structures in NVM that non-volatile after shutdown server or power-off and D-RAM structures that only need when the server is running. 
 * Case 1: First time the server start (fresh server): allocate structures in NVM
 * Case 2: server've had some data, NVM structures already existed. Just get them
 *
 * For both cases, we need to allocate structures in D-RAM
 * */
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
	/////////////////////////////////////////////////
	// PART 1: NVM structures
	// ///////////////////////////////////////////////
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
	////////////////////////////////////////////////////
	// Part 2: D-RAM structures and open file(s)
	// ///////////////////////////////////////////////////
	
#if defined( UNIV_PMEMOBJ_BUF_STAT)
	pm_buf_bucket_stat_init(pmw->pbuf);
#endif	
#if defined(UNIV_PMEMOBJ_BUF_FLUSHER)
	//init threads for handle flushing, implement in buf0flu.cc
	pm_flusher_init(pmw->pbuf, PMEM_N_FLUSH_THREADS);
#endif 
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pm_filemap_init(pmw->pbuf);
#endif

	//In any case (new allocation or resued, we should allocate the flush_events for buckets in DRAM
	pmw->pbuf->flush_events = (os_event_t*) calloc(PMEM_N_BUCKETS, sizeof(os_event_t));

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		sprintf(sbuf,"pm_flush_bucket%zu", i);
		pmw->pbuf->flush_events[i] = os_event_create(sbuf);
	}
		pmw->pbuf->free_pool_event = os_event_create("pm_free_pool_event");


	/*Alocate the param array
	 * Size of param array list should at least equal to PMEM_N_BUCKETS
	 * (i.e. one bucket has at least one param array)
	 * In case of one bucket receive heavily write such that the list
	 * is full while the previous list still not finish aio_batch
	 * In this situation, the params of the current list may overwrite the
	 * params of the previous list. We may encounter this situation with SINGLE_BUCKET partition
	 * where one space only map to one bucket, hence all writes are focus on one bucket
	 */
	PMEM_BUF_BLOCK_LIST* plist;
	ulint arr_size = 2 * PMEM_N_BUCKETS;

	pmw->pbuf->param_arr_size = arr_size;
	pmw->pbuf->param_arrs = static_cast<PMEM_AIO_PARAM_ARRAY*> (
		calloc(arr_size, sizeof(PMEM_AIO_PARAM_ARRAY)));	
	for ( i = 0; i < arr_size; i++) {
		//plist = D_RW(D_RW(pmw->pbuf->buckets)[i]);

		pmw->pbuf->param_arrs[i].params = static_cast<PMEM_AIO_PARAM*> (
		//pmw->pbuf->param_arrs[i] = static_cast<PMEM_AIO_PARAM*> (
				//calloc(plist->max_pages, sizeof(PMEM_AIO_PARAM)));
				calloc(PMEM_BUCKET_SIZE, sizeof(PMEM_AIO_PARAM)));
		pmw->pbuf->param_arrs[i].is_free = true;
	}
	pmw->pbuf->cur_free_param = 0; //start with the 0
	
	//Open file 
	pmw->pbuf->deb_file = fopen("pmem_debug.txt","w");
	
//	////test for recovery
//	printf("========== > Test for recovery\n");
//	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
//	PMEM_BUF_BLOCK_LIST* pcurlist;
//
//	printf("The bucket ====\n");
//	for (i = 0; i < PMEM_N_BUCKETS; i++) {
//		TOID_ASSIGN(cur_list, (D_RW(pmw->pbuf->buckets)[i]).oid);
//		plist = D_RW(cur_list);
//		printf("list %zu is_flush %d cur_pages %zu max_pages %zu\n",plist->list_id, plist->is_flush, plist->cur_pages, plist->max_pages );
//		
//		TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
//		printf("\t[next list \n");	
//		while( !TOID_IS_NULL(cur_list)) {
//			plist = D_RW(cur_list);
//			printf("\t\t next list %zu is_flush %d cur_pages %zu max_pages %zu\n",plist->list_id, plist->is_flush, plist->cur_pages, plist->max_pages );
//			TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
//		}
//		printf("\t end next list] \n");	
//
//		//print the linked-list 
//
//	}
//	printf("The free pool ====\n");
//	PMEM_BUF_FREE_POOL* pfree_pool = D_RW(pmw->pbuf->free_pool);	
//	printf("cur_lists = %zu max_lists=%zu\n",
//			pfree_pool->cur_lists, pfree_pool->max_lists);

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
		//free(pmw->pbuf->params_arr[i]);
		free(pmw->pbuf->param_arrs[i].params);
	}
	//free(pmw->pbuf->params_arr);
	free(pmw->pbuf->param_arrs);
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
 * This function only allocate structures that in NVM
 * Structures in D-RAM are allocated outside in pm_wrapper_buf_alloc_or_open() function
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

	pm_buf_lists_init(pop, pbuf, align_size, page_size);
	pmemobj_persist(pop, pbuf, sizeof(*pbuf));
	return pbuf;
} 

/*
 * Init in-PMEM lists
 * bucket lists
 * free pool lists
 * and the special list
 * */
void 
pm_buf_lists_init(
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
	PMEM_BUF_BLOCK_LIST* pspeclist;
	page_size_t page_size_obj(page_size, page_size, false);


	size_t n_pages = (total_size / page_size);


	size_t bucket_size = PMEM_BUCKET_SIZE * page_size;
	size_t n_pages_per_bucket = PMEM_BUCKET_SIZE;

	//we need one more list for the special list
	size_t n_lists_in_free_pool = n_pages / PMEM_BUCKET_SIZE - PMEM_N_BUCKETS - 1;

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

	//(1) Init the buckets
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

		//pmemobj_rwlock_wrlock(pop, &plist->lock);

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
		pm_buf_single_list_init(pop, D_RW(buf->buckets)[i], offset, args, n_pages_per_bucket, page_size);

		//POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
		//		sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		//for (j = 0; j < n_pages_per_bucket; j++) {
		//	args->pmemaddr = offset;
		//	TOID_ASSIGN(args->list, (D_RW(buf->buckets)[i]).oid);
		//	offset += page_size;	
		//	POBJ_NEW(pop, &D_RW(plist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		//}

		//pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		//pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		//pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		//pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		//pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		//pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		//pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		//pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		//pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		//pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));
		//pmemobj_persist(pop, plist, sizeof(*plist));

		//pmemobj_rwlock_unlock(pop, &plist->lock);

		// next bucket
	}

	//(2) Init the free pool
	POBJ_ZNEW(pop, &buf->free_pool, PMEM_BUF_FREE_POOL);	
	if(TOID_IS_NULL(buf->free_pool)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pfreepool = D_RW(buf->free_pool);
	pfreepool->max_lists = n_lists_in_free_pool;
	pfreepool->cur_lists = 0;
	
	for(i = 0; i < n_lists_in_free_pool; i++) {
		TOID(PMEM_BUF_BLOCK_LIST) freelist;
		POBJ_ZNEW(pop, &freelist, PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(freelist)) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		pfreelist = D_RW(freelist);

		//pmemobj_rwlock_wrlock(pop, &pfreelist->lock);

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
		pm_buf_single_list_init(pop, freelist, offset, args, n_pages_per_bucket, page_size);

		//POBJ_ALLOC(pop, &pfreelist->arr, TOID(PMEM_BUF_BLOCK),
		//		sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		//for (j = 0; j < n_pages_per_bucket; j++) {
		//	args->pmemaddr = offset;
		//	offset += page_size;	
		//	TOID_ASSIGN(args->list, freelist.oid);

		//	POBJ_NEW(pop, &D_RW(pfreelist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		//}

		//pmemobj_persist(pop, &pfreelist->cur_pages, sizeof(pfreelist->cur_pages));
		//pmemobj_persist(pop, &pfreelist->max_pages, sizeof(pfreelist->max_pages));
		//pmemobj_persist(pop, &pfreelist->is_flush, sizeof(pfreelist->is_flush));
		//pmemobj_persist(pop, &pfreelist->next_list, sizeof(pfreelist->next_list));
		//pmemobj_persist(pop, &pfreelist->prev_list, sizeof(pfreelist->prev_list));
		//pmemobj_persist(pop, &pfreelist->n_aio_pending, sizeof(pfreelist->n_aio_pending));
		//pmemobj_persist(pop, &pfreelist->n_sio_pending, sizeof(pfreelist->n_sio_pending));
		//pmemobj_persist(pop, &pfreelist->check, sizeof(pfreelist->check));
		//pmemobj_persist(pop, &pfreelist->list_id, sizeof(pfreelist->list_id));
		//pmemobj_persist(pop, &pfreelist->next_free_block, sizeof(pfreelist->next_free_block));
		//pmemobj_persist(pop, pfreelist, sizeof(*plist));

		//pmemobj_rwlock_unlock(pop, &pfreelist->lock);

		//insert this list in the freepool
		POBJ_LIST_INSERT_HEAD(pop, &pfreepool->head, freelist, list_entries);
		pfreepool->cur_lists++;
		//loop: init next buckes
	} //end init the freepool
	pmemobj_persist(pop, &buf->free_pool, sizeof(buf->free_pool));

	// (3) Init the special list used in recovery
	POBJ_ZNEW(pop, &buf->spec_list, PMEM_BUF_BLOCK_LIST);	
	if(TOID_IS_NULL(buf->spec_list)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pspeclist = D_RW(buf->spec_list);
	pspeclist->cur_pages = 0;
	pspeclist->max_pages = bucket_size / page_size;
	pspeclist->is_flush = false;
	pspeclist->n_aio_pending = 0;
	pspeclist->n_sio_pending = 0;
	pspeclist->list_id = cur_bucket;
	pspeclist->hashed_id = PMEM_ID_NONE;
	//pfreelist->flush_worker_id = PMEM_ID_NONE;
	cur_bucket++;
	pspeclist->check = PMEM_AIO_CHECK;
	TOID_ASSIGN(pspeclist->next_list, OID_NULL);
	TOID_ASSIGN(pspeclist->prev_list, OID_NULL);
	//init pages in spec list 
	pm_buf_single_list_init(pop, buf->spec_list, offset, args, n_pages_per_bucket, page_size);
	
}

/*
 * Allocate and init blocks in a PMEM_BUF_BLOCK_LIST
 * THis function is called in pm_buf_lists_init()
 * pop [in]: pmemobject pool
 * plist [in/out]: pointer to the list
 * offset [in/out]: current offset, this offset will increase during the function run
 * n [in]: number of pages 
 * args [in]: temp struct to hold info
 * */
void 
pm_buf_single_list_init(
		PMEMobjpool*				pop,
		TOID(PMEM_BUF_BLOCK_LIST)	inlist,
		size_t&						offset,
		struct list_constr_args*	args,
		const size_t				n,
		const size_t				page_size){
		
		ulint i;

		PMEM_BUF_BLOCK_LIST*	plist;
		plist = D_RW(inlist);

		POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
				sizeof(TOID(PMEM_BUF_BLOCK)) * n, NULL, NULL);

		for (i = 0; i < n; i++) {
			args->pmemaddr = offset;
			offset += page_size;	

			TOID_ASSIGN(args->list, inlist.oid);
			POBJ_NEW(pop, &D_RW(plist->arr)[i], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		}
		// Make properties in the list persist
		pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));

		pmemobj_persist(pop, plist, sizeof(*plist));
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
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY)
//page 0 is put in the special list
	if (page_id.page_no() == 0) {
		PMEM_BUF_BLOCK_LIST* pspec_list;
		PMEM_BUF_BLOCK*		pspec_block;
		fil_node_t*			node;

		node = pm_get_node_from_space(page_id.space());
		if (node == NULL) {
			printf("PMEM_ERROR node from space is NULL\n");
			assert(0);
		}

		pspec_list = D_RW(buf->spec_list); 

		pmemobj_rwlock_wrlock(pop, &pspec_list->lock);

		pdata = buf->p_align;
		//scan in the special list
		for (i = 0; i < pspec_list->cur_pages; i++){
			pspec_block = D_RW(D_RW(pspec_list->arr)[i]);

			if (pspec_block->state == PMEM_FREE_BLOCK){
				break;
			}	
			else if (pspec_block->state == PMEM_IN_USED_BLOCK) {
				if (pspec_block->id.equals_to(page_id) ||
						strstr(pspec_block->file_name, node->name) != 0) {
					//overwrite this spec block
					pspec_block->sync = sync;
					pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
					//update the file_name, page_id in case of tmp space
					strcpy(pspec_block->file_name, node->name);
					pspec_block->id.copy_from(page_id);

					pmemobj_rwlock_unlock(pop, &pspec_list->lock);

					return PMEM_SUCCESS;
				}
				//else: just skip this block
			}
			//next block
		}//end for
		
		if (i < pspec_list->cur_pages) {
			printf("PMEM_BUF Logical error when handle the special list\n");
			assert(0);
		}
		if (i == pspec_list->cur_pages) {
			pspec_block = D_RW(D_RW(pspec_list->arr)[i]);
			//add new block to the spec list
			pspec_block->sync = sync;

			pspec_block->id.copy_from(page_id);
			pspec_block->state = PMEM_IN_USED_BLOCK;
			//get file handle
			//[Note] is it safe without acquire fil_system->mutex?
			node = pm_get_node_from_space(page_id.space());
			if (node == NULL) {
				printf("PMEM_ERROR node from space is NULL\n");
				assert(0);
			}

			//printf ("PMEM_INFO add file %s fd %d\n", node->name, node->handle.m_file);
			//pspec_block->file_handle = node->handle;
			strcpy(pspec_block->file_name, node->name);

			pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
			++(pspec_list->cur_pages);

			printf("Add new block to the spec list, space_no %zu,file %s cur_pages %zu \n", page_id.space(),node->name,  pspec_list->cur_pages);

			//We do not handle flushing the spec list here
			if (pspec_list->cur_pages >= pspec_list->max_pages * PMEM_BUF_FLUSH_PCT) {
				printf("We do not handle flushing spec list in this version, adjust the input params to get larger size of spec list\n");
				assert(0);
			}
		}
		pmemobj_rwlock_unlock(pop, &pspec_list->lock);
		return PMEM_SUCCESS;
	} // end if page_no == 0
#endif //UNIV_PMEMOBJ_BUF_RECOVERY

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif

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

/* NOTE for recovery 
 *
 * Don't call pm_buf_handle_full_hashed_list before  innodb apply log records that re load spaces (MLOG_FILE_NAME)
 * Hence, we call pm_buf_handle_full_hashed_list only inside pm_buf_write and after the 
 * recv_recovery_from_checkpoint_finish() (through pm_buf_resume_flushing)
 * */
		if (buf->is_recovery	&&
			(phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT)) {
			pmemobj_rwlock_unlock(pop, &phashlist->lock);
			//printf("PMEM_INFO: call pm_buf_handle_full_hashed_list during recovering, hashed = %zu list_id = %zu, cur_pages= %zu, is_flush = %d...\n", hashed, phashlist->list_id, phashlist->cur_pages, phashlist->is_flush);
			pm_buf_handle_full_hashed_list(pop, buf, hashed);
			goto retry;
		}
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n wait for list %zu cur_pages = %zu max_pages= %zu flushing....",
		phashlist->list_id, phashlist->cur_pages, phashlist->max_pages);
#endif 
		
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
	//This code is test for recovery, it has lock/unlock mutex
	//If the performance reduce, then remove it
	fil_node_t*			node;

	node = pm_get_node_from_space(page_id.space());
	if (node == NULL) {
		printf("PMEM_ERROR node from space is NULL\n");
		assert(0);
	}
	strcpy(pfree_block->file_name, node->name);
	//end code
	
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
		
#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_flushed_lists;
#endif 

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		pm_buf_handle_full_hashed_list(pop, buf, hashed);

		//unblock the upcomming writes on this bucket
		os_event_set(buf->flush_events[hashed]);
	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}

	return PMEM_SUCCESS;
}

/*
 * VERSION 4
 * Similar with pm_buf_write_with_flusher but write in append mode
 * We handle remove old versions in flusher thread
 * Read to PMEM_BUF need to scan from the tail to the head in order to get
 * the lastest version
 * */
int
pm_buf_write_with_flusher_append(
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
	//(1) append only 
	for (i = phashlist->cur_pages; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);	
		if (pfree_block->state == PMEM_FREE_BLOCK) {
			break;	
		}
		else{
			printf("PMEM_ERROR: error in append mode\n");
			assert(0);
		}
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
	
	//bool is_lock_on_read = true;	
	ulint hashed;
	int i;

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
	
	assert(buf);
	assert(data);	

	//if (buf == NULL){
	//	printf("PMEM_ERROR, param buf is null in pm_buf_read\n");
	//	assert(0);
	//}

	//if (data == NULL){
	//	printf("PMEM_ERROR, param data is null in pm_buf_read\n");
	//	assert(0);
	//}

/*handle page 0
// Note that there are two case for reading a page 0
// Case 1: read through buffer pool (handle in this function, during the server working time)
// Case 2: read without buffer pool during the sever stat/stop 
*/
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY)
	if (page_id.page_no() == 0) {
		//PMEM_BUF_BLOCK_LIST* pspec_list;
		PMEM_BUF_BLOCK*		pspec_block;

		const PMEM_BUF_BLOCK_LIST* pspec_list = D_RO(buf->spec_list); 
		//pmemobj_rwlock_rdlock(pop, &pspec_list->lock);
		//scan in the special list
		for (i = 0; i < pspec_list->cur_pages; i++){
			//const PMEM_BUF_BLOCK* pspec_block = D_RO(D_RO(pspec_list->arr)[i]);
			if (	D_RO(D_RO(D_RO(buf->spec_list)->arr)[i]) != NULL && 
					D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->id.equals_to(page_id)) {
				pspec_block = D_RW(D_RW(D_RW(buf->spec_list)->arr)[i]);
				//if(is_lock_on_read)
				pmemobj_rwlock_rdlock(pop, &pspec_block->lock);
			//if (pspec_block != NULL &&
			//		pspec_block->state != PMEM_FREE_BLOCK &&
			//		pspec_block->id.equals_to(page_id)) {
				//found
				pdata = buf->p_align;
				memcpy(data, pdata + pspec_block->pmemaddr, pspec_block->size.physical()); 

				//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
				printf("==> PMEM_DEBUG read page 0 (case 1) of space %zu file %s\n",
				pspec_block->id.space(), pspec_block->file_name);

				//if(is_lock_on_read)
				pmemobj_rwlock_unlock(pop, &pspec_block->lock);
				return pspec_block;
			}
			//else: just skip this block
			//next block
		}//end for

		//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
		//this page 0 is not in PMEM, return NULL to read it from disk
		return NULL;
	} //end if page_no == 0
#endif //UNIV_PMEMOBJ_BUF_RECOVERY

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif

	TOID_ASSIGN(cur_list, (D_RO(buf->buckets)[hashed]).oid);

#if defined(UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_reads;
#endif

	if ( TOID_IS_NULL(cur_list)) {
		//assert(!TOID_IS_NULL(cur_list));
		printf("PMEM_ERROR error in get hashded list, but return NULL, check again! \n");
		return NULL;
	}
	//plist = D_RO(D_RO(buf->buckets)[hashed]);
	//assert(plist);

	//pblock = NULL;
	//found = -1;
	
	while ( !TOID_IS_NULL(cur_list) && (D_RO(cur_list) != NULL) ) {
		//plist = D_RW(cur_list);
		//pmemobj_rwlock_rdlock(pop, &plist->lock);
		//Scan in this list
		//for (i = 0; i < D_RO(cur_list)->max_pages; i++) {
		if (D_RO(cur_list) == NULL) {
			printf("===> ERROR read NULL list \n");
			assert(0);
		}
		for (i = 0; i < D_RO(cur_list)->cur_pages; i++) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK
			//if ( D_RO(D_RO(plist->arr)[i])->state != PMEM_FREE_BLOCK &&
			if (	D_RO(D_RO(D_RO(cur_list)->arr)[i]) != NULL && 
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.equals_to(page_id)) {
				pblock = D_RW(D_RW(D_RW(cur_list)->arr)[i]);
				//if(is_lock_on_read)
				pmemobj_rwlock_rdlock(pop, &pblock->lock);
				
				pdata = buf->p_align;

				memcpy(data, pdata + pblock->pmemaddr, pblock->size.physical()); 
				//bytes_read = pblock->size.physical();
#if defined (UNIV_PMEMOBJ_DEBUG)
				assert( pm_check_io(pdata + pblock->pmemaddr, pblock->id) ) ;
#endif
#if defined(UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_reads_hit;
				if (D_RO(cur_list)->is_flush)
					++buf->bucket_stats[hashed].n_reads_flushing;
#endif
				//if(is_lock_on_read)
				pmemobj_rwlock_unlock(pop, &pblock->lock);

				//return pblock;
				return D_RO(D_RO(D_RO(cur_list)->arr)[i]);
			}
		}//end for

		//next list
		if ( TOID_IS_NULL(D_RO(cur_list)->next_list))
			break;
		TOID_ASSIGN(cur_list, (D_RO(cur_list)->next_list).oid);
		if (TOID_IS_NULL(cur_list) || D_RO(cur_list) == NULL)
			break;

#if defined(UNIV_PMEMOBJ_BUF_STAT)
		cur_level++;
		if (buf->bucket_stats[hashed].max_linked_lists < cur_level)
			buf->bucket_stats[hashed].max_linked_lists = cur_level;
#endif
		
	} //end while
	
		return NULL;
}
/*
 * Use this function with pm_buf_write_with_flusher_append
 * */
const PMEM_BUF_BLOCK* 
pm_buf_read_lasted(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	const page_id_t		page_id,
	   	const page_size_t	size,
	   	byte*				data, 
		bool				sync) 
{
	
	bool is_lock_on_read = true;	
	ulint hashed;
	int i;

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
		//backward scan in the current list
		ulint cur_pages = D_RO(cur_list)->cur_pages;

		for (i = cur_pages - 1; i >= 0 ; i--) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK

			if (	(D_RO(D_RO(D_RO(cur_list)->arr)[i]) != NULL) &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.equals_to(page_id)) {
				pblock = D_RW(D_RW(D_RW(cur_list)->arr)[i]);
				if(is_lock_on_read)
					pmemobj_rwlock_rdlock(pop, &pblock->lock);

				//printf("====> PMEM_DEBUG found page_id[space %zu,page_no %zu] pmemaddr=%zu\n ", D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.space(), D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.page_no(), D_RO(D_RO(D_RO(cur_list)->arr)[i])->pmemaddr);
				//copy data from PMEM_BUF to buf	
				pdata = buf->p_align;
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

		return NULL;
}

/*handle page 0
// Note that there are two case for reading a page 0
// Case 1: read through buffer pool (handle in pm_buf_read  during the server working time)
// Case 2: read without buffer pool during the sever stat/stop  (this function)
*/
const PMEM_BUF_BLOCK*
pm_buf_read_page_zero(
		PMEMobjpool*		pop,
		PMEM_BUF*			buf,
		char*				file_name,
		byte*				data) {

	ulint i;
	byte* pdata;

	PMEM_BUF_BLOCK* pspec_block;

	PMEM_BUF_BLOCK_LIST* pspec_list = D_RW(buf->spec_list); 

	//scan in the special list with the scan key is file handle
	for (i = 0; i < pspec_list->cur_pages; i++){
		if (	D_RO(D_RO(D_RO(buf->spec_list)->arr)[i]) != NULL && 
				D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
				strstr(D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->file_name, file_name) != 0) {
			//if (pspec_block != NULL &&
			//	pspec_block->state != PMEM_FREE_BLOCK &&
			//	strstr(pspec_block->file_name,file_name)!= 0)  {
			pspec_block = D_RW(D_RW(D_RW(buf->spec_list)->arr)[i]);
			pmemobj_rwlock_rdlock(pop, &pspec_block->lock);
			//found
			printf("!!!!!!!! PMEM_DEBUG read_page_zero file= %s \n", pspec_block->file_name);
			pdata = buf->p_align;
			memcpy(data, pdata + pspec_block->pmemaddr, pspec_block->size.physical()); 

			//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
			pmemobj_rwlock_unlock(pop, &pspec_block->lock);
			return pspec_block;
		}
		//else: just skip this block
		//next block
		}//end for

	//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
	//this page 0 is not in PMEM, return NULL to read it from disk
	return NULL;

}

/*
 * Check full lists in the buckets and linked-list 
 * Resume flushing them 
   Logical of this function is similar with pm_buf_write
   in case the list is full

  This function should be called by only recovery thread. We don't handle thread lock here 
 * */
void
pm_buf_resume_flushing(
		PMEMobjpool*			pop,
		PMEM_BUF*				buf) {
	ulint i;
	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* phashlist;

	for (i = 0; i < PMEM_N_BUCKETS; i++) {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf ("\n====>resuming flush hash %zu\n", i);
#endif
		TOID_ASSIGN(cur_list, (D_RW(buf->buckets)[i]).oid);
		phashlist = D_RW(cur_list);
		if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {
			assert(phashlist->is_flush);
			assert(phashlist->hashed_id == i);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("\ncase 1 PMEM_RECOVERY ==> resume flushing for hashed_id %zu list_id %zu\n", i, phashlist->list_id);
#endif
			pm_buf_handle_full_hashed_list(pop, buf, i);
		}

		//(2) Check the linked-list of current list	
		TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
		while( !TOID_IS_NULL(cur_list)) {
			plist = D_RW(cur_list);
			if (plist->cur_pages >= plist->max_pages * PMEM_BUF_FLUSH_PCT) {
				//for the list in flusher, we only assign the flusher thread, no handle free list replace
				assert(plist->is_flush);
				assert(plist->hashed_id == i);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
				printf("\n\t\t case 2 PMEM_RECOVERY ==> resume flushing for linked list %zu of hashlist %zu hashed_id %zu\n", plist->list_id, phashlist->list_id, i);
#endif
				if (plist->list_id == 352){
					ulint test = 1;
				}
				pm_buf_assign_flusher(buf, plist);
			}

			TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("pm_buf_resume_flushing get next linked_list of hash_id %zu list_id %zu\n", i, plist->list_id);
#endif
			//next linked-list
		} //end while
		//next hashed list	
	} //end for
}

/*
 *Handle flushng a bucket list when it is full
 (1) Assign a pointer in worker thread to the full list
 (2) Swap the full list with the first free list from the free pool 
 * */
void
pm_buf_handle_full_hashed_list(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
		ulint			hashed) {

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);

	/*(1) Handle flusher */
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n[(1) begin handle assign flusher list %zu hashed %zu ===> ", phashlist->list_id, hashed);
#endif
	pm_buf_assign_flusher(buf, phashlist);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("(1) end handle assign flusher list %zu]\n", phashlist->list_id);
#endif


	//this assert inform that all write is async
	if (buf->is_async_only){ 
		if (phashlist->n_aio_pending != phashlist->cur_pages) {
			printf("!!!! ====> PMEM_ERROR: n_aio_pending=%zu != cur_pages = %zu. They should be equal\n", phashlist->n_aio_pending, phashlist->cur_pages);
			assert (phashlist->n_aio_pending == phashlist->cur_pages);
		}
	}

	/*    (2) Handle free list*/
get_free_list:
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n (2) begin get_free_list to replace full list %zu ==>", phashlist->list_id);
#endif
	//Get a free list from the free pool
	pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

	TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
	if (D_RW(buf->free_pool)->cur_lists == 0 ||
			TOID_IS_NULL(first_list) ) {
		pthread_t tid;
		tid = pthread_self();
		printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
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
	
	//tdnguyen test
	//printf("PMEM_INFO first_list id %zu \n", D_RW(first_list)->list_id);

	//Asign linked-list refs 
	TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
	TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
	TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n (2) end get_free_list %zu to replace full list %zu ==>", D_RW(first_list)->list_id, phashlist->list_id);
#endif

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	if ( !TOID_IS_NULL( D_RW(hash_list)->next_list ))
		++buf->bucket_stats[hashed].max_linked_lists;
#endif 

}

void
pm_buf_assign_flusher(
		PMEM_BUF*				buf,
		PMEM_BUF_BLOCK_LIST*	phashlist) {

	PMEM_FLUSHER* flusher = buf->flusher;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("[pm_buf_assign_flusher begin hashed_id %zu phashlist %zu flusher size = %zu cur request = %zu ", phashlist->hashed_id, phashlist->list_id, flusher->size, flusher->n_requested);
#endif
assign_worker:
	mutex_enter(&flusher->mutex);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf ("pass mutex enter hash_id %zu list_id %zu==>", phashlist->hashed_id, phashlist->list_id);
#endif

	//printf(" ==> pass the mutex enter ===> ");
	if (flusher->n_requested == flusher->size) {
		//all requested slot is full)
		printf("PMEM_INFO: all reqs are booked, sleep and wait \n");
		mutex_exit(&flusher->mutex);
		os_event_wait(flusher->is_req_full);	
		goto assign_worker;	
	}

	//find an idle thread to assign flushing task
	int64_t n_try = flusher->size;
	while (n_try > 0) {
		if (flusher->flush_list_arr[flusher->tail] == NULL) {
			//found
			flusher->flush_list_arr[flusher->tail] = phashlist;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("pm_buf_assign_flusher pointer id = %zu, list_id = %zu\n", flusher->tail, phashlist->list_id);
#endif
			++flusher->n_requested;
			//delay calling flush up to a threshold
			//printf("trigger worker...\n");
			//if (flusher->n_requested == flusher->size - 2) {
			if (flusher->n_requested == PMEM_FLUSHER_WAKE_THRESHOLD) {
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
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("pm_buf_assign_flusher n_try = %zu\n", n_try);
#endif
	} //end while 
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("pass while phashlist %zu flusher_tail = %zu flusher size = %zu n_requested %zu]\n", phashlist->list_id, flusher->tail, flusher->size, flusher->n_requested);
#endif
	//check
	if (n_try == 0) {
		/*This imply an logical error 
		 * */
		printf("PMEM_ERROR requested/size = %zu /%zu / %zu\n", flusher->n_requested, flusher->size);
		mutex_exit(&flusher->mutex);
		assert (n_try);
	}

	mutex_exit(&flusher->mutex);
	//end FLUSHER handling
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
	printf("bucket %zu [n_writes %zu,\t n_overwrites %zu,\t n_reads %zu, n_reads_hit %zu, n_reads_flushing %zu \tmax_linked_lists %zu, \tn_flushed_lists %zu] \n ",index,  p->n_writes, p->n_overwrites, p->n_reads, p->n_reads_hit, p->n_reads_flushing, p->max_linked_lists, p->n_flushed_lists); \
}while (0)


void pm_buf_bucket_stat_init(PMEM_BUF* pbuf) {
	int i;

	PMEM_BUCKET_STAT* arr = 
		(PMEM_BUCKET_STAT*) calloc(PMEM_N_BUCKETS, sizeof(PMEM_BUCKET_STAT));
	
	for (i = 0; i < PMEM_N_BUCKETS; i++) {
		arr[i].n_writes = arr[i].n_overwrites = 
			arr[i].n_reads = arr[i].n_reads_hit = arr[i].n_reads_flushing = arr[i].max_linked_lists =
			arr[i].n_flushed_lists = 0;
	}
	pbuf->bucket_stats = arr;
}

void pm_buf_stat_print_all(PMEM_BUF* pbuf) {
	ulint i;
	PMEM_BUCKET_STAT* arr = pbuf->bucket_stats;
	PMEM_BUCKET_STAT sumstat;

	sumstat.n_writes = sumstat.n_overwrites =
		sumstat.n_reads = sumstat.n_reads_hit = sumstat.n_reads_flushing = sumstat.max_linked_lists = sumstat.n_flushed_lists = 0;


	for (i = 0; i < PMEM_N_BUCKETS; i++) {
		sumstat.n_writes += arr[i].n_writes;
		sumstat.n_overwrites += arr[i].n_overwrites;
		sumstat.n_reads += arr[i].n_reads;
		sumstat.n_reads_hit += arr[i].n_reads_hit;
		sumstat.n_reads_flushing += arr[i].n_reads_flushing;
		sumstat.n_flushed_lists += arr[i].n_flushed_lists;

		if (sumstat.max_linked_lists < arr[i].max_linked_lists) {
			sumstat.max_linked_lists = arr[i].max_linked_lists;
		}

		PMEM_BUF_BUCKET_STAT_PRINT(pbuf, i);
	}

	printf("\n==========\n Statistic info:\n n_writes\t n_overwrites \t n_reads \t n_reads_hit \t n_reads_flushing \t max_linked_lists \t n_flushed_lists \n %zu \t %zu \t %zu \t %zu \t %zu \t %zu \t %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	printf("\n==========\n");

	fprintf(pbuf->deb_file, "\n==========\n Statistic info:\n n_writes\t n_overwrites \t n_reads \t n_reads_hit \t n_reads_flushing \t max_linked_lists \t n_flushed_lists \n %zu \t %zu \t %zu \t %zu \t %zu \t %zu \t %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	fprintf(pbuf->deb_file, "\n==========\n");

}

#endif //UNIV_PMEMOBJ_STAT
///////////////////// DEBUG Functions /////////////////////////

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
