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
//#include "pmem0buf.h"

#include "os0file.h"
//#include "hash0hash.h"
#include "buf0dblwr.h"

//#include "srv0start.h" //for SRV_SHUTDOWN_NONE

int pm_wrapper_buf_alloc(PMEM_WRAPPER* pmw, const size_t size, const size_t page_size) {
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
PMEM_BUF* pm_pop_buf_alloc(PMEMobjpool* pop, const size_t size, const size_t page_size) {
	char* p;
	size_t align_size;

	TOID(PMEM_BUF) buf; 

	POBJ_ZNEW(pop, &buf, PMEM_BUF);

	PMEM_BUF *pbuf = D_RW(buf);
	//align sizes to a pow of 2
	assert(ut_is_2pow(page_size));
	align_size = ut_uint64_align_up(size, page_size);

	pbuf->deb_file = fopen("debug.txt","w");

	pbuf->size = align_size;
	pbuf->page_size = page_size;
	pbuf->type = BUF_TYPE;
	pbuf->is_new = true;

	pbuf->data = pm_pop_alloc_bytes(pop, align_size);
	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(pbuf->data));
	assert(p);
	pbuf->p_align = static_cast<char*> (ut_align(p, page_size));
	pmemobj_persist(pop, pbuf->p_align, sizeof(*pbuf->p_align));

	if (OID_IS_NULL(pbuf->data)){
		//assert(0);
		return NULL;
	}

	pm_buf_list_init(pop, pbuf, align_size, page_size);
	
	pmemobj_persist(pop, pbuf, sizeof(*pbuf));
	return pbuf;
} 

void 
pm_buf_list_init(PMEMobjpool* pop, PMEM_BUF* buf, const size_t total_size, const size_t page_size)
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
	size_t bucket_size = total_size / (PMEM_N_BUCKETS * PMEM_MAX_LISTS_PER_BUCKET);
	size_t n_pages_per_bucket = bucket_size / page_size;

	size_t total_lists = total_size / bucket_size;
	size_t n_lists_in_free_pool = total_lists - PMEM_N_BUCKETS;


	printf("========= PMEM_INFO: n_pages = %zu bucket size = %f MB (%f %zu-KB pages) \n", n_pages, bucket_size*1.0 / (1024*1024), (bucket_size*1.0/page_size), (page_size/1024));

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
	//POBJ_ALLOC(pop, &buckets, PMEM_BUF_BLOCK_LIST*, 
	//		sizeof(PMEM_BUF_BLOCK_LIST*) * PMEM_N_BUCKETS,
	//		NULL, NULL);
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
		plist->max_pages = bucket_size / page_size;
		plist->list_id = cur_bucket;
		cur_bucket++;
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
		pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
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
		pfreelist->list_id = cur_bucket;
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
pm_buf_block_init(PMEMobjpool *pop, void *ptr, void *arg){
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
	//get the first object in pmem has type PMEM_DBW
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
 Note that this function may called my concurrency threads
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] bpage	pointer to buf_pge_t from the buffer pool, note that this bpage may change after this call
@param[in] src_data	data contains the bytes to write
@param[in] page_size
 * */
int
//pm_buf_write(PMEMobjpool* pop, PMEM_BUF* buf, buf_page_t* bpage, void* src_data, bool sync){
pm_buf_write(PMEMobjpool* pop, PMEM_BUF* buf, page_id_t page_id, page_size_t size, void* src_data, bool sync){
	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	char* pdata;
	//page_id_t page_id;
	size_t page_size;
	//size_t check;

	//Does some checks 
	assert(buf);
	assert(src_data);
	//assert(bpage);
	//we need to ensure we are writting a page in buffer pool
	//ut_a(buf_page_in_file(bpage));
	//ut_ad(!buf_page_get_mutex(bpage)->is_owned());
	//ut_ad(buf_page_get_io_fix(bpage) == BUF_IO_WRITE);

	//page_size = bpage->size.physical();
	page_size = size.physical();
	UNIV_MEM_ASSERT_RW(src_data, page_size);

	//PMEM_HASH_KEY(hashed, bpage->id.fold(), PMEM_N_BUCKETS);
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
retry:
	//plist = &(buf->bufs[hashed]);
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);
	
	//the safe check
	if (phashlist->is_flush) {
		os_thread_sleep(PMEM_WAIT_FOR_WRITE);

#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("\n\n    PMEM_DEBUG: in pmem_buf_write, the hash_list id %zu is flusing, wait and retry\n ", phashlist->list_id);
#endif	
		goto retry;

	}
	assert(!phashlist->is_flush);

	pdata = buf->p_align;
	//TOID_ASSIGN(free_block, OID_NULL);

	//lock the hashed list
	pmemobj_rwlock_wrlock(pop, &phashlist->lock);


	//(1) search in the hashed list for a first FREE block to write on 
	
	for (i = 0; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);

		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) {
			if (pfree_block->id.equals_to(page_id)) {
				//overwrite the old page
				pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
				if (pfree_block->sync != sync) {
					phashlist->n_aio_pending += (pfree_block->sync - sync);

				}
				pfree_block->sync = sync;
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
				//D_RW(free_block)->bpage = bpage;
				pfree_block->sync = sync;
				pmemobj_rwlock_unlock(pop, &pfree_block->lock);
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
		os_thread_sleep(PMEM_WAIT_FOR_WRITE);

	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
		goto retry;
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, the write page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	

	// (2) At this point, we get the free and un-blocked block, write data to this block

	//D_RW(free_block)->bpage = bpage;
	pfree_block->sync = sync;
	//we only pending aio when flush list
	if (!sync)
		phashlist->n_aio_pending++;		

	//D_RW(free_block)->id.copy_from(bpage->id);
	pfree_block->id.copy_from(page_id);
	
	//assert(D_RW(free_block)->size.equals_to(bpage->size));
	assert(pfree_block->size.equals_to(size));
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 

	pmemobj_persist(pop, &pfree_block->sync, sizeof(pfree_block->sync));
	pmemobj_persist(pop, &pfree_block->id, sizeof(pfree_block->id));
	pmemobj_persist(pop, &pfree_block->state, sizeof(pfree_block->state));
	//pmemobj_rwlock_unlock(pop, &D_RW(free_block)->lock);
	pmemobj_rwlock_unlock(pop, &pfree_block->lock);

	//handle hash_list, 
	//pmemobj_rwlock_wrlock(pop, &D_RW(hash_list)->lock);

	//TOID_ASSIGN(phashlist->next_free_block, free_block.oid);
	++(phashlist->cur_pages);
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_THRESHOLD) {
		//(3) The hashlist is nearly full, flush it and assign a free list 
		phashlist->is_flush = true;
		//after set phashlist->is_flush to true, we can safely release the lock without worry other threads effect this thread work because there is a checking conditional at the beginning of this function
		//we temporary unlock and then lock again the hashlist to avoid dead-lock in case the free_pool is empty and this thread wait for new free_list come to free_pool


get_free_list:
		//re acquire a lock on hashlist
		//pmemobj_rwlock_wrlock(pop, &phashlist->lock);
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
			os_thread_sleep(1000000);
			goto get_free_list;
		}
		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;

		assert(!TOID_IS_NULL(first_list));

		//Asign linked-list refs 
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
		TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
		TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);
		

		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		
		//request a flush from worker thread
		printf("request flush list_id %zu n_aio_pending %zu \n", phashlist->list_id, phashlist->n_aio_pending);
		pm_lc_request(hash_list);
		//pm_buf_flush_list(pop, buf, phashlist);

	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
	}

	return PMEM_SUCCESS;
}

/*
 * Async write pages from the list to datafile
 * The caller thread need to lock/unlock the plist
 * See buf_dblwr_write_block_to_datafile
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] plist_new	pointer to the list that plist_new->pext is the list need to be written 
 * */
void
pm_buf_flush_list(PMEMobjpool* pop, PMEM_BUF* buf, PMEM_LIST_CLEANER_SLOT* slot) {

	ulint i;
	ulint count;

	TOID(PMEM_BUF_BLOCK) flush_block;
	PMEM_BUF_BLOCK* pblock;
	PMEM_BUF_BLOCK_LIST* plist;

	char* pdata;
	assert(pop);
	assert(buf);
	
	plist = D_RW(slot->flush_list);
	pdata = buf->p_align;

	pmemobj_rwlock_wrlock(pop, &plist->lock);
	count = 0;
	

	for (i = 0; i < plist->max_pages; i++) {
		pblock = D_RW(D_RW(plist->arr)[i]);
		//Becareful with this assert
		//assert (pblock->state == PMEM_IN_USED_BLOCK);

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
		pblock->pslot = slot;

		ulint type = IORequest::PM_WRITE;
		if (pblock->sync) {
			type |= IORequest::DO_NOT_WAKE;
			//the sync IO is not pending
		}
		else {
			//debug
			count++;
		}


		IORequest request(type);

		dberr_t err = fil_io(request, 
				pblock->sync, pblock->id, pblock->size, 0, pblock->size.physical(),
				pdata + pblock->pmemaddr, D_RW(D_RW(plist->arr)[i]));

		pmemobj_rwlock_unlock(pop, &pblock->lock);

		if (err != DB_SUCCESS){
			printf("PMEM_ERROR: fil_io() in pm_buf_list_write_to_datafile() space_id = %"PRIu32" page_id = %"PRIu32" size = %zu \n ", pblock->id.space(), pblock->id.page_no(), pblock->size.physical());
			assert(0);
		}
	}

	//The aio complete thread will handle the post-processing of the flush list
	printf("\n   PMEM_DEBUG: end pm_buf_flush_list %zu  n_aio_pending %zu count %zu\n ", plist->list_id,  plist->n_aio_pending, count);

	pmemobj_rwlock_unlock(pop, &plist->lock);
}


/*
 *	Read a page from pmem buffer using the page_id as key
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *  @param[in] page_id	read key
 *  @param[out] data	read data if the page_id exist in the buffer
 *  @return:  size of read page
 * */
size_t 
pm_buf_read(PMEMobjpool* pop, PMEM_BUF* buf, const page_id_t page_id, const page_size_t size, void* data) {
	
	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	PMEM_BUF_BLOCK_LIST* plist;
	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	char* pdata;
	size_t bytes_read;
	
	assert(buf);
	assert(data);
	bytes_read = 0;

	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
	TOID_ASSIGN(cur_list, (D_RW(buf->buckets)[hashed]).oid);
	//plist = D_RW(D_RW(buf->buckets)[hashed]);
	//assert(plist);

	pblock = NULL;
	
	while ( !TOID_IS_NULL(cur_list) ) {
		plist = D_RW(cur_list);
	//	pmemobj_rwlock_rdlock(pop, &plist->lock);
		//Scan in this list
		for (i = 0; i < plist->max_pages; i++) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK
			if ( D_RW(D_RW(plist->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RW(D_RW(plist->arr)[i])->id.equals_to(page_id)) {
	//			pblock = D_RW(D_RW(plist->arr)[i]);
				assert(pblock->size.equals_to(size));
				break; // found the exist page
			}
		}//end for
	//	pmemobj_rwlock_unlock(pop, &plist->lock);

		//next list
		TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
	} //end while
		
	if (pblock == NULL) {
		// page is not exist
		return 0;
	}
//read_page:
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
		printf("PMEM_DEBUG: pm_buf_read from pmem page_id %zu space = %zu pmemaddr= %zu flush_list id=%zu\n", page_id.page_no(), page_id.space(), pblock->pmemaddr, plist->list_id);
#endif
	pdata = buf->p_align;
	//page exist in the list or the extend list, read page
	pmemobj_rwlock_rdlock(pop, &pblock->lock); 

	UNIV_MEM_ASSERT_RW(data, pblock->size.physical());
	//pmemobj_memcpy_persist(pop, data, pdata + pblock->pmemaddr, pblock->size.physical()); 
	memcpy(data, pdata + pblock->pmemaddr, pblock->size.physical()); 
	bytes_read = pblock->size.physical();

	pmemobj_rwlock_unlock(pop, &pblock->lock); 
	//pmemobj_rwlock_unlock(pop, &plist->lock);

	return bytes_read;
}
///////////////////// DEBUG Funcitons /////////////////////////
void pm_buf_print_lists_info(PMEM_BUF* buf){
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* plist;
	int i;

	printf("PMEM_DEBUG ==================\n");

	pfreepool = D_RW(buf->free_pool);
	printf("The free pool: curlists=%zd \n", pfreepool->cur_lists);

	printf("The buckets: \n");
	
	for (i = 0; i < PMEM_N_BUCKETS; i++){

		plist = D_RW( D_RW(buf->buckets)[i] );
		printf("\tBucket %d: list_id=%zu cur_pages=%zd ", i, plist->list_id, plist->cur_pages);
		printf("\n");
	}		
}
//////////////////////// THREAD HANDLER /////////////

