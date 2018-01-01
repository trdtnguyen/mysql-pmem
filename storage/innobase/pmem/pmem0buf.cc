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
	
	pbuf->size = align_size;
	pbuf->type = BUF_TYPE;
	pbuf->is_new = true;

	pbuf->data = pm_pop_alloc_bytes(pop, align_size);
	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(pbuf->data));
	assert(p);
	pbuf->p_align = static_cast<char*> (ut_align(p, page_size));

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
		plist->max_pages = bucket_size / page_size;
		plist->is_flush = false;
		plist->n_pending = 0;
		plist->list_id = cur_bucket;
		cur_bucket++;
		plist->check = PMEM_AIO_CHECK;
		//plist->pext_list = NULL;
		//TOID_ASSIGN(plist->pext_list, OID_NULL);
	
		//init pages in bucket
		for (j = 0; j < n_pages_per_bucket; j++) {
			args->pmemaddr = offset;
			TOID_ASSIGN(args->list, (D_RW(buf->buckets)[i]).oid);
			offset += page_size;	

			POBJ_LIST_INSERT_NEW_HEAD(pop, &plist->head, entries, sizeof(PMEM_BUF_BLOCK), pm_buf_block_init, args); 
			//plist->cur_pages++;
		}
		TOID_ASSIGN(plist->next_free_block, POBJ_LIST_FIRST(&plist->head).oid);

		pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		//pmemobj_persist(pop, &plist->pext_list, sizeof(plist->pext_list));
		pmemobj_persist(pop, &plist->n_pending, sizeof(plist->n_pending));
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
		pfreelist->n_pending = 0;
		pfreelist->list_id = cur_bucket;
		cur_bucket++;
		pfreelist->check = PMEM_AIO_CHECK;
	
		//init pages in bucket
		for (j = 0; j < n_pages_per_bucket; j++) {
			args->pmemaddr = offset;
			offset += page_size;	
			TOID_ASSIGN(args->list, freelist.oid);

			POBJ_LIST_INSERT_NEW_HEAD(pop, &pfreelist->head, entries, sizeof(PMEM_BUF_BLOCK), pm_buf_block_init, args); 
			//pfreelist->cur_pages++;
		}
		TOID_ASSIGN(pfreelist->next_free_block, POBJ_LIST_FIRST(&pfreelist->head).oid);

		pmemobj_persist(pop, &pfreelist->cur_pages, sizeof(pfreelist->cur_pages));
		pmemobj_persist(pop, &pfreelist->max_pages, sizeof(pfreelist->max_pages));
		pmemobj_persist(pop, &pfreelist->is_flush, sizeof(pfreelist->is_flush));
		//pmemobj_persist(pop, &pfreelist->pext_list, sizeof(pfreelist->pext_list));
		pmemobj_persist(pop, &pfreelist->n_pending, sizeof(pfreelist->n_pending));
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
	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;
	PMEM_BUF_BLOCK_LIST* plist_new;
	PMEM_BUF_BLOCK_LIST* pfree;

	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
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
	//pblock = NULL;
	
	POBJ_LIST_FOREACH(free_block, &phashlist->head, entries) {
		if (D_RW(free_block)->state == PMEM_FREE_BLOCK) {
			//found!
			pmemobj_rwlock_wrlock(pop, &D_RW(free_block)->lock);
			//TOID_ASSIGN(free_block, iter.oid);	
			break;	
		}
		else if(D_RW(free_block)->state == PMEM_IN_USED_BLOCK) {
			//if (D_RW(free_block)->id.equals_to(bpage->id)) {
			if (D_RW(free_block)->id.equals_to(page_id)) {
				//overwrite the old page
				pmemobj_rwlock_wrlock(pop, &D_RW(free_block)->lock);
				pmemobj_memcpy_persist(pop, pdata + D_RW(free_block)->pmemaddr, src_data, page_size); 
				//D_RW(free_block)->bpage = bpage;
				D_RW(free_block)->sync = sync;
				pmemobj_rwlock_unlock(pop, &D_RW(free_block)->lock);
				pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, OVERWRITTEN page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	
				return PMEM_SUCCESS;
			}
		}
		//next block
	}	
/*
	TOID_ASSIGN(iter, phashlist->next_free_block.oid);
	check = 0;
	TOID_ASSIGN(free_block, OID_NULL);
	while ( !TOID_IS_NULL(iter)) {
		pblock = D_RW(iter);
		//Note: in this impelementation, we allow multiple versions of a page_id exist

		if(pblock && pblock->state == PMEM_FREE_BLOCK){
			//we only get the un-locked block 
			if ( pmemobj_rwlock_trywrlock(pop, &pblock->lock) == 0) {
				assert(pblock->size.equals_to(bpage->size));
				TOID_ASSIGN(free_block, iter.oid);
				break;
			}
		}
		iter = POBJ_LIST_NEXT(iter, entries);
		check++;
		if (check > phashlist->max_pages) {
			printf("!!!!PMEM_ERROR Something are wrong in pmem_buf_write\n");
			assert(0);
		}
	}
	check = 0;
*/
	if ( TOID_IS_NULL(free_block) ) {
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
	D_RW(free_block)->sync = sync;

	//D_RW(free_block)->id.copy_from(bpage->id);
	D_RW(free_block)->id.copy_from(page_id);
	
	//assert(D_RW(free_block)->size.equals_to(bpage->size));
	assert(D_RW(free_block)->size.equals_to(size));
	assert(D_RW(free_block)->state == PMEM_FREE_BLOCK);
	D_RW(free_block)->state = PMEM_IN_USED_BLOCK;

	pmemobj_memcpy_persist(pop, pdata + D_RW(free_block)->pmemaddr, src_data, page_size); 

	pmemobj_persist(pop, &D_RW(free_block)->sync, sizeof(D_RW(free_block)->sync));
	pmemobj_persist(pop, &D_RW(free_block)->id, sizeof(D_RW(free_block)->id));
	pmemobj_persist(pop, &D_RW(free_block)->state, sizeof(D_RW(free_block)->state));
	//pmemobj_rwlock_unlock(pop, &D_RW(free_block)->lock);
	pmemobj_rwlock_unlock(pop, &D_RW(free_block)->lock);

	//handle hash_list, 
	//pmemobj_rwlock_wrlock(pop, &D_RW(hash_list)->lock);

	TOID_ASSIGN(phashlist->next_free_block, free_block.oid);
	phashlist->cur_pages++;
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_THRESHOLD) {
		//(3) The hashlist is nearly full, flush it and assign a free list 
		//
		phashlist->is_flush = true;
		phashlist->n_pending = phashlist->cur_pages;
get_free_list:
		//Get a free list from the free pool
		pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

		TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
		if (D_RW(buf->free_pool)->cur_lists == 0 ||
				TOID_IS_NULL(first_list) ) {
			printf("PMEM_INFO: free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
			pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
		//	pmemobj_rwlock_unlock(pop, &phashlist->lock);
			os_thread_sleep(PMEM_WAIT_FOR_WRITE);
			goto get_free_list;
		}
		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;

		assert(!TOID_IS_NULL(first_list));

		//Asign current hash list to new list
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);

		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

		//pmemobj_rwlock_unlock(pop, &D_RW(hash_list)->lock);

		pm_buf_flush_list(pop, buf, phashlist);
		

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
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
//pm_buf_write_list_to_datafile(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK_LIST) list_new, PMEM_BUF_BLOCK_LIST* plist) {
//pm_buf_flush_list(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK_LIST)flush_list) {
pm_buf_flush_list(PMEMobjpool* pop, PMEM_BUF* buf, PMEM_BUF_BLOCK_LIST* plist) {

	//PMEM_BUF_BLOCK_LIST* plist;

	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK)* piter;
	TOID(PMEM_BUF_BLOCK) flush_block;
	PMEM_BUF_BLOCK* pblock;
	size_t n_flush;
	//buf_page_t* bpage;

	ulint type = IORequest::WRITE | IORequest::DO_NOT_WAKE;
	IORequest request(type);

	char* pdata;
	assert(pop);
	assert(buf);
	//assert(!TOID_IS_NULL(list_new));

//	printf("\n   PMEM_DEBUG: begin pm_buf_flush_list %zu\n ", plist->list_id);

	//plist = D_RW(flush_list);

	//pdata = static_cast<char*>( pmemobj_direct(buf->data));
	pdata = buf->p_align;
	//D_RW(flush_list)->n_pending = D_RW(flush_list)->cur_pages;

	//We don't lock the flush_list here, that prevent AIO complete thread access to the flush_list
	
	n_flush = 0;
	//pmemobj_rwlock_wrlock(pop, &plist->lock);
	POBJ_LIST_FOREACH(flush_block, &plist->head, entries) {
		pblock = D_RW(flush_block);
		//PMEM_BUF_BLOCK* pt = D_RW(iter);
		//Becareful with this assert
		//assert (pblock->state == PMEM_IN_USED_BLOCK);
		if (D_RO(flush_block)->state == PMEM_FREE_BLOCK) {
			printf("!!!!!PMEM_WARNING: in list %zu don't write a FREE BLOCK, skip to next block\n", plist->list_id);
			continue;
		}
		pmemobj_rwlock_wrlock(pop, &pblock->lock);
		
		assert( pblock->pmemaddr < buf->size);
		UNIV_MEM_ASSERT_RW(pdata + pblock->pmemaddr, page_size);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	//	printf("PMEM_DEBUG: aio request page_id %zu space %zu pmemaddr %zu flush_list id=%zu\n", pblock->id.page_no(), pblock->id.space(), pblock->pmemaddr, plist->list_id);
#endif
			
		//pblock->state = PMEM_FREE_BLOCK;	
		//We did it in init instead
		//TOID_ASSIGN(pblock->list, flush_list.oid);
		dberr_t err = fil_io(request, 
				pblock->sync, pblock->id, pblock->size, 0, pblock->size.physical(),
				pdata + pblock->pmemaddr, pblock);

		n_flush++;
		if (err != DB_SUCCESS){
			printf("PMEM_ERROR: fil_io() in pm_buf_list_write_to_datafile()\n ");
			assert(0);
		}
		pmemobj_rwlock_unlock(pop, &pblock->lock);

		if(n_flush >= plist->cur_pages)
			break;
	}
	if (n_flush != plist->cur_pages) {
		size_t n_frees = 0;
		size_t n_useds = 0;
		size_t c_flush = 0;
		POBJ_LIST_FOREACH(flush_block, &plist->head, entries) {
			if (D_RO(flush_block)->state == PMEM_FREE_BLOCK)
				n_frees++;
			else if (D_RO(flush_block)->state == PMEM_IN_FLUSH_BLOCK)
				c_flush++;
			else
				n_useds++;
		}
		printf("PMEM_ERROR: in pm_buf_flush_list, n_flush = %zu != flush_list->cur_pages= %zu c n_frees = %zu n_used= %zu c_flush = %zu \n", n_flush, plist->cur_pages, n_frees, n_useds, c_flush);
		assert(n_flush == plist->cur_pages);
	}
	//The aio complete thread will handle the post-processing of the flush list
//	printf("\n   PMEM_DEBUG: end pm_buf_flush_list %zu\n ", plist->list_id);
	//pm_buf_print_lists_info(buf);
	//pmemobj_rwlock_unlock(pop, &plist->lock);
}

/*
 *  This function is call after AIO complete
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] pblock	pointer to the completed block
 * */
/*
void
pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK)* ptoid_block) {
//pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK) toid_block) {

//pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, PMEMoid* poid) {
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	printf("==== AIO ==== \nPMEM_DEBUG: pm_buf_write_aio_complete\n");
#endif
	PMEM_BUF_BLOCK_LIST* plist_new;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* pfreelist;
	TOID(PMEM_BUF_BLOCK) block;
	PMEM_BUF_BLOCK* pblock;
	TOID(PMEM_BUF_BLOCK_LIST) list;

	assert(pop);
	assert(buf);
	assert(ptoid_block);
	//assert(!TOID_IS_NULL(toid_block));
	//assert(poid);
	
	block = *(ptoid_block);
	assert(!TOID_IS_NULL(block));

	//TOID_ASSIGN(block,  toid_block.oid);
	//TOID_ASSIGN(block,  toid_block.oid);
	//TOID_ASSIGN(block,  *poid);

	pblock = D_RW(block);
	assert(pblock);
	plist_new = D_RW(pblock->list);
	assert(plist_new);
	
	//list = *(plist_new->pext_list);
	TOID_ASSIGN(list , (plist_new->pext_list).oid);
	plist = D_RW(list);
	assert(plist);
	
	pfreelist = D_RW(buf->free);
	//remove block from the plist
	pmemobj_rwlock_wrlock(pop, &plist->lock);
	POBJ_LIST_REMOVE(pop, &plist->head, block, entries);
	plist->cur_pages--;
	pmemobj_persist(pop, &plist->cur_pages, sizeof( plist->cur_pages));
	pmemobj_rwlock_unlock(pop, &plist->lock);

	//Reset the removed block
	pmemobj_rwlock_wrlock(pop, &pblock->lock);
	TOID_ASSIGN(pblock->list, OID_NULL) ;
	pblock->state=PMEM_FREE_BLOCK;

	//insert the removed block to the free list
	pmemobj_rwlock_wrlock(pop, &pfreelist->lock);
	POBJ_LIST_INSERT_TAIL(pop, &(D_RW(buf->free)->head), block, entries);
	D_RW(buf->free)->cur_pages++;
	pmemobj_persist(pop, &D_RW(buf->free)->cur_pages, sizeof( D_RW(buf->free)->cur_pages));

	pmemobj_rwlock_unlock(pop, &pfreelist->lock);
	pmemobj_rwlock_unlock(pop, &pblock->lock);

	if(plist->cur_pages == 0) {
		//free the list
		//plist_new->pext_list = NULL;
		TOID_ASSIGN(plist_new->pext_list, OID_NULL);
		pmemobj_rwlock_unlock(pop, &plist->lock);
		POBJ_FREE(&list);
	}	
	else {
		//do nothing
	//	pmemobj_rwlock_unlock(pop, &plist->lock);
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
			printf("PMEM_DEBUG: aio complete page_id %zd pmemaddr %zd \n", pblock->id.page_no(), pblock->pmemaddr);
#endif
}
*/

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
	PMEM_BUF_BLOCK_LIST* plist;
	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	char* pdata;
	size_t bytes_read;
	size_t check;

	
	assert(buf);
	assert(data);
	bytes_read = 0;

	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
	plist = D_RW(D_RW(buf->buckets)[hashed]);
	assert(plist);

	pblock = NULL;

	//We scan from the tail of the hash_list so that the read page (if any) is the lastest version
	TOID_ASSIGN(iter, plist->next_free_block.oid);
	TOID(PMEM_BUF_BLOCK) first = POBJ_LIST_FIRST(&plist->head);

	check = 0;

	while ( !TOID_IS_NULL(iter)) {

		if ( D_RW(iter)->state != PMEM_FREE_BLOCK &&
				D_RW(iter)->id.equals_to(page_id)) {
			pblock = D_RW(iter);
			assert(pblock->size.equals_to(size));
			break; // found the exist page
		}
		if ( TOID_EQUALS(iter, first) ) {
			break;
		}
		check++;
		if (check >= plist->max_pages + 10) {
			printf("PMEM_ERROR pm_buf_read has some problems\n");
			assert(0);
		}
		iter = POBJ_LIST_PREV(iter, entries);
	}
		
	if (pblock == NULL) {
		//page is not in this hashed list
		//Try to read the extend list
/*
		if( !TOID_IS_NULL(plist->pext_list)) {
			plist = D_RW(plist->pext_list);
			POBJ_LIST_FOREACH(iter, &plist->head, entries) {
				if ( D_RW(iter)->id.equals_to(page_id)) {
					pblock = D_RW(iter);
					//assert(pblock->size == page_size);
					break; // found the exist page
				}
			}
			if (pblock != NULL) {
				goto read_page;
			}
		}
*/
		// page is not exist
		return 0;
	}
read_page:
	//pdata = static_cast<char*>( pmemobj_direct(buf->data));
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
		printf("PMEM_DEBUG: pm_buf_read from pmem page_id %zu space = %zu pmemaddr= %zu flush_list id=%zu\n", page_id.page_no(), page_id.space(), pblock->pmemaddr, plist->list_id);
#endif
	pdata = buf->p_align;
	//page exist in the list or the extend list, read page
	pmemobj_rwlock_rdlock(pop, &pblock->lock); 

	UNIV_MEM_ASSERT_RW(data, pblock->size.physical());
	pmemobj_memcpy_persist(pop, data, pdata + pblock->pmemaddr, pblock->size.physical()); 
	bytes_read = pblock->size.physical();

	pmemobj_rwlock_unlock(pop, &pblock->lock); 
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

