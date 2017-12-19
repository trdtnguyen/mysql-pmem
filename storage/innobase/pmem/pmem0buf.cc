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
#include "hash0hash.h"

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

	TOID(PMEM_BUF) buf; 

	POBJ_ZNEW(pop, &buf, PMEM_BUF);

	PMEM_BUF *pbuf = D_RW(buf);

	pbuf->size = size;
	pbuf->type = BUF_TYPE;
	pbuf->is_new = true;

	pbuf->data = pm_pop_alloc_bytes(pop, size);
	if (OID_IS_NULL(pbuf->data)){
		//assert(0);
		return NULL;
	}

	pm_buf_list_init(pop, pbuf, size, page_size);
	
	pmemobj_persist(pop, pbuf, sizeof(*pbuf));
	return pbuf;
} 

void 
pm_buf_list_init(PMEMobjpool* pop, PMEM_BUF* buf, const size_t list_size, const size_t page_size)
{
	uint64_t i;
	size_t offset;
	PMEM_BUF_BLOCK_LIST* pfreelist;
	PMEM_BUF_BLOCK_LIST* plist;

	size_t n_pages = (list_size / page_size) + 1;

	//The free list
	POBJ_ZNEW(pop, &buf->free, PMEM_BUF_BLOCK_LIST);
	pfreelist = D_RW(buf->free);

	pfreelist->cur_size = 0;
	pfreelist->max_size = list_size;
	pfreelist->is_flush = false;
	//pfreelist->pext_list = NULL;
	TOID_ASSIGN(pfreelist->pext_list, OID_NULL) ;

	
	offset = 0;
	struct list_constr_args* args = (struct list_constr_args*) malloc(sizeof(struct list_constr_args)); 

	for (i = 0; i < n_pages; i++) {
		//struct list_constr_args args = {0,0,size};
		args->size = page_size;
		args->check = PMEM_AIO_CHECK;
		args->bpage = NULL;
		//args->list = NULL;
		TOID_ASSIGN(args->list, OID_NULL);
		args->pmemaddr = offset;
		offset += page_size;	

		POBJ_LIST_INSERT_NEW_HEAD(pop, &( D_RW(buf->free)->head), entries, sizeof(PMEM_BUF_BLOCK), pm_buf_block_init, args); 
		D_RW(buf->free)->cur_size++;
	}
	free(args);

	pmemobj_persist(pop, &pfreelist->cur_size, sizeof(pfreelist->cur_size) );
	pmemobj_persist(pop, &pfreelist->max_size, sizeof(pfreelist->max_size) );
	pmemobj_persist(pop, &pfreelist->is_flush, sizeof(pfreelist->is_flush) );
	pmemobj_persist(pop, &pfreelist->pext_list, sizeof(pfreelist->pext_list) );

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
		POBJ_ZNEW(pop, &D_RW(buf->buckets)[i], PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(D_RW(buf->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		plist = D_RW(D_RW(buf->buckets)[i]);
		plist->cur_size = 0;
		plist->max_size = list_size;
		plist->is_flush = false;
		//plist->pext_list = NULL;
		TOID_ASSIGN(plist->pext_list, OID_NULL);

		pmemobj_persist(pop, &plist->cur_size, sizeof(plist->cur_size));
		pmemobj_persist(pop, &plist->max_size, sizeof(plist->max_size));
		pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		pmemobj_persist(pop, &plist->pext_list, sizeof(plist->pext_list));
		pmemobj_persist(pop, plist, sizeof(*plist));
	}

}

/*This function is called as the func pointer in POBJ_LIST_INSERT_NEW_HEAD()*/
int
pm_buf_block_init(PMEMobjpool *pop, void *ptr, void *arg){
	struct list_constr_args *args = (struct list_constr_args *) arg;
	PMEM_BUF_BLOCK* block = (PMEM_BUF_BLOCK*) ptr;
//	block->id = args->id;
	block->id.copy_from(args->id);
	block->size = args->size;
	block->check = args->check;
	block->bpage = args->bpage;
	TOID_ASSIGN(block->list, (args->list).oid);
	//block->list = args->list;
	block->pmemaddr = args->pmemaddr;

	pmemobj_persist(pop, &block->id, sizeof(block->id));
	pmemobj_persist(pop, &block->size, sizeof(block->size));
	pmemobj_persist(pop, &block->check, sizeof(block->check));
	pmemobj_persist(pop, &block->bpage, sizeof(block->bpage));
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
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] page_id	page_id
@param[in] src_data	data contains the page
@param[in] page_size
 * */
int
pm_buf_write(PMEMobjpool* pop, PMEM_BUF* buf, buf_page_t* bpage, void* src_data){
	ulint hashed;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* plist_new;
	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	char* pdata;
	//page_id_t page_id;
	size_t page_size;


	assert(buf);
	assert(src_data);
	assert(bpage);

	//page_id.copy_from(bpage->id);
	page_size = bpage->size.physical();

	PMEM_HASH_KEY(hashed, bpage->id.fold(), PMEM_N_BUCKETS);
	//plist = &(buf->bufs[hashed]);
	plist = D_RW( D_RW(buf->buckets)[hashed]);
	assert(plist);

	pblock = NULL;
	POBJ_LIST_FOREACH(iter, &plist->head, entries) {
		if ( D_RW(iter)->id.equals_to(bpage->id)) {
			pblock = D_RW(iter);
			assert(pblock->size == page_size);
			break; // found the exist page
		}
	}
	
	pdata = static_cast<char*>( pmemobj_direct(buf->data));

	if (pblock == NULL) {
		//Get a free block from the free list
		free_block = POBJ_LIST_FIRST( &( D_RW(buf->free)->head) );
		if (TOID_IS_NULL(free_block)) {
			printf("PMEM_INFO: there is no free block in pmem buffer\n");
			//[TODO] wait from some seconds or extend the buffer
			return PMEM_ERROR;
		}
		
		D_RW(free_block)->id.copy_from(bpage->id);
		D_RW(free_block)->size = page_size;
		//save the reference to bpage 
		D_RW(free_block)->bpage=  bpage; 


		pmemobj_memcpy_persist(pop, pdata + D_RW(free_block)->pmemaddr, src_data, page_size); 

		//move the block from the free list to the buf list
		POBJ_LIST_MOVE_ELEMENT_TAIL(pop, &( D_RW(buf->free)->head), &(plist->head), free_block, entries, entries);
		D_RW(buf->free)->cur_size--;
		plist->cur_size++;

		//If the current buf is nearly full, flush pages to disk
		if (plist->cur_size >= plist->max_size * PMEM_BUF_THRESHOLD) {
			plist->is_flush = true;
			//Allocate a new list
			TOID(PMEM_BUF_BLOCK_LIST) list_new;
			POBJ_ZNEW(pop, &list_new, PMEM_BUF_BLOCK_LIST);	
			plist_new = D_RW(list_new);
			plist_new->cur_size = 0;
			plist_new->max_size = plist->max_size;
			plist_new->is_flush = false;
		
			//save the reference to the old list	
			//plist_new->pext_list = buf->buckets[hashed];
			TOID_ASSIGN(plist_new->pext_list, (D_RW(buf->buckets)[hashed]).oid);

			//buf->buckets[hashed] = &list_new;
			TOID_ASSIGN(D_RW(buf->buckets)[hashed], list_new.oid);
			//flush the plist async
			pm_buf_write_list_to_datafile(pop, buf, list_new, plist);
		}
	}
	else {
		//Overwrite the exist page
		pmemobj_memcpy_persist(pop, pdata + pblock->pmemaddr, src_data, page_size); 
	}

	return PMEM_SUCCESS;
}

/*
 * Async write pages from the list to datafile
 * See buf_dblwr_write_block_to_datafile
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] plist_new	pointer to the list that plist_new->pext is the list need to be written 
 * */
void
pm_buf_write_list_to_datafile(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK_LIST) list_new, PMEM_BUF_BLOCK_LIST* plist) {

	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK)* piter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	buf_page_t* bpage;

	ulint type = IORequest::WRITE;
	IORequest request(type);


	char* pdata;
	assert(pop);
	assert(buf);
	assert(!TOID_IS_NULL(list_new));


	
	pdata = static_cast<char*>( pmemobj_direct(buf->data));
	
	//for each block on the old list
	POBJ_LIST_FOREACH(iter, &plist->head, entries) {
			piter = &iter;
			pblock = D_RW(iter);
			//save the reference to the plist_new
			//pblock->list = list_new;
			TOID_ASSIGN(pblock->list,  list_new.oid);

			bpage = pblock->bpage;
			if (bpage->zip.data != NULL) {
				ut_ad(bpage->size.is_compressed());
				fil_io(request, false, bpage->id, bpage->size, 0,
						bpage->size.physical(),
						(void*) bpage->zip.data,
						(void*) bpage);
			}
			else {
				ut_ad(!bpage->size.is_compressed());
			
				fil_io(request, 
						false, bpage->id, bpage->size, 0, bpage->size.physical(),
						pdata + pblock->pmemaddr, piter);
			}
	} //End POBJ_LIST_FOREACH
	//check fil_aio_wait
}

/*
 *  This function is call after AIO complete
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] pblock	pointer to the completed block
 * */
void
pm_buf_write_aio_complete(PMEMobjpool* pop, PMEM_BUF* buf, TOID(PMEM_BUF_BLOCK) toid_block) {

	PMEM_BUF_BLOCK_LIST* plist_new;
	PMEM_BUF_BLOCK_LIST* plist;
	TOID(PMEM_BUF_BLOCK) block;
	PMEM_BUF_BLOCK* pblock;
	TOID(PMEM_BUF_BLOCK_LIST) list;

	assert(pop);
	assert(buf);
	assert(!TOID_IS_NULL(toid_block));
	
//	block = *(ptoid_block);
	TOID_ASSIGN(block,  toid_block.oid);

	pblock = D_RW(block);
	plist_new = D_RW(pblock->list);
	assert(plist_new);
	
	//list = *(plist_new->pext_list);
	TOID_ASSIGN(list , (plist_new->pext_list).oid);
	plist = D_RW(list);
	assert(plist);

	//move the block to the free list (re-use)
	pblock->bpage = NULL;
	TOID_ASSIGN(pblock->list, OID_NULL) ;
	
	POBJ_LIST_MOVE_ELEMENT_TAIL(pop, &(plist->head), &(D_RW(buf->free)->head), block, entries, entries); 
	D_RW(buf->free)->cur_size++;
	pmemobj_persist(pop, &D_RW(buf->free)->cur_size, sizeof( D_RW(buf->free)->cur_size));

	plist->cur_size--;
	pmemobj_persist(pop, &plist->cur_size, sizeof( plist->cur_size));

	if(plist->cur_size == 0) {
		//free the list
		//plist_new->pext_list = NULL;
		TOID_ASSIGN(plist_new->pext_list, OID_NULL);
		POBJ_FREE(&list);
	}	

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
pm_buf_read(PMEMobjpool* pop, PMEM_BUF* buf, page_id_t page_id, void* data) {
	
	ulint hashed;
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
	plist = D_RW(D_RW(buf->buckets)[hashed]);
	assert(plist);

	pblock = NULL;
	POBJ_LIST_FOREACH(iter, &plist->head, entries) {
		if ( D_RW(iter)->id.equals_to(page_id)) {
			pblock = D_RW(iter);
			//assert(pblock->size == page_size);
			break; // found the exist page
		}
	}
	
	pdata = static_cast<char*>( pmemobj_direct(buf->data));

	if (pblock == NULL) {
		//Try to read the extend list
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
		// page is not exist
		return 0;
	}
read_page:
		//page exist in the list or the extend list, read page
		pmemobj_memcpy_persist(pop, data, pdata + pblock->pmemaddr, pblock->size); 
		bytes_read = pblock->size;
		return bytes_read;
}

