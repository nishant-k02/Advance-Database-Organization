#include "buffer_mgr.h"
#include <stdlib.h>
#include <string.h>

// helpers

// Initialize the frame.
PageFrameInfo *initFrame()
{
	PageFrameInfo *framePtr = malloc(sizeof(PageFrameInfo));
	framePtr->pageData = calloc(PAGE_SIZE, sizeof(char)); // TODO: free it .. Done in destroyFrame
	framePtr->isDirty = framePtr->fixCnt = 0;
	framePtr->pageNum = -1;
	return framePtr;
}

// setting a frame as dirty
void setDirty(BM_BufferPool *bm, PageFrameInfo *framePtr, bool flag)
{
	framePtr->isDirty = ((PoolMgmtInfo *)(bm->mgmtData))->stats->dirtyFlags[framePtr->statsPos] = flag;
}

// Cleaning frame resources
void destroyFrame(PageFrameInfo *framePtr)
{
	free(framePtr->pageData);
	free(framePtr);
	// TODO: remove from hashmap if it is there .. Done in replacement
	// TODO: remove from linked list .. Done in replacement
}

void freeResource(PoolMgmtInfo *pool){
	free(pool->fHandle);
	free(pool->stats->fixCounts);
	free(pool->stats->pageNums);
	free(pool->stats->dirtyFlags);
	free(pool->stats);
	free(pool);
}

// finding a frame by page number
PageFrameInfo *getFrameByPageNumber(BM_BufferPool *bm, PageNumber pageNum)
{
	Node *node = (Node *)(hmGet(bm->mgmtData->hm, pageNum));
	if (node == NULL)
		return NULL;
	return ((PageFrameInfo *)(node->data));
}

// fix count inc and dec when pin and unpin
void incDecfixCnt(BM_BufferPool *bm, PageFrameInfo *framePtr, bool isInc)
{
	int i;
	if (isInc)
		i = 1;
	else
		i = -1;
	bm->mgmtData->stats->fixCounts[framePtr->statsPos] += i;
	framePtr->fixCnt += i;
}

// setting the stats values for each frame
void setStatistics(BM_BufferPool *bm, PageFrameInfo *framePtr, int pos)
{
	bm->mgmtData->stats->dirtyFlags[pos] = framePtr->isDirty;
	bm->mgmtData->stats->fixCounts[pos] = framePtr->fixCnt;
	bm->mgmtData->stats->pageNums[pos] = framePtr->pageNum;
	framePtr->statsPos = pos;
}

// writing a page back to file
RC writeDirtyPage(BM_BufferPool *bm, PageFrameInfo *framePtr)
{
	// Write dirty page into memory
	RC writeStatus = writeBlock(framePtr->pageNum, bm->mgmtData->fHandle, framePtr->pageData);
	if (writeStatus != RC_OK)
	{
		return writeStatus;
	}
	bm->mgmtData->stats->writeCnt++;
	setDirty(bm, framePtr, false);
	return RC_OK;
}

// frame replacement strategy, works well for fifo and for LRU, since LRU shuffling is done somewhere else.
RC replacement(BM_BufferPool *bm, PageFrameInfo *framePtr, PageNumber pageNum)
{
	readBlock(pageNum, bm->mgmtData->fHandle, framePtr->pageData);
	bm->mgmtData->stats->readCnt++;
	int pos;
	if (bm->mgmtData->stats->readCnt > bm->numPages)
	{
		PageFrameInfo *toReplace = NULL;
		// here goes the replacement
		Node *temp = bm->mgmtData->head;
		while (true)
		{
			if (((PageFrameInfo *)(temp->data))->fixCnt == 0){
				toReplace = (PageFrameInfo *)(temp->data);
				pos = toReplace->statsPos;
				break;
			}
			temp = temp->next;
			//if we don't find any space in the buffer
			if (temp == NULL)
				return RC_NO_SPACE_IN_POOL;
		}
		//write page if its dirty
		if (toReplace->isDirty)
			writeDirtyPage(bm, toReplace);
		// remove from linked list
		if (temp == bm->mgmtData->tail)
		{
			bm->mgmtData->tail = temp->prev;
			bm->mgmtData->tail->next = NULL;
		}
		else if (temp == bm->mgmtData->head)
		{
			temp->next->prev = NULL;
			bm->mgmtData->head = temp->next;
		}
		else
		{
			temp->prev->next = temp->next;
			temp->next->prev = temp->prev;
		}
		hmDelete(bm->mgmtData->hm, toReplace->pageNum);
		destroyFrame(toReplace);
		free(temp);
	}
	else
	{
		pos = bm->mgmtData->stats->readCnt - 1;
	}
	framePtr->pageNum = pageNum;
	Node *node = malloc(sizeof(Node)); // TODO: free it .. Done in replacement
	node->next = node->prev = NULL;
	node->data = framePtr;
	if (bm->mgmtData->head == NULL && bm->mgmtData->tail == NULL)
	{
		bm->mgmtData->head = node;
		bm->mgmtData->tail = node;
	}
	else
	{
		bm->mgmtData->tail->next = node;
		node->prev = bm->mgmtData->tail;
		bm->mgmtData->tail = node;
	}
	setStatistics(bm, framePtr, pos);
	hmInsert(bm->mgmtData->hm, framePtr->pageNum, node); // TODO: free using hmDelete() .. Done in replacement
	return RC_OK;
}

// FIFO strategy, works only when adding new page from file.
RC fifo(BM_BufferPool *bm, PageFrameInfo *framePtr, PageNumber pageNum)
{
	if (framePtr->pageNum == NO_PAGE)
		return replacement(bm, framePtr, pageNum);
	return RC_OK;
}

// adding new page from file, or shuffle the frame pos when it is pinned
RC lru(BM_BufferPool *bm, PageFrameInfo *framePtr, PageNumber pageNum)
{
	if (framePtr->pageNum == NO_PAGE)
		return replacement(bm, framePtr, pageNum);
	else
	{
		// shuffle the linked list
		Node *node = hmGet(bm->mgmtData->hm, pageNum);
		if (node == bm->mgmtData->tail)
			return RC_OK;
		if (node == bm->mgmtData->head)
		{
			// removing node from head
			bm->mgmtData->head->prev = NULL;
			bm->mgmtData->head = node->next;
		}
		else
		{
			// removing node from middle
			node->next->prev = node->prev;
			node->prev->next = node->next;
		}
		// attaching node to tail
		node->prev = bm->mgmtData->tail;
		node->next = NULL;
		bm->mgmtData->tail->next = node;
		bm->mgmtData->tail = node;
		return RC_OK;
	}
}

// called on each pin
RC execStrategy(BM_BufferPool *bm, PageFrameInfo *framePtr, PageNumber pageNum)
{
	if (bm->strategy == RS_LRU)
		return lru(bm, framePtr, pageNum);
	else if (bm->strategy == RS_FIFO)
		return fifo(bm, framePtr, pageNum);
	else
		return RC_STRATEGY_NOT_SUPPORTED;
}

// functionality

/* linked list to hold frames, and stats to hold stats, and hashmap for direct access
 Initialize buffer pool with all the data_structures */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
	bm->pageFile = (char *)pageFileName; // TODO: free it .. Done in shutdownBufferPool
	bm->numPages = numPages;
	bm->strategy = strategy;
	PoolMgmtInfo *pmi = malloc(sizeof(PoolMgmtInfo)); // TODO: free it .. Done in shutdownBufferPool
	pmi->fHandle = malloc(sizeof(SM_FileHandle));	   // TODO: free it .. Done in shutdownBufferPool 
	openPageFile((char *)pageFileName, pmi->fHandle);					   // TODO: close it .. Done in shutdownBufferPool
	
	pmi->hm = hmInit();								  // TODO: free it, use hmDestroy(hm) .. Done in shutdownBufferPool
	bm->mgmtData = pmi;

	pmi->stats = malloc(sizeof(Stats));								   // TODO: free it .. Done in shutdownBufferPool
	pmi->stats->pageNums = malloc(sizeof(int) * numPages);			   // TODO: free it .. Done in shutdownBufferPool
	pmi->stats->dirtyFlags = calloc(sizeof(int) * numPages, sizeof(int)); // TODO: free it .. Done in shutdownBufferPool
	pmi->stats->fixCounts = calloc(sizeof(int) * numPages, sizeof(int));  // TODO: free it .. Done in shutdownBufferPool
	memset(pmi->stats->pageNums, NO_PAGE, sizeof(int) * numPages);
	pmi->stats->writeCnt = pmi->stats->readCnt = 0;
	pmi->tail = pmi->head = NULL;

	return RC_OK;
}

// Shuting down the buffer pool and release all the resources, writing all dirty pages back to disk
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	if (bm == NULL || bm->mgmtData == NULL) {
        return RC_BUFFER_NOT_INIT;
    }
	Node *temp,*node = bm->mgmtData->head;
	PageFrameInfo *framePtr;
	while (node != NULL)
	{
		framePtr = (PageFrameInfo *)(node->data);
		//write it back if its dirty
		if (framePtr->isDirty)
			writeDirtyPage(bm, framePtr);
		destroyFrame(framePtr);
		temp = node;
		node = node->next;
		free(temp);
	}
	hmDestroy(bm->mgmtData->hm);
	closePageFile(bm->mgmtData->fHandle);
	freeResource(bm->mgmtData);
	return RC_OK;
}

// writting all dirty pages back to file
RC forceFlushPool(BM_BufferPool *const bm)
{
	if (bm == NULL || bm->mgmtData == NULL) {
        return RC_BUFFER_NOT_INIT;
    }
	Node *temp = bm->mgmtData->head;
	PageFrameInfo *framePtr;
	while (temp != NULL)
	{
		framePtr = (PageFrameInfo *)(temp->data);
		if (framePtr != NULL && framePtr->isDirty && framePtr->fixCnt == 0)
			writeDirtyPage(bm, framePtr);
		temp = temp->next;
	}
	return RC_OK;
}

// pin a page and return a pointer to it's data and page number
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	if (bm == NULL || bm->mgmtData == NULL) {
        return RC_BUFFER_NOT_INIT;
    }
	PageFrameInfo *framePtr = getFrameByPageNumber(bm, pageNum);
	if (framePtr == NULL)
		framePtr = initFrame(); // TODO: free, use destroyFrame(frame) .. Done in shutdownBufferPool, replacement
	
	RC err = execStrategy(bm, framePtr, pageNum);
	if (err == RC_OK)
	{
		page->data = framePtr->pageData;
		incDecfixCnt(bm, framePtr, true); // true means increase
		page->pageNum = framePtr->pageNum;
		return RC_OK;
	}
	else{
		destroyFrame(framePtr);
		return err;
	}
}

// release the page
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrameInfo *framePtr = getFrameByPageNumber(bm, page->pageNum);
	if (framePtr == NULL)
	{
		return RC_ERROR_NO_PAGE;
	}
	incDecfixCnt(bm, framePtr, false); // false means decrease
	return RC_OK;
}

// mark a page as dirty to be written back to disk eventually
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrameInfo *framePtr = getFrameByPageNumber(bm, page->pageNum);
	if (framePtr == NULL)
	{
		return RC_ERROR_NO_PAGE;
	}
	setDirty(bm, framePtr, true);
	return RC_OK;
}

// force write single page to disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrameInfo *framePtr = getFrameByPageNumber(bm, page->pageNum);
	if (framePtr == NULL)
		return RC_ERROR_NO_PAGE;
	if (framePtr->fixCnt > 0)
		return RC_ERROR_NOT_FREE_FRAME;
	if (!framePtr->isDirty)
		return RC_OK;
	return writeDirtyPage(bm, framePtr);
}

// return array of the current page numbers
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	return bm->mgmtData->stats->pageNums;
}

// return dirty flags for all the currently loaded pages
bool *getDirtyFlags(BM_BufferPool *const bm)
{
	return bm->mgmtData->stats->dirtyFlags;
}

// return fix count for each page currently loaded
int *getFixCounts(BM_BufferPool *const bm)
{
	return bm->mgmtData->stats->fixCounts;
}

// return read disk IO count since the time the pool was initialized
int getNumreadCnt(BM_BufferPool *const bm)
{
	return bm->mgmtData->stats->readCnt;
}

// return write disk IO count since the time the pool was initialized
int getNumwriteCnt(BM_BufferPool *const bm)
{
	return bm->mgmtData->stats->writeCnt;
}
