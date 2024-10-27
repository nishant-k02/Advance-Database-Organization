#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"

//initialize the page queue
void initPageQueue(BM_BufferPool *const bm){
	//init queue members which doesn't need memory allocation
	bm->buffQueue->framesCnt = bm->numPages;
	bm->buffQueue->filledFramesCnt = 0;
    //allocate memory to linked list
    //PageInfo *addNewPage[bufferPool->numPages];
    PageInfo **pageList = malloc(bm->numPages * sizeof(PageInfo *));
    for(int i=0;i<bm->numPages;i++){
        pageList[i] = (PageInfo *)malloc(sizeof(PageInfo));
    }
	
    for(int i=0;i<bm->numPages;i++){
        //init the pageinfo members
        pageList[i]->data=(char *)calloc(PAGE_SIZE, sizeof(char));
        pageList[i]->frNum=i;
        pageList[i]->pageNum=NO_PAGE;
        pageList[i]->isDirty=false;
        pageList[i]->refBit=false;
        pageList[i]->fixedCnt=0;
        if(i==0){ //if first node
            pageList[i]->prev=NULL;
            pageList[i]->next=pageList[i+1];
			bm->buffQueue->head = pageList[i];
        }else if (i == bm->numPages-1)
        {
            pageList[i]->prev=pageList[i-1];
            pageList[i]->next=NULL;
			bm->buffQueue->tail = pageList[i];
        }else{
            pageList[i]->next=pageList[i+1];
            pageList[i]->prev=pageList[i-1];
        }
    }
}

// Function to check if the buffer pool is empty
bool isBufferEmpty(BM_BufferPool *const bm)
{
	if (bm->buffQueue->filledFramesCnt == 0)
		return true;
	else
		return false;

}

//create new node for the bufferpool
PageInfo *getNewNode(const PageNumber pageNumber)
{

	PageInfo *node = (PageInfo *)malloc(sizeof(PageInfo));

	node->data = (char *)calloc(PAGE_SIZE, sizeof(char));
	node->frNum = 0;
	node->pageNum = pageNumber;
	node->fixedCnt = 1;
	node->isDirty = false;
	node->prev = CHECK_NULL;
	node->next = CHECK_NULL;
	return node;
}

// Function to remove a page from the buffer pool
RC dequeue(BM_BufferPool *const bm)
{
	// Check if the buffer queue is empty

	if (isBufferEmpty(bm))
	{
		return RC_QUEUE_IS_EMPTY;
	}

	// Initialize a pointer to the head of the buffer queue

	PageInfo *node = bm->buffQueue->head;

	int f = 0;
	while (f < bm->buffQueue->filledFramesCnt)
	{
		// If this is the last frame in the queue

		if (f == (bm->buffQueue->filledFramesCnt - 1))
		{
			// Update the tail pointer to this frame
			bm->buffQueue->tail = node;
		}
		else
		{
			// Move to the next frame in the queue
			node = node->next;
		}
		++f;
	}

	int endPage;
	node = bm->buffQueue->tail;
	int delPage = 0;

	 // Loop through all the frames in the buffer queue
	for (int k = 0; k < bm->buffQueue->framesCnt; ++k)
	{
		if(node->fixedCnt == 0){
			if (bm->buffQueue->tail->pageNum == node->pageNum){
				 // If the tail frame is to be removed, update the tail pointer
				bm->buffQueue->tail = (bm->buffQueue->tail->prev);
				delPage = node->pageNum;
				bm->buffQueue->tail->next = NULL;
				break;
			}
			else{
				 // Remove the frame from the middle of the queue
				node->prev->next = node->next;
				delPage = node->pageNum;
				node->next->prev = node->prev;
				break;
			}
		}else{
			// If the frame is still in use, move to the previous frame
			endPage = node->pageNum;
			node = node->prev;
		}
	}
	  // If the removed frame is dirty, write it back to storage
	if (node->isDirty == 1)
	{
		writeBlock(node->pageNum, bm->fh, node->data);
		bm->writesCnt++;
	}
	  // Check if the tail page number matches the previously noted end page
	if (bm->buffQueue->tail->pageNum == endPage)
	{
		return RC_OK;
	}
	// Decrement the count of filled frames in the buffer queue
	bm->buffQueue->filledFramesCnt--;
	return delPage;
}

// Function to add a page to the buffer pool
RC enqueue(BM_PageHandle *const pageHandle, const PageNumber pageNumber, BM_BufferPool *const bm)
{
	int deletedPage = NO_PAGE;

	   // Create a new node for the page to be added
	PageInfo *node = getNewNode(pageNumber);
	if ((*bm->buffQueue).framesCnt == (*bm->buffQueue).filledFramesCnt)
	{
		deletedPage = dequeue(bm);
	}
	if (isBufferEmpty(bm))
	{
		 // Read the page from storage into the new node
		
		readBlock(node->pageNum, bm->fh, node->data);
		bm->readsCnt++;

		  // Update the page handle with the page number and data
		(*pageHandle).pageNum = pageNumber;
		(*pageHandle).data = node->data;

		 // Link the new node at the front of the queue
		(*node).next = bm->buffQueue->head;
		(*node).frNum = bm->buffQueue->head->frNum;
		(*bm->buffQueue).head = node;
		bm->buffQueue->head->prev = node;
		(*node).pageNum = pageNumber;
	}
	else
	{
		// Read the page from storage into the new node
		readBlock(pageNumber, bm->fh, node->data);
		
		// Determine the frame number for the new node
		if (deletedPage == NO_PAGE)
		{
			(*node).frNum = bm->buffQueue->head->frNum + 1;
		}
		else
		{
			(*node).frNum = deletedPage;
		}
		// Update the page handle with the page number and data
		(*pageHandle).data = node->data;
		bm->readsCnt++;

		// Link the new node at the front of the queue
		(*node).next = bm->buffQueue->head;
		bm->buffQueue->head->prev = node;
		(*bm->buffQueue).head = node;
		(*pageHandle).pageNum = pageNumber;
	}
	 // Increment the count of filled frames in the buffer queue
	bm->buffQueue->filledFramesCnt++;
	return RC_OK;
}

// Function to pin a page in the buffer pool (FIFO strategy)
RC stratForFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	bool isPageAvailable = false;
	int dummyCode = -1;
	PageInfo *node = bm->buffQueue->head;
	PageInfo *newNode = (PageInfo *)malloc(sizeof(PageInfo));

	for(int i=0;i < bm->numPages;i++)
	{
		  // Check if the page is already available in the buffer
		if (pageNum == node->pageNum)
		{
			isPageAvailable = true;
			break;
		}
		else
		{
			node = node->next;
		}
	}

	switch (isPageAvailable)
	{

	 // Page is already in the buffer, increment its fixed page count	
	case 1:
	{
		node->fixedCnt++;
		page->pageNum = pageNum;
		page->data = node->data;
		return RC_OK;
	}
	}

	node = bm->buffQueue->head;

	int k = 0;
	while (bm->buffQueue->filledFramesCnt < bm->buffQueue->framesCnt)
	{
		// Find an empty frame in the buffer to load the page
		if (node->pageNum == -1)
		{
			bm->buffQueue->filledFramesCnt++;
			(*node).isDirty = false;
			(*node).fixedCnt = 1;
			page->pageNum = node->pageNum = pageNum;

			// Read the page from storage into the buffer

			readBlock((*node).pageNum, bm->fh, node->data);
			bm->readsCnt++;
			page->data = (*node).data;

			dummyCode = 0;
			break;
		}
		else
		{
			node = node->next;
		}
		k++;
	}

	newNode->isDirty = false;
	newNode->fixedCnt = 1;
	newNode->data = NULL;
	page->pageNum = newNode->pageNum = pageNum;
	newNode->prev = (*bm->buffQueue).tail;
	newNode->next = NULL;
	node = (*bm->buffQueue).head;

	if (dummyCode == 0)
	{
		
		return RC_OK;
	}

	int f = 0;
	while (f < bm->numPages)
	{
		 // Find the first frame with fixed page count equal to 0
		if (node->fixedCnt == RC_OK)
		{
			break;
		}
		else
		{
			node = (*node).next;
		}
		++f;
	}
	if(node==NULL)
		return RC_BUFFER_POOL_NOT_FREE;

	if (bm->numPages == 0)
	{
		return RC_BUFFER_POOL_IS_EMPTY;
	}

	if (node->isDirty == 1)
	{
		writeBlock((*node).pageNum, bm->fh, (*node).data);
		bm->writesCnt++;
	}

	if ((*bm->buffQueue).tail == node)
	{
		(*bm->buffQueue).tail = node->prev;
		newNode->prev = bm->buffQueue->tail;
	}
	else if (bm->buffQueue->head == node)
	{
		bm->buffQueue->head->prev = NULL;
		bm->buffQueue->head = bm->buffQueue->head->next;
	}
	else
	{
		node->next->prev = node->prev;
		node->prev->next = node->next;
	}

	newNode->frNum = node->frNum;
	newNode->data = node->data;
	 // Read the requested page from storage into the new node
	readBlock(pageNum, bm->fh, newNode->data);
	bm->readsCnt++;
	page->data = (*newNode).data;

	// Update the tail of the buffer queue
	bm->buffQueue->tail->next = newNode;
	bm->buffQueue->tail = newNode;

	return RC_OK;
}
// Function to pin a page in the buffer pool (LRU strategy)
RC stratForLRU(BM_BufferPool *const bm, BM_PageHandle *const pageHandle, const PageNumber pageNumber)
{
	bool isPageAvailable = 0;
	PageInfo *node = bm->buffQueue->head;

	for (int i = 0; i < bm->numPages; i++)
	{
			if (node->pageNum == pageNumber)
			{
				isPageAvailable = true;
				break;
			}
			else
			{
				node = node->next;
			}
	}
	if (isPageAvailable)
	{
		(*node).fixedCnt++;
		(*pageHandle).pageNum = pageNumber;
		(*pageHandle).data = (*node).data;

		if ((*bm->buffQueue).head == node)
		{
			(*node).next = bm->buffQueue->head;
			(*bm->buffQueue).head = node;
			(*bm->buffQueue).head->prev = node;
		}
		else
		{
			node->prev->next = node->next;
			if (node->next)
			{
				(*node).next->prev = node->prev;
				if (bm->buffQueue->tail == node)
				{
					(*bm->buffQueue).tail->next = NULL;
					(*bm->buffQueue).tail = node->prev;
				}
				node->prev = NULL;
				node->next = bm->buffQueue->head;
				node->next->prev = node;
				(*bm->buffQueue).head = node;
			}
		}
	}
	else
	{
		enqueue(pageHandle, pageNumber, bm);
	}
	return RC_OK;
}

// Function to pin a page in the buffer pool (CLOCK strategy)
RC stratForClock(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    bool isPageAvailable = false;
    PageInfo *node = bm->buffQueue->head;
    PageInfo *clockHand = bm->buffQueue->head;

    // Check if the page is already in the buffer
    for (int i = 0; i < bm->numPages; i++) {
        if (node->pageNum == pageNum) {
            isPageAvailable = true;
            break;
        }
        node = node->next;
    }

    if (isPageAvailable) {
        node->fixedCnt++;
        node->refBit = true;
        page->pageNum = pageNum;
        page->data = node->data;
        return RC_OK;
    }

    // Page not in buffer, find a frame to replace
    while (true) {
        if (clockHand->fixedCnt == 0) {
            if (clockHand->refBit) {
                clockHand->refBit = false;
            } else {
                // Found a page to replace
                if (clockHand->isDirty) {
                    writeBlock(clockHand->pageNum, bm->fh, clockHand->data);
                    bm->writesCnt++;
                }
                clockHand->pageNum = pageNum;
                clockHand->fixedCnt = 1;
                clockHand->isDirty = false;
                clockHand->refBit = true;
                readBlock(pageNum, bm->fh, clockHand->data);
                bm->readsCnt++;
                page->pageNum = pageNum;
                page->data = clockHand->data;
                return RC_OK;
            }
        }
        clockHand = clockHand->next;
        if (clockHand == NULL) {
            clockHand = bm->buffQueue->head;
        }
    }

    return RC_BUFFER_POOL_NOT_FREE;
}


// Function to pin a page in the buffer pool (generic)
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	if(pageNum<0)
		return RC_BUFFER_POOL_PINPAGE_ERROR;
	switch (bm->strategy)
	{
	case (RS_FIFO):
		return stratForFIFO(bm,page,pageNum);
	case (RS_LRU):
		return stratForLRU(bm,page,pageNum);
	case (RS_CLOCK):
		return stratForClock(bm,page,pageNum);
	}
}

// Function to initialize the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->writesCnt = bm->readsCnt = 0;
	bm->fh = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	char *bufferSize = (char *)calloc(numPages, sizeof(char) * PAGE_SIZE);
	bm->mgmtData = bufferSize;
	openPageFile(bm->pageFile, bm->fh);
	bm->buffQueue = (BufferPool *)malloc(sizeof(BufferPool));
	initPageQueue(bm);
	return RC_OK;
}

// Function to shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageInfo *node =bm->buffQueue->head;
	for (int i =0;i < bm->buffQueue->filledFramesCnt;i++)
	{
		if (node->fixedCnt == 0 && node->isDirty)
		{
			writeBlock(node->pageNum, bm->fh, node->data);
			bm->writesCnt++;
			node->isDirty = false;
		}
		node = node->next;
	}

	closePageFile(bm->fh);
	return RC_OK;
}

// Function to force flushing of all dirty pages to disk
RC forceFlushPool(BM_BufferPool *const bm)
{
	PageInfo *node = (*bm->buffQueue).head;

	for (int i = 0; i < bm->buffQueue->framesCnt; i++)
	{
		if ((node->fixedCnt == 0) && (node->isDirty))
		{
			writeBlock(node->pageNum, bm->fh, node->data);
			bm->writesCnt++;
			node->isDirty = false;
		}
		node = node->next;
	}
	return RC_OK;
}

// Function to unpin a page in the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	bool pageFound = false;
	PageInfo *node = bm->buffQueue->head;

	for (int i = 0; i < bm->numPages; i++)
	{
		if ((*node).pageNum == (*page).pageNum)
		{
			pageFound=true;
			if (node->fixedCnt > 0)
				node->fixedCnt--;
			return RC_OK;
		}
		node = (*node).next;
	}

	if(!pageFound)
		return RC_READ_NON_EXISTING_PAGE;

	//return RC_OK;
}

// Function to force a page to disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageInfo *node = (*bm->buffQueue).head;
	for (int i = 0; i < bm->numPages; i++)
	{
		if (node->pageNum == page->pageNum){
			if (writeBlock(node->pageNum, bm->fh, node->data) == RC_OK){
				bm->writesCnt++;
				return RC_OK;
			}
			else
				return RC_WRITE_FAILED;
		}
		node = (*node).next;
	}

	if (bm->numPages == 0)
		return RC_BUFFER_POOL_IS_EMPTY;

	//return RC_OK;
}

// Function to mark a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageInfo *node = (*bm->buffQueue).head;
	if ((*bm).numPages == 0)
	{
		return RC_BUFFER_POOL_IS_EMPTY;
	}
	for (int i = 0; i < (*bm).numPages; i++)
	{
		if ((*node).pageNum == (*page).pageNum){
			(*node).isDirty = true;
			return RC_OK;
		}
		node = node->next;
	}

	//return RC_OK;
}

// Get the contents of frames in the buffer pool
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	PageInfo *node;
	PageNumber(*numPageFile)[bm->numPages];
	numPageFile = calloc(bm->numPages, sizeof(PageNumber));

	for (int i=0;i < bm->numPages;i++)
	{
		node = bm->buffQueue->head;
		while (node != NULL)
		{
			if (node->frNum == i)
			{
				(*numPageFile)[i] = node->pageNum;
				break;
			}
			node = node->next;
		}
	}

	return *numPageFile;
}



// Get the dirty flags of pages in the buffer pool

bool *getDirtyFlags(BM_BufferPool *const bm)
{
    PageInfo *node = bm->buffQueue->head;
	bool *scanDirtyFlags = calloc(bm->numPages, sizeof(bool));

	while (node != NULL) {
        if (node->frNum < bm->numPages) {
            scanDirtyFlags[node->frNum] = node->isDirty;
        }
        node = node->next;
    }
    return scanDirtyFlags;
}

// Get the fix counts of pages in the buffer pool
int *getFixCounts(BM_BufferPool *const bm)
{
	int *fixCounts = calloc(bm->numPages, sizeof(int));
    PageInfo *node = bm->buffQueue->head;
	
    while (node != NULL) {
        if (node->frNum < bm->numPages) {
            fixCounts[node->frNum] = node->fixedCnt;
        }
        node = node->next;
    }

    return fixCounts;
}

// Get the number of read I/O operations
int getNumReadIO(BM_BufferPool *const bm)
{
	return bm->readsCnt;
}

// Get the number of write I/O operations
int getNumWriteIO(BM_BufferPool *const bm)
{
	return bm->writesCnt;
}