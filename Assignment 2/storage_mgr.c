#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "dberror.h"
#include "storage_mgr.h"

//file pointer
FILE *filePtr;

//init the file pointer to null
void initStorageManager(void){
	filePtr = NULL;
	printf("init: Storage Manager\n");
}


//create new page file named fileName
RC createPageFile (char *fileName){
    filePtr = fopen(fileName,"w+");
    if(filePtr==NULL) //check if pageFile is created or not
        return RC_FILE_NOT_FOUND;
    else{
    	//initilize the block with '\0' bytes
		//char *memBlock = malloc(PAGE_SIZE * sizeof(char));
    	//memset(memBlock, '\0', PAGE_SIZE);
		SM_PageHandle memBlock = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));
		//write the block to the file
		fwrite(memBlock, sizeof(char), PAGE_SIZE, filePtr);
		//release allocated memory after memory after writing it into file
		free(memBlock);
		fclose(filePtr);
		return RC_OK;
    }
}

//opens an existing page file: fileName
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	filePtr = fopen(fileName, "r+");
	if(filePtr == NULL)
		return RC_FILE_NOT_FOUND;
	fHandle->curPagePos = 0;
	fHandle->fileName = fileName;
	//calculate the totalNumPages
	fseek(filePtr, 0, SEEK_END);
	fHandle->totalNumPages = ftell(filePtr) / PAGE_SIZE;
	//set the cursor to the start of page file
	fseek(filePtr,0,SEEK_SET);
	return RC_OK;
}

//close an existing page file using file handle
RC closePageFile(SM_FileHandle *fHandle)
{
	if (filePtr==NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	if (fclose(filePtr) == 0){
		filePtr=NULL;
		return RC_OK;
	}
	else
		return RC_FILE_NOT_FOUND;
}

//delete a page file
RC destroyPageFile(char *fileName)
{
	if(remove(fileName) == 0){
		filePtr=NULL;
		return RC_OK;
	}
	else
		return RC_FILE_NOT_FOUND;
}

//read block at position: pageNum of the file from given file handle and store its content in page handle
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(filePtr==NULL)
        return RC_FILE_NOT_FOUND;
	if(pageNum > (fHandle->totalNumPages - 1) || pageNum < 0)
		return RC_READ_NON_EXISTING_PAGE;
	if(fseek(filePtr,pageNum * PAGE_SIZE, SEEK_SET) != 0)
		return RC_READ_NON_EXISTING_PAGE;
	fread(memPage,sizeof(char),PAGE_SIZE,filePtr);
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

//getter method for curPagePos of file handle
int getBlockPos(SM_FileHandle *fHandle){
	return (fHandle->curPagePos);
}

//setter method for curPagePos of file handle
void setCurrentPagePos(SM_FileHandle *fHandle, int pos){
	fHandle->curPagePos = pos;
}

//read first block of the file from given file handle: fHandle and store its content in page handle
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(0,fHandle,memPage);
}

/*read previous block relative to the current page position in the file handle
	and store its content in page handle*/
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle) - 1, fHandle, memPage);
}

/*read current block relative to the current page position in the file handle
	and store its content in page handle*/
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle),fHandle,memPage);
}

/*read next block relative to the current page position in the file handle
	and store its content in page handle*/
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle) + 1, fHandle, memPage);
}

//read last block of the file from given file handle: fHandle and store its content in page handle
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

//write block at position: pageNum of the file from given file handle and store its content in page handle
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(filePtr==NULL)
        return RC_FILE_NOT_FOUND;
	//ensure that pageNum is in valid range
	if (pageNum > (fHandle->totalNumPages) || pageNum < 0)
		return RC_WRITE_FAILED;
	//ensure that fHandle is initilized 
	//check if the file pos. indicator successfully the given pageNum
	else if (fseek(filePtr, (PAGE_SIZE * pageNum), SEEK_SET) != 0){
		return RC_WRITE_FAILED;
	}
	//write content from memPage to file for the given pageNum
	fwrite(memPage, sizeof(char), PAGE_SIZE, filePtr);
	//update the current page pos
	//take the cursor to the end of the to calculate number of pages
	fseek(filePtr, 0, SEEK_END);
	fHandle->totalNumPages = ftell(filePtr) / PAGE_SIZE;
	fseek(filePtr, 0, SEEK_SET);
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

/*read current block relative to the current page position in the file handle
	and store its content in page handle*/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	int currentBlock = getBlockPos(fHandle);
	return writeBlock(currentBlock,fHandle,memPage);
}

//append the empty block at the end of file
RC appendEmptyBlock (SM_FileHandle *fHandle) {
	if(filePtr == NULL)
		return RC_FILE_NOT_FOUND;
	if(fseek(filePtr, 0, SEEK_END) == 0){
		SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));
		fwrite(emptyBlock,sizeof(char),PAGE_SIZE, filePtr);
		setCurrentPagePos(fHandle,(fHandle->totalNumPages));
		fHandle->totalNumPages++;
		free(emptyBlock);
		return RC_OK;
	}else
		return RC_WRITE_FAILED;
}

//ensure that file has atleast capacity of the given number of pages
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	if(filePtr==NULL)
		return RC_FILE_NOT_FOUND;
	while(fHandle->totalNumPages < numberOfPages){
		appendEmptyBlock(fHandle);
	}
	return RC_OK;
}