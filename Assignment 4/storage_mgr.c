#include "storage_mgr.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>


// Get the file descriptor from the file handle
int getFd(SM_FileHandle *fHandle) {
  int fd = *((int*)fHandle->mgmtInfo);
  return fd;
}


// Set the file descriptor from the file handle
void setFD(SM_FileHandle *fHandle, int fd) {
  fHandle->mgmtInfo = malloc(sizeof(int));
  *((int*)fHandle->mgmtInfo) = fd;
}

//print error while while initializing
void throwError() {
  printf("OOPS!!! Something went wrong with file system, (%s).\n", strerror(errno));
  exit(EXIT_FAILURE);
}
/*
  initialize the storage manager
  create the database directory to some pre configured path
  change directory to that path
  this function will crash whenever there is an error, because if something went wrong, nothing can be done afterward
*/
void initStorageManager (void) {
  struct stat state;
  int ret;
  // folder does not Exists, create it
  if (stat(PATH_DIR, &state) == -1) {
    if(mkdir(PATH_DIR , DEFAULT_MODE) == -1)
      return throwError();
  }
  if (chdir(PATH_DIR) == -1)
    return throwError();
}


// Create a file and write an empty block to it
RC createPageFile (char *fileName) {
  /*
   S_IRUSR : Read permission to user; S_IWUSR : Write permissions to users
   S_IRGRP : Read permission to group; S_IRGRP : Write permission to group
   S_IROTH : Read permission to others; S_IWOTH : Write permission to others
  */
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH | O_TRUNC;
  int fd = creat(fileName, mode);
  if (fd == -1)
    return RC_FS_ERROR;

  // write an empty block to file upon creation
  SM_PageHandle memPage = (char *) calloc(PAGE_SIZE, sizeof(char));
  size_t writeSize = pwrite(fd, memPage, (size_t) PAGE_SIZE, (off_t) 0);
  if (writeSize == -1) {
    free(memPage);
    return RC_FS_ERROR;
  }
  free(memPage);
  // close the file, since it will be reopened when needed
  if(close(fd) == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


// Open a file and copy it's properties to the file handle
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
  if(access(fileName, F_OK) == -1) {
    return RC_FILE_NOT_FOUND;
  }
  int fd = open(fileName, O_RDWR);
  if (fd == -1) {
    return RC_FS_ERROR;
  }

  struct stat state;
  stat(fileName, &state);
  fHandle->fileName = fileName;
  fHandle->curPagePos = 0;
  fHandle->totalNumPages = (int) (state.st_size/PAGE_SIZE);
  setFD(fHandle, fd);
  return RC_OK;
}

/* Close an opened file */
RC closePageFile (SM_FileHandle *fHandle) {
  if (fHandle->mgmtInfo == NULL) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  if (close(getFd(fHandle)) == -1) {
    free(fHandle->mgmtInfo);
    return RC_FS_ERROR;
  }
  free(fHandle->mgmtInfo);
  return RC_OK;
}


/* Remove a file by it's name */
RC destroyPageFile (char *fileName) {
  if(access(fileName, F_OK) == -1) {
    return RC_FILE_NOT_FOUND;
  }
  if (unlink(fileName) == -1)
    return RC_FS_ERROR;
  return RC_OK;
}


/* Read a block from a file by the block number */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (pageNum < 0)
    return RC_BLOCK_POSITION_ERROR;
  if (fHandle->mgmtInfo == NULL)
    return RC_FILE_HANDLE_NOT_INIT;
  if (pageNum >= fHandle->totalNumPages) {
    RC err = ensureCapacity(pageNum, fHandle);
    if (err != RC_OK)
      return err;
  }
  // Choosing pread/pwrite is more stable in performance and faster than seek+read/write and it is multi thread friendly
  if (pread(getFd(fHandle), memPage, (size_t) PAGE_SIZE, (off_t) pageNum * PAGE_SIZE) == -1)
    return RC_FS_ERROR;
  fHandle->curPagePos = pageNum + 1;
  return RC_OK;
}


/* get the current block position */
int getBlockPos (SM_FileHandle *fHandle) {
  return fHandle->curPagePos;
}


/* Read the first Block of the File */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(0, fHandle, memPage);
}


/* Read the block that is previous to current position */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(getBlockPos(fHandle) - 1, fHandle, memPage);
}


/* Read the block that is currently pointed to by the page position pointer */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(getBlockPos(fHandle), fHandle, memPage);
}


/* Read the block that is next to current position */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(getBlockPos(fHandle) + 1, fHandle, memPage);
}


/* Read the last block in the file */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}


/* write a block to a file by the block number */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (fHandle->mgmtInfo == NULL)
    return RC_FILE_HANDLE_NOT_INIT;
  // here is only > so that we can append to the end of the file but we can not skip a block
  if (pageNum < 0 || pageNum > fHandle->totalNumPages)
    return RC_BLOCK_POSITION_ERROR;
  // Choosing pread/pwrite is more stable in performance and faster than seek+read/write and it is multi thread friendly
  size_t writeSize = pwrite(getFd(fHandle), memPage, (size_t) PAGE_SIZE, (off_t) pageNum * PAGE_SIZE);
  if (writeSize == -1)
    return RC_WRITE_FAILED;
  //if we added new block
  if (pageNum == fHandle->totalNumPages)
    fHandle->totalNumPages++;
  fHandle->curPagePos = pageNum + 1;
  return RC_OK;
}


/* write the block that is currently pointed to by the page position pointer */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return writeBlock(getBlockPos(fHandle), fHandle, memPage);
}


/* Extend the file by adding new empty block */
RC appendEmptyBlock (SM_FileHandle *fHandle) {
  if (fHandle->mgmtInfo == NULL)
    return RC_FILE_HANDLE_NOT_INIT;
  SM_PageHandle emptyBlock = (char *) calloc(PAGE_SIZE, sizeof(char));
  RC err;
  err = writeBlock(fHandle->totalNumPages, fHandle, emptyBlock);
  free(emptyBlock);
  return err;
}


//ensure that file has atleast capacity of the given number of pages
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
  while(fHandle->totalNumPages < numberOfPages){
		appendEmptyBlock(fHandle);
	}
	return RC_OK;
}