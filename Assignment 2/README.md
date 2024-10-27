# Assignment 2 Readme File

## RUNNING THE SCRIPT  

1\) Go to Project root (Assignment 2\) using Terminal.

2\) Type `ls` to list the files and check that we are in the correct directory.

3\) Type `make clean` to delete old compiled .o files.

4\) Type `make` to compile all project files. 

5\) Type `./test_assign2_1` to run the executable file.


## SOLUTION DESCRIPTION  

We are implementing a simple buffer manager that works ontop of what we created in `assignment_1`. The buffer manager manages the `page frames (frames)`. The combination of a page file and the page frames from that file is a `Buffer Pool`. We have implemented FIFO (First In First Out), LRU (Least Recently Used and CLOCK page replacement algorithms.

### 1\. BUFFER POOL FUNCTIONS

The buffer pool functions are used to create and initialize buffer pool in memory, close buffer pool and write all dirty pages back to disk. We use functions like- initBufferPool(...), forceFlushPool(...), forceFlushPool(...) for above tasks.  

initBufferPool(...)  
--> this function is used to create and initialize the bufferpool to manage pages in file.  
--> We use parameters such as bm- which is a pointer pointing to buffer pool structure, PageFileName- The name of selected file, numPages- total pages stored in bufferpool, strategy- strategy used to replace page when pool is full for instance FIFO, LRU.  
-->It will open a pagefile and check for success, initialize attributes like number of pages, filename, counters and also initialize metadata for buffer.

shutdownBufferPool(...)  
-->This functions clears out the buffer pool and shuts it down.  
-->The parameter to the function used is pointer to the bufferpool to be deleted  i.e bm.  
-->If pages not pinned and are dirty i.e if pages in memory are edited it writes the edited data back to disk.  
--> It clears all metadata, free the resources and shuts down the bufferpool.

forceFlushPool(...)  
-->Dirty pages are the pages that are edited or modified in memory.   
-->This function checks if page is dirty, then checks if it is pinned.  
-->On looping through all pages in bufferpool, if page is dirty and unpinned i.e not in use by client the checks condition (fixCnt == 0) then it delete the page.  
-->Return RC_OK if all pages are successfully flushed else returns zero.

### 2\. PAGE MANAGEMENT FUNCTIONS

The page management functions include pinning pages (pinPage) – the ability to retrieve a copy of a page into the buffer pool, unpinning pages (unpinPage), which helps to reduce the number of pinned pages in the buffer pool, setting or marking a page as dirty and flushing or writing back a particular page to disk.  

pinPage(...)  
--> This function pins the page number pageNum i.e, it reads the page from the page file present on disk and stores it in the buffer pool.  
--> Before pinning a page, it checks if the buffer pool has any empty space. If it has an empty space, then the page frame can be stored in the buffer pool else a page replacement strategy has to be used in order to replace a page in the buffer pool.  
--> We have implemented FIFO, LRU and CLOCK page replacement strategies which are used while pinning a page.  


unpinPage(...)  
--> Its purpose is to unpin a page from the buffer pool by decreasing the pin count (fixed count) of the specified page.  


makeDirty(...)  
--> The function sets a flag indicating that the page in the buffer pool which the function deals with has been changed dirty but not flushed on disk.

forcePage(....)  
--> The aim of this function is to force a page to be written back to disk from the buffer pool.   
--> It finds the page that matches the given page number by iterating through the buffer pool.  
--> Once the matching page is found, it writes the page’s data back to the disk.  


### 3\. STATISTICS FUNCTIONS

Statistics functions gather metadata about the bufferpool. It gives information about total pages in bufferpool, page numbers, dirty status of pages, fixcounts etc.

getFrameContents(...)  
--> Returns the list of all contents of the pages stored in the buffer pool.

getDirtyFlags(...)  
--> Returns the list of all dirty pages stored in the buffer pool

getFixCounts(...)   
--> Returns the fix count of all pages stored in the buffer pool.

getNumReadIO(...)  
--> The counts of all read I/O operation done on the buffer pool are obtained from this function.  

getNumWriteIO(...)  
--> The number of write I/O operations performed on the buffer pool are retrieved using this function.  