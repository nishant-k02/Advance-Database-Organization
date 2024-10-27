# we have added the additional test cases in the file named test_assign1_2.c and check running instructions in the running section about how to test these additional test cases.
# also we have added the function named setCurrentPagePos to set the curPagePos of file handle (basically it is a setter method for the curPagePos)

# RUNNING 
===========================

1. Goto the root directory of your folder in your terminal

2. Type `make clean` to remove any unwanted objects and executable files and it also removes test_assign1 and test_assign2 if they are present.

3. Type `make` to create executable “test_assign1” (for given test cases)

4. Type `make test_assign1_2` to create executable “test_assign1_2” (for extra test cases)

5. Type `./test_assign1` to run the executable file.

6. Type `./test_assign1_2` to run the executable file.



1. FILE RELATED FUNCTIONS
===========================


initStorageManager():
--> In this method, initialisation of the storage manager is done by setting the file pointer to NULL. File pointer filePtr stores the address of pagefile.

createPageFile(...)
--> This function creates a new pagefile using the fopen().
--> The new page file is opened in “w+” mode which enables both read and write.
--> We initialise the block with ‘\0’ bytes. The memory allocation is done using malloc() method.
--> At last, we free the utilised memory  using free() method and close the file with fclose()

openPageFile(...)
--> This function is used to open an existing pagefile using the fopen() method. Pagefile is open in ‘r+’ mode which enables both read and write.
--> We calculate the total number of pages in a file and add this metadata to struct SM_FileHandle.
--> fseek() function is used to set the cursor (position indicator) at the end of file. This helps in calculating the total size of the file.

closePageFile(...)
--> This method closes the file using the fclose() method.
--> The file pointer is set to null.

destroyPageFile:
--> We use remove() methode which takes filename as parameter and deletes the pagefile from memory.


2. READ RELATED FUNCTIONS
===========================

readBlock(...)
--> This method checks if the page is already existing or not, if not it returns error code RC_READ_NON_EXISTING_PAGE.
--> It then uses the fseek() to set the cursor (position indicator) to the given page number and if the fseek() is not successful then it returns `RC_WRITE_FAILED`.
--> fread() method takes parameters like memory block, file pointer, pagesize and reads data from disk to memory.

getBlockPos(...)
--> This method gets us the position of the current page from fHandle of struct SM_FileHandle that contains pagefile metadata.

readFirstBlock(...)
--> This method reads the first block of the file.
--> It calls the function readBlock(...) with the argument pageNumber as 0.

readPreviousBlock(...)
--> This method reads the previous block of the file.
--> It calls the function readBlock(...) with the argument pageNumber as  (current page position - 1).

readCurrentBlock(...):
--> This method reads the current block of the file.
--> It calls the function readBlock(...) with the argument pageNumber as  current page position(curPagePos).

readNextBlock(...):
--> This method reads the next present block of the file.
--> It calls the function readBlock(...) with the argument pageNumber as (current page position + 1).

readLastBlock(...):
--> This method reads the Last block of the file.
--> It calls the function readBlock(...) with the argument pageNumber as (total number of pages - 1).

setCurrentPagePos(...):
--> It sets the current position of page for the given SM_FileHandle


3. WRITING BLOCKS TO PAGE FILE
===============================

writeBlock():
--> It checks if the pageNum given is in valid range and if not then return `RC_WRITE_FAILED`.
--> It then uses the fseek() to set the cursor (position indicator) to the given page number and if the fseek() is not successful then return `RC_WRITE_FAILED`.
--> It then writes the given content to the file or disk.
--> After the writing is done, update the cursor to the file end using fseek. SEEK_END indicates the end of the file.
--> Then update the members of the SM_FileHandle (totalNumPages and curPagePos).

writeCurrentBlock(...):
--> It calls the function writeBlock(...) with the argument curPagePos of the SM_FileHandle.

appendEmptyBlock():
--> It returns `RC_FILE_NOT_FOUND` if the filePtr is NULL.
--> It then uses fseek() to set the cursor (position indicator) to the end of the file and if the fseek() is not successful then it returns `RC_WRITE_FAILED`.
--> It then creates the emptyBlock using calloc()(it is filled zero bytes) and then write that to the file using fwrite()
--> It then updates the members for SM_FileHandle(totalNumPages and curPagePos).
--> Frees the memory used by emptyBlock using free().

ensureCapacity():
--> It returns `RC_FILE_NOT_FOUND` if the filePtr is NULL.
--> It appends the empty blocks to the file if the file doesn’t have the provided number of pages to it. It calls appendEmptyBlock().