#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testMultiplePageContent(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testMultiplePageContent();

  return 0;
}

//test multiple page content
void testMultiplePageContent(void){
	SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");

  TEST_CHECK(appendEmptyBlock(&fh));
  printf("append last page\n");
  
  TEST_CHECK(readLastBlock(&fh,ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("last block was empty\n");

  //read the last block again but using readCurrentBlock this time
  TEST_CHECK(readCurrentBlock(&fh,ph));
  printf("last block was empty (using readCurrentBlock)\n");
  
  //write currentblock 
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeCurrentBlock (&fh,ph));
  printf("writing current block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readCurrentBlock(&fh,ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("read current block\n");
  
  //read next block after last page should give an error
  ASSERT_TRUE((readNextBlock(&fh,ph) != RC_OK), "reading page after the last page should cause an error");

  //read previous block
  TEST_CHECK(readPreviousBlock(&fh,ph));
  printf("read previous block\n");

  //read next block
  TEST_CHECK(readNextBlock(&fh,ph));
  printf("read next block\n");

  //read first block using readBlock
  TEST_CHECK(readBlock(0,&fh,ph));
  printf("read first block (using readBlock)\n");

  //read previous block after first page should give an error
  ASSERT_TRUE((readPreviousBlock(&fh,ph) != RC_OK), "reading page before the first page should cause an error");

  //if file has less block then the given pageNum it should give an error
  ASSERT_TRUE((readBlock(4,&fh,ph) != RC_OK), "reading page - pageNum exceeds file should cause an error");

  //ensure file has 4 pages before reading it
  TEST_CHECK(ensureCapacity(4,&fh));
  printf("ensure file has 4 pages\n");

  //read the 3rd page 
  TEST_CHECK(readBlock(3,&fh,ph));
  printf("reading the 4th page\n");
  
  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));
  
  TEST_DONE();
}
