#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "expr.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <float.h>
#include <math.h>



// helpers
//Creates new string and copies the value
char *copyString(char *_string) {
  char *string = (char *) calloc(strlen(_string) + 1, sizeof(char));
  strcpy(string, _string);
  return string;
}

//for free schema
void freeSchemaObjects(Schema *s,bool withSchema) {
  if(s->attrNames!=NULL){
    int i;
    while(i<s->numAttr){
      free(s->attrNames[i++]);
    }
    free(s->attrNames);
  free(s->keyAttrs);
  free(s->dataTypes);
  free(s->typeLength);
  }
  //free schema itself is true
  if(withSchema){
    free(s);
  }
}


//pin the page
RC atomicPinPage(BM_BufferPool *bm, BM_PageHandle **resultPage, int pageNum) {
  BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
  RC err = pinPage(bm, page, pageNum);
  if (err != RC_OK) {
    free(page);
    return err;
  }
  //store the pointer to pinned page
  *(resultPage) = page;
  return RC_OK;
}

//unpin page
RC atomicUnpinPage(BM_BufferPool *bm, BM_PageHandle *page, bool isDirty) {
  RC err=RC_OK;
  if (isDirty)
    err = markDirty(bm, page);
  if (err == RC_OK) //if no error so far then unpin
    err = unpinPage(bm, page);
  free(page);
  return err;
}


//convert schema into string
char * stringifySchema(Schema *schema) {
  char *string = (char *) calloc(PAGE_SIZE, sizeof(char));
  //int formatLen = 8;
  char *format = "%08x"; //format hexadecimal
  char intString[9];// +1 for \0 terminator
  
  sprintf(&intString[0], format, 0); // placeholder for schema length
  sprintf(string + strlen(string), "%s,", intString);
  //append number of attributes
  sprintf(&intString[0], format, schema->numAttr);
  sprintf(string + strlen(string), "%s,", intString);
  int i=0;
  while(i < schema->numAttr) {
    //append cols
    sprintf(&intString[0], format, schema->attrNames[i]);
    sprintf(string + strlen(string), "%s,", intString);
    //append dtypes
    sprintf(&intString[0], format, schema->dataTypes[i]);
    sprintf(string + strlen(string), "%s,", intString);
    //append length of dtype
    sprintf(&intString[0], format, schema->typeLength[i]);
    sprintf(string + strlen(string), "%s,", intString);
    i++;
  }
  //append keys size
  sprintf(&intString[0], format, schema->keySize);
  sprintf(string + strlen(string), "%s,", intString);
  i = 0;
  //append keyattrs
  while(i < schema->keySize) {
    sprintf(&intString[0], format, schema->keyAttrs[i]);
    sprintf(string + strlen(string), "%s", intString);
    if (i < schema->keySize - 1) { // no DELIMITER for the last value
      strcat(string, DELIMITER);
    }
    i++;
  }
  int length = (int) strlen(string);
  length++; // include space for \0 terminator
  snprintf(intString, sizeof(intString), format, length);
  memcpy(string, &intString, 8);
  return string;
}

//Method to parse Schema (convert to string)
Schema * parseSchema(char *_string) {
  //tokenize string
  char *token = strtok(_string, DELIMITER);
  //get attr num
  token = strtok(NULL, DELIMITER);
  int numAttr = (int) strtol(token, NULL, HEXADECIMAL_BASE);

  //allocate memory for attr
  DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
  char **attrNames = (char **) malloc(sizeof(char *) * numAttr);
  int *typeLength = (int *) malloc(sizeof(int) * numAttr);
  int i;
  while(i < numAttr){
    token = strtok(NULL, DELIMITER);
    attrNames[i] = (char *) calloc(strlen(token)+1, sizeof(char)); //+1 for \0 terminator
    strcpy(attrNames[i], token); //get cols

    token = strtok(NULL, DELIMITER);
    dataTypes[i] = (DataType) strtol(token, NULL, HEXADECIMAL_BASE); //get dtypes

    token = strtok(NULL, DELIMITER);
    typeLength[i] = (int) strtol(token, NULL, HEXADECIMAL_BASE); //get length of dtype
    i++;
  }
  token = strtok(NULL, DELIMITER);
  int keySize = (int) strtol(token, NULL, HEXADECIMAL_BASE); //get keysize
  int *keyAttrs = (int *) malloc(sizeof(int) * keySize);
  i=0;
  while (i < keySize) {
    token = strtok(NULL, DELIMITER);
    keyAttrs[i] = (int) strtol(token, NULL, HEXADECIMAL_BASE);
    i++;
  }
  //create schema using parsed string/information
  Schema *s = createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
  freeSchemaObjects(s,false);
  return createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
}

//Method to get pointer position
int getAttrStartPos(Schema *schema, int attrNum) {
  int i,pos = 0;
  for (i = 0; i < attrNum; i++) {
    //get pos based on dtype size
    switch (schema->dataTypes[i]) {
      case DT_BOOL:
        pos += sizeof(bool);
        break;
      case DT_FLOAT:
        pos += sizeof(float);
        break;
      case DT_INT:
        pos += sizeof(int);
        break;
      case DT_STRING:
        pos += schema->typeLength[i] + 1; // +1 for \0 terminator
        break;
    }
  }
  return pos;
}


//Gets size of record in Bytes
int getRecordSizeInBytes(Schema *schema, bool withTerminators) {
  int i, size=0;
  for (i = 0; i < schema->numAttr; i++) {
    //get size based on dtype
    switch (schema->dataTypes[i]) {
      case DT_BOOL:
        size += sizeof(bool);
        break;
      case DT_FLOAT:
        size += sizeof(float);
        break;
      case DT_INT:
        size += sizeof(int);
        break;
      case DT_STRING:
        size += schema->typeLength[i];
        if (withTerminators)
          size++; // +1 for \0 terminator
        break;
    }
  }
  return size;
}

//if bit is set on bm
bool isSetBit(char *bitMap, int bitIdx) {
  int bitSeq = NUM_BITS - (bitIdx % NUM_BITS) - 1;
  int byteIdx = bitIdx / NUM_BITS;
  if (bitMap[byteIdx] & (1 << (bitSeq))) { //if specific bit is set using AND
    return true;
  }
  return false;
}

//Used to set memory indicators
void setBit(char *bitMap, int bitIdx) {
  int bitSeq = NUM_BITS - (bitIdx % NUM_BITS) - 1;
  int byteIdx = bitIdx / NUM_BITS;
  bitMap[byteIdx] |= 1 << (bitSeq); //set specific bit using OR
}

//Used to reset memory indicators
void unsetBit(char *bitMap, int bitIdx) {
  int bitSeq = NUM_BITS - (bitIdx % NUM_BITS) - 1;
  int byteIdx = bitIdx / NUM_BITS;
  bitMap[byteIdx] = bitMap[byteIdx] & ~(1 << (bitSeq)); //clear specific bit using AND, NOT
}

//get free memory locations
int getUnsetBitIndex(char *bitMap, int bitmapSize) {
  int bytesCount = 0;
  unsigned char byte;
  for(bytesCount=0;bytesCount<bitmapSize; bytesCount++){
    byte = bitMap[bytesCount];
    if(byte < UCHAR_MAX){ //to find unset bits
      for(int i=0;i<NUM_BITS;i++){
        if(!(byte & (1 << (NUM_BITS - 1 - i)))) { //check if bit is unset
          return (bytesCount * NUM_BITS) + i; //return ind of unset bit
        }
      }
    }
  }
  return -1; //if nounset bit
}

//get empty page num from bm
RC getEmptyPage(RM_TableData *rel, int *pageNum) {
  BM_PageHandle *page;
  // page 1 free pages bitmap
  RC err = atomicPinPage(rel->bm, &page, 1);
  if (err!=RC_OK)
    return err;

  //assign free page index
  *(pageNum) = getUnsetBitIndex(page->data, PAGE_SIZE);
  return atomicUnpinPage(rel->bm, page, false);
}

//change fillbit for specific page in bitmap
RC changePageFillBit(RM_TableData *rel, int pageNum, bool bitVal) {
  BM_PageHandle *page;
  // page 1 free pages bitmap
  RC err = atomicPinPage(rel->bm, &page, 1);
  if (err!=RC_OK) {
    return err;
  }

  if (bitVal)// mark as full
    setBit(page->data, pageNum); //indicate page is full
  else
    unsetBit(page->data, pageNum);
  return atomicUnpinPage(rel->bm, page, true); //indicate page is free
}

//get size of record
int getRecordSize (Schema *schema) {
  return getRecordSizeInBytes(schema, false);
}

//functionalities

//initialize record manager
RC initRecordManager (void *mgmtData) {
  initStorageManager();
  return RC_OK;
}


/*Create table functionality
Tables are stored Page2 onwards */
RC createTable (char *name, Schema *schema) {
  RC err;
  err = createPageFile(name);
  if (err!=RC_OK)
    return err;

  // No need to ensureCapacity, because creating a file already ensures one block, we dont need more for now.
  char *schemaString = stringifySchema(schema); //convert schema to string
  SM_FileHandle fileHandle;
  err = openPageFile(name, &fileHandle);
  if (err != RC_OK)
    return err;
  err = writeBlock(0, &fileHandle, schemaString); //write schema string to first page
  if (err != RC_OK) {
    free(schemaString);
    return err;
  }
  free(schemaString);
  err = closePageFile(&fileHandle);
  return err;
}

//get total num of tuples in table
int getNumTuples (RM_TableData *rel) {
  forceFlushPool(rel->bm); //ensure all modifications are flushed
  SM_FileHandle *fHandle = rel->bm->mgmtData->fHandle;

  //get total num of pages excluding header pages
  int totalNumDataPages = fHandle->totalNumPages - TABLE_HEADER_PAGES_LEN;
  BM_PageHandle *page;
  int i = 0, numTuples= 0;
  for(i=0;i < totalNumDataPages;i++)  //loop to count numoftuples
    if (atomicPinPage(rel->bm, &page, i + TABLE_HEADER_PAGES_LEN) != RC_OK)
      return -1; //return on pin error
    short totalSlots = *((short *)page->data);
    numTuples += totalSlots;
    if (atomicUnpinPage(rel->bm, page, false) != RC_OK)
      return -1; //return on unpin error
  return numTuples;
}

//page#0 have schema info of table and page#1 have bitmap for pages having atleast 1 emptpy slot
//create schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,\
  int *typeLength, int keySize, int *keys){
  Schema *s = (Schema *) malloc(sizeof(Schema));
  s->numAttr = numAttr;
  s->attrNames = (char **) malloc(sizeof(char *) * numAttr);
  s->dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
  s->typeLength = (int *) malloc(sizeof(int) * numAttr);
  int i;
  for (i = 0; i < numAttr; i++) {
    s->dataTypes[i] = dataTypes[i]; //copy dtypes
    s->attrNames[i] = copyString(attrNames[i]); //copy attribute name
    switch (dataTypes[i]) { //copy typelength based on dtype
      case DT_INT:
        s->typeLength[i] = sizeof(int);
        break;
      case DT_FLOAT:
        s->typeLength[i] = sizeof(float);
        break;
      case DT_BOOL:
        s->typeLength[i] = sizeof(bool);
        break;
      case DT_STRING:
        s->typeLength[i] = typeLength[i];
        break;
    }
  }
  s->keySize = keySize; //set keysize
  s->keyAttrs = (int *) malloc(sizeof(int) * s->keySize);
  for (i = 0; i < s->keySize; i++) {
    s->keyAttrs[i] = keys[i]; //copy key attributes
  }
  return s; //return created schema
}

//Opens the table, close the table after completing operations
RC openTable (RM_TableData *rel, char *name) {
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
  err = initBufferPool(bm, name, PER_TBL_BUF_SIZE, RS_LRU, NULL); //init buffer pool
  if (err != RC_OK) {
    free(bm);
    return err;
  }
  BM_PageHandle *pageHandle;
  err = atomicPinPage(bm, &pageHandle, 0); //pin 1st page to read schema
  if (err != RC_OK) {
    free(bm);
    return err;
  }
  //assign values for relation tables
  rel->name = copyString(name);
  rel->schema = parseSchema(pageHandle->data);
  rel->bm = bm;
  int pageSize = PAGE_SIZE - PAGE_HEADER_LEN;
  rel->recordByteSize = getRecordSizeInBytes(rel->schema, true);
  rel->maxSlotsPerPage = floor((pageSize * NUM_BITS) / (rel->recordByteSize * NUM_BITS + 1)); // pageSize = (X * recordSize) + (X / NUM_BITS)
  rel->slotsBitMapSize = ceil(((float) rel->maxSlotsPerPage) / NUM_BITS);
  return atomicUnpinPage(bm, pageHandle, false); // false for not markDirty
}


//Close the table, and releases the bufferPool and schema
RC closeTable (RM_TableData *rel) {
	RC err=RC_OK;
  err = shutdownBufferPool(rel->bm);
  if (err!=RC_OK)
    return err;
  free(rel->name);
  free(rel->bm);
  err = freeSchema(rel->schema);
  if (err!=RC_OK)
    return err;
  return err;
}

//delete table
RC deleteTable (char *name) {
  return destroyPageFile(name);
}



//free memory
RC freeSchema (Schema *schema) {
  if(schema!=NULL) {
    freeSchemaObjects(schema,true);
  }
  return RC_OK;
}

//free record
RC freeRecord (Record *record) {
  free(record->data); free(record);
  return RC_OK;
}

//create record
RC createRecord (Record **record, Schema *schema) {
  Record *r = (Record *) malloc(sizeof(Record));
  r->data = (char *) malloc(sizeof(char) * getRecordSizeInBytes(schema, true));
  *record = r; //assign created record to
  return RC_OK;
}

//Set the valuess in record
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
  char *ptr = record->data + getAttrStartPos(schema, attrNum);
  switch(value->dt) { //check dtype and set value based on that
    case DT_BOOL:
      if (value->v.boolV != true && value->v.boolV != false)
        return RC_RM_LIMIT_EXCEEDED;
      *((bool *)ptr) = value->v.intV;
      break;
    case DT_INT:
      if (value->v.intV > INT_MAX || value->v.intV < INT_MIN)
        return RC_RM_LIMIT_EXCEEDED;
      *((int *)ptr) = value->v.intV;
      break;
    case DT_FLOAT:
      if (value->v.floatV > FLT_MAX || value->v.floatV < FLT_MIN)
        return RC_RM_LIMIT_EXCEEDED;
      *((float *)ptr) = value->v.intV;
      break;
    case DT_STRING:
      int size = schema->typeLength[attrNum];
      if (size < strlen(value->v.stringV))
        return RC_RM_LIMIT_EXCEEDED;
      memcpy(ptr, value->v.stringV, size+1); //copy string to record
      ptr[size] = '\0'; // Ensure null termination
      break;
  }
  return RC_OK;
}

//get attribute from record
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
  int pos = getAttrStartPos(schema, attrNum); //get start pos
  Value *val = (Value *) malloc(sizeof(Value));
  char *ptr = record->data + pos; //pointer for specified attr
  val->dt = schema->dataTypes[attrNum];
  switch (val->dt) { //check dtype and set value based on that
    case DT_INT:
      val->v.intV = *(int *) ptr;
      break;
    case DT_FLOAT:
      val->v.intV = *(float *) ptr;
      break;
    case DT_STRING:
      int size = schema->typeLength[attrNum] + 1;
      val->v.stringV = (char *) malloc(sizeof(char) * size);
      memcpy(val->v.stringV, ptr, size);
      break;
    case DT_BOOL:
      val->v.intV = *(bool *) ptr;
      break;
    default:
      return RC_RM_UNKOWN_DATATYPE;
  }
  *value = val; //assign value
  return RC_OK;
}

//Insert the record into the table mentioned in the relation
//Method sets the RID in the record for future use
RC insertRecord (RM_TableData *rel, Record *record) {
  RC err;
  BM_PageHandle *page;
  int pageNum = 0, slotNum;
  short totalSlots;
  err = getEmptyPage(rel, &pageNum);
  if (err!=RC_OK)
    return err;
  record->id.page = pageNum;

  //leave 2 page as first two pages are for header
  err = atomicPinPage(rel->bm, &page, pageNum + TABLE_HEADER_PAGES_LEN);
  if (err!=RC_OK)
    return err;

  char *ptr = page->data;
  memcpy(&totalSlots, ptr, sizeof(short));
  ptr += PAGE_HEADER_LEN;
  record->id.slot = slotNum = getUnsetBitIndex(ptr, rel->slotsBitMapSize);

  //calculate pos for new record
  int pos = PAGE_HEADER_LEN + rel->slotsBitMapSize + (rel->recordByteSize * slotNum);
  memcpy(page->data + pos, record->data, rel->recordByteSize);

  //update slots and bm
  totalSlots++;
  memcpy(page->data, &totalSlots, sizeof(short)); // writing the totalSlots
  setBit(ptr, slotNum); // set slot bit
  err = atomicUnpinPage(rel->bm, page, true);
  if (err!=RC_OK)
    return err;

  //if page is full then update the fill bit
  if (totalSlots >= rel->maxSlotsPerPage) {
    err = changePageFillBit(rel, pageNum, true);
    if (err!=RC_OK)
      return err;
  }
  return RC_OK;
}

//get the record
RC getRecord (RM_TableData *rel, RID id, Record *record) {
  BM_PageHandle *page;
  RC err;
  err = atomicPinPage(rel->bm, &page, id.page + TABLE_HEADER_PAGES_LEN);
  if (err!=RC_OK) {
    return err;
  }
  char *ptr = page->data; //pointer to page data
  ptr += PAGE_HEADER_LEN; //set after header

  if (!isSetBit(ptr, id.slot)) { //if slot corresponding to RID is set
    atomicUnpinPage(rel->bm, page, false); 
    return RC_RM_NO_SUCH_TUPLE;
  }
  int recordSize = rel->recordByteSize;
  int position = PAGE_HEADER_LEN + rel->slotsBitMapSize + (recordSize * id.slot);
  ptr = page->data + position; // pointer to bytes array
  memcpy(record->data, ptr, recordSize);
  return atomicUnpinPage(rel->bm, page, false);
}

//updates the record
RC updateRecord (RM_TableData *rel, Record *record) {
  BM_PageHandle *page;
  RC err;
  int pageNum = record->id.page; //get details from RID
  int slotNum = record->id.slot;
  err = atomicPinPage(rel->bm, &page, pageNum + TABLE_HEADER_PAGES_LEN);
  if (err!=RC_OK)
    return err;

  char *bmptr = page->data + PAGE_HEADER_LEN; //to track which record to update

  if (!isSetBit(bmptr, slotNum)) {//if slot corresponding to RID is set
    atomicUnpinPage(rel->bm, page, false); 
    return RC_RM_NO_SUCH_TUPLE;
  }
  //calculate pos of record data in page
  int position = PAGE_HEADER_LEN + rel->slotsBitMapSize + (rel->recordByteSize * slotNum);
  bmptr = page->data + position;
  memcpy(bmptr, record->data, rel->recordByteSize); //copy updated record into page
  return atomicUnpinPage(rel->bm, page, true);
}

//delete the record in rid
RC deleteRecord (RM_TableData *rel, RID id) {
  RC err;
  BM_PageHandle *page;
  int pageNum = id.page; //get details from RID
  int slotNum = id.slot;
  err = atomicPinPage(rel->bm, &page, pageNum + TABLE_HEADER_PAGES_LEN);
  if (err!=RC_OK) {
    return err;
  }

  char *bmptr = page->data + PAGE_HEADER_LEN; //to track which record to delete

  if (!isSetBit(bmptr, slotNum)) { //if slot corresponding to RID is set
    atomicUnpinPage(rel->bm, page, false);
    return RC_RM_NO_SUCH_TUPLE;
  }
  //mark record as deleted
  unsetBit(bmptr, slotNum);
  short *totalSlots = (short *)page->data;
  (*totalSlots)--;
  err = atomicUnpinPage(rel->bm, page, true); //unpin and indicate it as dirty
  if (err!=RC_OK)
    return err;
  if (*totalSlots == rel->maxSlotsPerPage - 1)// if it was full page, mark it as unfull now
    err = changePageFillBit(rel, pageNum, false);
  return err;
}

//shutdown record manager
RC shutdownRecordManager () {
  return RC_OK;
}

//start scan
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
  forceFlushPool(rel->bm); //ensure all modifications are flushed
  scan->rel = rel; 
  ScanMgmtInfo *smi = (ScanMgmtInfo *) malloc(sizeof(ScanMgmtInfo));
  SM_FileHandle *fHandle = rel->bm->mgmtData->fHandle;
  //set scan management information
  smi->page = 0; //start scan from 1st page
  smi->slot = -1; // so that the next slot is 0
  smi->totalPages = fHandle->totalNumPages - TABLE_HEADER_PAGES_LEN;
  smi->maxSlots = rel->maxSlotsPerPage;
  smi->condition = cond;
  scan->smi = smi;
  return RC_OK;
}


//used in conjuction with start scan to get next recod in scan
RC next (RM_ScanHandle *scan, Record *record) {
  Value *val;
  RC err;
  //get to next record
  if (++scan->smi->slot >= scan->smi->maxSlots) {
    scan->smi->slot = 0; //reset if slot exceeds max
    if(++scan->smi->page >= scan->smi->totalPages) {
      return RC_RM_NO_MORE_TUPLES;
    }
  }
  record->id.page = scan->smi->page; //get details from RID
  record->id.slot = scan->smi->slot;

  //get record using current RID
  err = getRecord(scan->rel, record->id, record);
  if (err == RC_RM_NO_SUCH_TUPLE)
    return next(scan, record); //call recursive to get to next one
  else if(err!=RC_OK)
    return err;
  //evaluate condition expression on the record
  err = evalExpr(record, scan->rel->schema, scan->smi->condition, &val);
  if (err!=RC_OK) {
    return err;
  }
	if (val->v.boolV == 1) {
    free(val);
    return RC_OK;
  }
  free(val);
  return next(scan, record);
}

//close the scan
RC closeScan (RM_ScanHandle *scan) {
  free(scan->smi);
	return RC_OK;
}