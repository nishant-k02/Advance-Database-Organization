#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"
#include "buffer_mgr.h"

//extra struct
typedef struct TreeMgr
{
	BM_BufferPool *bm;
	BM_PageHandle *page;
	int globalCount;//the num of values one tree has
	int keyNum;//num of keys atmost one key can have
  int lastPage;
  int scanCnt;
}TreeMgr;

typedef struct BTNode
{
	RID left;
	int value1;
	RID mid;
	int value2;
	RID right;
}BTNode;

typedef struct BTREE
{
    int *key;
    struct BTREE **next;
    RID *id;
} BTree;

// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
  TreeMgr *treeMgr;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  int scanCnt;
  void *mgmtData;
  int *values;
} BT_ScanHandle;







// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

#endif // BTREE_MGR_H
