#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "storage_mgr.h"
#include "tables.h"
#include "dberror.h"
#include "expr.h"
#include "btree_mgr.h"
#include "buffer_mgr.h"


const RID INIT_RID={-1,-1};

//init index manager
extern RC initIndexManager (void *mgmtData)
{
	return RC_OK;
}

//shutdown index manager
extern RC shutdownIndexManager ()
{
	return RC_OK;
}

RC solution(){ return RC_OK; }

//create btree
extern RC createBtree (char *idxId, DataType keyType, int n)
{
	//create page file for the given btree
	
	createPageFile(idxId);

	SM_FileHandle fh;
	openPageFile(idxId, &fh);
	ensureCapacity(1, &fh);
	closePageFile(&fh);

	return RC_OK;
}

//open btree
extern RC openBtree (BTreeHandle **tree, char *idxId)
{
	//init the structs
	TreeMgr* trInfo=(TreeMgr *)malloc(sizeof(TreeMgr));
	trInfo->page=(BM_PageHandle *) malloc (sizeof(BM_PageHandle));
	trInfo->bm=(BM_BufferPool *) malloc (sizeof(BM_BufferPool));
	trInfo->globalCount=0;
	trInfo->lastPage=trInfo->scanCnt=0;

	initBufferPool(trInfo->bm,idxId, 10, RS_FIFO, NULL);
	pinPage(trInfo->bm, trInfo->page,1);
	trInfo->keyNum=*((int *)trInfo->page->data);
	unpinPage(trInfo->bm, trInfo->page);

	//init the btreehandle
	BTreeHandle* treeTemp=(BTreeHandle*)malloc(sizeof(BTreeHandle));
	treeTemp->keyType=DT_INT;
	treeTemp->idxId=idxId;
	treeTemp->treeMgr=trInfo;

	*tree=treeTemp;
	
 
	return RC_OK;
}

//close btree
extern RC closeBtree (BTreeHandle *tree)
{
	//free the assigned memory and close the tree
	tree->treeMgr->lastPage=0;
	free(tree->treeMgr->page);
	shutdownBufferPool(tree->treeMgr->bm);
	free(tree);

	return RC_OK;
}

//delete btree
extern RC deleteBtree (char *idxId)
{
	return destroyPageFile(idxId);
}

//get number of nodes
extern RC getNumNodes (BTreeHandle *tree, int *result)
{
	//assign numNodes to the given argument
	*result=tree->treeMgr->lastPage+1;
	return RC_OK;
}

//get number of entries
extern RC getNumEntries (BTreeHandle *tree, int *result)
{
	//assign numEntries to the given argument
	*result=tree->treeMgr->globalCount;
	solution();
}

//get key type
extern RC getKeyType (BTreeHandle *tree, DataType *result)
{
	//assign keyType to the given argument
	*result = tree->keyType;
	solution();
}

//find given key and store it in result
extern RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
	//declare the vars
	int index, findKey=key->v.intV;
	BTNode* node;
	bool find=false;

	for(index=1;index<=tree->treeMgr->lastPage;index++)
	{
		//go through the btree
		pinPage(tree->treeMgr->bm, tree->treeMgr->page, index);
		node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
		//if key is found then assign it to given argument result
		if(findKey==node->value1)
		{
			*result=node->left;
			find=true;
			break;
		}
		if(findKey==node->value2)
		{
			*result=node->mid;
			find=true;
			break;
		}
		unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
	}
	//return keyNotFound if the key is not found
	return find ? RC_OK : RC_IM_KEY_NOT_FOUND;
}

//insert given key
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	BTNode* node;
	RID *res;
	
	//checkk if key already exists
	if(findKey(tree,key,res)==false)
		return RC_IM_KEY_ALREADY_EXISTS;
	if(tree->treeMgr->lastPage==0) //it is the first key to be inserted
	{
		tree->treeMgr->lastPage=1;


		pinPage(tree->treeMgr->bm, tree->treeMgr->page, tree->treeMgr->lastPage);
		markDirty(tree->treeMgr->bm, tree->treeMgr->page);
		
		//set page as not full
		*(bool*)tree->treeMgr->page->data=false;
		//insert the key
		node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
		node->left=rid;
		node->value1=key->v.intV;
		node->value2=-1;
		node->mid=node->right=INIT_RID;

		unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
	}
	else
	{

		pinPage(tree->treeMgr->bm, tree->treeMgr->page, tree->treeMgr->lastPage);
		markDirty(tree->treeMgr->bm, tree->treeMgr->page);


		//if page is full
		if((*(bool*)tree->treeMgr->page->data)==true)
		{
			tree->treeMgr->lastPage++;
			unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
			pinPage(tree->treeMgr->bm, tree->treeMgr->page, tree->treeMgr->lastPage);

			//set page as not full
			*(bool*)tree->treeMgr->page->data=false;
			//insert the key
			node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
			node->value1=key->v.intV;
			node->left=rid;
			node->value2=-1;
			node->mid=node->right=INIT_RID;

			unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
		}
		else//if the page is not full
		{
			//move last page val to current page val2
			node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
			node->value2=key->v.intV;
			node->mid=rid;
			*(bool*)tree->treeMgr->page->data=true;

			unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
		}
	}

	tree->treeMgr->globalCount++;		//increase total val count
	return RC_OK;
}

//delete given key
extern RC deleteKey (BTreeHandle *tree, Value *key)
{
	int index, findKey=key->v.intV, valueNum=0, moveValue;
	bool find=false;
	BTNode* node;
	RID moveRID;

	for(index=1;index<=tree->treeMgr->lastPage;index++)
	{
		pinPage(tree->treeMgr->bm, tree->treeMgr->page, index);
		markDirty(tree->treeMgr->bm, tree->treeMgr->page);
		node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
		if(findKey==node->value1)
		{
			valueNum=1;
			find=true;
			break;
		}
		if(findKey==node->value2)
		{
			valueNum=2;
			find=true;
			break;
		}
		unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
	}

	if(!find)
		return RC_IM_KEY_NOT_FOUND;
	else//if key exists
	{
		pinPage(tree->treeMgr->bm, tree->treeMgr->page, tree->treeMgr->lastPage);
		markDirty(tree->treeMgr->bm, tree->treeMgr->page);

		if(index==tree->treeMgr->lastPage)
		{//if key is at last page
			node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
			if(valueNum==2)//if last value
			{			
				*(bool*)tree->treeMgr->page->data=false;			
				node->value2=-1;
				node->mid=INIT_RID;
			}
			else
			{
				//if second last 
				if((*(bool*)tree->treeMgr->page->data)==true)
				{
					node->left=moveRID=node->mid;
					node->value1=moveValue=node->value2;
					node->value2=-1;
					node->mid=INIT_RID;
					*(bool*)tree->treeMgr->page->data=false;
				}
				else//if first
				{
					node->value1=-1;
					node->left=INIT_RID;
					tree->treeMgr->lastPage--;
				}
			}
			unpinPage(tree->treeMgr->bm, tree->treeMgr->page);//unpin the last page
		}
		else
		{//if key is not at last page
			//if the last page is full
			if((*(bool*)tree->treeMgr->page->data)==true)
			{
				//set page as not full
				*(bool*)tree->treeMgr->page->data=false;
				
				node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
				moveRID=node->mid;
				moveValue=node->value2;

				node->value2=-1;
				node->mid=INIT_RID;

				unpinPage(tree->treeMgr->bm, tree->treeMgr->page);

				pinPage(tree->treeMgr->bm, tree->treeMgr->page, index);
				markDirty(tree->treeMgr->bm, tree->treeMgr->page);
				if(valueNum==1)
				{
					//move last page val to current page val1
					node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
					node->left=moveRID;
					node->value1=moveValue;
					unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
				}
				else
				{
					//move last page val to current page val2
					node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
					node->mid=moveRID;
					node->value2=moveValue;
					unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
				}
			}
			else//if last page is not full
			{
				node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
				moveRID=node->left;
				node->left=INIT_RID;
				moveValue=node->value1;
				node->value1=-1;
				tree->treeMgr->lastPage--;

				unpinPage(tree->treeMgr->bm, tree->treeMgr->page);

				pinPage(tree->treeMgr->bm, tree->treeMgr->page, index);
				markDirty(tree->treeMgr->bm, tree->treeMgr->page);
				if(valueNum==1)
				{
					//move last page val to current page val1
					node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
					node->left=moveRID;
					node->value1=moveValue;
					unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
				}
				else
				{
					//move last page val to current page val2
					node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
					node->mid=moveRID;
					node->value2=moveValue;
					unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
				}
			}
		}
		tree->treeMgr->globalCount--;	//decrease total val count
	}

	return RC_OK;
}

//open scan and traverse in sorted order
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	*handle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
	BTNode *node;
	int index,i=0,j=0,temp1,temp2,min;
	(*handle)->tree = tree;
	(*handle)->scanCnt = 0;
	(*handle)->values = (int*)malloc(sizeof(int)*(tree->treeMgr->globalCount));

	for(index=1;index<=tree->treeMgr->lastPage;index++)
	{
		pinPage(tree->treeMgr->bm, tree->treeMgr->page, index);
		node=(BTNode*)tree->treeMgr->page->data+sizeof(bool);
		int v1=node->value1;
		int v2=node->value2;
		//add values to handle
		if(v1!=-1)
		{
			(*handle)->values[i]=node->value1;
			i++;
		}
		if(v2!=-1)
		{
			(*handle)->values[i]=node->value2;
			i++;
		}
		unpinPage(tree->treeMgr->bm, tree->treeMgr->page);
	}

	//sort values
	for(i=0;i<(tree->treeMgr->globalCount);i++)
	{
		min=i;
		for(j=i+1;j<(tree->treeMgr->globalCount);j++)
		{
			if((*handle)->values[min]>(*handle)->values[j])
				min=j;
		}
		//swap values
		temp1=(*handle)->values[min];
		temp2=(*handle)->values[i];
		(*handle)->values[min]=temp2;
		(*handle)->values[i]=temp1;
	}

	return RC_OK;
}

//retrives next entry in tree scan
extern RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	Value* vl=(Value*)malloc(sizeof(Value));
	vl->dt=DT_INT;
	vl->v.intV=handle->values[handle->scanCnt];

	if(handle->scanCnt!=(handle->tree->treeMgr->globalCount))
	{
		findKey (handle->tree, vl, result);
		handle->scanCnt++;
		return RC_OK;
	}
	else
	{
		return RC_IM_NO_MORE_ENTRIES;
	}

}

//close the scan
extern RC closeTreeScan (BT_ScanHandle *handle)
{
	handle->scanCnt=0;
	free(handle->values);
	return RC_OK;
}

//print tree
extern char *printTree (BTreeHandle *tree)
{
	return tree->idxId;
}
