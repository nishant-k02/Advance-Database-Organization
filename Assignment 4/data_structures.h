#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include "dt.h"

// 1250 * 4KB(PAGE_SIZE is 4096) = 5MB, assuming we will need 5MB for every pool, 1259 is the closest prime number to 1250
#define HASH_LEN 1259

// linked list Node
typedef struct Node {
  void *data; // general pointer to be used with different data types
  struct Node *next;
  struct Node *prev;
} Node;

// key, value combination for hashmap
typedef struct HM_Comb {
  int key;
  void *val;
} HM_Comb;

// hashmap
typedef struct HM {
  Node *tbl[HASH_LEN]; // table of linked list to solve hashmap collision
} HM;

// hashmap management functions
HM *hmInit();
int hash(int key);
int hmInsert(HM *hm, int key, void *val);
Node *hmGet(HM *hm, int key);
int hmDelete(HM *hm, int key);
void hmDestroy(HM *hm);

#endif