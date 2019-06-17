#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

typedef struct KeyPtr {
  Value key;
  int pageNum;
  RID rid;
} KeyPtr;

typedef struct Node {
 int currentPageNum;
 int parentPageNum;
 int nextPageNum;
 bool leaf;
 int numKeys;
 KeyPtr *contents;
} Node;

typedef struct BT_MgmtData {
  BM_BufferPool *bm;
  int numNodes;
  int numKeys;
  int n;
  int rootPageNum;
  BM_PageHandle *pHandle;
} BT_MgmtData;


typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  BT_MgmtData *mgmtData;
} BTreeHandle;

// Bookkeeping for scans
typedef struct BT_ScanMgmtData
{
  Node *node;
  int idx;
} BT_ScanMgmtData;

typedef struct BT_ScanHandle {
  //This BT_MgmtData can be obtained from BTreeHandle
  BTreeHandle *tree;
  BT_ScanMgmtData *mgmtData;
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

// node I/O
extern RC readNode (BTreeHandle *tree, int pageNum, Node *node);
extern RC writeNode (BTreeHandle *tree, int pageNum, Node *node);

#endif // BTREE_MGR_H
