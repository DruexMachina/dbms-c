#include "dberror.h"
#include "tables.h"
#include "expr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "btree_mgr.h"

#include <stdlib.h>
#include <string.h>


// init and shutdown index manager
RC initIndexManager (void *mgmtData){

  return RC_OK;

}
RC shutdownIndexManager (){

  return RC_OK;

}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n){

  SM_FileHandle fHandle;
  createPageFile(idxId);
  openPageFile(idxId, &fHandle);
  ensureCapacity(2, &fHandle);

  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  pHandle->data = (char*) malloc(PAGE_SIZE);
  sprintf(pHandle->data, "%d,%d,%d,%d,%d,", keyType, 1, 0, n, 1);
  writeBlock(0, &fHandle, pHandle->data);
  memset(pHandle->data, 0, strlen(pHandle->data));
  sprintf(pHandle->data, "%d,%d,%d,%d,%d,", 1, -1, -1, 1, 0);
  writeBlock(1, &fHandle, pHandle->data);
  closePageFile(&fHandle);
  memset(pHandle->data, 0, sizeof(pHandle->data));
  free(pHandle);

  return RC_OK;

}
RC openBtree (BTreeHandle **tree, char *idxId){

  BT_MgmtData *mgmtData = (BT_MgmtData*) malloc(sizeof(BT_MgmtData));
  BM_BufferPool *bm = MAKE_POOL();
  SM_FileHandle fHandle;
  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  pHandle->data = (char*) malloc(PAGE_SIZE);
  *tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));

  openPageFile(idxId, &fHandle);
  initBufferPool(bm, idxId, 1000, RS_FIFO, NULL);
  readBlock(0, &fHandle, pHandle->data);
  char *p, *start = pHandle->data, *end, *result = (char*) malloc(20);
  end = strstr(start, ",");
  strncpy(result, start, end - start);
  result[end - start] = '\0';
  (*tree)->keyType = (DataType) strtol(result, &p, 10);
  start = end + 1;
  end = strstr(start, ",");
  strncpy(result, start, end - start);
  result[end - start] = '\0';
  mgmtData->numNodes = strtol(result, &p, 10);
  start = end + 1;
  end = strstr(start, ",");
  strncpy(result, start, end - start);
  result[end - start] = '\0';
  mgmtData->numKeys = strtol(result, &p, 10);
  start = end + 1;
  end = strstr(start, ",");
  strncpy(result, start, end - start);
  result[end - start] = '\0';
  mgmtData->n = strtol(result, &p, 10);
  start = end + 1;
  end = strstr(start, ",");
  strncpy(result, start, end - start);
  result[end - start] = '\0';
  mgmtData->rootPageNum = strtol(result, &p, 10);
  (*tree)->idxId = idxId;
  mgmtData->bm = bm;
  mgmtData->pHandle = pHandle;
  (*tree)->mgmtData = mgmtData;
  memset(pHandle->data, 0, sizeof(pHandle->data));
  free(pHandle);

  return RC_OK;

}
RC closeBtree (BTreeHandle *tree) {

  free(tree->mgmtData->bm);
  free(tree->mgmtData);
  free(tree);

	return RC_OK;

}
RC deleteBtree (char *idxId){

  destroyPageFile(idxId);

  return RC_OK;

}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result){

  *result = tree->mgmtData->numNodes;

  return RC_OK;

}
RC getNumEntries (BTreeHandle *tree, int *result){

  *result = tree->mgmtData->numKeys;

  return RC_OK;

}
RC getKeyType (BTreeHandle *tree, DataType *result){

  *result = tree->keyType;

  return RC_OK;

}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result) {

  // Identify correct node
  Node *node = (Node*) malloc(sizeof(Node));
  node->contents = (KeyPtr*) malloc(sizeof(KeyPtr) * tree->mgmtData->n);
  readNode(tree, tree->mgmtData->rootPageNum, node);
  int flag;
  while (node->leaf == FALSE) {
    flag = 0;
    for (int i = 0; i < node->numKeys; i++) {
      if (key->v.intV < node->contents[i].key.v.intV) {
        readNode(tree, node->contents[i].pageNum, node);
        flag = 1;
        break;
      }
    }
    if (flag == 0) {
      readNode(tree, node->nextPageNum, node);
    }
  }
  // Determine if key exists
  for (int i = 0; i < node->numKeys; i++) {
    if (key->v.intV == node->contents[i].key.v.intV) {
      result->page = node->contents[i].rid.page;
      result->slot = node->contents[i].rid.slot;
      return RC_OK;
    }
  }
  result->page = result->slot = -1;
  free(node->contents);
  free(node);

  return RC_OK;

}
RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
  // Determine if key already exists
  printf("##INSERT %d %d.%d\n", key->v.intV, rid.page, rid.slot);
  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  pHandle->data = (char*)malloc(1000);
  RID result;
  findKey(tree, key, &result);
  if (result.page != -1) {
    printf("LOCATE: Key is already in the tree\n");
    return RC_OK;
  }
  // Identify leaf node to insert key into
  Node *node = (Node*) malloc(sizeof(Node));
  node->contents = (KeyPtr*) malloc(sizeof(KeyPtr) * tree->mgmtData->n + 1);
  readNode(tree, tree->mgmtData->rootPageNum, node);
  printf("LOCATE: Starting at root\n");
  int flag;
  while (node->leaf == FALSE) {
    flag = 0;
    for (int i = 0; i < node->numKeys; i++) {
      if (key->v.intV < node->contents[i].key.v.intV) {
        readNode(tree, node->contents[i].pageNum, node);
        flag = 1;
        break;
      }
    }
    if (flag == 0) {
      readNode(tree, node->nextPageNum, node);
    }
    printf("LOCATE: Moving to page %d\n", node->currentPageNum);
  }

  // Insert key
  Node *nodeSplit = node;
  Value keyNew;
  keyNew.dt = key->dt;
  keyNew.v = key->v;
  int pageNumNew = 1;
  while (1) {
    printf("INSERT: Inserting key into Page %d\n", nodeSplit->currentPageNum);
    nodeSplit->contents[nodeSplit->numKeys].key = keyNew;
    if (nodeSplit->leaf == TRUE) {
      nodeSplit->contents[nodeSplit->numKeys].rid.page = rid.page;
      nodeSplit->contents[nodeSplit->numKeys].rid.slot = rid.slot;
    } else {
      nodeSplit->contents[nodeSplit->numKeys].pageNum = pageNumNew;
    }
    nodeSplit->numKeys++;
    // Sort keys in node
    int flag = 0;
    for (int i = nodeSplit->numKeys - 1; i > 0; i--) {
      if (nodeSplit->contents[i].key.v.intV < nodeSplit->contents[i - 1].key.v.intV) {
        flag = 1;
        KeyPtr tmp;
        tmp.key = nodeSplit->contents[i - 1].key;
        tmp.pageNum = nodeSplit->contents[i - 1].pageNum;
        tmp.rid = nodeSplit->contents[i - 1].rid;
        nodeSplit->contents[i - 1].key = nodeSplit->contents[i].key;
        nodeSplit->contents[i - 1].pageNum = nodeSplit->contents[i].pageNum;
        nodeSplit->contents[i - 1].rid = nodeSplit->contents[i].rid;
        nodeSplit->contents[i].key = tmp.key;
        nodeSplit->contents[i].pageNum = tmp.pageNum;
        nodeSplit->contents[i].rid = tmp.rid;
      }
    }
    if (node->leaf == TRUE) {
      tree->mgmtData->numKeys++;
    }
    if (nodeSplit->leaf == FALSE) {
      if (flag == 0) {
        int tmpPageNum = nodeSplit->nextPageNum;
        nodeSplit->nextPageNum = nodeSplit->contents[nodeSplit->numKeys - 1].pageNum;
        nodeSplit->contents[nodeSplit->numKeys - 1].pageNum = tmpPageNum;
      }
      if (nodeSplit->contents[0].pageNum == pageNumNew) {
        int tmpPageNum = nodeSplit->contents[0].pageNum;
        nodeSplit->contents[0].pageNum = nodeSplit->contents[1].pageNum;
        nodeSplit->contents[1].pageNum = tmpPageNum;
      }
    }
    printf("INSERT: Key inserted\n");
    // Free slot available
    if (nodeSplit->numKeys <= tree->mgmtData->n) {
      printf("INSERT: Free slot available\n");
      writeNode(tree, nodeSplit->currentPageNum, nodeSplit);
      printf("INSERT: Verfication\n");
      for (int i = 1; i <= tree->mgmtData->numNodes; i++) {
        pHandle->pageNum = i;
        pinPage(tree->mgmtData->bm, pHandle, i);
        printf("%d: %s\n", i, pHandle->data);
      }
      printf("\n");
      return RC_OK;
    } else {
      // Node overflow...create new node and copy keys/pointers over
      printf("OVERFLOW: No room in node\n");
      Node *nodeNew = (Node*) malloc(sizeof(Node));
      nodeNew->contents = (KeyPtr*) malloc(sizeof(KeyPtr) * tree->mgmtData->n);
      nodeNew->leaf = nodeSplit->leaf;
      nodeNew->currentPageNum = tree->mgmtData->numNodes + 1;
      nodeNew->parentPageNum = nodeSplit->parentPageNum;
      if (nodeNew->leaf == TRUE) {
        for (int i = 0; i < tree->mgmtData->n / 2; i++) {
          printf("OVERFLOW: Moving %i.%i.%i-%i\n", nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i + 1].key.v.intV, nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].pageNum, nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].rid.page, nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].rid.slot);
          nodeNew->contents[i].key = nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i + 1].key;
          nodeNew->contents[i].rid = nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i + 1].rid;
          nodeSplit->numKeys--;
          nodeNew->numKeys++;
        }
        nodeNew->nextPageNum = nodeSplit->nextPageNum;
        nodeSplit->nextPageNum = nodeNew->currentPageNum;
      } else {
        for (int i = 0; i < tree->mgmtData->n / 2; i++) {
          printf("OVERFLOW: Moving %i.%i.%i-%i\n", nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].key.v.intV, nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].pageNum, nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].rid.page, nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].rid.slot);
          nodeNew->contents[i].key = nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].key;
          nodeNew->contents[i].rid = nodeSplit->contents[(tree->mgmtData->n + 1) / 2 + i].rid;
          nodeSplit->numKeys--;
          nodeNew->numKeys++;
        }
        nodeSplit->numKeys--;
        nodeNew->nextPageNum = nodeSplit->nextPageNum;
        nodeSplit->nextPageNum = nodeSplit->contents[nodeSplit->numKeys].pageNum;
      }
      tree->mgmtData->numNodes++;
      printf("OVERFLOW: New node created\n");
      // If the root was just split, create new root
      if (nodeSplit->parentPageNum == -1) {
        printf("OVERFLOW: The root was just split...creating a new root\n");
        Node *nodeRoot = (Node*) malloc(sizeof(Node));
        nodeRoot->currentPageNum = tree->mgmtData->numNodes + 1;
        nodeRoot->parentPageNum = -1;
        nodeRoot->nextPageNum = nodeNew->currentPageNum;
        nodeRoot->leaf = FALSE;
        nodeRoot->numKeys = 1;
        nodeRoot->contents = (KeyPtr*) malloc(sizeof(KeyPtr) * tree->mgmtData->n);
        if (nodeSplit->leaf == TRUE) {
          nodeRoot->contents[0].key = nodeNew->contents[0].key;
        } else {
          nodeRoot->contents[0].key = nodeSplit->contents[tree->mgmtData->n / 2].key;
        }
        nodeRoot->contents[0].pageNum = nodeSplit->currentPageNum;
        tree->mgmtData->numNodes++;
        nodeSplit->parentPageNum = nodeNew->parentPageNum = tree->mgmtData->rootPageNum = tree->mgmtData->numNodes;
        writeNode(tree, nodeRoot->currentPageNum, nodeRoot);
        writeNode(tree, nodeSplit->currentPageNum, nodeSplit);
        writeNode(tree, nodeNew->currentPageNum, nodeNew);
        printf("OVERFLOW: Root created\n");
        //printTree(tree);
        printf("OVERFLOW: Verfication\n");
        for (int i = 1; i <= tree->mgmtData->numNodes; i++) {
          pHandle->pageNum = i;
          pinPage(tree->mgmtData->bm, pHandle, i);
          printf("%d: %s\n", i, pHandle->data);
        }
        printf("\n");
        return RC_OK;
      }
      writeNode(tree, nodeSplit->currentPageNum, nodeSplit);
      writeNode(tree, nodeNew->currentPageNum, nodeNew);
      printf("OVERFLOW: Verfication\n");
      for (int i = 1; i <= tree->mgmtData->numNodes; i++) {
        pHandle->pageNum = i;
        pinPage(tree->mgmtData->bm, pHandle, i);
        printf("%d: %s\n", i, pHandle->data);
      }
      printf("\n");
      // Reassign nodeSplit, keyNew, ptrNew for next iteration
      keyNew = nodeNew->contents[0].key;
      pageNumNew = nodeNew->currentPageNum;
      readNode(tree, nodeNew->parentPageNum, nodeSplit);
    }
  }
  printf("FOO\n");
  return RC_IM_NO_MORE_ENTRIES;
}
RC deleteKey (BTreeHandle *tree, Value *key) {
  // Determine if key exists
/*  printf("Checking if key exists!\n");
  RID result;
  findKey(tree, key, &result);
  if (!result.page) {
    printf("Key not in tree...\n");
    return RC_IM_KEY_NOT_FOUND;
  }
  printf("Key exists!\n");
  // Delete key
  Node *node = locateNode(tree, key);
  Node *nodeSplit = node;
  Value *keyDel = key;
  void *ptrDel = &result;
  while (nodeSplit) {
    printf("Deleting key %d!\n", keyDel->v.intV);
    for (int i = 0; i < node->numKeys; i++) {
      if (node->contents[i].key.v.intV == key->v.intV) {
        for (int j = i; j < node->numKeys; j++) {
          node->contents[i].key = node->contents[i + 1].key;
          node->contents[i].ptr = node->contents[i + 1].ptr;
          node->numKeys--;
          printf("Key deleted!\n");
        }
        if (node == tree->mgmtData->bt->root) {
          printf("Root node; exiting...\n");
          return RC_OK;
        }
        break;
      }
    }
    printf("Checking for underflow!\n");
    Node* nodePre;
    Node* postNode;
    if (node->leaf == TRUE) {
      printf("This is a child node.\n");
      if (node->numKeys <= tree->mgmtData->n / 2) {
        printf("Underflow detected!\n");
        for (int i = 0; i < node->parentPageNum->numKeys; i++) {
          if (node->parentPageNum->contents[i].ptr == node) {
            if (i > 0) {
              nodePre = node->parentPageNum->contents[i - 1].ptr;
              if (i == node->parentPageNum->numKeys - 1) {
                printf("Node at end of parent node contents\n");
                postNode = node->parentPageNum->next;
              } else {
                printf("Node at middle of parent node contents\n");
                postNode = node->parentPageNum->contents[i + 1].ptr;
              }
            } else {
              printf("Node at start of parent contents\n");
              postNode = node->parentPageNum->contents[i + 1].ptr;
            }
            break;
          }
        }
        if (!nodePre && !postNode) {
          printf("Node at parent next pointer\n");
          nodePre = node->parentPageNum->contents[node->parentPageNum->numKeys].ptr;
        }
        if (nodePre && node->numKeys + nodePre->numKeys - tree->mgmtData->n / 2 > tree->mgmtData->n / 2) {
          printf("Pre-node has surplus keys\n");
          int nXfer = nodePre->numKeys - tree->mgmtData->n / 2;
          for (int i = 0; i < node->numKeys; i++) {
            node->contents[node->numKeys + nXfer - i - 1] = node->contents[node->numKeys - i - 1];
          }
          for (int i = nXfer; i >= 0; i++) {
            node->contents[i] = nodePre->contents[nodePre->numKeys - nXfer - i - 1];
          }
          printf("Changing pre-node key in parent\n");
          for (int i = 0; i < node->parentPageNum->numKeys; i++) {
            if (node->parentPageNum->contents[i].ptr == nodePre) {
              node->parentPageNum->contents[i].key = node->contents[0].key;
              printf("Key reset in parent\n");
            }
          }
          node->numKeys = node->numKeys + nodePre->numKeys / 2;
          nodePre->numKeys = nodePre->numKeys - nodePre->numKeys / 2;
        } else if (postNode && postNode->numKeys > tree->mgmtData->n / 2) {
          printf("Post-node has surplus keys\n");
          for (int i = 0; i < postNode->numKeys / 2; i++) {
            node->contents[node->numKeys + i] = postNode->contents[i];
            postNode->contents[i] = postNode->contents[postNode->numKeys / 2];
          }
          node->numKeys = node->numKeys + postNode->numKeys / 2;

          printf("Changing node key in parent\n");
          for (int i = 0; i < node->parentPageNum->numKeys; i++) {
            if (node->parentPageNum->contents[i].ptr == node) {
              node->parentPageNum->contents[i].key = postNode->contents[0].key;
              printf("Key reset in parent\n");
            }
          }
        } else {
          printf("Neither node has surplus keys...merging with pre-node\n");
          for (int i = 0; i < node->numKeys; i++) {
            nodePre->contents[nodePre->numKeys + i].key = node->contents[i].key;
            nodePre->contents[nodePre->numKeys + i].ptr = node->contents[i].ptr;
            nodePre->numKeys++;
          }
          // if
        }
      }
    } else {
      printf("This is a parent node.\n");
      if (node->numKeys < (tree->mgmtData->n + 1) / 2 - 1) {

      }
    }
  }*/
}
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){

  BT_ScanMgmtData* mgmtData = (BT_ScanMgmtData*) malloc(sizeof(BT_ScanMgmtData));
  *handle = (BT_ScanHandle*) malloc(sizeof(BT_ScanHandle));
  (*handle)->tree = tree;
  Node *node = (Node*) malloc(sizeof(Node));
  node->contents = (KeyPtr*) malloc(sizeof(KeyPtr) * tree->mgmtData->n + 1);
  readNode(tree, tree->mgmtData->rootPageNum, node);
  mgmtData->idx = 0;
  mgmtData->node = node;
  while(mgmtData->node->leaf == FALSE) {
    readNode(tree, node->contents[0].pageNum, node);
  }
  (*handle)->mgmtData = mgmtData;

  return RC_OK;

}
RC nextEntry (BT_ScanHandle *handle, RID *result){

  Value key;
  Node *node = handle->mgmtData->node;
  if (handle->mgmtData->idx == node->numKeys) {
    if (node->nextPageNum == -1) {
      return RC_IM_NO_MORE_ENTRIES;
    } else {
      handle->mgmtData->idx = 0;
      readNode(handle->tree, node->nextPageNum, node);
    }
  }
  result->page = node->contents[handle->mgmtData->idx].rid.page;
  result->slot = node->contents[handle->mgmtData->idx].rid.slot;
  handle->mgmtData->idx++;

  return RC_OK;

}
RC closeTreeScan (BT_ScanHandle *handle){

  free(handle->mgmtData->node);
  free(handle->mgmtData);

  return RC_OK;

}

// debug and test functions
char *printTree (BTreeHandle *tree){

  Node *node = (Node*) malloc(sizeof(Node));
  node->contents = (KeyPtr*) malloc(sizeof(KeyPtr) * tree->mgmtData->n);
  readNode(tree, tree->mgmtData->rootPageNum, node);
  printf("--START PRINT--\n");
  printf("(0)[");
  if (node->leaf == TRUE) {
    for (int i = 0; i < node->numKeys; i++) {
      printf("%d,%d.%d,", node->contents[i].key.v.intV, node->contents[i].rid.page, node->contents[i].rid.slot);
    }
    printf("%d]\n", node->nextPageNum);
  } else {
    for (int i = 0; i < node->numKeys; i++) {
      printf("%d,%d,", node->contents[i].key.v.intV, node->contents[i].pageNum);
    }
    printf("%d]\n", node->nextPageNum);
    int idx[100], ptr = 0, num = 1;
    idx[0] = 0;
    do {
      if (idx[ptr] > node->numKeys) {
        readNode(tree, node->parentPageNum, node);
        ptr--;
        idx[ptr]++;
      } else {
        if (idx[ptr] < node->numKeys) {
          readNode(tree, node->contents[idx[ptr]].pageNum, node);
        } else {
          readNode(tree, node->nextPageNum, node);
        }
        ptr++;
        idx[ptr] = 0;
        printf("(%d)[", num);
        if (node->leaf == TRUE) {
          for(int i = 0; i < node->numKeys; i++){
            printf("%d,%d.%d,", node->contents[i].key.v.intV, node->contents[i].rid.page, node->contents[i].rid.slot);
          }
          printf("%d]\n", node->nextPageNum);
          if (node->nextPageNum != -1) {
            readNode(tree, node->parentPageNum, node);
            ptr--;
            idx[ptr]++;
          }
          num++;
        } else {
          for (int i = 0; i < node->numKeys; i++) {
            printf("%d,%d,", node->contents[i].key.v.intV, node->contents[i].pageNum);
          }
          printf("%d]\n", node->nextPageNum);
          num++;
        }
      }
    } while (node->nextPageNum != -1);
  }
  printf("--END PRINT--\n");

}

// node I/O
RC readNode (BTreeHandle *tree, int pageNum, Node *node) {

  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  pHandle->pageNum = pageNum;
  pHandle->data = (char*) malloc(PAGE_SIZE);
  pinPage(tree->mgmtData->bm, pHandle, pageNum);
  sscanf(pHandle->data, "%d,%d,%d,%hd,%d", &node->currentPageNum, &node->parentPageNum, &node->nextPageNum, &node->leaf, &node->numKeys);
  if (node->numKeys > 0) {
    char *p, *start = pHandle->data, *end, *result = (char*) malloc(20);
    for (int i = 0; i < 5; i++) {
      start = strstr(start, ",") + 1;
    }
    for (int i = 0; i < node->numKeys; i++) {
      end = strstr(start, ",");
      strncpy(result, start, end - start);
      result[end - start] = '\0';
      node->contents[i].key.v.intV = strtol(result, &p, 10);
      start = end + 1;
    }
    if (node->leaf == TRUE) {
      for (int i = 0; i < node->numKeys; i++) {
        end = strstr(start, "-");
        strncpy(result, start, end - start);
        result[end - start] = '\0';
        node->contents[i].rid.page = strtol(result, &p, 10);
        start = end + 1;
        end = strstr(start, ",");
        strncpy(result, start, end - start);
        result[end - start] = '\0';
        node->contents[i].rid.slot = strtol(result, &p, 10);
        start = end + 1;
      }
    } else {
      for (int i = 0; i < node->numKeys; i++) {
        end = strstr(start, ",");
        strncpy(result, start, end - start);
        result[end - start] = '\0';
        node->contents[i].pageNum = strtol(result, &p, 10);
        start = end + 1;
      }
    }
  }
  unpinPage(tree->mgmtData->bm, pHandle);
/*  memset(pHandle->data, 0, sizeof(pHandle->data));
  free(pHandle);*/

  return RC_OK;

}

RC writeNode (BTreeHandle *tree, int pageNum, Node *node) {

  SM_FileHandle fHandle;
  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  pHandle->pageNum = pageNum;
  pHandle->data = (char*) malloc(PAGE_SIZE);
  openPageFile(tree->idxId, &fHandle);
  ensureCapacity(pageNum, &fHandle);
  closePageFile(&fHandle);
  pinPage(tree->mgmtData->bm, pHandle, pageNum);
  memset(pHandle->data, 0, strlen(pHandle->data));
  sprintf(pHandle->data, "%d,%d,%d,%hd,%d,", node->currentPageNum, node->parentPageNum, node->nextPageNum, node->leaf, node->numKeys);
  if (node->numKeys > 0) {
    for (int i = 0; i < node->numKeys; i++) {
      sprintf(pHandle->data + strlen(pHandle->data), "%d,", node->contents[i].key.v.intV);
    }
    if (node->leaf == TRUE) {
      for (int i = 0; i < node->numKeys; i++) {
        sprintf(pHandle->data + strlen(pHandle->data), "%d-%d,", node->contents[i].rid.page, node->contents[i].rid.slot);
      }
    } else {
      for (int i = 0; i < node->numKeys; i++) {
        sprintf(pHandle->data + strlen(pHandle->data), "%d,", node->contents[i].pageNum);
      }
    }
  }
  markDirty(tree->mgmtData->bm, pHandle);
  unpinPage(tree->mgmtData->bm, pHandle);
  forcePage(tree->mgmtData->bm, pHandle);
  //free(pHandle);

  return RC_OK;

}
