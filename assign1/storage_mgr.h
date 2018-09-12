#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
    char *fileName;
    int totalNumPages;
    int curPagePos;
    void *mgmtInfo;
} SM_FileHandle;

typedef struct SM_MetaData {
    int totalNumPages;
    FILE* db;
} SM_MetaData;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName, SM_MetaData *metadata);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle, SM_MetaData *metadata);
extern RC closePageFile (SM_FileHandle *fHandle, SM_MetaData *metadata);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata);
extern RC appendEmptyBlock (SM_FileHandle *fHandle, SM_MetaData *metadata);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle, SM_MetaData *metadata);

#endif
