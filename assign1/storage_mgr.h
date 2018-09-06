#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
	char *fileName;
	int totalNumPages;
	int curPagePos;
	void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName){
    int i;
    typedef struct SM_
    fopen(*fileName, "w");
    fwrite(1); //totalNumPages
    for (i = 0; i < PAGE_SIZE; i++) {
        fwrite('\0');
    }
    fclose();
}
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    fopen(*fileName, "r");
    fHandle->*fileName = *fileName;
    fHandle->totalNumPages = ; //read totalNumPages from start of file
    fHandle->curPagePos = 0;
}

extern RC closePageFile (SM_FileHandle *fHandle){
    fclose(fHandle->fileName);
    //update metadata
}
    
extern RC destroyPageFile (char *fileName){
    remove(fileName);
}

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    fseek(fHandle->fileName, 1, (pageNum - 1) * PAGE_SIZE);
    fwrite(memPage, PAGE_SIZE, 1, fHandle->fileName);
}
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    fseek(fHandle->fileName, 1, fHandle->curPagePos * PAGE_SIZE);
    fwrite(memPage, PAGE_SIZE, 1, fHandle->fileName);
}
extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
    int i;
    fseek(fHandle->fileName, 1, fHandle->totalNumPages * PAGE_SIZE);
    fHandle->totalNumPages++;
    fwrite(fHandle->totalNumPages);
    for (i = 0; i < PAGE_SIZE; i++) {
        fwrite('\0');
    }
}
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle->totalNumPages < numberOfPages) {
        for (int i = 0; i < (numberOfPages - fHandle->totalNumPages); i++) {
            appendEmptyBlock (fHandle);
        }
    }
}

#endif
