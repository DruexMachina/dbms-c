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
    fopen(fileName, "w");
    fwrite(1); //totalNumPages
    fwrite('\0', sizeof('\0'), PAGE_SIZE - sizeof(1), fileName);
    fclose();
}
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    fopen(fileName, "r+");
    fHandle->fileName = fileName;
    fread(fHandle->totalNumPages, sizeof(fHandle->totalNumPages), 1, fileName); //read totalNumPages from start of file
    fHandle->curPagePos = 0;
}

extern RC closePageFile (SM_FileHandle *fHandle){
    fclose(fHandle->fileName);
    //TO-DO: update metadata
}
    
extern RC destroyPageFile (char *fileName){
    remove(fileName);
}

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->totalNumPages < pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        fread(memPage, PAGE_SIZE, fHandle->totalNumPages, fhandle->*fileName);
    }
}
extern int getBlockPos (SM_FileHandle *fHandle){
    return (fhandle->curPagePos);
}
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(0, fHandle, memPage);
}
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(getBlockPos(fHandle) == 0){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(getBlockPos(fHandle)-1, fHandle, memPage);
    }
}
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    readBlock(fHandle->curPagePos, fHandle, memPage);

}
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(getBlockPos(fHandle)==(PAGE_SIZE-1){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(getBlockPos(fHandle) + 1, fHandle, memPage);
        
    }
    
}
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock((PAGE_SIZE-1), fHandle, memPage);
    
}
    
    

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
    fwrite('\0', sizeof('\0'), PAGE_SIZE - sizeof(fHandle->totalNumPages), fHandle->fileName);
}
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle->totalNumPages < numberOfPages) {
        for (int i = 0; i < (numberOfPages - fHandle->totalNumPages); i++) {
            appendEmptyBlock(fHandle);
        }
    }
}

#endif
