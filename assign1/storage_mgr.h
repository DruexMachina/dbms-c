#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cerrno>

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
typedef struct SM_MetaData {
    int totalNumPages;
};

extern void initStorageManager (void);
extern RC createPageFile (char *fileName, SM_MetaData *metadata){
    fopen(fileName, "w");
    metadata -> totalNumPages = 1;
    fwrite(metadata, sizeof(metadata), 1, fileName);
    fwrite('\0', sizeof('\0'), PAGE_SIZE - sizeof(metadata), fileName);
    fclose(fileName);
    return RC_OK
}
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle, SM_MetaData *metadata){
    fopen(fileName, "r+");
    if (errno){
        return RC_FILE_NOT_FOUND
    else {
        fHandle->*fileName = fileName;
        fread(metadata, sizeof(metadata), 1, fileName);
        fHandle->curPagePos = 0;
        return RC_OK
    }
}
extern RC closePageFile (SM_FileHandle *fHandle){
    fclose(fHandle->fileName);
}
extern RC destroyPageFile (char *fileName){
    remove(fileName);
}

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    if(metadata->totalNumPages < pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        fread(memPage, PAGE_SIZE - sizeof(metadata), fHandle->totalNumPages, fhandle->*fileName);
        return RC_OK;
    }
}
extern int getBlockPos (SM_FileHandle *fHandle){
    return PAGE_SIZE * (curPagePos - 1) + 1;
}
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(0, fHandle, memPage);
    return RC_OK;
}
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->curPagePos == 0){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos - 1, fHandle, memPage);
        return RC_OK;
    }
}
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(fHandle->curPagePos, fHandle, memPage);
    return RC_OK;

}
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    if(fHandle->curPagePos == metadata->totalNumPages - 1){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos + 1, fHandle, memPage);
        return RC_OK;
    }
}
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    readBlock(metadata->totalNumPages - 1, fHandle, memPage);
    return RC_OK
}

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata) {
    if (pageNum >= metadata->totalNumPages) {
        return RC_WRITE_FAILED;
    } else {
        fseek(fHandle->fileName, 1, (pageNum - 1) * PAGE_SIZE);
        fwrite()
        fwrite(memPage, PAGE_SIZE, 1, fHandle->fileName);
    }

}
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata) {
    fseek(fHandle->fileName, 1 + sizeof(metadata), fHandle->curPagePos * PAGE_SIZE);
    fwrite(memPage, PAGE_SIZE - sizeof(metdata), 1, fHandle->fileName);
    return RC_OK;
}
extern RC appendEmptyBlock (SM_FileHandle *fHandle, SM_MetaData *metadata) {
    fseek(fHandle->fileName, 1, metadata->totalNumPages * PAGE_SIZE);
    metadata->totalNumPages++;
    fwrite(metadata, sizeof(metadata), 1, fileName);
    fwrite('\0', sizeof('\0'), PAGE_SIZE - sizeof(metadata), fHandle->fileName);
    return RC_OK;
}
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle, SM_MetaData *metadata) {
    if (metadata->totalNumPages < numberOfPages) {
        for (int i = 0; i < (numberOfPages - metadata->totalNumPages); i++) {
            appendEmptyBlock(fHandle, metadata);
        }
    }
    return RC_OK;
}

#endif
