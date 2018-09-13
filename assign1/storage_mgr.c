#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

using namespace std;

/* manipulating page files */

void initStorageManager (void){}
RC createPageFile (char *fileName, SM_FileHandle *fHandle, SM_MetaData *metadata){
    char null = '\0';
    fHandle->db = fopen(fileName, "w+");
    if (!fHandle->db) {
        return RC_FILE_NOT_FOUND;
    }
    metadata->totalNumPages = 0;
    appendEmptyBlock(fHandle, metadata);
    fclose(fHandle->db);
    return RC_OK;
}
RC openPageFile (char *fileName, SM_FileHandle *fHandle, SM_MetaData *metadata){
    fHandle->db = fopen(fileName, "r+");
    if (!fHandle->db){
        return RC_FILE_NOT_FOUND;
    } else {
        fHandle->fileName = fileName;
        fread(metadata, sizeof(SM_MetaData), 1, fHandle->db);
        fHandle->curPagePos = 0;
        return RC_OK;
    }
}
RC closePageFile (SM_FileHandle *fHandle, SM_MetaData *metadata){
    fclose(fHandle->db);
}
RC destroyPageFile (char *fileName){
    remove(fileName);
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    if(metadata->totalNumPages < pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        fseek(fHandle->db, pageNum * PAGE_SIZE + sizeof(SM_MetaData), SEEK_SET);
        fread(memPage, PAGE_SIZE, 1, fHandle->db);
        return RC_OK;
    }
}
int getBlockPos (SM_FileHandle *fHandle){
    return PAGE_SIZE * (fHandle->curPagePos - 1) + 1;
}
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    readBlock(0, fHandle, memPage, metadata);
    return RC_OK;
}
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    if(fHandle->curPagePos == 0){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos - 1, fHandle, memPage, metadata);
        return RC_OK;
    }
}
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    readBlock(fHandle->curPagePos, fHandle, memPage, metadata);
    return RC_OK;

}
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    if(fHandle->curPagePos == metadata->totalNumPages - 1){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos + 1, fHandle, memPage, metadata);
        return RC_OK;
    }
}
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    readBlock(metadata->totalNumPages - 1, fHandle, memPage, metadata);
    return RC_OK;
}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata) {
    if (pageNum >= metadata->totalNumPages) {
        return RC_WRITE_FAILED;
    } else {
        fseek(fHandle->db, pageNum * PAGE_SIZE + sizeof(SM_MetaData), SEEK_SET);
        fwrite(memPage, PAGE_SIZE, 1, fHandle->db);
        return RC_OK;
    }

}
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata) {
    fseek(fHandle->db, 1 + sizeof(SM_MetaData), fHandle->curPagePos * PAGE_SIZE);
    fwrite(memPage, PAGE_SIZE - sizeof(SM_MetaData), 1, fHandle->db);
    return RC_OK;
}
RC appendEmptyBlock (SM_FileHandle *fHandle, SM_MetaData *metadata) {
    char null = '\0';
    fseek(fHandle->db, 0, SEEK_END);
    metadata->totalNumPages++;
    fwrite(metadata, sizeof(SM_MetaData), 1, fHandle->db);
    for (int i = 1; i < PAGE_SIZE; i++) {
        fwrite(&null, sizeof('\0'), 1, fHandle->db);
    }
    return RC_OK;
}
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle, SM_MetaData *metadata) {
    if (metadata->totalNumPages < numberOfPages) {
        for (int i = 0; i < (numberOfPages - metadata->totalNumPages); i++) {
            appendEmptyBlock(fHandle, metadata);
        }
    }
    return RC_OK;
}
