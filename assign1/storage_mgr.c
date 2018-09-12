#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* manipulating page files */

void initStorageManager (void){
    return;
}
RC createPageFile (char *fileName, SM_MetaData *metadata){
    metadata->db = fopen(fileName, "w+");
    if (!metadata->db) {
        return RC_FILE_NOT_FOUND;
    }
    metadata -> totalNumPages = 1;
    fwrite(metadata, sizeof(metadata), 1, metadata->db);
    fwrite('\0', sizeof('\0'), PAGE_SIZE - sizeof(metadata), metadata->db);
    fclose(metadata->db);
    return RC_OK;
}
RC openPageFile (char *fileName, SM_FileHandle *fHandle, SM_MetaData *metadata){
    metadata->db = fopen(fileName, "r+");
    if (!metadata->db){
        return RC_FILE_NOT_FOUND;
    } else {
        fHandle->fileName = fileName;
        fread(metadata, sizeof(metadata), 1, metadata->db);
        fHandle->curPagePos = 0;
        return RC_OK;
    }
}
RC closePageFile (SM_FileHandle *fHandle, SM_MetaData *metadata){
    fclose(metadata->db);
}
RC destroyPageFile (char *fileName){
    remove(fileName);
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata){
    if(metadata->totalNumPages < pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        fread(memPage, PAGE_SIZE - sizeof(metadata), metadata->totalNumPages, metadata->db);
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
        fseek(metadata->db, 1, (pageNum - 1) * PAGE_SIZE);
        fwrite(memPage, PAGE_SIZE, 1, metadata->db);
    }

}
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage, SM_MetaData *metadata) {
    fseek(metadata->db, 1 + sizeof(metadata), fHandle->curPagePos * PAGE_SIZE);
    fwrite(memPage, PAGE_SIZE - sizeof(metadata), 1, metadata->db);
    return RC_OK;
}
RC appendEmptyBlock (SM_FileHandle *fHandle, SM_MetaData *metadata) {
    fseek(metadata->db, 1, metadata->totalNumPages * PAGE_SIZE);
    metadata->totalNumPages++;
    fwrite(metadata, sizeof(metadata), 1, metadata->db);
    fwrite('\0', sizeof('\0'), PAGE_SIZE - sizeof(metadata), metadata->db);
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
