#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct SM_Header {
    int totalNumPages;
} SM_Header;

void initStorageManager (void){}

RC createPageFile (char *fileName){
    FILE* db = fopen(fileName, "w+");
    struct SM_Header header;
    header.totalNumPages = 1;
    char null = '\0';
    if (!db){
        return RC_FILE_NOT_FOUND;
    }
    fwrite(&header, sizeof(SM_Header), 1, db);
    for (int i = 1; i < PAGE_SIZE; i++){
        fwrite(&null, sizeof('\0'), 1, db);
    }
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    struct SM_Header header;
    fHandle->db = fopen(fileName, "r+");
    if (!fHandle->db){
        return RC_FILE_NOT_FOUND;
    } else {
        fread(&header, sizeof(SM_Header), 1, fHandle->db);
        fHandle->fileName = fileName;
        fHandle->totalNumPages = header.totalNumPages;
        fHandle->curPagePos = 0;
        return RC_OK;
    }
}

RC closePageFile (SM_FileHandle *fHandle){
    fclose(fHandle->db);
}

RC destroyPageFile (char *fileName){
    remove(fileName);
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->totalNumPages < pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        fseek(fHandle->db, pageNum * PAGE_SIZE + sizeof(SM_Header), SEEK_SET);
        fHandle->curPagePos = pageNum;
        fread(memPage, PAGE_SIZE, 1, fHandle->db);
        fHandle->curPagePos++;
        return RC_OK;
    }
}

int getBlockPos (SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(0, fHandle, memPage);
    return RC_OK;
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->curPagePos == 0){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos - 1, fHandle, memPage);
        return RC_OK;
    }
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(fHandle->curPagePos, fHandle, memPage);
    return RC_OK;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->curPagePos == fHandle->totalNumPages - 1){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos + 1, fHandle, memPage);
        return RC_OK;
    }
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
    return RC_OK;
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (pageNum >= fHandle->totalNumPages){
        return RC_WRITE_FAILED;
    } else {
        fseek(fHandle->db, pageNum * PAGE_SIZE + sizeof(SM_Header), SEEK_SET);
        fHandle->curPagePos = pageNum;
        fwrite(memPage, PAGE_SIZE, 1, fHandle->db);
        return RC_OK;
    }

}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    writeBlock(fHandle->curPagePos, fHandle, memPage);
    return RC_OK;
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
    char null = '\0';
    fseek(fHandle->db, 0, SEEK_END);
    for (int i = 1; i < PAGE_SIZE; i++){
        fwrite(&null, sizeof('\0'), 1, fHandle->db);
    }
    fHandle->totalNumPages++;
    struct SM_Header header;
    header.totalNumPages = fHandle->totalNumPages;
    fwrite(&header, sizeof(SM_Header), 1, fHandle->db);
    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    if (fHandle->totalNumPages < numberOfPages){
        int to_create = numberOfPages - fHandle->totalNumPages;
        for (int i = 0; i < to_create; i++){
            appendEmptyBlock(fHandle);
            fHandle->totalNumPages++;
            fHandle->curPagePos = fHandle->totalNumPages - 1;
        }
    }
    return RC_OK;
}
