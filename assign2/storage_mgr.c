#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

void initStorageManager (void){}

RC createPageFile (char *fileName){
    FILE* db = fopen(fileName, "w+");
    char null = '\0';
    if (!db){
        return RC_FILE_NOT_FOUND;
    }
    char *curPageStr = malloc(6);
    curPageStr = "Page-0";
    fwrite(curPageStr, strlen(curPageStr), 1, db);
    for (int i = 0; i < PAGE_SIZE - strlen(curPageStr); i++){
        fwrite(&null, sizeof(null), 1, db);
    }
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    fHandle->db = fopen(fileName, "r+");
    if (!fHandle->db){
        return RC_FILE_NOT_FOUND;
    } else {
        struct stat fileInfo;
        stat(fileName, &fileInfo);
        int size = fileInfo.st_size;
        fHandle->totalNumPages = (int) size / PAGE_SIZE;
        fHandle->fileName = fileName;
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
        fseek(fHandle->db, pageNum * PAGE_SIZE, SEEK_SET);
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
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->curPagePos == 0){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos - 1, fHandle, memPage);
    }
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->curPagePos == fHandle->totalNumPages - 1){
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        readBlock(fHandle->curPagePos + 1, fHandle, memPage);
    }
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (pageNum >= fHandle->totalNumPages){
        return RC_WRITE_FAILED;
    } else {
        fseek(fHandle->db, pageNum * PAGE_SIZE, SEEK_SET);
        fHandle->curPagePos = pageNum;
        char* str = malloc(12);
        sprintf(str, "%d", pageNum);
        char *curPageStr = malloc(5 + strlen(str));
        strcpy(curPageStr, "Page-");
        strcat(curPageStr, str);
        fwrite(memPage, PAGE_SIZE, 1, fHandle->db);
        free(str);
        free(curPageStr);
        return RC_OK;
    }

}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
    char null = '\0';
    fseek(fHandle->db, 0, SEEK_END);
    char* str = malloc(12);
    sprintf(str, "%d", fHandle->totalNumPages);
    char *curPageStr = malloc(5 + strlen(str));
    strcpy(curPageStr, "Page-");
    strcat(curPageStr, str);
    fwrite(curPageStr, strlen(curPageStr), 1, fHandle->db);
    fwrite(&null, sizeof('\0'), PAGE_SIZE - strlen(curPageStr), fHandle->db);
    fHandle->totalNumPages++;
    free(str);
    free(curPageStr);
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
