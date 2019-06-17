#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "dt.h"

#include <stdlib.h>
#include <string.h>

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
        const int numPages, ReplacementStrategy strategy, void *stratData) {
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->openPage = 0;
    bm->oldestPage = 0;
    bm->strategy = strategy;
    BM_Frame* buffer = malloc(numPages * sizeof(BM_Frame));
    for (int i = 0; i < numPages; i++) {
        buffer[i].frameData = malloc(PAGE_SIZE);
        buffer[i].frameContents = NO_PAGE;
        buffer[i].dirtyFlag = false;
        buffer[i].fixCount = 0;
        buffer[i].prevFrame = NULL;
        buffer[i].nextFrame = NULL;
    }
    BM_MgmtData* mgmtData = malloc(sizeof(BM_MgmtData));
    mgmtData->numReadIO = 0;
    mgmtData->numWriteIO = 0;
    mgmtData->buffer = buffer;
    mgmtData->firstFrame = &buffer[0];
    mgmtData->lastFrame = &buffer[0];
    bm->mgmtData = mgmtData;
    /*if (!stratData) {
        bm->mgmtData->stratData = stratData;
    }*/
    return RC_OK;
}
RC shutdownBufferPool(BM_BufferPool *const bm) {
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        if (frame->fixCount != 0) {
            return RC_WRITE_FAILED;
        } else if (frame->dirtyFlag == true) {
            forceFlushPool(bm);
            break;
        }
    }
    BM_MgmtData *mgmtData = bm->mgmtData;
    free(mgmtData);

    return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm) {
    SM_FileHandle fHandle;
    BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
    pHandle->data = malloc(PAGE_SIZE);
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        if (frame->fixCount == 0 && frame->dirtyFlag == true) {
            pHandle->pageNum = frame->frameContents;
            strcpy(pHandle, frame->frameData);
            openPageFile(bm->pageFile, &fHandle);
            writeBlock(pHandle->pageNum, &fHandle, pHandle);
            closePageFile(&fHandle);
            frame->dirtyFlag = false;
            bm->mgmtData->numWriteIO++;
        }
    }
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        if (frame->frameContents == page->pageNum) {
            frame->dirtyFlag = true;

            return RC_OK;
        }
    }

    return RC_WRITE_FAILED;
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        if (frame->frameContents == page->pageNum) {
            frame->fixCount--;
            return RC_OK;
        }
    }

    return RC_WRITE_FAILED;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        if (frame->frameContents == page->pageNum) {
            SM_FileHandle fHandle;
            openPageFile(bm->pageFile, &fHandle);
            writeBlock(page->pageNum, &fHandle, page->data);
            closePageFile(&fHandle);
            bm->mgmtData->numWriteIO++;

            return RC_OK;
        }
    }
    return RC_WRITE_FAILED;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
        const PageNumber pageNum) {
    page->data = malloc(PAGE_SIZE);
    // Checks to see if page is already in buffer
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        if (frame->frameContents == pageNum) {
            frame->fixCount++;
            page->pageNum = frame->frameContents;
            strcpy(page->data, frame->frameData);
            if (bm->openPage > 1) {
                if (frame == bm->mgmtData->lastFrame) {
                    bm->mgmtData->lastFrame = bm->mgmtData->lastFrame->prevFrame;
                }
                if (frame->prevFrame != NULL) {
                    frame->prevFrame->nextFrame = frame->nextFrame;
                }
                if (frame->nextFrame != NULL) {
                    frame->nextFrame->prevFrame = frame->prevFrame;
                }
                frame->nextFrame = bm->mgmtData->firstFrame;
                frame->prevFrame = NULL;
                bm->mgmtData->firstFrame->prevFrame = frame;
                bm->mgmtData->firstFrame = frame;
            }

            return RC_OK;
        }
    }
    // Page not in buffer
    BM_Frame *newFrame = malloc(sizeof(BM_Frame));
    newFrame->frameData = malloc(PAGE_SIZE);
    SM_FileHandle fHandle;
    openPageFile(bm->pageFile, &fHandle);
    ensureCapacity(pageNum + 1, &fHandle);
    readBlock(pageNum, &fHandle, page->data);
    closePageFile(&fHandle);
    page->pageNum = pageNum;
    newFrame->frameContents = pageNum;
    strcpy(newFrame->frameData, page->data);
    newFrame->dirtyFlag = false;
    newFrame->fixCount = 1;
    bm->mgmtData->numReadIO++;
    BM_Frame *frame;
    BM_PageHandle *pHandle;
    // Replacement strategy implementation
    if (bm->openPage == bm->numPages) {
        int i = 0;
        bool flag = false;
        switch(bm->strategy) {
            // FIFO
            case RS_FIFO :
                while (flag == false) {
                    frame = &bm->mgmtData->buffer[(bm->oldestPage + i) % bm->numPages];
                    if (frame->fixCount == 0) {
                        flag = true;
                    }
                    i++;
                }
                bm->openPage--;
                break;
            // LRU
            case RS_LRU :
                frame = bm->mgmtData->lastFrame;
                while (flag == false) {
                    if (frame != bm->mgmtData->lastFrame) {
                        frame = frame->prevFrame;
                    }
                    if (frame->fixCount == 0) {
                        flag = true;
                    }
                    if (frame->prevFrame == NULL && flag == false) {
                        return RC_WRITE_FAILED;
                    }
                }
                if (frame == bm->mgmtData->lastFrame) {
                    bm->mgmtData->lastFrame = bm->mgmtData->lastFrame->prevFrame;
                }
                if (frame->prevFrame != NULL) {
                    frame->prevFrame->nextFrame = frame->nextFrame;
                }
                if (frame->nextFrame != NULL) {
                    frame->nextFrame->prevFrame = frame->prevFrame;
                }
                newFrame->nextFrame = bm->mgmtData->firstFrame;
                newFrame->prevFrame = NULL;
                bm->mgmtData->firstFrame->prevFrame = newFrame;
                bm->mgmtData->firstFrame = newFrame;
                bm->openPage--;
                break;
            // CLOCK
            case RS_CLOCK :
                break;
            // LFU
            case RS_LFU :
                break;
            // LRU_K
            case RS_LRU_K :
                break;
        }
    } else {
        frame = &bm->mgmtData->buffer[bm->openPage];

    }
    if (frame->dirtyFlag == true) {
        pHandle = MAKE_PAGE_HANDLE();
        pHandle->data = malloc(PAGE_SIZE);
        pHandle->pageNum = frame->frameContents;
        pHandle->data = frame->frameData;
        forcePage(bm, pHandle);
    }
    frame->frameContents = newFrame->frameContents;
    frame->frameData = newFrame->frameData;
    frame->dirtyFlag = newFrame->dirtyFlag;
    frame->fixCount = newFrame->fixCount;
    bm->oldestPage = (bm->oldestPage + 1) % bm->numPages;
    bm->openPage++;

    return RC_OK;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    PageNumber *frameContents = malloc(bm->numPages * sizeof(PageNumber));
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        frameContents[i] = frame->frameContents;
    }
    return frameContents;
}
bool *getDirtyFlags (BM_BufferPool *const bm) {
    bool *dirtyFlags = malloc(bm->numPages * sizeof(bool));
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        dirtyFlags[i] = frame->dirtyFlag;
    }
    return dirtyFlags;
}
int *getFixCounts (BM_BufferPool *const bm) {
    int *fixCounts = malloc(bm->numPages * sizeof(int));
    for (int i = 0; i < bm->numPages; i++) {
        BM_Frame *frame = &bm->mgmtData->buffer[i];
        fixCounts[i] = frame->fixCount;
    }
    return fixCounts;
}
int getNumReadIO (BM_BufferPool *const bm) {
    return bm->mgmtData->numReadIO;
}
int getNumWriteIO (BM_BufferPool *const bm) {
    return bm->mgmtData->numWriteIO;
}
