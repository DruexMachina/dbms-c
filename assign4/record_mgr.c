#include "dberror.h"
#include "dt.h"
#include "expr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

#include <stdlib.h>
#include <string.h>


// table and manager
RC initRecordManager (void *mgmtData) {
    return RC_OK;
}
RC shutdownRecordManager () {
    return RC_OK;
}
RC createTable (char *name, Schema *schema) {
    SM_FileHandle fHandle;
    destroyPageFile(name); //For testing only
    createPageFile(name);
    openPageFile(name, &fHandle);
    ensureCapacity(2, &fHandle);
    char *tableInfo = malloc(10000);
    char *serialSchema = serializeSchema(schema);
    sprintf(tableInfo, "%s&%i&[%i-%i]", serialSchema, 0, 1, 0);
    writeBlock(0, &fHandle, tableInfo);
    closePageFile(&fHandle);
    free(tableInfo);
    return RC_OK;
}
RC openTable (RM_TableData *rel, char *name) {
    SM_FileHandle fHandle;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    RM_MgmtData* mgmtData = malloc(sizeof(RM_MgmtData));
    Schema *schema;
    char *start, *end, *target;
    char *result = (char*) malloc(1000);
    // Pin page with table info
    initBufferPool(bm, name, 5, RS_FIFO, NULL);
    pinPage(bm, page, 0);
    unpinPage(bm, page);
    start = page->data;
    // Read schema
    // Identify numAttr
    target = "<";
    start = strstr(start, target) + 1;
    target = ">";
    end = strstr(start, target);
    strncpy(result, start, end - start);
    result[end - start] = '\0';
    char *p;
    int numAttr = strtol(result, &p, 10);
    // Identify other schema parameters
    char **cpNames = malloc(sizeof(char*) * numAttr);
    DataType *cpDt = malloc(sizeof(DataType) * numAttr);
    int* cpSizes = malloc(sizeof(int) * numAttr);
    target = "(";
    start = strstr(start, target) + 1;
    for(int i = 0; i < numAttr; i++) {
        // attrNames
        target = ":";
        end = strstr(start, target);
        cpNames[i] = (char*) malloc(end - start + 1);
        strncpy(cpNames[i], start, end - start);
        cpNames[i][end - start] = '\0';
        // dataTypes, typeLength
        start = end + 2;
        if (i == numAttr - 1) {
            target = ")";
        } else {
            target = ",";
        }
        end = strstr(start, target);
        memset(result, '\0', sizeof(result));
        strncpy(result, start, end - start);
        result[end - start] = '\0';
        target = "]";
        if (strstr(result, target) == NULL) {
            if (strcmp(result, "INT") == 0) {
                cpDt[i] = DT_INT;
            } else if (strcmp(result, "FLOAT") == 0) {
                cpDt[i] = DT_FLOAT;
            } else {
                cpDt[i] == DT_BOOL;
            }
            cpSizes[i] = 0;
            start = end + 2;
        } else {
            end = strstr(start, target);
            target = "[";
            start = strstr(start, target) + 1;
            cpDt[i] = DT_STRING;
            memset(result, 0, sizeof(result));
            strncpy(result, start, end - start);
            result[end - start] = '\0';
            cpSizes[i] = strtol(result, &p, 10);
            start = end + 3;
        }
    }
    // keySize
    int nKeys;
    for (nKeys = 0; start[nKeys]; start[nKeys] == ',' ? nKeys++ : *start++);
    // keyAttrs
    int *cpKeys = malloc(sizeof(int) * (nKeys + 1));
    target = "(";
    start = strstr(end, target) + 1;
    for (int i = 0; i < nKeys + 1; i++) {
        if (i == nKeys) {
            target = ")";
        } else {
            target = ",";
        }
        end = strstr(start, target);
        for (int j = 0; j < numAttr; j++) {
            if (strncmp(cpNames[j], start, end - start) == 0) {
                cpKeys[i] = j;
            }
        }
        start = end + 2;
    }
    schema = createSchema(numAttr, cpNames, cpDt, cpSizes, nKeys + 1, cpKeys);
    // Populate mgmtData
    mgmtData->bm = bm;
    target = "&";
    start = strstr(start, target) + 1;
    end = strstr(start, target);
    memset(result, 0, sizeof(result));
    strncpy(result, start, end - start);
    result[end - start] = '\0';
    mgmtData->numTuples = strtol(result, &p, 10);
    int i = 0;
    int j = 10;
    RID *freespaces = (RID*) malloc(sizeof(RID) * j);
    target = "-";
    while (strstr(start, target) != NULL) {
        if (i == j) {
            j *= 2;
            freespaces = (RID*) realloc(freespaces, sizeof(RID) * j);
        }
        RID id;
        start = end + 2;
        end = strstr(start, target);
        memset(result, 0, sizeof(result));
        strncpy(result, start, end - start);
        result[end - start] = '\0';
        id.page = strtol(result, &p, 10);
        start = end + 1;
        target = "]";
        end = strstr(start, target);
        memset(result, 0, sizeof(result));
        strncpy(result, start, end - start);
        result[end - start] = '\0';
        id.slot = strtol(result, &p, 10);
        freespaces[i] = id;
        target = "-";
        i++;
    }
    mgmtData->lastPageNum = freespaces[0].page;
    mgmtData->lastRecord = freespaces[0].slot - 1;
    mgmtData->freespaces = freespaces;
    mgmtData->freespacesLen = i;
    mgmtData->freespacesCap = j;
    // Populate rel
    rel->name = name;
    rel->schema = schema;
    rel->mgmtData = mgmtData;
    // Cleanup
    free(result);
    free(page->data);
    free(page);
    return RC_OK;
}
RC closeTable (RM_TableData *rel) {
    SM_FileHandle fHandle;
    openPageFile(rel->mgmtData->bm->pageFile, &fHandle);
    char *tableInfo = (char*) malloc(10000);
    char *serialSchema = serializeSchema(rel->schema);
    sprintf(tableInfo, "%s&%i&", serialSchema, rel->mgmtData->numTuples);
    for (int i = 0; i < rel->mgmtData->freespacesLen; i++) {
        int page = rel->mgmtData->freespaces[i].page;
        int slot = rel->mgmtData->freespaces[i].slot;
        sprintf(tableInfo + strlen(tableInfo), "[%i-%i]", page, slot);
    }
    writeBlock(0, &fHandle, tableInfo);
    closePageFile(&fHandle);
    free(tableInfo);
    shutdownBufferPool(rel->mgmtData->bm);
    free(rel->mgmtData->bm);
    return RC_OK;
}
RC deleteTable (char *name) {
    remove(name);
    return RC_OK;
}
int getNumTuples (RM_TableData *rel) {
    return rel->mgmtData->numTuples;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record) {
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    char *start, *end, *target = "&", *r;
    for (int i = rel->mgmtData->freespacesLen - 1; i >= 0; i--) {
        record->id.page = rel->mgmtData->freespaces[i].page;
        record->id.slot = rel->mgmtData->freespaces[i].slot;
        r = serializeRecord(record, rel->schema);
        pinPage(rel->mgmtData->bm, page, record->id.page);
        start = page->data;
        for (int j = 0; j < record->id.slot; j++) {
            start = strstr(start, target) + 1;
        }
        if (i == 0) {
            end = page->data + PAGE_SIZE - sizeof(int) - 6;
        } else {
            end = strstr(start, target);
        }
        if (end - start < strlen(r)) {
            if (i == 0) {
                SM_FileHandle fHandle;
                openPageFile(rel->mgmtData->bm->pageFile, &fHandle);
                ensureCapacity(fHandle.totalNumPages + 1, &fHandle);
                rel->mgmtData->freespaces[0].page++;
                rel->mgmtData->freespaces[0].slot = 0;
                i++;
            }
            unpinPage(rel->mgmtData->bm, page);
        } else {
            if (i == 0) {
                strcat(r, target);
                strcpy(start, r);
                rel->mgmtData->freespaces[0].slot++;
            } else {
                strncpy(start, r, end - start);
                for (int j = i; j < rel->mgmtData->freespacesLen; j++) {
                    rel->mgmtData->freespaces[j] = rel->mgmtData->freespaces[j + 1];
                }
                rel->mgmtData->freespacesLen--;
            }
            markDirty(rel->mgmtData->bm, page);
            unpinPage(rel->mgmtData->bm, page);
            forcePage(rel->mgmtData->bm, page);
            rel->mgmtData->numTuples++;
            break;
        }
    }
    rel->mgmtData->lastPageNum = rel->mgmtData->freespaces[0].page;
    rel->mgmtData->lastRecord = rel->mgmtData->freespaces[0].slot - 1;
    free(page->data);
    free(page);
    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id) {
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData->bm, page, id.page);
    char *start = page->data, *end, *target = "&";
    for (int i = 0; i < id.slot; i++) {
        start = strstr(start, target) + 1;
    }
    end = strstr(start, target);
    if (id.page == rel->mgmtData->lastPageNum && id.slot == rel->mgmtData->lastRecord) {
        memset(start, '@', end - start + 1);
        rel->mgmtData->lastRecord--;
    } else {
        memset(start, '@', end - start);
    }
    if (rel->mgmtData->freespacesLen == rel->mgmtData->freespacesCap) {
        rel->mgmtData->freespacesCap *= 2;
        rel->mgmtData->freespaces = (RID*) realloc(rel->mgmtData->freespaces, sizeof(RID) * rel->mgmtData->freespacesCap);
    }
    rel->mgmtData->freespaces[rel->mgmtData->freespacesLen] = id;
    rel->mgmtData->freespacesLen++;
    markDirty(rel->mgmtData->bm, page);
    unpinPage(rel->mgmtData->bm, page);
    forcePage(rel->mgmtData->bm, page);
    rel->mgmtData->numTuples--;
    free(page->data);
    free(page);
    return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record) {
    deleteRecord(rel, record->id);
    insertRecord(rel, record);
    return RC_OK;
}
RC getRecord (RM_TableData *rel, RID id, Record *record) {
    record->id.page = id.page;
    record->id.slot = id.slot;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData->bm, page, id.page);
    char *start = page->data, *end, *targetStart = "&", *targetEnd = ",";
    for (int i = 0; i < id.slot; i++) {
        start = strstr(start, targetStart) + 1;
        if (start == NULL) {
            return RC_RM_NO_MORE_TUPLES;
        }
    }
    targetStart = ":";
    for (int i = 0; i < rel->schema->numAttr; i++) {
        start = strstr(start, targetStart) + 1;
        if (i == rel->schema->numAttr - 1) {
            targetEnd = ")";
        }
        end = strstr(start, targetEnd);
        char *val = malloc(end - start + 2);
        if (rel->schema->dataTypes[i] == DT_INT) {
            strncpy(val, "i", 1);
        } else if (rel->schema->dataTypes[i] == DT_FLOAT) {
            strncpy(val, "f", 1);
        } else if (rel->schema->dataTypes[i] == DT_BOOL) {
            strncpy(val, "b", 1);
        } else {
            strncpy(val, "s", 1);
        }
        strncpy(val + 1, start, end - start);
        val[end - start + 1] = '\0';
        Value *value = stringToValue(val);
        setAttr(record, rel->schema, i, value);
        free(val);
    }
    unpinPage(rel->mgmtData->bm, page);
    free(page->data);
    free(page);
    return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    RM_ScanMgmtData* mgmtData = (RM_ScanMgmtData*) malloc(sizeof(RM_ScanMgmtData));
    RID id;
    id.page = 1;
    id.slot = 0;
    mgmtData->cond = cond;
    mgmtData->id = id;
    scan->rel = rel;
    scan->mgmtData = mgmtData;
    return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record) {
    Value* val;
    if (scan->mgmtData->id.page == scan->rel->mgmtData->lastPageNum && scan->mgmtData->id.slot > scan->rel->mgmtData->lastRecord) {
        return RC_RM_NO_MORE_TUPLES;
    }
    int RC = getRecord(scan->rel, scan->mgmtData->id, record);
    if (RC != RC_OK) {
        scan->mgmtData->id.page++;
        scan->mgmtData->id.slot = 0;
        getRecord(scan->rel, scan->mgmtData->id, record);
    }
    scan->mgmtData->id.slot++;
    evalExpr(record, scan->rel->schema, scan->mgmtData->cond, &val);
    if(val->v.boolV != 1) {
        return next(scan, record);
    } else {
        return RC_OK;
    }
}
RC closeScan (RM_ScanHandle *scan){
    free(scan->mgmtData);
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema) {
    int recordSize = 0;
    for(int i = 0; i < schema->numAttr; i++) {
        if (schema->dataTypes[i] == DT_INT) {
            recordSize += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            recordSize += sizeof(float);
        } else if (schema->dataTypes[i] == DT_BOOL) {
            recordSize += sizeof(bool);
        } else {
            recordSize += schema->typeLength[i];
        }
    }
    return recordSize;
}
// if schema struct is present, sum typelength
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    Schema *schema = (Schema*) malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    return schema;
}
//Set all the attributes of the schema to NULL in order to edit the schema.
RC freeSchema (Schema *schema) {
    free(schema);
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema) {
    *record = (Record*) malloc(sizeof(Record));
    (*record)->data = (char*) malloc(getRecordSize(schema));
    return RC_OK;
}
RC freeRecord (Record *record) {
    free(record);
    return RC_OK;
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
    char *data = record->data;
    for (int i = 0; i < attrNum; i++) {
        if (schema->dataTypes[i] == DT_INT) {
            data += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            data += sizeof(float);
        } else if (schema->dataTypes[i] == DT_BOOL) {
            data += sizeof(bool);
        } else {
            data += sizeof(schema->typeLength[i] + 1);
        }
    }
    *value = (Value*) malloc(sizeof(value));
    (*value)->dt = schema->dataTypes[attrNum];
    if (schema->dataTypes[attrNum] == DT_INT) {
        memcpy(&((*value)->v.intV), data, sizeof(int));
    } else if (schema->dataTypes[attrNum] == DT_FLOAT) {
        memcpy(&((*value)->v.floatV), data, sizeof(float));
    } else if (schema->dataTypes[attrNum] == DT_BOOL) {
        memcpy(&((*value)->v.boolV), data, sizeof(bool));
    } else {
        char *str = (char*) malloc(schema->typeLength[attrNum] + 1);
        strncpy(str, data, schema->typeLength[attrNum]);
        str[schema->typeLength[attrNum]] = '\0';
        (*value)->v.stringV = str;
    }
    return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
    char *data = record->data;
    for (int i = 0; i < attrNum; i++) {
        if (schema->dataTypes[i] == DT_INT) {
            data += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            data += sizeof(float);
        } else if (schema->dataTypes[i] == DT_BOOL) {
            data += sizeof(bool);
        } else {
            data += sizeof(schema->typeLength[i] + 1);
        }
    }
    if (schema->dataTypes[attrNum] == DT_INT) {
        memcpy(data, &(value->v.intV), sizeof(int));
    } else if (schema->dataTypes[attrNum] == DT_FLOAT) {
        memcpy(data, &(value->v.floatV), sizeof(float));
    } else if (schema->dataTypes[attrNum] == DT_BOOL) {
        memcpy(data, &(value->v.boolV), sizeof(bool));
    } else {
        memcpy(data, value->v.stringV, schema->typeLength[attrNum] );
    }
    return RC_OK;
}
