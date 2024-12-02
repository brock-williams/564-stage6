#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
                      const int attrCnt, 
                      const attrInfo attrList[])
{
    Status status;
    
    // Get relation's attribute information from catalog
    int relAttrCnt;
    AttrDesc *attrs = NULL;
    status = attrCat->getRelInfo(relation, relAttrCnt, attrs);
    if (status != OK) return status;

    // Verify attribute count matches
    if (attrCnt != relAttrCnt) {
        delete[] attrs;
        return ATTRTYPEMISMATCH;
    }

    // Create buffer for new record
    int recordLength = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        recordLength += attrs[i].attrLen;
    }
    char* recData = new char[recordLength];
    memset(recData, 0, recordLength);

    // For each catalog attribute, find its value in attrList
    for (int i = 0; i < relAttrCnt; i++) {
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
                if (attrList[j].attrValue == NULL) {
                    delete[] attrs;
                    delete[] recData;
                    return ATTRNOTFOUND;
                }

                if (attrs[i].attrType == INTEGER) {
                    // Convert string to integer
                    int value = atoi((char*)attrList[j].attrValue);
                    memcpy(recData + attrs[i].attrOffset, &value, sizeof(int));
                } 
                else if (attrs[i].attrType == FLOAT) {
                    float value = atof((char*)attrList[j].attrValue);
                    memcpy(recData + attrs[i].attrOffset, &value, sizeof(float));
                }
                else { // STRING
                    strncpy(recData + attrs[i].attrOffset,
                           (char*)attrList[j].attrValue,
                           attrs[i].attrLen);
                }
                break;
            }
        }
    }

    // Insert the record
    InsertFileScan* inserter = new InsertFileScan(relation, status);
    if (status != OK) {
        delete[] attrs;
        delete[] recData;
        return status;
    }

    Record rec;
    rec.data = recData;
    rec.length = recordLength;
    RID outRid;
    status = inserter->insertRecord(rec, outRid);

    // Clean up
    delete inserter;
    delete[] attrs;
    delete[] recData;

    return status;
}