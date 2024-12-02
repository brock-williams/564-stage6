#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
                      const string & attrName, 
                      const Operator op,
                      const Datatype type, 
                      const char *attrValue)
{
    cout << "Doing QU_Delete" << endl;
    Status status;

    // If attrName is empty, delete all records
    if (attrName.empty()) {
        HeapFileScan* scanner = new HeapFileScan(relation, status);
        if (status != OK) return status;

        status = scanner->startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) {
            delete scanner;
            return status;
        }

        RID rid;
        while (scanner->scanNext(rid) == OK) {
            status = scanner->deleteRecord();
            if (status != OK) break;
        }

        scanner->endScan();
        delete scanner;
        return status;
    }

    // Get attribute information for the filter
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) return status;

    // Create scanner
    HeapFileScan* scanner = new HeapFileScan(relation, status);
    if (status != OK) return status;

    // Convert value if needed for integers
    char* filterValue = NULL;
    if (attrValue != NULL) {
        filterValue = new char[attrDesc.attrLen];
        if (filterValue == NULL) {
            delete scanner;
            return INSUFMEM;
        }

        switch(type) {
            case INTEGER: {
                int intVal = atoi(attrValue);
                memcpy(filterValue, &intVal, sizeof(int));
                break;
            }
            case FLOAT: {
                float floatVal = atof(attrValue);
                memcpy(filterValue, &floatVal, sizeof(float));
                break;
            }
            case STRING:
                strncpy(filterValue, attrValue, attrDesc.attrLen);
                break;
        }
    }

    // Start filtered scan
    status = scanner->startScan(attrDesc.attrOffset, 
                               attrDesc.attrLen,
                               (Datatype)attrDesc.attrType, 
                               filterValue, 
                               op);

    if (status != OK) {
        if (filterValue) delete[] filterValue;
        delete scanner;
        return status;
    }

    // Delete matching records
    RID rid;
    while (scanner->scanNext(rid) == OK) {
        status = scanner->deleteRecord();
        if (status != OK) break;
    }

    // Clean up
    scanner->endScan();
    if (filterValue) delete[] filterValue;
    delete scanner;

    return status;
}