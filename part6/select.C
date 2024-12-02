#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
                      const int projCnt, 
                      const attrInfo projNames[],
                      const attrInfo *attr, 
                      const Operator op, 
                      const char *attrValue)
{
    cout << "Doing QU_Select" << endl;
    Status status;
    
    // Get input relation name from first projection attribute
    string inputRel = projNames[0].relName;
    
    // Get source relation's attribute information from catalog
    int attrCnt;
    AttrDesc *attrs = NULL;
    status = attrCat->getRelInfo(inputRel, attrCnt, attrs);
    if (status != OK) return status;

    // Calculate total length needed for projected record
    int projRecLen = 0;
    for (int i = 0; i < projCnt; i++) {
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(projNames[i].attrName, attrs[j].attrName) == 0) {
                projRecLen += attrs[j].attrLen;
                break;
            }
        }
    }

    // Create scanner and inserter
    HeapFileScan* scanner = NULL;
    InsertFileScan* inserter = NULL;
    void* filterValue = NULL;

    // Set up the scan
    try {
        scanner = new HeapFileScan(inputRel, status);
        if (status != OK) throw status;

        inserter = new InsertFileScan(result, status);
        if (status != OK) throw status;

        if (attr != NULL) {
            // Find attribute offset for the filter
            int filterOffset = 0;
            int filterLen = 0;
            for (int i = 0; i < attrCnt; i++) {
                if (strcmp(attr->attrName, attrs[i].attrName) == 0) {
                    filterOffset = attrs[i].attrOffset;
                    filterLen = attrs[i].attrLen;
                    break;
                }
            }

            // Convert filter value to correct type
            filterValue = malloc(filterLen);
            if (!filterValue) throw INSUFMEM;

            switch(attr->attrType) {
                case INTEGER:
                    *(int*)filterValue = atoi(attrValue);
                    break;
                case FLOAT:
                    *(float*)filterValue = atof(attrValue);
                    break;
                case STRING:
                    strncpy((char*)filterValue, attrValue, filterLen);
                    break;
            }

            status = scanner->startScan(filterOffset, filterLen,
                                      (Datatype)attr->attrType, 
                                      (char*) filterValue, op);
        } else {
            status = scanner->startScan(0, 0, STRING, NULL, EQ);
        }
        if (status != OK) throw status;

        // Process matching records
        Record rec;
        RID rid;
        while (scanner->scanNext(rid) == OK) {
            status = scanner->getRecord(rec);
            if (status != OK) throw status;

            // Create projected record
            char* projData = new char[projRecLen];
            int projOffset = 0;

            // Copy only requested attributes
            for (int i = 0; i < projCnt; i++) {
                for (int j = 0; j < attrCnt; j++) {
                    if (strcmp(projNames[i].attrName, attrs[j].attrName) == 0) {
                        memcpy(projData + projOffset,
                            (char*)rec.data + attrs[j].attrOffset,  // Cast void* to char*
                            attrs[j].attrLen);
                        projOffset += attrs[j].attrLen;
                        break;
                    }
                }
            }

            // Insert projected record
            Record projRec;
            projRec.data = projData;
            projRec.length = projRecLen;
            RID outRid;
            status = inserter->insertRecord(projRec, outRid);
            delete[] projData;
            
            if (status != OK) throw status;
        }

    } catch (Status error_status) {
        status = error_status;
    }

    // Clean up
    if (scanner) {
        scanner->endScan();
        delete scanner;
    }
    if (inserter) delete inserter;
    if (filterValue) free(filterValue);
    if (attrs) delete[] attrs;

    return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}