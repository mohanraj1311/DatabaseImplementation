#ifndef SORTEDHEAP_H
#define SORTEDHEAP_H
#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"

class BigQ;
class SortedHeap :public GenericDBFile{
    private:
        File *fileptr;
        Page *readpageptr;
        Page *writepageptr;
        int currentpage;
        bool writemode;
        Pipe *in;
        Pipe *out;
        BigQ *bq;
        char *f_path;
        int runlen;
        OrderMaker *sortorder;        
        OrderMaker * CnfQuerySortOrder;
        bool binSearchDone;

    public:
        SortedHeap(int,OrderMaker &,char *fpath);
        ~SortedHeap();
        int Create (char *fpath, fType file_type, void *startup);
        int Open (char *fpath);
        int Close ();
        void Load (Schema &myschema, char *loadpath);
        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        bool getnextrecord(int size,int run,Record* r,File *fileptr,int* currentpagenoptr,Page *pgptr);        int WriteMode();
        int AddAtPage(Page *wrtpage);
        void merge();
        void PrepareOrderMakerQuery(CNF &cnf);
        int BinarySearchOnPages(Record &fetchme, Record &literal,int start, int end);
};
#endif

