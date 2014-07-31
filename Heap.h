#ifndef HEAP_H
#define HEAP_H
#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


class Heap :public GenericDBFile{
private:
    File *fileptr;
    Page *readpageptr;
    Page *writepageptr;
    int currentpage;
    bool dirtybit;
    int firstpageflag;
public:
    Heap();
    ~Heap();
    int Create (char *fpath, fType file_type, void *startup);
    int Open(char *fpath);
    int Close();
    void Load (Schema &myschema, char *loadpath);
    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int WriteMode();
    int AddAtPage(Page *wrtpage);
};
#endif
