#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

struct SortInfo {
OrderMaker *myOrder;
int runLength;
};
typedef enum {heap, sorted, tree} fType;
class GenericDBFile {

public:
	virtual int Create (char *fpath, fType file_type, void *startup)=0;
	virtual int Open (char *fpath)=0;
	virtual int Close ()=0;
	virtual void Load (Schema &myschema, char *loadpath)=0;
	virtual void MoveFirst ()=0;
	virtual void Add (Record &addme)=0;
	virtual int GetNext (Record &fetchme)=0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;	
	virtual int AddAtPage(Page *wrtpage)=0;
};
#endif
