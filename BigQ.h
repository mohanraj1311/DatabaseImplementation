#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <vector>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <time.h>
#include "DBFile.h"
#include <stdlib.h>
#include <queue>
#include <sys/time.h>
#include <sstream>



using namespace std;

class myTime
{
public:
static string getusec()
{
struct timeval tv;
gettimeofday(&tv, NULL);
stringstream ss;
ss << tv.tv_sec;
ss << ".";
ss << tv.tv_usec;
return ss.str();
}
};


class SortedRun
{
    private:
        int endpageno;
        int currentpageno;
    public:
    Page *pgptr;
    SortedRun(int cp,int ep)
    {
        endpageno =  ep;
        currentpageno =  cp;
        pgptr = new Page();
    }
    int getcurrentpageno()
    {
        return currentpageno;
    }
    int getendpageno()
    {
        return endpageno;
    }
    void incrcurrpageno()
    {
        currentpageno++;
    }
};

class RecordRun{
public:
int runno;
Record *r;
OrderMaker *sortingorder;
RecordRun(int run,Record *rec,OrderMaker *sortorder)
{
    runno = run;
    r=new Record();
    r->Copy(rec);
    //r=rec;
    sortingorder = sortorder;
}
Record* get_record()
{
    return r;
}
~RecordRun() {}
bool operator<(const RecordRun &right) const
{
    ComparisonEngine tempengine;
    RecordRun* right1 = const_cast<RecordRun*>(&right);
    RecordRun* left1 = const_cast<RecordRun*>(this);
    if (tempengine.Compare(left1->get_record(),right1->get_record(),sortingorder)>0)
        return true;
    else
        return false;
}
};

class BigQ {
    vector<SortedRun*> runs;
    int totalnopages;
    Pipe *in, *out;
    OrderMaker *sortorder;
    int runlen;

public:
    void QuickSort(Record recordbuffer[],int left,int right,OrderMaker &sortorder);
    void* start();
    static void* startHelper(void*);
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	void multi_way_merge(int no_runs,Pipe &out,char* outfile,OrderMaker &sortorder);
	bool getnextrecord(int run,Record* r,File *fileptr);
	~BigQ ();
};


#endif
