#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Heap.h"
#include "Defs.h"
#include<iostream>

Heap::Heap () {
    fileptr=new File();
    readpageptr=NULL;
    writepageptr=new Page();
    currentpage=1;
    dirtybit=0;
    firstpageflag=0;
}

int Heap::WriteMode()
{
    if( dirtybit == 1)
    {
       if(firstpageflag==0)
        {
                fileptr->AddPage(writepageptr,fileptr->GetLength()-2);
        }
        else
            fileptr->AddPage(writepageptr,fileptr->GetLength()-1);
     dirtybit = 0 ;
    }
}

int Heap::Create (char *f_path, fType file_type, void *startup){
    /* Create a normal File*/
    fileptr->Open(0,f_path);
    fileptr->AddPage(writepageptr,0);//Creating the first page
    return 1;
    //doubt-Check how to implement Return codes
}

void Heap::Load (Schema &f_schema, char *loadpath) {
    readpageptr=new Page();
    cout<<"\n LoadPath:"<<loadpath;
	FILE *tableFile = fopen (loadpath, "r");
	fseek (tableFile,0,SEEK_SET);
    Record temp;
    while(temp.SuckNextRecord(&f_schema,tableFile)==1)
    {
        Add(temp);
    }
    if(firstpageflag==0)
        fileptr->AddPage(writepageptr,fileptr->GetLength()-2);//getlenth-2 as we r only modifying the last page of the file
    else
        fileptr->AddPage(writepageptr,fileptr->GetLength()-1);

    dirtybit=0;
    firstpageflag=0;
}

int Heap::Open (char *f_path) {
    fileptr->Open(1,f_path);
    readpageptr=new Page();
    writepageptr=new Page();
    fileptr->GetPage(writepageptr,fileptr->GetLength()-2);
    return 1;
}

void Heap::MoveFirst () {
    WriteMode();
    currentpage=1; /// Changed in pro 2.2
    fileptr->GetPage(readpageptr,0);
}

int Heap::Close ()
{
    delete readpageptr;
    readpageptr=NULL;
    if(dirtybit == 1)
    {
       if(firstpageflag==0)
        {
                fileptr->AddPage(writepageptr,fileptr->GetLength()-2);
        }
        else
            fileptr->AddPage(writepageptr,fileptr->GetLength()-1);
        delete writepageptr;
        writepageptr=NULL;
        dirtybit=0;
    }
    fileptr->Close();
    delete fileptr;
    fileptr=NULL;
}

void Heap::Add (Record &rec) {
    if(!dirtybit)
            fileptr->GetPage(writepageptr,fileptr->GetLength()-2);

    int writesuccess=writepageptr->Append(&rec);
    if(!writesuccess)
    {
        if(firstpageflag==0) /// it was firstpageflag==0 changing....
        {
                fileptr->AddPage(writepageptr,fileptr->GetLength()-2);
                firstpageflag++;
        }
        else
            fileptr->AddPage(writepageptr,fileptr->GetLength()-1);//GetLength because we are adding a new page at the end of the file
        delete writepageptr;
        writepageptr=NULL;
        writepageptr=new Page();
        writepageptr->Append(&rec);
    }
    dirtybit=1;
}

int Heap::AddAtPage(Page *wrtpage)
{
    WriteMode();
    if(firstpageflag==0)
    {
        fileptr->AddPage(wrtpage,fileptr->GetLength()-2); //removed writepageptr
        firstpageflag++;
    }
    else
        fileptr->AddPage(wrtpage,fileptr->GetLength()-1);
}
int Heap::GetNext (Record &fetchme) {
    int outofrecords = (readpageptr->GetFirst(&fetchme)==0); //checkin if current page is out of records
    while(outofrecords&& (currentpage<fileptr->GetLength()-1) ) //No need of while loop when you could do Page Compaction in future..
    {
        delete readpageptr;
        readpageptr=NULL;
        readpageptr=new Page();
        fileptr->GetPage(readpageptr,currentpage);
        currentpage = currentpage + 1;
        outofrecords = (readpageptr->GetFirst(&fetchme)==0);
    }
    return !outofrecords;
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine comp;
    while (GetNext(fetchme))
    {
        if(comp.Compare (&fetchme, &literal, &cnf))
        {
            return 1;
        }
    }
    return 0;
}
