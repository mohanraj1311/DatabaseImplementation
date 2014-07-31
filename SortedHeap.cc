#include "SortedHeap.h"
#include <iostream>

SortedHeap::SortedHeap (int runln,OrderMaker &s,char *fpath) {
    fileptr=new File();
    f_path = fpath;
    readpageptr=NULL;
    CnfQuerySortOrder = NULL;
    writepageptr=new Page();
    in = new Pipe(1000);
    out = new Pipe(1000);
    sortorder = &s;
    runlen=runln;
 ///////************   bq = new BigQ(*in,*out,s,runlen);    removed this in project 3.0*///////////////////////
    currentpage=1;        
    writemode=false;    
    binSearchDone=false;
}

SortedHeap::~SortedHeap () {
    delete in;
    delete out;

}

void SortedHeap::Add (Record &rec) {
    writemode=true;    
    if(bq==NULL) bq = new BigQ(*in,*out,*sortorder,runlen);
    in->Insert(&rec);
}

void SortedHeap::merge()
{
    char tempfile[100];
    strcpy(tempfile,f_path);
    strcat(tempfile,".new");

    DBFile dbfile;
    dbfile.Create(tempfile,heap,NULL);
    
    priority_queue <RecordRun> pq;    

    int currentpageno=1;
    Page *pgptr = new Page();
    
    
    fileptr->GetPage(pgptr,currentpageno-1);
    int size = fileptr->GetLength();    
    Record *rec1 = new Record();
    Record *rec2 = new Record();
    int cnt=1;
    while(cnt<=2)
   {
        if(out->Remove(rec1))
        {
        RecordRun recordrun(0,rec1,sortorder);
        pq.push(recordrun);
        rec1= new Record();
        }
        if(pgptr->GetFirst(rec2))
        {
        RecordRun recordrun(0,rec2,sortorder);
        pq.push(recordrun);
        rec2= new Record();
        }
        cnt++;
    }
    RecordRun *rrptr;    
    int reccnt=0;
    while(!pq.empty())
    {
        rrptr = const_cast<RecordRun*>(&pq.top());
        Record outputrec;
        outputrec.Consume(rrptr->get_record());
        dbfile.Add(outputrec);
        pq.pop();
        Record recptr;
        if(getnextrecord(size,rrptr->runno,&recptr,fileptr,&currentpageno,pgptr))
        {
            RecordRun newrun(rrptr->runno,&recptr,sortorder);
            pq.push(newrun);
        }
        else if(getnextrecord(size,!rrptr->runno,&recptr,fileptr,&currentpageno,pgptr))
        {
            RecordRun newrun(!rrptr->runno,&recptr,sortorder);
            pq.push(newrun);
        }
        rrptr=NULL;
        reccnt++;
    }    

    fileptr->Close();
    dbfile.Close();

    char metfile[100];
    strcpy(metfile,tempfile);
    strcat(metfile,".meta");
    remove(metfile);

    remove(f_path);
    rename(tempfile,f_path);

    //create meta file
    char oldmetfile[100];
    strcpy(oldmetfile,f_path);
    strcat(oldmetfile,".meta");
    remove(oldmetfile);

    ofstream myfile (oldmetfile);
     if (myfile.is_open())
        {
            myfile << "sorted\n";
            myfile << runlen <<"\n";
            sortorder->PrintToFile(myfile); // serialize the OrderMaker Object
            myfile.close();
        }  
    fileptr->Open(1,f_path);
    delete bq;
}


bool SortedHeap::getnextrecord(int size,int run,Record* r,File *fileptr,int* currentpagenoptr,Page *pgptr)
{
    if (run==0)
    {
        if(out->Remove(r))
        {
            return true;
        }
        return false;        
    }
    else if (run==1)
    {
        if(pgptr->GetFirst(r))
        {
            return true;
        }
        (*currentpagenoptr)++;
        if(*currentpagenoptr>=size)
           {
             return false;
            }
            fileptr->GetPage(pgptr,*currentpagenoptr-1);
            pgptr->GetFirst(r);
           return true;
        }
}

int SortedHeap::Close ()
{
    delete readpageptr;
    readpageptr=NULL;
    if(writemode)
    {
        writemode=false;
        in->ShutDown();
        merge();        
    }
    fileptr->Close();
    delete fileptr;
    fileptr=NULL;
}
void SortedHeap::MoveFirst () {
    if(writemode)
    {
        writemode=false;
        in->ShutDown();
        merge();
    }
    binSearchDone =false;
    currentpage = 1;
    fileptr->GetPage(readpageptr,0);
}

int SortedHeap::Open (char *f_path) {  
    fileptr->Open(1,f_path);
    readpageptr=new Page();
    writepageptr=new Page();
    fileptr->GetPage(writepageptr,fileptr->GetLength()-2);
    return 1;
}

int SortedHeap::Create (char *fpath, fType file_type, void *startup){
    fileptr->Open(0,f_path);
    fileptr->AddPage(writepageptr,0);//Creating the first page
    return 1;
}

int SortedHeap::GetNext (Record &fetchme) {
     if(writemode){
        writemode = false;
        in->ShutDown();
        merge();
        MoveFirst();
    }
    int outofrecords = (readpageptr->GetFirst(&fetchme)==0); //checkin if current page is out of records
    while(outofrecords&& (currentpage<fileptr->GetLength()-1) ) //No need of while loop when you could do Page Compaction in future..
    {
        delete readpageptr;
        readpageptr=NULL;
        readpageptr=new Page();
        fileptr->GetPage(readpageptr,currentpage);
        currentpage = currentpage + 1;
        outofrecords = (readpageptr->GetFirst(&fetchme)==0);

       //debug
       /* if(fetchme.bits==NULL)
            cout<<"Record is NULL!!!";*/
    }
    return !outofrecords;
}


void SortedHeap::Load (Schema &f_schema, char *loadpath) {
    readpageptr=new Page();
    FILE *tableFile = fopen (loadpath, "r");
    fseek (tableFile,0,SEEK_SET);
    Record temp;
    while(temp.SuckNextRecord(&f_schema,tableFile)==1)
    {
        Add(temp);
    }
    in->ShutDown();
    merge();
    writemode=0;    
}

int SortedHeap::AddAtPage(Page *wrtpage)
{
    //This function is not used in Sorted Heap.
    return -1;
}
//GETNEXT CNF-


int SortedHeap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

    if(writemode){
        writemode = false;
        in->ShutDown();
        merge();
        MoveFirst();
    }

    int beginpagemarker = currentpage;
    ComparisonEngine CompEngine;

    // First check the current page linearly till end of page,The next page will be Beginning marker
    // for the binary Search.
    
    while (GetNext(fetchme) )
    {        
        if(CompEngine.Compare (&fetchme, &literal, &cnf))
        {
            return 1;
        }
        if(beginpagemarker!=currentpage)
        {
            break;

        }
    }
    //set the beginpage marker
    beginpagemarker = currentpage;
    
    if(!binSearchDone){
        CnfQuerySortOrder=cnf.PrepareCnfQueryOrderMaker(*sortorder);
         // Do Binary Search when there are Matching Attributes.
        if(CnfQuerySortOrder!=NULL)
        {
            int lastpageindex = fileptr->GetLength()-1;
            int retstatus;
            if(lastpageindex > 1){
                retstatus = BinarySearchOnPages(fetchme,literal,0, lastpageindex);
            }
            else{
                retstatus = GetNext(fetchme);
            }
            if(retstatus){
                binSearchDone = true;
                if(CompEngine.Compare(&fetchme, &literal, &cnf))
                    return 1;
            }
       }
        else
        {
            binSearchDone = true; // This is to prevent trying to get the Matching Attributes again.
        }
    
    //Go Back from the Page where u found a match.
    int gobackpage = currentpage-1;
    Page *GoBackPageptr = new Page();
    bool brkflag=false;
    while(gobackpage>=beginpagemarker && CnfQuerySortOrder!=NULL)
    {
         fileptr->GetPage(GoBackPageptr,gobackpage-1);
         GoBackPageptr->GetFirst(&fetchme);
         if(CompEngine.Compare (&fetchme, &literal, &cnf))
        {
            gobackpage--;
        }
         else
         {
             while(GoBackPageptr->GetFirst(&fetchme))
             {
                 if(CompEngine.Compare (&fetchme, &literal, &cnf))
                 {
                     delete readpageptr;
                     readpageptr = GoBackPageptr;
                     return 1;
                 }                 
             }
             brkflag=true;
         }
         if(brkflag)
         {
             break;
         }
       }
    //spcal case
    if(brkflag==true)
    {
     fileptr->GetPage(GoBackPageptr,gobackpage);
     GoBackPageptr->GetFirst(&fetchme);
     if(CompEngine.Compare (&fetchme, &literal, &cnf))
        {
         delete readpageptr;
         readpageptr = GoBackPageptr;
         return 1;
        }
    }

    }
    // Doing a linear Scan from the Record where Binary Search Stopped
    while ( CnfQuerySortOrder!=NULL && GetNext(fetchme))
    {        
        if(CompEngine.Compare(&literal, CnfQuerySortOrder,&fetchme, sortorder) < 0)
            return 0;
        //check if the literal matches fetcme on full cnf.
        if(CompEngine.Compare (&fetchme, &literal, &cnf))
            return 1;
    }
    //Case where binSearch was Attempted but no matching Attributes in sortOrder and CNF.
    // So doing linear Search
    while (CnfQuerySortOrder==NULL && GetNext(fetchme))
    {
        if(CompEngine.Compare (&fetchme, &literal, &cnf))
        {
            return 1;
        }
    }
    return 0;
}


int SortedHeap::BinarySearchOnPages(Record &fetchme, Record &literal,int low, int high){
    ComparisonEngine CompEng;
    int mid = low + (high - low)/2;
    while(low <= high){
           if(mid != 0)
                  fileptr->GetPage(readpageptr,mid-1);
           else
                  fileptr->GetPage(readpageptr,0);
           readpageptr->GetFirst(&fetchme);
           //Check if the literal matches the record
           if (CompEng.Compare(&literal, CnfQuerySortOrder, &fetchme, sortorder)==0){
               currentpage = mid-1;
               return 1;
           }
           //if literal is less than the first record in the page search for the Top half.
           else if (CompEng.Compare( &literal, CnfQuerySortOrder, &fetchme, sortorder)<0){
               high = mid - 1;
           }
           //if literal is greater than the first record in the page search for the bottom half.
           //scan the entire page for the possible match
           // -- if u reach the end of page.. search for the bottom half.
           //-- if in a page u reached a place where value of literal < the key(where u have started
           //with literal >key)then there is no record matching so return 0;
	   if (CompEng.Compare(&literal, CnfQuerySortOrder,&fetchme, sortorder)>0){
                    while(readpageptr->GetFirst(&fetchme)){

                        if(CompEng.Compare(&literal, CnfQuerySortOrder,&fetchme, sortorder)<0){
                        	return 0;
                        }
                        else if(CompEng.Compare(&literal, CnfQuerySortOrder, &fetchme, sortorder)==0){
                            currentpage = mid-1;
                            return 1;
                        }
                    }
                    low = mid+1;
           }
           mid = low+(high - low)/2;
       }
       return 0;
}


