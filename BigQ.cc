#include "BigQ.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include <time.h>




void BigQ::multi_way_merge(int no_runs,Pipe &out,char* outfile,OrderMaker &sortorder)
{
    //priority_queue<RecordRun,vector<RecordRun>,less<vector<RecordRun>::value_type>> pq;
    //priority_queue < RecordRun, vector <RecordRun>,less<vector<RecordRun>::value_type> > pq;
   priority_queue <RecordRun> pq;
   // priority_queue < RecordRun, vector <RecordRun>, less<vector<RecordRun>::value_type> > pq;
    File *fileptr;
    fileptr=new File();
    fileptr->Open(1,outfile);
    // Initialize the Pages
   //cout<<"\n outfile"<<outfile;

    for(int i=0;i<no_runs;i++)
    {
        //cout<<"\n Inside for:File length:"<<fileptr->GetLength()<<" CurrentRun:"<<i;
        //cout<<"\n StartPage:"<<runs[i]->getcurrentpageno()<<" end page:"<<runs[i]->getendpageno();
        fileptr->GetPage(runs[i]->pgptr,runs[i]->getcurrentpageno()-1);
        //runs[i]->pgptr
    }

   // Record *rec = new Record();
   // runs[1]->pgptr->GetFirst(rec);
   // RecordRun recordrun(0,rec,&sortorder);
   // pq.push(recordrun);
    for(int i=0;i<no_runs;i++)
    {
        Record *rec = new Record();
        if(runs[i]->pgptr->GetFirst(rec))
        {
          //cout<<"\n fetching record"<<"Current run:"<<i;
            RecordRun recordrun(i,rec,&sortorder);
           pq.push(recordrun);
          // cout<<"\n Aftet pushing record into PQ"<<i;
        }
        rec=NULL;
    }
     for(int i=0;i<no_runs;i++)
    {
        Record *rec = new Record();
        if(runs[i]->pgptr->GetFirst(rec))
        {
         // cout<<"\n fetching record"<<"Current run:"<<i;
            RecordRun recordrun(i,rec,&sortorder);
           pq.push(recordrun);
          // cout<<"\n Aftet pushing record into PQ"<<i;
        }
        rec=NULL;
    }
    //do merging
   // cout<<"Starting merge phase";
    RecordRun *rrptr;
    while(!pq.empty())
    {
        rrptr = const_cast<RecordRun*>(&pq.top());
        //cout<<"\n Got top record";
        Record outputrec;
        outputrec.Consume(rrptr->get_record());
        //cout<<"\n Inserting into outpipe";
        out.Insert(&outputrec);
        pq.pop();
        //get next input record from the current run.
        Record recptr;
        //cout<<"Record Run:"<<rrptr->runno;
        if(getnextrecord(rrptr->runno,&recptr,fileptr))
        {
            RecordRun newrun(rrptr->runno,&recptr,&sortorder);
            pq.push(newrun);
        }
        rrptr=NULL;
        //cout<<"\n Inside While pq size"<<pq.size();
    }
    fileptr->Close();
}


bool BigQ::getnextrecord(int run,Record* r,File *fileptr)
{    
    if(runs[run]->pgptr->GetFirst(r))
    {
        return true;
    }
    else
    {
        runs[run]->incrcurrpageno();
        //cout<<"\n currentpage:"<<
       // cout<<"\nCurrent PageNo:"<<runs[run]->getcurrentpageno()<<" EndPageNumber:"<<runs[run]->getendpageno()<<" run:"<<run;
        if(runs[run]->getcurrentpageno()>runs[run]->getendpageno())
            {
                return false;
            }
            else
            {               
                fileptr->GetPage(runs[run]->pgptr,runs[run]->getcurrentpageno()-1);
                runs[run]->pgptr->GetFirst(r);
                return true;
            }
    }
}

void BigQ::QuickSort(Record recordbuffer[],int left,int right,OrderMaker &sortorder)
{
    int i, last;
    if (left >= right ) /* do nothing if array contains */
       return; /* fewer than two elements */
    last = left; /* to v[0] */
    ComparisonEngine tempengine;
    Record temp;
    for (i = left + 1; i <= right; i++) /* partition */
    {
       // cout<<"\n printing sortorder";
        //sortorder.Print();
        if (tempengine.Compare(&recordbuffer[left],&recordbuffer[i],&sortorder)>0)
        {
           last++;
            if(last!=i)
            {
                temp.Consume(&recordbuffer[last]);
                recordbuffer[last].Consume(&recordbuffer[i]);
                recordbuffer[i].Consume(&temp);
            }
        }
    }
    if(left!=last)
    {
    temp.Consume(&recordbuffer[left]);
    recordbuffer[left].Consume(&recordbuffer[last]);
    recordbuffer[last].Consume(&temp);
    }

    QuickSort(recordbuffer, left, last-1,sortorder);
    QuickSort(recordbuffer, last+1, right,sortorder);
}

BigQ :: BigQ (Pipe &in1, Pipe &out1, OrderMaker &sortorder1, int runlen1) : runlen(runlen1)
{
    //member variables
    in = &in1;
    out = &out1;
    sortorder = &sortorder1;    
    totalnopages=1;
    pthread_t sortThread;
    pthread_create(&sortThread, NULL, &startHelper, (void*)this);
//  start(in1,out1,sortorder1,runlen1);
}

void* BigQ::startHelper(void* context)
{
    ((BigQ *)context)->start();
}

void* BigQ::start()
{
    //Creating Temp File with Timestamp, to hold Sorted Runs.
    //time_t rawtime;
    //rawtime = time(NULL);
    string rawtime = myTime::getusec();
    char outfile[100];
    sprintf (outfile, "%s.bin", rawtime.c_str());
    DBFile tempdbfile;
    tempdbfile.Create (outfile, heap, NULL);
    //////////////// Phase I - Generating Sorted Runs //////////////////////
    Record rec;
    Page pageBuffer[runlen];
    int pgcnt=0;
    int recordcnt=0,j=0;
    Record *pageRecord=new Record();
    Page *wrtpage = new Page();
    SortedRun *A;
    while(in->Remove(&rec))
    {
        //cout<<"\n consuming record";
        recordcnt++; ////Keep Track of Total Record count of a Run, to get array size
        pageRecord->Consume(&rec);

        if(!pageBuffer[pgcnt].Append(pageRecord)) // if you are out of the page try to get new page
        {
            pgcnt++;
            if(pgcnt>=runlen) // if you are done taking in runlen no of pages
            {
                Record recordbuffer[recordcnt-1];
                j=0;
                for(int x=0;x<runlen;x++)
                {
                    Record temprecord;
                    while(pageBuffer[x].GetFirst(&temprecord))
                    {
                        recordbuffer[j].Consume(&temprecord);
                        j++;
                    }
                }
                //Sorting the run
                QuickSort(recordbuffer,0,recordcnt-2,*sortorder);
                //Dumping the Sorted Run into File
                int start=totalnopages;//
                //cout<<"\n RecordCount:"<<recordcnt-1;
                for(int x=0;x<recordcnt-1;x++)
                {

                   if(!wrtpage->Append(&recordbuffer[x]))
                   {
                       totalnopages++;
                       tempdbfile.AddAtPage(wrtpage);
                       //wrtpage->EmptyItOut();
                       delete wrtpage;
                       wrtpage= new Page();
                       wrtpage->Append(&recordbuffer[x]);
                   }
                }
                ////Storing information about run start and run end in a vector
                //cout<<"Storing information about run start and run end in a vector";
                //cout<<"\nUsed during Phase 2";
                tempdbfile.AddAtPage(wrtpage);
                A= new SortedRun(start,totalnopages);
                delete wrtpage;
                wrtpage=new Page();
                totalnopages++;
                runs.push_back(A);
                pgcnt=0;
                for(int x=0;x<runlen;x++)
                {
                    pageBuffer[x].EmptyItOut();
                }
                pageBuffer[pgcnt].Append(pageRecord);
                recordcnt=1;
            }
            else //just add to the page
            {
                pageBuffer[pgcnt].Append(pageRecord);
            }
        }
    }
   // cout<<"\nSorting the Last Run!";
    Record recordbuffer[recordcnt];
    j=0;
    for(int x=0;x<=pgcnt;x++)
    {
        Record temprecord;
        while(pageBuffer[x].GetFirst(&temprecord))
        {
            recordbuffer[j].Consume(&temprecord);
            j++;
        }
    }
    //Dumping the Last Sorted Run into Temp File
   // cout<<"Dumping the Last Sorted Run into Temp File";
    int start=totalnopages; // start page of the Run
    QuickSort(recordbuffer,0,recordcnt-1,*sortorder);
    //cout<<"\n Record count:"<<recordcnt;
    for(int x=0;x<recordcnt;x++)
    {
        if(!wrtpage->Append(&recordbuffer[x]))
       {
           totalnopages++;
           tempdbfile.AddAtPage(wrtpage);
           //wrtpage->EmptyItOut();
           delete wrtpage;
           wrtpage= new Page();
           wrtpage->Append(&recordbuffer[x]);
       }
    }
    //cout<<"\nDumping the Last Page";
    tempdbfile.AddAtPage(wrtpage);
    A= new SortedRun(start,totalnopages);
    // totalnopages++;
    //cout<<"\n//Inserting information about the Last Run in the vector";
    runs.push_back(A);
    tempdbfile.Close();
   // cout<<"\n done with phase one outfile:"<<outfile;
    ///////////////// Phase 2 - MultiWay Merge of Sorted Runs////////////////
    multi_way_merge(runs.size(),*out,outfile,*sortorder);

    /////////////Done dethe line item that i test on waslete the Temp File and Shutdown the Pipe////////////
    remove(outfile);
    char name[100];
    strcpy(name,outfile);
    strcat(name,".meta");  
    remove(name);
    out->ShutDown();    
}

BigQ::~BigQ () {
}
