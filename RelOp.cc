#include "RelOp.h"

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inPipe = &inPipe;
    this->outPipe=&outPipe;
    this->selOp=&selOp;
    this->literal=&literal;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);

}

void* SelectPipe::doOpHelper(void* context)
{
    ((SelectPipe *)context)->doOperation();
}

void* SelectPipe::doOperation()
{
    ComparisonEngine cmpEngine;
    Record temp;
    while (inPipe->Remove(&temp))
    {
        if(selOp!=NULL)
        {
            if(cmpEngine.Compare(&temp,literal,selOp))
            {
                outPipe->Insert(&temp);
            }
        }
        else
        {
          outPipe->Insert(&temp);
        }
    }
    outPipe->ShutDown();
}
void SelectPipe::WaitUntilDone () {
	 pthread_join (op_thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
    return;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {    
    this->inFile=&inFile;
    this->outPipe=&outPipe;
    this->selOp=&selOp;
    this->literal=&literal;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
}

void* SelectFile::doOpHelper(void* context)
{
    ((SelectFile *)context)->doOperation();
}

void* SelectFile::doOperation()
{
    Record temp;
    inFile->MoveFirst();
    ComparisonEngine CE;
        
    while(inFile->GetNext(temp))
    {
        if(selOp!=NULL)
        {
        if(CE.Compare(&temp,literal,selOp))
            {
            outPipe->Insert(&temp);            
            }
        }
        else
        {
               outPipe->Insert(&temp);                 
        }
      
    }

    /*
    if(selOp!=NULL)
    {
    while(inFile->GetNext(temp,*selOp,*literal))
    {
        outPipe->Insert(&temp);
    }
    }
    else
    {
        while(inFile->GetNext(temp))
        {
        outPipe->Insert(&temp);
        }
    }*/
    outPipe->ShutDown();

}

void SelectFile::WaitUntilDone () {
	 pthread_join (op_thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
    return;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    this->inPipe=&inPipe;
    this->outFile=outFile;
    this->mySchema=&mySchema;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
}

void* WriteOut::doOpHelper(void* context)
{
    ((WriteOut *)context)->doOperation();
}

void* WriteOut::doOperation()
{
    
    Record temp;
    long reccnt=0;
	// loop through all of the attributes
        int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();
        while (inPipe->Remove(&temp) )
        {
            reccnt++;
	// loop through all of the attributes
	for (int i = 0; i < n; i++) {
            // print the attribute name
                fprintf(outFile,"%s: ",atts[i].name);
		// use the i^th slot at the head of the record to get the
		// offset to the correct attribute in the record
		int pointer = ((int *) temp.bits)[i + 1];

		// here we determine the type, which given in the schema;
		// depending on the type we then print out the contents
                fprintf(outFile,"%c",'[');
		// first is integer
		if (atts[i].myType == Int) {
			int *myInt = (int *) &(temp.bits[pointer]);
                        fprintf(outFile,"%d",*myInt);

		// then is a double
		} else if (atts[i].myType == Double) {
			double *myDouble = (double *) &(temp.bits[pointer]);
                        fprintf(outFile,"%f",*myDouble);
		// then is a character string
		} else if (atts[i].myType == String) {
			char *myString = (char *) &(temp.bits[pointer]);
                        fprintf(outFile,"%s",myString);
		}

		fprintf(outFile,"%c",']');

		// print out a comma as needed to make things pretty
		if (i != n - 1) {
			fprintf(outFile,"%c",',');
		}
	}
        fprintf(outFile,"%c",'\n');
        }
        fclose(outFile);
        cout<<"\nRecord count written:"<<reccnt<<"\n";
}

void WriteOut::WaitUntilDone () {
	 pthread_join (op_thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
    return;
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
    this->inPipe=&inPipe;
    this->outPipe=&outPipe;
    this->computeMe=&computeMe;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
}

void* Sum::doOpHelper(void* context)
{
    ((Sum *)context)->doOperation();
}


void* Sum::doOperation()
{
    int intRes;
    double dblRes;
    Record curr;
    Type retype;
    int sumint=0;
    double sumdbl = 0;
    while(inPipe->Remove(&curr))
    {
        retype = computeMe->Apply(curr,intRes,dblRes);
        if(retype==Int)
        {
            sumint = sumint + intRes;
        }
        //Can each iteration can lead to Double / Int or Only Dbl or Only Int?
        else if(retype==Double)
        {
            sumdbl = sumdbl + dblRes;
        }
    }
    // can the return type be String?
    // create temperory schema, with one attribute - sum
    Record *resultRec = new Record();
    if(retype==Int)
    {
    Attribute att = {(char*)"sum", Int};
    Schema sum_schema((char*)"sumschema_file_unused",1,&att);
    char sumstring[30];
    sprintf(sumstring, "%f|", sumint);
    resultRec->ComposeRecord(&sum_schema,sumstring);
    }
    else if (retype ==Double)
    {
     Attribute att = {(char*)"sum", Double};
     Schema sum_schema((char*)"sumscheme_file_unused",1,&att);
     char sumstring[30];
     sprintf(sumstring, "%f|", sumdbl);
     resultRec->ComposeRecord(&sum_schema,sumstring);
     cout<<"ResultRecord SUM:";
     resultRec->Print(&sum_schema);
    }        
    outPipe->Insert(resultRec);
    outPipe->ShutDown();
}

void Sum::WaitUntilDone ()
{
    pthread_join (op_thread, NULL);
}

void Sum::Use_n_Pages (int n)
{
 return;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
    this->inPipe=&inPipe;
    this->outPipe=&outPipe;
    this->keepMe=keepMe;
    this->numAttsInput=numAttsInput;
    this->numAttsOutput=numAttsOutput;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
}

void* Project::doOpHelper(void* context)
{
    ((Project *)context)->doOperation();
}


void* Project::doOperation()
{
    Record rec;
        // While records are coming from inPipe,
        // modify records and keep only desired attributes
        // and push the modified records into outPipe
        while(inPipe->Remove(&rec))
        {
                rec.Project(keepMe, numAttsOutput, numAttsInput);
                // Push this modified rec in outPipe
                outPipe->Insert(&rec);
        }

        //Shut down the outpipe
        outPipe->ShutDown();
}

void Project::WaitUntilDone ()
{
    pthread_join (op_thread, NULL);
}

void Project::Use_n_Pages (int n)
{
 return;
}


void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal,bool leftflag,bool rightflag)
{
    runlen=MIN_RUNLEN; // just in case if the test case forgets to set runlen;
    this->inPipeL=&inPipeL;
    this->inPipeR=&inPipeR;
    this->outPipe=&outPipe;
    this->selOp = &selOp;
    this->literal=&literal;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
    BypassSrtLeft=leftflag;
    BypassSrtright=rightflag;
}

void* Join::doOpHelper(void* context)
{
    ((Join *)context)->doOperation();
}

void* Join::LeftBypasserhelper(void *context)
{
    ((Join *)context)->LeftBypasser();
}
void* Join::LeftBypasser()
{
    Record temp;
    while(inPipeL->Remove(&temp))
    {
        lsrtdoutpipe->Insert(&temp);
    }
    lsrtdoutpipe->ShutDown();
}
void* Join::RightBypasserhelper(void *context)
{
    ((Join *)context)->RightBypasser();
}
void* Join::RightBypasser()
{
    Record temp;
    while(inPipeR->Remove(&temp))
    {

        rsrtdoutpipe->Insert(&temp);
    }
    rsrtdoutpipe->ShutDown();
}
void* Join::doOperation()
{

    #ifdef _Join_test
        int cnt =0,lpipcnt=0,rpipcnt=0,reszero=0,respos=0,resneg=0;
    #endif
    OrderMaker oleft,oright;     
    selOp->GetCNFSortOrders(oleft,oright);
    lsrtdoutpipe=new Pipe(1000);
    rsrtdoutpipe=new Pipe(1000);
    if(oleft.numAtts==oright.numAtts && oleft.numAtts>0)
    {
        int lpipnotempty,rpipnotempty;
        //Pipe lsrtdoutpipe(1000);
        //Pipe rsrtdoutpipe(1000);
        int cnt=0;
        bool quitflag=false;
        BigQ *ltable;
       if(!BypassSrtLeft)
       {
            ltable=new BigQ(*inPipeL,*lsrtdoutpipe,oleft,runlen);
       }
       else
       {
        pthread_create(&lbp_thread, NULL, LeftBypasserhelper,(void*)this);
       }

                
        vector<Record*> lvec;
        vector<Record*> rvec;

        Record lprev,rprev,lcurr,rcurr;
        Record *templrecord,*temprrecord;

        bool lvecfilled,rvecfilled;
        ComparisonEngine compengine;
        
        lpipnotempty=lsrtdoutpipe->Remove(&lprev);
        
        templrecord = new Record();
        templrecord->Copy(&lprev);
        lvec.push_back(templrecord);
        BigQ *rtable;
       if(!BypassSrtright)
        {
        rtable=new BigQ(*inPipeR,*rsrtdoutpipe,oright,runlen);
        }
        else
        {
        pthread_create(&rbp_thread, NULL, RightBypasserhelper,(void*)this);
        }
        rpipnotempty=rsrtdoutpipe->Remove(&rprev);
        temprrecord = new Record();
        temprrecord->Copy(&rprev);        
        rvec.push_back(temprrecord);

        lpipnotempty=lsrtdoutpipe->Remove(&lcurr);
        rpipnotempty=rsrtdoutpipe->Remove(&rcurr);
        lvecfilled=rvecfilled=false;

        // for merging 2 records, necessary stuff
        int nolatts=((int *)lprev.bits)[1]/sizeof(int)-1;
        int noratts=((int *)rprev.bits)[1]/sizeof(int)-1;
        int noofatts=nolatts+noratts;
        int *attstokeep=new int[noofatts];
        int j=0;
        for(int i=0;i<nolatts;i++)
            attstokeep[j++]=i;
        for(int k=0;k<noratts;k++)
            attstokeep[j++]=k;
        //////merge stuff initialized////////
        // start of the main loop...////
        while(((rpipnotempty==1) || (lpipnotempty==1))&&!quitflag)
        {        
            while((lpipnotempty==1)&&(compengine.Compare(&lprev,&lcurr,&oleft)==0) && !lvecfilled)
            {
                templrecord= new Record();
                templrecord->Copy(&lcurr);
                lvec.push_back(templrecord);
                lpipnotempty=lsrtdoutpipe->Remove(&lcurr);
            }           
            lvecfilled=true;
            while((rpipnotempty==1)&&(compengine.Compare(&rcurr,&rprev,&oright)==0) && !rvecfilled)
            {
                temprrecord = new Record();
                temprrecord->Copy(&rcurr);
                rvec.push_back(temprrecord);               
                rpipnotempty=rsrtdoutpipe->Remove(&rcurr);
            }
            rvecfilled=true;        
            Record joinrecord;
            ComparisonEngine ce;         
            int result=compengine.Compare(&lprev,&oleft,&rprev,&oright);

            if(result==0)
            {                
                for(int i=0;i<lvec.size();i++)
                {
                    for(int j=0;j<rvec.size();j++)
                    {
                        if(ce.Compare(lvec.at(i),rvec.at(j),literal, selOp) == 1)
                        {
                          Record copyRec;
                          copyRec.Copy(rvec.at(j));                          
                          joinrecord.MergeRecords(lvec.at(i),&copyRec,nolatts,noratts,attstokeep,noofatts,nolatts);
                          outPipe->Insert(&joinrecord);
                        }
                          
                    }
                }
                deleteandclear(lvec);
                deleteandclear(rvec);
                lvecfilled=false;
                rvecfilled=false;

                if((rpipnotempty==1) && (lpipnotempty==1))
                {
                lprev.Consume(&lcurr);
     
                lpipnotempty=lsrtdoutpipe->Remove(&lcurr);
                
                rprev.Consume(&rcurr);
                rpipnotempty=rsrtdoutpipe->Remove(&rcurr);

                templrecord=new Record();
                temprrecord=new Record();

                templrecord->Copy(&lprev);
                temprrecord->Copy(&rprev);

                lvec.push_back(templrecord);
                rvec.push_back(temprrecord);

                }
                else
                {
                    quitflag = true;
                }
            }
            else if(result<0)
            {               
                lvecfilled=false;
                deleteandclear(lvec);
                
                if(lpipnotempty==1)
                {
                lprev.Consume(&lcurr);
                lpipnotempty=lsrtdoutpipe->Remove(&lcurr);
                templrecord=new Record();
                templrecord->Copy(&lprev);                
                lvec.push_back(templrecord);
                }
                else
                {
                    quitflag=true;
                }
            }
            else
            {                
                rvecfilled=false;
                deleteandclear(rvec);
                
                if(rpipnotempty==1)
                {
                rprev.Consume(&rcurr);
                rpipnotempty=rsrtdoutpipe->Remove(&rcurr);
                temprrecord=new Record();
                temprrecord->Copy(&rprev);
                rvec.push_back(temprrecord);
                }
                else
                {
                    quitflag=true;
                }
            }
        }
        while(lpipnotempty==1)
        {
            Record rec;
            lpipnotempty=lsrtdoutpipe->Remove(&rec);
        }
        while(rpipnotempty==1)
        {
            Record rec;
            rpipnotempty=rsrtdoutpipe->Remove(&rec);
        }

    }

    /*
     Block Nested Join - (8 min on Q6)
     1.Store the entire right part of the relation on to the disk in a heap file.
     2.Get runlen no of records = approximating to 600*runlen (600 pages/rec)  [BLOCK]
       of the Left Relation from the Pipe.
     3.Get pages from disk one by one of right part and join with the records
       in Left Relation.Put the results in Outputpipe.
     4.Get the next 600*runlen records from the Pipe and repeat step 3 , until there
       are no records left.
     5.Delete the Temp Heap File of Right Relation.
     */

    else
    {

        string rawtime = myTime::getusec();
        char outfile[100];
        sprintf (outfile, "%s.bin", rawtime.c_str());

        DBFile crtempdB;
        crtempdB.Create (outfile, heap, NULL);
        crtempdB.Close();

        DBFile *tempdB;
        tempdB = new DBFile();
        tempdB->Open(outfile);
        Record recR,recL;
        long int totalreccnt=0;
        if(inPipeL->Remove(&recL) && inPipeR->Remove(&recR))
        {
        // for merging 2 records, necessary stuff
        int nolatts=((int *)recL.bits)[1]/sizeof(int)-1;
        int noratts=((int *)recR.bits)[1]/sizeof(int)-1;
        int noofatts=nolatts+noratts;
        int *attstokeep=new int[noofatts];
        int j=0;
        for(int i=0;i<nolatts;i++)
            attstokeep[j++]=i;
        for(int k=0;k<noratts;k++)
            attstokeep[j++]=k;
        
        Record joinrecord;
        ComparisonEngine ce;
        //////merge stuff initialized////////

        tempdB->Add(recL);
        while(inPipeL->Remove(&recL))
        {
            tempdB->Add(recL);
        }
        

        int blocksize =runlen<MIN_RUNLEN? 600*MIN_RUNLEN: 600*runlen;
        Record *recBuff[blocksize];

        Record *recptr;
        //initialize the recBuffers
        for (int i=0;i<blocksize;i++)
        {
            recptr= new Record();
            recBuff[i] = recptr;
        }

        Record* lrecbuffer[blocksize];
        for (int i=0;i<blocksize;i++)
        {
                recptr= new Record();
                lrecbuffer[i] = recptr;
        }

        bool pipeempty = false;
        bool firstflag = true;
        int reccnt;
        
        tempdB->MoveFirst();

        while(!pipeempty)
        {
            reccnt=0;
            if(firstflag) {  recBuff[reccnt]->Copy(&recR);reccnt++;firstflag=false;}
            while(!pipeempty && reccnt<blocksize)
            {
                int val = inPipeR->Remove(recBuff[reccnt]);
                if(val==1)
                {
                    reccnt++;                    
                }
                else if(val==0)
                    pipeempty = true;
            }
                                  
            //nested -loop join
            Record lrec;
                      
            
            int lreccnt=0;
            

            bool fileempty=false;
            while(!fileempty)
            {
                lreccnt=0;

                while(!fileempty && lreccnt<blocksize)
                {
                    Record rec;
                    if(tempdB->GetNext(rec))
                    {
                        lrecbuffer[lreccnt]->Copy(&rec);
                        lreccnt++;
                    }
                    else
                        fileempty=true;
                }

                for(int i=0;i<reccnt;i++)
                {
                   for(int j=0;j<lreccnt;j++)
                   {
                    totalreccnt++;
                    if(ce.Compare(lrecbuffer[j],recBuff[i],literal, selOp) == 1)
                    {
                          joinrecord.MergeRecords(lrecbuffer[j],recBuff[i],nolatts,noratts,attstokeep,noofatts,nolatts);
                          outPipe->Insert(&joinrecord);
                    }
                   // cout<<"TotalReccnt"<<totalreccnt;
                   }
                }
            }
                tempdB->Close();
                delete tempdB;
                tempdB = new DBFile();
                tempdB->Open(outfile);
                tempdB->MoveFirst();
            
             }
               
            }                                                                  
        tempdB->Close();
        remove(outfile);
        cout<<"Total Rec Count:"<<totalreccnt;
    }

    outPipe->ShutDown();
    


#ifdef _Join_test
    cout<<"\nRec count merged : "<<cnt;
    cout<<"\nRes 0 : "<<reszero;
    cout<<"\nRes + : "<<respos;
    cout<<"\nRes - : "<<resneg;
    cout<<"\nRPIPE CNT:"<<rpipcnt;
    cout<<"\nLPIPE CNT:"<<lpipcnt;
    cout<<"\nleft pipe not empty:"<<lpipnotempty;
    cout<<"\n right pipe not empty:"<<rpipnotempty;
#endif
}
void Join::deleteandclear(vector<Record*> &vec)
{
    for(int i=0;i<vec.size();i++)
    {
        delete vec.at(i);
    }
    vec.clear();
}
void Join::WaitUntilDone ()
{
    pthread_join (op_thread, NULL);
}

void Join::Use_n_Pages (int n)
{
    runlen=n;
 return;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
    runlen=MIN_RUNLEN; // just in case if the test case forgets to set runlen;
    this->inPipe=&inPipe;
    this->outPipe=&outPipe;
    this->groupAtts = &groupAtts;
    this->computeMe = &computeMe;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
}

void* GroupBy::doOpHelper(void* context)
{
    ((GroupBy *)context)->doOperation();
}


void* GroupBy::doOperation()
{
    /*Logic::
    1.Sort the records in the input pipe based on the grouping attributes
    2.Perform Sum of function for the records in the same group.
    3.Build Schema for the Sum Record,create record,insert record into output pipe.
    4.Repeat step 2 & 3 for the next group.
     */
    Pipe sortedOutPipe(1000);
    BigQ recSortQ(*inPipe,sortedOutPipe,*groupAtts,runlen);
    
    Record *grpAttRec = new Record();

    int intRes;
    double dblRes;
    Record *curr=NULL,*prev=NULL;
    Type retype;
    int sumint;
    double sumdbl;

    ComparisonEngine cmpEngine;
    bool groupchanged=false;
    bool pipeempty = false;
    

    prev = new Record();
    if(!sortedOutPipe.Remove(prev))
    {
        outPipe->ShutDown();
        return 0;
    }


    while(!pipeempty)
    {    
        sumint =0;sumdbl=0;intRes=0;dblRes=0;
        curr = new Record();
        groupchanged = false;
    
        while(!pipeempty && !groupchanged)
        {
            sortedOutPipe.Remove(curr);
            if(curr->bits!=NULL)
            {
                if(cmpEngine.Compare(curr,prev,groupAtts)!=0)
                {
                    grpAttRec->Copy(prev);
                    groupchanged=true;
                }
            }
            else
                pipeempty = true;

            retype = computeMe->Apply(*prev,intRes,dblRes);
            if(retype==Int)
                {
                    sumint = sumint + intRes;
                }
            else if(retype==Double)
            {
                sumdbl = sumdbl + dblRes;
            }
            prev->Consume(curr);
        }
    
        Record *sumRec = new Record();
        if(retype==Int)
        {
            Attribute att = {(char*)"sum", Int};
            Schema sum_schema((char*)"sumschema_file_unused",1,&att);
            char sumstring[30];
            sprintf(sumstring, "%d|", sumint);
            sumRec->ComposeRecord(&sum_schema,sumstring);
        }
        else if (retype ==Double)
        {
            Attribute att = {(char*)"sum", Double};
            Schema sum_schema((char*)"sumscheme_file_unused",1,&att);
            char sumstring[30];
            sprintf(sumstring, "%f|", sumdbl);
            sumRec->ComposeRecord(&sum_schema,sumstring);
        }
        Record resultRec;
        int numsumAtts = groupAtts->numAtts+1;
        int sumAtts[numsumAtts];
        sumAtts[0]=0;
        for(int i=1;i<numsumAtts;i++)
        {
            sumAtts[i]=groupAtts->whichAtts[i-1];
        }
        resultRec.MergeRecords(sumRec,grpAttRec,1,numsumAtts-1,sumAtts,numsumAtts,1);
        outPipe->Insert(&resultRec);
    }

    outPipe->ShutDown();
}

void GroupBy::WaitUntilDone ()
{
    pthread_join (op_thread, NULL);
}

void GroupBy::Use_n_Pages (int n)
{
 runlen = n;
 return;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
    runlen=MIN_RUNLEN; // just in case if the test case forgets to set runlen;
    this->inPipe=&inPipe;
    this->outPipe=&outPipe;
    this->mySchema=&mySchema;
    pthread_create(&op_thread, NULL, doOpHelper,(void*)this);
}

void* DuplicateRemoval::doOpHelper(void* context)
{
    ((DuplicateRemoval *)context)->doOperation();
}


void* DuplicateRemoval::doOperation()
{        
    Pipe *sortedOutPipe= new Pipe(1000);
    OrderMaker sortorder(mySchema);
    ComparisonEngine cmpEngine;
    BigQ sortingQ(*inPipe,*sortedOutPipe,sortorder,runlen);
    Record prev;
    Record curr;
    sortedOutPipe->Remove(&prev);
   // cout<<"\n printing Prev";
    //prev.Print(mySchema);
    while(sortedOutPipe->Remove(&curr))
    {
       // cout<<"\n printing curr";
            //curr.Print(mySchema);
        if(cmpEngine.Compare(&prev,&curr,&sortorder)!=0)
        {
            outPipe->Insert(&prev);
            prev.Consume(&curr);
        }
    }
    outPipe->Insert(&prev);
    outPipe->ShutDown();      
}

void DuplicateRemoval::WaitUntilDone ()
{
    pthread_join (op_thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n)
{
 runlen=n;
 return;
}

