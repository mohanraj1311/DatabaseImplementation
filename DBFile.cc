#include "DBFile.h"
DBFile::DBFile () {
    dbptr=NULL;
    sortorder=NULL;
    srunlen = 0;
}
DBFile::~DBFile () {
    sortorder = NULL;
    delete dbptr;
    dbptr=NULL;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    char metafile[100];
    strcpy(metafile,f_path);
    strcat(metafile,".meta");
    
    ofstream myfile (metafile);

    if(f_type==heap)
    {
        if (myfile.is_open())
        {
            myfile << "heap\n";
            myfile.close();
        }
        else {return -1;}
        dbptr = new Heap();
    }
    else if (f_type==sorted)
    {
        srunlen = ((SortInfo*)startup)->runLength;
        sortorder = ((SortInfo*) startup)->myOrder;
        if (myfile.is_open())
        {
            myfile << "sorted\n";
            myfile << srunlen <<"\n";
            sortorder->PrintToFile(myfile); // serialize the OrderMaker Object
            myfile.close();
        }
        else {return -1;}
        dbptr = new SortedHeap(srunlen,*sortorder,f_path);
        
    }
    if(!dbptr->Create(f_path,f_type,startup))
    {
        return -1;
    }
    /*if(!Open (f_path))
    {
        return -1;
    }*/
    return 1;
}

int DBFile::Open (char *f_path) {
    if(dbptr==NULL) //1-Heap;2-Sorted Heap;3-Btree;
    {
        char metafile[100];
        strcpy(metafile,f_path);
        strcat(metafile,".meta");
        string status;
        ifstream myfile (metafile);
        if (myfile.is_open())
        {
            getline (myfile,status);
        }
        else
        {
            return -1;
        }
        if(status.compare("heap")==0)
		{
            dbptr = new Heap();
		}
		else if (status.compare("sorted")==0)
		{
                    sortorder = new OrderMaker();
                    srunlen = sortorder->CreateFromFile(myfile);
                    dbptr = new SortedHeap(srunlen,*sortorder,f_path);
		}
		myfile.close();
    }
    return dbptr->Open(f_path);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    dbptr->Load(f_schema,loadpath);
}

void DBFile::MoveFirst() {
    dbptr->MoveFirst();
}

int DBFile::Close (){
   return dbptr->Close();
}

void DBFile::Add (Record &rec) {
    dbptr->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
   return dbptr->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return dbptr->GetNext(fetchme,cnf,literal);
}

int DBFile::AddAtPage(Page *wrtpage)
{
    return dbptr->AddAtPage(wrtpage);
}