#include "Compiler.h"
#include "RelOp.h"
char *projcatpath = "catalog";
char *binfilespath="";
char *tblfilespath="/tmp/DBI/tbl/";

Attribute IntA = {"int",Int};
Attribute DobA = {"double",Double};
Attribute StrA = {"string",String};
string enumvals[11]={"Project", "GroupBy", "Sum", "Join","Distinct","SelectFile","SelectPipe","Create","Drop","Insert","Set"};
string types[3]={"Int", "Double", "String"};


CatalogSingleton* CatalogSingleton::getInstance()
{
    if(obj==NULL)
    {
        obj=new CatalogSingleton();
    }
    return obj;
}
void CatalogSingleton::initializeCatalogDS()
{
     /*Logic:
      Open the File, Fill int the inmemory HashMaps , by scanning from BEGIN to END for
      each Relation

      If the File doesnt exist or if file is empty quit
     */
        FILE *fptr=NULL;    
    fptr = fopen("catalog","r");
    char strRead[200];
    while(fptr!=NULL && fscanf(fptr,"%s",strRead)!=EOF)
    {
        if(strcmp(strRead,"BEGIN")==0)
        {
            char relname[200];
            fscanf(fptr,"%s",relname);            
            char attrib[200];
            char type[20];
            CatAttribs *ca;
            fscanf(fptr,"%s",attrib);
            catalogTableHash[string(relname)]=string(attrib);
            //cout<<"\n "<<string(relname)<<":"<<string(attrib);
            fscanf(fptr,"%s",attrib);            
            while(strcmp(attrib,"END")!=0){
                fscanf(fptr,"%s",type);
                ca = new CatAttribs(string(attrib),string(type));
                catalogTableAttribHash[string(relname)].push_back(ca);
                map<string,CatalogTables*>::iterator itr=catalogAttribHash.find(string(attrib));
                if(itr==catalogAttribHash.end())
                {
                    CatalogTables *head=new CatalogTables(string(relname));
                    catalogAttribHash[string(attrib)]=head;
                }
                else
                {
                    CatalogTables *node=new CatalogTables(string(relname));
                    node->next=catalogAttribHash[string(attrib)];
                    catalogAttribHash[string(attrib)]=node;
                }
                fscanf(fptr,"%s",attrib);
            }
            
        }
    }
        
}
void Compiler::RunDDLquery()
{
    if(outputmode==1)
    {
        cout<<"\n setting RunFlag to 1";
        Compiler::runQueryFlag=true;
        Compiler::outFile=NULL;
    }
    else if(outputmode==2)
    {
        Compiler::runQueryFlag=true;
        Compiler::outFile=outputfile;
        cout<<"\n outputfile:"<<outFile;
    }
    else if(outputmode==3)
    {
        cout<<"\n setting RunFlag to 3";
        Compiler::runQueryFlag=false;
    }
    else if(droptablename!=NULL)
    {
        CatalogSingleton *c = myCatalog->getInstance();
        map<string,vector<CatAttribs*> >::iterator catAttItr = c->catalogTableAttribHash.find(string(droptablename));
        if(catAttItr==c->catalogTableAttribHash.end())
        {
            cout<<"error:No Table "<<droptablename<<" In DataBase:";
            return;
        }
        c->catalogTableAttribHash.erase(catAttItr);
        catAttItr =  c->catalogTableAttribHash.begin();
        FILE *f =fopen(projcatpath,"w");
        while(catAttItr!=c->catalogTableAttribHash.end())
        {
            fprintf(f,"\n%s","BEGIN");
            fprintf(f,"\n%s",catAttItr->first.c_str());
            fprintf(f,"\n%s",c->catalogTableHash[catAttItr->first].c_str());
            int vecsize = catAttItr->second.size();
            for(int i=0;i<vecsize;i++)
            {
                fprintf(f,"\n%s %s",catAttItr->second.at(i)->attrib.c_str(),catAttItr->second.at(i)->type.c_str());
            }
            fprintf(f,"\nEND");
            catAttItr++;
        }
        fclose(f);

        string filepath=string(binfilespath)+string(droptablename)+".bin";
        char *tmp = new char[filepath.length()];
        strcpy(tmp,filepath.c_str());
        remove(tmp);
        delete tmp;
        filepath=string(binfilespath)+string(droptablename)+".bin"+".meta";
        tmp = new char[filepath.length()];
        strcpy(tmp,filepath.c_str());
        remove(tmp);
        delete tmp;
        
        c->catalogAttribHash.clear();
        c->catalogTableHash.clear();
        c->catalogTableAttribHash.clear();
        c->initializeCatalogDS();


    }
    else if(createTable!=NULL)
    {
        //Create (char *f_path, fType f_type, void *startup)
        CatalogSingleton *c = myCatalog->getInstance();
        CatAttribs *ca ;
        struct NameList *sortkeys=createTable->sortkeys;
        struct TableAtts* atts=createTable->atts;
        char *tablename=createTable->tableName;
        string tab = string(createTable->tableName);
        c->catalogTableHash[tab] = tab+".tbl";
        
        map<string,vector<CatAttribs*> >::iterator catAttItr = c->catalogTableAttribHash.find(tab);
        if(catAttItr!=c->catalogTableAttribHash.end())
        {
            cout<<"error:Table "<<tab<<" already exists in DataBase:";
            return;
        }

        while(atts!=NULL)
        {
            struct CrAttr *Op=atts->Op;
            map<string,CatalogTables*>::iterator itr=c->catalogAttribHash.find(string(Op->value));
             if(itr==c->catalogAttribHash.end())
             {
                    CatalogTables *head=new CatalogTables(tab);
                    c->catalogAttribHash[string(Op->value)]=head;
             }
             else
             {
                    CatalogTables *node=new CatalogTables(tab);
                    node->next=c->catalogAttribHash[string(Op->value)];
                    c->catalogAttribHash[string(Op->value)]=node;
             }            
 
            if(strcmp(Op->type,"INTEGER")==0)
            {
              ca = new CatAttribs(string(Op->value),string("Int"));
              c->catalogTableAttribHash[tab].push_back(ca);
            }

            else if(strcmp(Op->type,"STRING")==0)
            {
              ca = new CatAttribs(string(Op->value),string("String"));
              c->catalogTableAttribHash[tab].push_back(ca);
            }

            else if(strcmp(Op->type,"DOUBLE")==0)
            {
                ca = new CatAttribs(string(Op->value),string("Double"));
              c->catalogTableAttribHash[tab].push_back(ca);
            }
            Op->value;
            atts=atts->next;
        }
     
        
        catAttItr =  c->catalogTableAttribHash.begin();
        FILE *f =fopen(projcatpath,"w");
        
        while(catAttItr!=c->catalogTableAttribHash.end())
        {
            fprintf(f,"\n%s","BEGIN");
            fprintf(f,"\n%s",catAttItr->first.c_str());
            fprintf(f,"\n%s",c->catalogTableHash[catAttItr->first].c_str());
            int vecsize = catAttItr->second.size();
            for(int i=0;i<vecsize;i++)
            {
                fprintf(f,"\n%s %s",catAttItr->second.at(i)->attrib.c_str(),catAttItr->second.at(i)->type.c_str());
            }
            fprintf(f,"\nEND");
            catAttItr++;
        }
        fclose(f);
        DBFile db;
        string filepath=string(binfilespath)+string(tablename)+".bin";
        if(createTable->sortkeys==NULL)
        {
            char *tmp = new char[filepath.length()];
            strcpy(tmp,filepath.c_str());
            db.Create(tmp,heap,NULL);
        }
        else
        {
            OrderMaker *orm = new OrderMaker();
            int i = 0;
            struct NameList *satts = createTable->sortkeys;
            Schema *s = new Schema(projcatpath,tablename);
            while(satts!=NULL)
            {
            int res = s->Find(satts->name);
            Type restype = s->FindType(satts->name);
            if(res!=-1)
            {
                orm->whichAtts[i] = res;
                orm->whichTypes[i] = restype;
                i++;
            }
            satts = satts->next;
            }
            orm->numAtts=i;
            int runlen=10;
            struct {OrderMaker *o; int l;} startup = {orm, runlen};
            char *tmp = new char[filepath.length()];
            strcpy(tmp,filepath.c_str());
            db.Create(tmp,sorted,&startup);
        }
        db.Close();     
    }
    else if(insertinto!=NULL)
    {
        DBFile insertdb;
        string fpath = string(binfilespath)+string(insertinto->dbfile)+".bin";

        char *tmp = new char[fpath.length()];
        strcpy(tmp,fpath.c_str());
        //insertdb.Create(tmp,heap,NULL);
        cout<<"\n filepath:"<<fpath;
        insertdb.Open(tmp);
        Schema s(projcatpath,insertinto->dbfile);

        fpath = string(tblfilespath)+string(insertinto->filename);
        tmp = new char[fpath.length()];
        cout<<"\n filepath:"<<fpath;
        strcpy(tmp,fpath.c_str());
        cout<<"\n"<<insertinto->filename;
        insertdb.Load(s,tmp);
        insertdb.Close();
    }
}
bool Compiler::Parse()
{
    QueryOps op;
    
    if(pi->ParseQuery())
    {
     if(DDL)
     {
         RunDDLquery();
         return true;
     }
     
    for(int x=0;x<=10;x++)
         {
        queryOperations[x]=false;
        }
        if(pi->ErrorCheckQuery())
        {
                                    
            if(tables->next!=NULL)
            {
                op = JoinOp;
                queryOperations[op]=true;
            }
            if(finalFunction!=NULL)
            {
                if(groupingAtts!=NULL)
                {
                    op = GroupByOp;
                    queryOperations[op]=true;
                }
                else
                {
                    op = SumOp;
                    queryOperations[op]=true;
                }
            }            
            if( queryOperations[GroupByOp]==false && queryOperations[SumOp]==false)
            {
                op = ProjectOp;
                queryOperations[op]=true;
            }
            if(distinctAtts!=0 ||distinctFunc!=0)
            {
                op = DistinctOp;
                queryOperations[op]=true;
            }            
            oi->SetQueryOps(queryOperations,QOPSSIZE,&root);

           return true;
        }
    }
    return false;
}
void Compiler::Optimze()
{    
    if(queryOperations[JoinOp])
     oi->DetermineJoinOrder();
    oi->GenerateQueryPlan();
}
void Compiler::Runquery()
{
    qi->setroot(root);
    qi->RunQuery();
}
void Compiler::Compile()
{
    
    if(Parse())
    {
        if(DDL)
            return;
        Optimze();

        Runquery();
    }
    else
    {
        cout<<"Parser Error:Syntax or Semantic issue";
    }
}

bool MyParser::ParseQuery()
{
    int retstatus = yyparse();

    struct TableList * ptr = tables;
    struct TableList * temp ;
    struct TableList * previous = NULL;
    while(ptr != NULL) {
        temp = ptr->next;
        ptr->next = previous;
        previous = ptr;
        ptr = temp;
    }
    tables = previous;


    if(retstatus==0)
    {
        struct TableList *tableList = tables;
        while(tableList!=NULL)
        {
            tableAliasHashDS[string(tableList->tableName)]= string(tableList->aliasAs);
            aliasTableHashDS[string(tableList->aliasAs)]=string(tableList->tableName);
            tableList = tableList->next;
        }
    }
    return (retstatus == 0);
}

void MyParser::Split(string str)
{
    size_t pos = str.find('.',0);
    if(pos==string::npos)
    {
        alias="";
        attr=str;
    }
    else
    {
    alias= str.substr(0,pos);
    attr = str.substr(pos+1);
    }    
}

bool MyParser::ErrorCheckQuery()
{
//Check if SelectAtts are all in groupAtts
    struct NameList *groupingAttsitr=groupingAtts; // grouping atts (NULL if no grouping)
    struct NameList *attsToSelectitr=attsToSelect;
    struct TableList *tablelist=tables;
    struct AndList *andList=boolean;
    bool result=false;

    map<string,string>::iterator tabItr;
    map<string,CatalogTables*>::iterator attitr;
    map<string,int> queryattribs;
    
    while(tablelist!=NULL)
    {
       tabItr=myCatalog->catalogTableHash.find(string(tablelist->tableName));

        if(tabItr==myCatalog->catalogTableHash.end())
        {
            cout<<"\n Error:Invalid table: "<<tablelist->tableName;
            return false;
        }
        tablelist=tablelist->next;
    }
    //fill in the hash map
    while(attsToSelectitr!=NULL)
    {
        queryattribs[string(attsToSelectitr->name)]=1;
        attsToSelectitr=attsToSelectitr->next;
    }
    while(groupingAttsitr!=NULL)
    {
        queryattribs[string(groupingAttsitr->name)]=1;
        groupingAttsitr=groupingAttsitr->next;

    }
    while(andList!=NULL)
    {
        struct OrList *orlist=andList->left;                
        while(orlist!=NULL)
        {
            ComparisonOp *Op=orlist->left;
            
            if(Op->left->code==4)
            {
                queryattribs[string(Op->left->value)]=1;
                Split(Op->left->value);
                
            }
            if(Op->right->code==4)
                queryattribs[string(Op->right->value)]=1;
            orlist=orlist->rightOr;

        }
        andList=andList->rightAnd;
    }
    //check for validity of all attribs in hashmap
    map<string,int>::iterator queryattribsItr = queryattribs.begin();
    while(queryattribsItr!=queryattribs.end())
    {
        if(!isAttValid(queryattribsItr->first))
        {
            cout<<"\n Error:Invalid Attribute "<<queryattribsItr->first;
            return false;
        }
        queryattribsItr++;
    }

    //Checking grouping Semantics;
    groupingAttsitr=groupingAtts;
    attsToSelectitr=attsToSelect;
    if(groupingAttsitr!=NULL)
    {
      while(attsToSelectitr!=NULL)
      {
        groupingAttsitr=groupingAtts;
        while(groupingAttsitr!=NULL)
        {
            Split(string(attsToSelectitr->name));
            string selAttr(attr);
            string selalias(alias);
            Split(string(groupingAttsitr->name));
            string grpAttr(attr);
            string grpalias(alias);
            if(selalias.compare("")!=0 && grpalias.compare("")!=0)
            {
                if(selalias.compare(grpalias)!=0)
                {
                    cout<<"\n Error:Group must contain "<<attsToSelectitr->name;
                    return false;
                }
            }
            if(selAttr.compare(grpAttr)==0)
                break;
            groupingAttsitr=groupingAttsitr->next;
        }
        if(groupingAttsitr==NULL)
        {
            cout<<"\n Error:Group must contain "<<attsToSelectitr->name;
            return false;
        }
       attsToSelectitr=attsToSelectitr->next;
      }
    }
return true;
}

bool MyParser::isAttValid(string attribute)
{
    Split(attribute);
    int count;
    string table;
    map<string,string>::iterator itr;
    map<string,CatalogTables*>::iterator attItr;
    if(alias.compare("")!=0)
    {
        table = aliasTableHashDS[alias];                
        //check if the attribute is in the catalog
        attItr = myCatalog->catalogAttribHash.find(attr);
        if(attItr==myCatalog->catalogAttribHash.end())
            return false;
        else
        {
            CatalogTables* ptr = attItr->second;
            while(ptr!=NULL)
            {
                if(table.compare(ptr->tableName)==0)
                    break;
                ptr = ptr->next;
            }
            if(ptr == NULL)
                return false;
        }
    }
    else
    {
        attItr = myCatalog->catalogAttribHash.find(attr);
        if(attItr==myCatalog->catalogAttribHash.end())
            return false;
        else
        {
        count=0;
        struct TableList *tableList=tables;
        while(tableList!=NULL && count<2)
        {
            
            CatalogTables* ptr = attItr->second;
            string table = string(tableList->tableName);
            while(ptr!=NULL)
            {
                if(table.compare(ptr->tableName)==0)
                    break;
                ptr = ptr->next;
            }
            if(ptr != NULL)
                count++;
         tableList=tableList->next;
        }
        if(count != 1)
            return false;
        }
    }
    return true;
}


void MyOptimizer::DetermineJoinOrder()
{

    Statistics *statsPtr = myCatalog->stats;    
    Preprocess();
    map<struct AndList*,vector<string> >::iterator andRelitr;
    //put all single relations;
    andRelitr = andRelHash.begin();
    while(andRelitr!=andRelHash.end())
    {
        vector<struct AndList *> vec;
        if(andRelitr->second.size()==1)
        {
            queryExecHash[andRelitr->second.at(0)] = vec;
        }
        andRelitr++;
    }
    
    andRelitr = andRelHash.begin();
    while(andRelitr!=andRelHash.end())
    {
        //cout<<"\n andRelItr second size:"<<andRelitr->second.size();
        if(andRelitr->second.size()==1)
        {
          //  cout<<"\n"<<andRelitr->second.at(0)<<" Added var to QueryExec hash second\n";
            queryExecHash[andRelitr->second.at(0)].push_back(andRelitr->first);
            //cout<<"\n "<<andRelitr->first <<" "<<andRelitr->second.at(0)<<" "<<queryExecHash[andRelitr->second.at(0)].size();
        }
        andRelitr++;
    }
    //put all two relations;
    struct TableList *tl=tables,*tl2;

    while(tl!=NULL)
    {
        string c1 = tableIdHashDS[string(tl->tableName)];
        tl2 = tl->next;
        while(tl2!=NULL)
            {
                vector<struct AndList *> vec;
                string c2 = tableIdHashDS[string(tl2->tableName)];
                //cout<<"\n c1:"<<c1<<" c2:"<<c2;
                InsertAnd(c1,c2,vec);
                char **rnames = new char*[2];

                rnames[0] = tl->aliasAs;
                rnames[1] = tl2->aliasAs;
    
                struct AndList *head = createAndList(c1+c2);

                double val = statsPtr->Estimate(head,rnames,2);                
                QueryExecStatsNode *node = new QueryExecStatsNode(c1+c2,val,0);
                queryEstimateHash[c1+c2]=node;
                tl2=tl2->next;
            }
        tl=tl->next;
        }

    
    string Ids[tableCnt];
    map<string,string>::iterator itr=idTableHashDS.begin();
    for(int i=0;i<tableCnt;i++,itr++)
        Ids[i]=itr->first;
    
    int i = 3;
    map<string,QueryExecStatsNode*>::iterator estimateItr;
    estimateItr=queryEstimateHash.begin();
    
    while(i<=tableCnt)
    {
        estimateItr=queryEstimateHash.begin();
        while(estimateItr!=queryEstimateHash.end() )
        {
            if(estimateItr->first.length()!=(i-1))
            {
                estimateItr++;
                continue;
            }
            string relList = estimateItr->first;
            for(int k =0;k<tableCnt;k++)
            {
                if(relList.find(Ids[k]) == string::npos)
                {
                    string newrelList = relList + Ids[k];
                    //cout<<"\newrelList:"<<newrelList;
                    char **rnames = new char*[i];
                    for(int j=0;j<i;j++)
                    {
                        char x[2]={newrelList.at(j),'\0'};                       
                        string table = idTableHashDS[string(x)];                        
                        rnames[j]=new char[tableAliasHashDS[table].length()+1];
                       // cout<<"\n tableAliasHashDS[table]"<<tableAliasHashDS[table];
                        strcpy(rnames[j],tableAliasHashDS[table].c_str());
                        //cout<<"\n RelName:"<<rnames[j];
                        
                    }
                    vector<struct AndList *> vec;
                    InsertAnd(relList,Ids[k],vec);
                    struct AndList *head = createAndList(newrelList);
                    double val = statsPtr->Estimate(head,rnames,i);
                    double cost = queryEstimateHash[estimateItr->first]->estimateTuples+queryEstimateHash[estimateItr->first]->costEstimate;
                    string angram=getAnagram(newrelList);
                    cout<<"\n expression:"<<newrelList<<" cost:"<<cost<<" estimate:"<<val;
                   if(angram.compare("")==0)
                    {
                    QueryExecStatsNode *node = new QueryExecStatsNode(newrelList,val,cost);
                    queryEstimateHash[newrelList]=node;
                    }
                    else if(queryEstimateHash[angram]->costEstimate>cost)
                    {
                        queryEstimateHash[angram]->costEstimate=cost;
                        queryEstimateHash[angram]->expr=newrelList;
                        queryEstimateHash[angram]->estimateTuples=val;
                    }
                }
            }
            estimateItr++;
        }
        //delete all length i-1;
        delFrmEstHash(i-1);
        i++;
    }
    estimateItr=queryEstimateHash.begin();
    
    cout<<"\n\n Printing Final List";
    while(estimateItr!=queryEstimateHash.end() )
    {
        cout<<"\n expression:"<<estimateItr->second->expr<<" cost:"<<estimateItr->second->costEstimate<<" estimate:"<<estimateItr->second->estimateTuples;
        resultJoinExpr = estimateItr->second->expr;
        estimateItr++;
    }

 //   cout<<"\n End Of Generating Join Order";


    
}
void MyOptimizer::delFrmEstHash(int len)
{
    map<string,QueryExecStatsNode*>::iterator estimateItr=queryEstimateHash.begin();
    while(estimateItr!=queryEstimateHash.end())
    {
        if(estimateItr->first.length()==len)
        {
            delete estimateItr->second;
            string temp=estimateItr->first;
            //cout<<"\n Erasing:"<<estimateItr->first;
            estimateItr++;
            queryEstimateHash.erase(temp);

        }
        else
        {
        estimateItr++;
        }                      
    }
}
string MyOptimizer::getAnagram(string newrelList)
{
    map<string,QueryExecStatsNode*>::iterator estimateItr=queryEstimateHash.begin();
    while(estimateItr!=queryEstimateHash.end())
    {
        if(isAnagram(newrelList,estimateItr->first))
            return estimateItr->first;
        estimateItr++;
    }
    return "";
}

struct AndList* MyOptimizer::createAndList(string str)
{
    int size = str.length();
    map<string,vector<struct AndList *> >::iterator queryExecHashItr;
    struct AndList dummy;
    dummy.rightAnd=NULL;
    struct AndList *temp=&dummy;
    //cout<<"\n str size:"<<size<<" "<<str;
    queryExecHashItr = queryExecHash.begin();

    for(int i=0;i<size;i++)
    {
        //cout<<"\n substring"<<str.substr(i,i);
       // cout<<"\n substring"<<str.at(i);
        char x[2]={str.at(i),'\0'};


       queryExecHashItr = queryExecHash.find(string(x));
       

       if(queryExecHashItr != queryExecHash.end())
       {
           //cout<<"\nIn Create ANDLIST queryExec find on "<<x;
           int veclength=queryExecHashItr->second.size();
           //cout<<"\n veclength:"<<veclength;
           for(int j=0;j<veclength;j++)
           {
           temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
           temp=temp->rightAnd;
           temp->rightAnd=NULL;
           temp->left=queryExecHashItr->second.at(j)->left;
           //cout<<"\n left:"<<temp->left->left->code<<" "<<temp->left->left->left->value;
           }           
       }       
    }
    for(int i=2;i<=size;i++)
    {
        //Bad Hash Entries Fixes;
        if(queryExecHash.find(str.substr(0,i))!=queryExecHash.end())
        {
        int veclength=queryExecHash[str.substr(0,i)].size();
      //  cout<<"\nqueryExecHash[str.substr(0,i)].size():"<<queryExecHash[str.substr(0,i)].size();
       // cout<<"\nqueryExecHash:"<<str.substr(0,i);
        for(int j=0;j<veclength;j++)
        {
           temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
           temp=temp->rightAnd;
           temp->rightAnd=NULL;           
                temp->left=queryExecHash[str.substr(0,i)].at(j)->left;
        }
        }            
    }


    //*Special Case OR * DELETE HERE If Error*/
    /*
    map<string,vector<struct AndList *> >::iterator qSelItr=  estSelPipeHash.begin();
    while(qSelItr!=estSelPipeHash.end())
            {
                string joinOn=str;
                map<char,int> charhash;
                int len = qSelItr->first.length();
                for(int i=0;i<len;i++)
                         charhash[qSelItr->first.at(i)]=1;
                len = joinOn.size();
                for(int i=0;i<len;i++)
                    charhash.erase(joinOn.at(i));

                if(charhash.empty())
                {
                    int vecs=qSelItr->second.size();
                    cout<<"\n Vecs size:"<<vecs;
                      for(int i=0;i<vecs;i++)
                    {
                    temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
                    temp=temp->rightAnd;
                    temp->rightAnd=NULL;
                    temp->left=qSelItr->second.at(i)->left;
                    }
                    cout<<"\n Added OrList for :"<<qSelItr->first;
                     estSelPipeHash.erase(qSelItr);

                }
                qSelItr++;
            }
*/
    return dummy.rightAnd;
}

Schema* MyOptimizer::ConstructJSchema(Schema *s1,Schema *s2)
{
 int numAtts1 = s1->GetNumAtts();
 int numAtts2 = s2->GetNumAtts();
 Attribute *att1 = s1->GetAtts();
 Attribute *att2 = s2->GetAtts();

 int resultnumAtt = numAtts1 + numAtts2;
 Attribute *res = new Attribute[resultnumAtt];
 int i=0;
 for(i=0;i<numAtts1;i++)
 {
     res[i].myType = att1[i].myType;
     res[i].name = strdup(att1[i].name);
 }
 int j=i;
 for(i=0;i<numAtts2;i++,j++)
 {
    res[j].myType = att2[i].myType;
    res[j].name = strdup(att2[i].name);
 }
 Schema *s3 = new Schema("dummy",resultnumAtt,res);
 return s3;
}

void MyOptimizer::ChangeLists()
{
    struct AndList* start=boolean;
    while(start!=NULL)
    {
        struct OrList* orlist=start->left;
        while(orlist!=NULL)
        {
            if(orlist->left->left->code==4)
            {
                Split(string(orlist->left->left->value));
                strcpy(orlist->left->left->value,attr.c_str());
            }
            if(orlist->left->right->code==4)
            {
                Split(string(orlist->left->right->value));
                strcpy(orlist->left->right->value,attr.c_str());
            }
            orlist=orlist->rightOr;
        }
        start=start->rightAnd;
    }
    struct NameList *atts=groupingAtts;
    while(atts!=NULL)
    {
        Split(string(atts->name));
         strcpy(atts->name,attr.c_str());
        atts=atts->next;
    }
    atts=attsToSelect;
    while(atts!=NULL)
    {
        Split(string(atts->name));
        strcpy(atts->name,attr.c_str());
        atts=atts->next;
    }
    ChangeFunctionList(finalFunction);

}
void MyOptimizer::ChangeFunctionList(struct FuncOperator *func )
{
    if(func==NULL)
        return;
    if(func->leftOperand!=NULL)
    {
    Split(string(func->leftOperand->value));
    strcpy(func->leftOperand->value,attr.c_str());
    }
    ChangeFunctionList(func->leftOperator);
    ChangeFunctionList(func->right);
    
}
void MyOptimizer::GenerateQueryPlan()
{
    ChangeLists();
    QueryPlanNode *lchild=NULL;
    QueryPlanNode *rchild=NULL;    
    
    int pipecnt=-1;
    //create the join part of the query plan tree;
    if(queryOps[JoinOp]==true)
    {        
        //Do a select File for 2 operands
        //Select Node 1:
        char str1[2]= {resultJoinExpr.at(0),'\0'};
        char *tabName1 = new char[idTableHashDS[string(str1)].length()];
        strcpy(tabName1,idTableHashDS[string(str1)].c_str());

        CNF *c1=new CNF();
        Schema *s1=new Schema(projcatpath,tabName1);
        Record *lit1 = new Record();

        //What will happen when and List is NULL;
        int relcnt=0;

        //cout<<"\nQuery Exec Hash:";
        map<string,vector<struct AndList *> >::iterator queryExecItr = queryExecHash.begin();
        while(queryExecItr!=queryExecHash.end())
        {
            //cout<<"\nFirst QHASH:"<<queryExecItr->first;
            queryExecItr++;
        }


        queryExecItr = queryExecHash.find(resultJoinExpr.substr(relcnt,1));
      //  cout<<"\nFind on :"<<resultJoinExpr.substr(relcnt,1);

        if(queryExecItr!=queryExecHash.end())
        {                        
              struct AndList dummy;
              dummy.rightAnd=NULL;
              struct AndList *temp=&dummy;
              int vecs=queryExecItr->second.size();
              for(int i=0;i<vecs;i++)
                {
                temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
                temp=temp->rightAnd;
                temp->rightAnd=NULL;
                temp->left=queryExecItr->second.at(i)->left;
                }
              c1->GrowFromParseTree(dummy.rightAnd,s1,*lit1);           
        }
        else
        {           
            c1=NULL;
            lit1 = NULL;
            //cout<<"\nSelect File CNF is NULL";
        }
        
        QueryPlanNode *newNode = new QueryPlanNode(SelectFileOp,s1,c1,NULL,NULL,NULL,lit1,string(tabName1),tableAliasHashDS[string(tabName1)]);
        newNode->leftptr = NULL;
        newNode->rightptr = NULL;
        newNode->outPipe=++pipecnt;
        lchild = newNode;
        
        relcnt++; //To go to the next relation in Join Order;

        //Select Node 2:
        char str2[2]= {resultJoinExpr.at(1),'\0'};
        char *tabName2 = new char[idTableHashDS[string(str2)].length()];
        strcpy(tabName2,idTableHashDS[string(str2)].c_str());
        
        CNF *c2=new CNF();
        Schema *s2=new Schema(projcatpath,tabName2);
        Record *lit2 = new Record();
        //What will happen when and List is NULL;        
        queryExecItr = queryExecHash.find(resultJoinExpr.substr(relcnt,1));
       // cout<<"\nFind on :"<<resultJoinExpr.substr(relcnt,1);
        if(queryExecItr!=queryExecHash.end())
        {
              struct AndList dummy;
              dummy.rightAnd=NULL;
              struct AndList *temp=&dummy;
              int vecs=queryExecItr->second.size();
              for(int i=0;i<vecs;i++)
                {
                temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
                temp=temp->rightAnd;
                temp->rightAnd=NULL;
                temp->left=queryExecItr->second.at(i)->left;
                }
              c2->GrowFromParseTree(dummy.rightAnd,s2,*lit2);           
        }
        else
        {
            c2=NULL;
            lit2=NULL;            
        }
 
        newNode = new QueryPlanNode(SelectFileOp,s2,c2,NULL,NULL,NULL,lit2,string(tabName2),tableAliasHashDS[string(tabName2)]);
        newNode->leftptr=NULL;
        newNode->rightptr=NULL;
        newNode->outPipe=++pipecnt;
        rchild = newNode;

        relcnt++;
        //Do a join of  Relations
        //----------Here---------//

        //Loop and Do Join on the result with new Tables
        int jcnt=1;
        do
        {        
        Record *lit3 = new Record();
        CNF *c3 = new CNF();
        Schema *s3 = ConstructJSchema(lchild->outSchema,rchild->outSchema);        
        queryExecItr = queryExecHash.find(resultJoinExpr.substr(0,jcnt+1));
        cout<<"\n Join On:"<<resultJoinExpr.substr(0,jcnt+1);
        if(queryExecItr!=queryExecHash.end())
        {
              struct AndList dummy;
              dummy.rightAnd=NULL;
              struct AndList *temp=&dummy;
              int vecs=queryExecItr->second.size();
              for(int i=0;i<vecs;i++)
                {
                temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
                temp=temp->rightAnd;
                temp->rightAnd=NULL;
                temp->left=queryExecItr->second.at(i)->left;
                }
              c3->GrowFromParseTree(dummy.rightAnd,lchild->outSchema,rchild->outSchema,*lit3);              
        }
        else
        {
            c3=NULL;
            lit3=NULL;
        }
        
        newNode = new QueryPlanNode(JoinOp,s3,c3,NULL,NULL,NULL,lit3,"","");
        newNode->leftptr=lchild;
        newNode->rightptr=rchild;
        newNode->inputPipe1 = lchild->outPipe;
        newNode->inputPipe2 = rchild->outPipe;
        newNode->outPipe = ++pipecnt;
        lchild = newNode;
        rchild = NULL;
        
        // Check For Select PIPE OP; If any problem *DELETE HERE*
        if(queryOps[SelectPipeOp]==true)
        {
            
            struct AndList dummy;
            dummy.rightAnd=NULL;
            struct AndList *temp=&dummy;
            map<string,vector<struct AndList *> >::iterator qSelItr=  queryExecSelPipeHash.begin();
            while(qSelItr!=queryExecSelPipeHash.end())
            {
                string joinOn=resultJoinExpr.substr(0,jcnt+1);
                map<char,int> charhash;
                int len = qSelItr->first.length();
                for(int i=0;i<len;i++)
                         charhash[qSelItr->first.at(i)]=1;
                len = joinOn.size();
                for(int i=0;i<len;i++)
                    charhash.erase(joinOn.at(i));
                
                if(charhash.empty())
                {
                    int vecs=qSelItr->second.size();
                    for(int i=0;i<vecs;i++)
                    {
                    temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
                    temp=temp->rightAnd;
                    temp->rightAnd=NULL;
                    temp->left=qSelItr->second.at(i)->left;
                    }
                     queryExecSelPipeHash.erase(qSelItr);
                }
                qSelItr++;
            }
            if(dummy.rightAnd!=NULL)
            {
            CNF *cs = new CNF();
            Record *slit = new Record();
            cs->GrowFromParseTree(dummy.rightAnd,lchild->outSchema,*slit);
            newNode = new QueryPlanNode(SelectPipeOp,lchild->outSchema,cs,NULL,NULL,NULL,slit,"","");
            newNode->leftptr=lchild;
            newNode->rightptr=NULL;
            newNode->inputPipe1 = lchild->outPipe;            
            newNode->outPipe = ++pipecnt;
            lchild = newNode;
            }
        }

        jcnt++;
        if(jcnt<tableCnt)
        {
                char str4[2]= {resultJoinExpr.at(jcnt),'\0'};
                char *tabName4 = new char[idTableHashDS[string(str4)].length()];
                strcpy(tabName4,idTableHashDS[string(str4)].c_str());

                CNF *c4=new CNF();
                Schema *s4=new Schema(projcatpath,tabName4);
                Record *lit4 = new Record();              
                queryExecItr = queryExecHash.find(resultJoinExpr.substr(relcnt,1));              
                if(queryExecItr!=queryExecHash.end())
                {
                      struct AndList dummy;
                      dummy.rightAnd=NULL;
                      struct AndList *temp=&dummy;
                      int vecs=queryExecItr->second.size();
                      for(int i=0;i<vecs;i++)
                        {
                        temp->rightAnd=(struct AndList*)malloc(sizeof(struct AndList));
                        temp=temp->rightAnd;
                        temp->rightAnd=NULL;
                        temp->left=queryExecItr->second.at(i)->left;
                        }
                      c4->GrowFromParseTree(dummy.rightAnd,s4,*lit4);                   
                }
                else
                {
                    c4=NULL;
                    lit4=NULL;                    
                }
                newNode = new QueryPlanNode(SelectFileOp,s4,c4,NULL,NULL,NULL,lit4,string(tabName4),tableAliasHashDS[string(tabName4)]);
                newNode->leftptr=NULL;
                newNode->rightptr=NULL;
                newNode->outPipe=++pipecnt;
                rchild = newNode;
                relcnt++;
        }
     }while(jcnt<tableCnt);        
    }
    else
    {
        CNF *c=new CNF();
        Schema *s=new Schema(projcatpath,tables->tableName);
        Record *lit = new Record();
        c->GrowFromParseTree(boolean,s,*lit);
        QueryPlanNode *newNode = new QueryPlanNode(SelectFileOp,s,c,NULL,NULL,NULL,lit,string(tables->tableName),string(tables->aliasAs));
        newNode->leftptr=NULL;
        newNode->rightptr=NULL;
        newNode->outPipe=++pipecnt;
        lchild = newNode;
        rchild = NULL;        
    }

    //Remaining Operations After Join:
     if(queryOps[GroupByOp]==true)
     {
            Function *f = new Function();
            f->GrowFromParseTree(finalFunction,*lchild->outSchema);
            OrderMaker *o = new OrderMaker();
            CreateGroupByOm(o,lchild->outSchema);
            //Some output Schema; Anway we are not going to use the Output Schema from group
            //By Further;
            Schema *group_schema = new Schema("group_schema",1,&DobA);                        
            QueryPlanNode *newGrpNode = new QueryPlanNode(GroupByOp,group_schema,NULL,f,o,NULL,NULL,"","");
            newGrpNode->leftptr = lchild;
            newGrpNode->rightptr = NULL;
            newGrpNode->inputPipe1=lchild->outPipe;
            newGrpNode->outPipe=++pipecnt;
            lchild = newGrpNode;
            rchild = NULL;            
     }
     else if (queryOps[SumOp]==true)
     {
            Function *f = new Function();            
            f->GrowFromParseTree(finalFunction,*lchild->outSchema);
            Schema *sum_schema = new Schema("sum_schema",1,&DobA);
            QueryPlanNode *newSumNode = new QueryPlanNode(SumOp,sum_schema,NULL,f,NULL,NULL,NULL,"","");
            newSumNode->leftptr = lchild;
            newSumNode->rightptr = NULL;
            newSumNode->inputPipe1=lchild->outPipe;
            newSumNode->outPipe=++pipecnt;
            lchild = newSumNode;
            rchild = NULL;
     }
     else if (queryOps[ProjectOp]==true)
        {
         int numAtts = lchild->outSchema->GetNumAtts();
         Attribute *atts = lchild->outSchema->GetAtts();
         struct NameList *attSel=attsToSelect;         
         int cnt=0;
         while(attSel!=NULL)
         {
             cnt++;
             attSel = attSel->next;
         }
         Attribute *projectoutAtts = new Attribute[cnt];
         attSel=attsToSelect;
         int *attsToKeep = new int[cnt];
         vector<int> vec;
         while(attSel!=NULL)
         {
          for (int i = 0,j=0; i < numAtts; i++) {
            if(strcmp(atts[i].name,attSel->name)==0)
             {            
              vec.push_back(i);
             }
            }
            attSel = attSel->next;
         }
         sort(vec.begin(),vec.end());
         for(int i=0;i<vec.size();i++)
         {
             attsToKeep[i]=vec.at(i);
             //cout<<"\n attsToKeep[i]:"<<attsToKeep[i];
             projectoutAtts[i].myType = atts[vec.at(i)].myType;
             projectoutAtts[i].name = strdup(atts[vec.at(i)].name);

         }
//Testing
         Schema *ps = new Schema("ps",cnt,projectoutAtts);
         //cout<<"\n Number of input atts:"<<numAtts<<" outputatts:"<<cnt;
         ProjectParams *pp = new ProjectParams(attsToKeep,numAtts,cnt);

         QueryPlanNode *newProjNode = new QueryPlanNode(ProjectOp,ps,NULL,NULL,NULL,pp,NULL,"","");
         newProjNode->leftptr = lchild;
         newProjNode->rightptr = NULL;
         newProjNode->inputPipe1=lchild->outPipe;
         newProjNode->outPipe=++pipecnt;
         lchild = newProjNode;
         rchild=NULL;
        }

    if(queryOps[DistinctOp]==true && queryOps[SumOp]==false)
    {
        //Use the output Schema as the Schema to use for Duplicate Removal
         QueryPlanNode *newDistNode = new QueryPlanNode(DistinctOp,lchild->outSchema,NULL,NULL,NULL,NULL,NULL,"","");
         newDistNode->leftptr = lchild;
         newDistNode->rightptr = NULL;
         newDistNode->inputPipe1=lchild->outPipe;
         newDistNode->outPipe=++pipecnt;
         lchild = newDistNode;
         rchild=NULL;
    }
    *qproot = lchild;
    //return lchild;
}    

void MyOptimizer::Split(string str)
{
    size_t pos = str.find('.',0);
    if(pos==string::npos)
    {
        alias="";
        attr=str;
    }
    else
    {
    alias= str.substr(0,pos);
    attr = str.substr(pos+1);
    }
}

string MyOptimizer::FindTable()
{
    if(alias!="")
    {
        return aliasTableHashDS[alias];
    }
    else
    {
        map<string,CatalogTables*>::iterator attItr;
        attItr = myCatalog->catalogAttribHash.find(attr);
        struct TableList *tableList=tables;
        while(tableList!=NULL)
        {
            CatalogTables* ptr = attItr->second;
            string table = string(tableList->tableName);
            while(ptr!=NULL)
            {
                if(table.compare(ptr->tableName)==0)
                {
                    return table;                    
                }
                ptr = ptr->next;
            }
         tableList=tableList->next;
        }
      }   
}
bool MyOptimizer::isAnagram(string first,string second)
{
    bool flag;
    if(first.length() == second.length())
    {
        for (int i=0;i<first.length();i++)
        {
            flag = false;
            for (int j=0;j<second.length();j++)
            {
                if(first.at(i) == second.at(j)) 
                {
                    flag = true;
                    break;
                }
            }
            if(flag == false)
                return false;
        }
    }
    else
        return false;

    return true;
}

void MyOptimizer::CreateGroupByOm(OrderMaker *o,Schema *s)
{    
    int i = 0;    
    struct NameList *gatts = groupingAtts;
    while(gatts!=NULL)
    {
        int res = s->Find(gatts->name);
        Type restype = s->FindType(gatts->name);
        if(res!=-1)
        {
         o->whichAtts[i] = res;
         o->whichTypes[i] = restype;
         i++;
        }
        gatts = gatts->next;
    }
    o->numAtts=i;                
}

void MyOptimizer::InsertAnd(string expr,string newtabl,vector<struct AndList *> &vec)
{
    string finalexpr=expr+newtabl;
    int finalsize=finalexpr.size();
    map<struct AndList*,vector<string> > ::iterator itr=andRelHash.begin();
    int count;
    int size;    
    string tempexpr=expr;
    while(itr!=andRelHash.end())
    {
        count=itr->second.size();
        size=count;        
        if(count!=1)
        {
            for(int i=0;i<size;i++)
            {
                string temptabl=itr->second.at(i);                
                for(int j=0;j<finalsize;j++)
                {
                    //cout<<"\n finalexpr.at(j)==temptabl.at(0)"<<finalexpr.at(j)<<" "<<temptabl.at(0);
                    if(finalexpr.at(j)==temptabl.at(0))
                        count--;
                }
            }
            if(count==0)
            {

                bool flag=true;
                tempexpr=expr;
        vector<struct AndList *>::iterator expritr;
        while(tempexpr.length()!=0)
        {
        // Hash Bad entries Fix;
            if(queryExecHash.find(tempexpr)!=queryExecHash.end())
            {
            expritr=queryExecHash[tempexpr].begin();

                while(expritr!=queryExecHash[tempexpr].end())
                {
           
                    if(*expritr==itr->first)
                    {
                        flag=false;
                        break;
                    }
                    expritr++;
                }
            }
        if(!flag)
        {
            break;
        }
       
        string t=tempexpr.substr(0,tempexpr.length()-1);;
        tempexpr=t;
            
        }
                if(flag==true)
                {
                    vec.push_back(itr->first);
                   // cout<<"\n Pushing into "<<finalexpr<<" "<<itr->first->left->left->left->value;
                }
            }
        }
        // cout<<"\n end of while loop count:"<<itr->second.size()<<" first:"<<itr->first->left->left->left->value;
        itr++;
    }
    queryExecHash[finalexpr]=vec;
//    cout<<"\nInserted "<<finalexpr<<" into the QueryExecHash!!";
}
void MyOptimizer::Preprocess()
{
     struct TableList *tableList = tables;
     int i=0;
     Statistics *statsPtr = myCatalog->stats;
     while(tableList!=NULL)
        {
            string table = string(tableList->tableName);
            tableAliasHashDS[table]=string(tableList->aliasAs);
            aliasTableHashDS[string(tableList->aliasAs)]=table;

            statsPtr->CopyRel(tableList->tableName,tableList->aliasAs);
            //cout<<"\n Table:"<<tableList->tableName<<" alias:"<<tableList->aliasAs;
            char c = 'A'+i;
            char cstr[2] = {c,'\0'};
            string s(cstr);
            //Assign Ids to Tables;
            idTableHashDS[s] = table;
            tableIdHashDS[table]=s;
            cout<<"\n Table:"<<s<<" alias:"<<table;
            tableList = tableList->next;
            i++;
        }
    // record the no of tables in the query;
    tableCnt = i;
    struct AndList *andList = boolean;
    while(andList!=NULL)
    {        
        struct OrList *orlist=andList->left;
        bool specialOr = false;
        if(orlist->rightOr!=NULL)
        {
         vector<string> orvec;
         while(orlist!=NULL)
            {
            
            ComparisonOp *Op=orlist->left;
            if(Op->left->code==4)
            {
                Split(Op->left->value);
                orvec.push_back(tableIdHashDS[FindTable()]);
            }

            if(Op->right->code==4)
            {
                Split(Op->right->value);
                orvec.push_back(tableIdHashDS[FindTable()]);
            }
            orlist=orlist->rightOr;
            }
         
            sort(orvec.begin(),orvec.end());
            orvec.erase(unique(orvec.begin(),orvec.end()),orvec.end());
            int vecsize = orvec.size();
            if(vecsize>1)
            {
                specialOr = true;
                string str;
                for(i=0;i<vecsize;i++)
                {
                    str = str + orvec.at(i);
                }                
                queryExecSelPipeHash[str].push_back(andList);
               // estSelPipeHash[str].push_back(andList);
                //cout<<"\n Pushed into orHAS for "<<str;
                queryOps[SelectPipeOp] = true;
            }
        }

        
        if(!specialOr)
        {
        vector<string> vec;
        andRelHash[andList] = vec;
        orlist=andList->left;
        while(orlist!=NULL)
        {
            ComparisonOp *Op=orlist->left;
            if(Op->left->code==4)
            {
                Split(Op->left->value);                
                andRelHash[andList].push_back(tableIdHashDS[FindTable()]);
            //cout<<"\n tableId:"<<tableIdHashDS[FindTable()];
            }
            
            if(Op->right->code==4)
            {
                Split(Op->right->value);
                andRelHash[andList].push_back(tableIdHashDS[FindTable()]);
            }

        orlist=orlist->rightOr;
        }
        //if any problem Delete This;
	sort(andRelHash[andList].begin(),andRelHash[andList].end());
        andRelHash[andList].erase(unique(andRelHash[andList].begin(),andRelHash[andList].end()),andRelHash[andList].end());        
        }
        andList=andList->rightAnd;
    }
}

 
void MyQueryRunner::RunQuery()
{
    QueryPlanNode *tree=root;      
    if(!Compiler::runQueryFlag)
    {
        InorderPrint(tree);
    }
    else
    {        
        noofPipes = root->outPipe;                        
        performoperation(root);        
    }
}

void MyQueryRunner::performoperation(QueryPlanNode *node)
{
    struct TableList *tab = tables;
    int tablecount=0;
    while(tab!=NULL)
    {
        tab=tab->next;
        tablecount++;
    }
    //create a Stack;
    QueryPlanNode *treeroot = node;
    Pipe **pipes = new Pipe*[noofPipes+1];
    int x;
    for(int i=0;i<=noofPipes;i++)
    {
            pipes[i] = new Pipe(100);
    }
    DBFile **dbfiles = new DBFile*[tablecount];
    for(int i=0;i<tablecount;i++)
    {
        dbfiles[i]=new DBFile();
    }

    SelectFile **sfileOp = new SelectFile*[tablecount];
    for(int i=0;i<tablecount;i++)
    {
        sfileOp[i]=new SelectFile();
    }

    SelectPipe **spipeOp = new SelectPipe*[tablecount];
    for(int i=0;i<tablecount;i++)
    {
        spipeOp[i]=new SelectPipe();
    }
    int spcount=0;

    Join **jOp = new Join*[tablecount];
    for(int i=0;i<tablecount;i++)
    {
        jOp[i]=new Join();
    }
    int jcount=0;

    Sum *sOp = new Sum();
    Project *pOp = new Project();
    DuplicateRemoval *dOp = new DuplicateRemoval();
    GroupBy *gOp = new GroupBy();

    int currentDBFile=0;
    stack<QueryPlanNode *> postOrderStack;
    stack<QueryPlanNode *> runStack;

    //Post Order Iterative Traversal
    postOrderStack.push(treeroot);
    while(!postOrderStack.empty())
    {
        treeroot = postOrderStack.top();
        postOrderStack.pop();
        runStack.push(treeroot);
        if(treeroot->leftptr!=NULL)
        {
            postOrderStack.push(treeroot->leftptr);
        }
        if(treeroot->rightptr!=NULL)
        {
            postOrderStack.push(treeroot->rightptr);
        }

    }

    //Execution of the Operations
    while(!runStack.empty())
    {    
        treeroot=runStack.top();
        runStack.pop();
        x=treeroot->opType;
       cout<<"\n Optype:"<<treeroot->opType;
        if(x==ProjectOp)
        {         
            pOp->Use_n_Pages(10);
            int inputcnt=treeroot->prp->numAttsInput;
            int outputcnt=treeroot->prp->numAttsOut;
            int *x=treeroot->prp->attsToKeep;
            pOp->Run(*pipes[treeroot->inputPipe1],*pipes[treeroot->outPipe],x,inputcnt,outputcnt);
        }
    
        else if(x==GroupByOp)
        {                
            gOp->Use_n_Pages(10);
            gOp->Run(*pipes[treeroot->inputPipe1],*pipes[treeroot->outPipe],*treeroot->om,*treeroot->func);
        }
        else if(x==SumOp)
        {         
            sOp->Use_n_Pages(10);
            sOp->Run(*pipes[treeroot->inputPipe1],*pipes[treeroot->outPipe],*treeroot->func);
        }
        else if(x==JoinOp)
        {         
            jOp[jcount]->Use_n_Pages(10);
            bool leftflag,rightflag;
            leftflag=rightflag=false;
            int x,y;
            x=y=-2;
            treeroot->cnf->leftrightJoinAtts(&x,&y);
            if(treeroot->leftptr->opType==SelectFileOp)
            {

                char metafile[100];
                string str = binfilespath+treeroot->leftptr->tableName + ".bin.meta";
                cout<<"\n\n Left File:"<<str;
                strcpy(metafile,str.c_str());
                ifstream myfile (metafile);
                OrderMaker *lsortorder = new OrderMaker();
                lsortorder->CreateFromFile(myfile);

                cout<<"\n lsortorder->whichAtts[0]:"<<lsortorder->whichAtts[0]<<" x:"<<x;;
                if(lsortorder->whichAtts[0]==x)
                   leftflag=true;


            }
            if(treeroot->rightptr->opType==SelectFileOp)
            {
                char metafile[100];
                string str = binfilespath+treeroot->rightptr->tableName + ".bin.meta";
                cout<<"\n\n Right file:"<<str;
                strcpy(metafile,str.c_str());
                ifstream myfile (metafile);
                OrderMaker *rsortorder = new OrderMaker();
                rsortorder->CreateFromFile(myfile);
                 cout<<"\n rsortorder->whichAtts[0]:"<<rsortorder->whichAtts[0]<<" y:"<<y<<endl;
                if(rsortorder->whichAtts[0]==y)
                   rightflag=true;
            }
            jOp[jcount]->Run(*pipes[treeroot->inputPipe1],*pipes[treeroot->inputPipe2],*pipes[treeroot->outPipe],*treeroot->cnf,*treeroot->lit,leftflag,rightflag);
            jcount++;
        }
        else if(x==DistinctOp)
        {              
              dOp->Use_n_Pages(10);
              dOp->Run(*pipes[treeroot->inputPipe1],*pipes[treeroot->outPipe],*treeroot->outSchema);

        }
        else if (x==SelectPipeOp)
        {
            spipeOp[spcount]->Use_n_Pages(10);
            spipeOp[spcount]->Run(*pipes[treeroot->inputPipe1],*pipes[treeroot->outPipe],*treeroot->cnf,*treeroot->lit);
            spcount++;
        }
        else if(x==SelectFileOp)
        {
                sfileOp[currentDBFile]->Use_n_Pages(10);
                string path=string(binfilespath);
                string filepath=path+treeroot->tableName+".bin";
                char *f = new char[filepath.length()];
                strcpy(f,filepath.c_str());                
                dbfiles[currentDBFile]->Open(f);
                sfileOp[currentDBFile]->Run(*dbfiles[currentDBFile],*pipes[treeroot->outPipe],*treeroot->cnf,*treeroot->lit);
                currentDBFile++;                
        }
        else if(x==6)
        {

        }
        else if(x==7)
        {

        }
        else if(x==8)
        {

        }
        else if(x==9)
        {

        }
        else
        {
            cout<<"\n Invalid operation";
        }
}
//Print out the records;
    if(Compiler::outFile==NULL)
    {
        Record rec;
        int rowcnt=0;
        while(pipes[noofPipes]->Remove(&rec))
        {
            rec.Print(root->outSchema);
            rowcnt++;
        }
        cout<<"\nRecords returned:"<<rowcnt<<endl;
    }
    else
    {
        WriteOut w;
        FILE *f=fopen(Compiler::outFile,"w");
        w.Run(*pipes[noofPipes],f,*root->outSchema);
        w.WaitUntilDone();
        //fclose(f);

    }
 //remove all resourses;
 for(int i=0;i<=noofPipes;i++)
    {
            delete pipes[i];
    }
    for(int i=0;i<tablecount;i++)
    {
        dbfiles[i]->Close();
        delete dbfiles[i];
    }
    for(int i=0;i<tablecount;i++)
    {
        delete sfileOp[i];
    }
    for(int i=0;i<tablecount;i++)
    {
        delete spipeOp[i];
    }
    for(int i=0;i<tablecount;i++)
    {
        delete jOp[i];
    }
}

void MyQueryRunner::InorderPrint(QueryPlanNode *root)
{
    if(root==NULL)
        return;

   InorderPrint(root->leftptr);   
   
   cout<<"\n\n Operation:"<<enumvals[root->opType];
   cout<<"\n In Pipe 1:"<<root->inputPipe1;
   cout<<"\n In Pipe 2:"<<root->inputPipe2;
   cout<<"\n OutPipe:"<<root->outPipe;
   Attribute *atts=root->outSchema->GetAtts();
   int numatts=root->outSchema->GetNumAtts();
   for(int i=0;i<numatts;i++)
   {
       cout<<"\n "<<atts[i].name<<" "<<types[atts[i].myType];
   }
   if(root->cnf!=NULL)
   {
       cout<<"\n CNF:";
       root->cnf->Print();
   }   
   if(root->om!=NULL)
   {
       cout<<"\n OrderMaker";
       root->om->Print();
   }
   if(root->prp!=NULL)
   {
       cout<<"\n Atts to keep";
       struct NameList *attstokeep=attsToSelect;
       while(attstokeep!=NULL)
       {
           cout<<"\n Att:"<<attstokeep->name;
           attstokeep=attstokeep->next;
       }
   }
   InorderPrint(root->rightptr);
}

