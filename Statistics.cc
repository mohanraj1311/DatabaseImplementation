#include "Statistics.h"

Statistics::Statistics()
{
}

Statistics::Statistics(Statistics &copyMe)
{
    map<string,TableStats*> *ptr = copyMe.GetDbStats();
    map<string,TableStats*>::iterator itr;
    TableStats *tbptr;
    //Iterate over the CopyMe HashMap and copy it over
    for(itr=ptr->begin();itr!=ptr->end();itr++)
    {        
        tbptr=new TableStats(*itr->second);
        dbStats[itr->first] = tbptr;
    }
}

Statistics::~Statistics()
{
    map<string,TableStats*>::iterator itr;
    TableStats *tb=NULL;
    //Iterate over the HashMap and delete the tablestat objects and then clear the HashMap
    for(itr=dbStats.begin();itr!=dbStats.end();itr++)
    {
        tb = itr->second;
        delete tb;
        tb=NULL;
    }
    dbStats.clear();
}

void Statistics::AddRel(char *relName, int numTuples)
{
 /*Logic:
  If the HashMap contains the relation, update the no of tuples, otherwise add new entry*/
    map<string,TableStats*>::iterator itr;
    TableStats *tbptr;
    itr = dbStats.find(string(relName));
    if(itr!=dbStats.end())
    {
        dbStats[string(relName)]->UpdateData(numTuples);
        dbStats[string(relName)]->SetGroupDetails(relName,1);
    }
    else
    {
        tbptr= new TableStats(numTuples,string(relName));
        dbStats[string(relName)]=tbptr;
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
/*Logic:
  If the HashMap contains the relation, update the attribs in TableStats, otherwise report error*/
  map<string,TableStats*>::iterator itr;
  itr = dbStats.find(string(relName));
  if(itr!=dbStats.end())
  {
      dbStats[string(relName)]->UpdateData(string(attName),numDistincts);
  }  
}

#ifdef debug
void Statistics::printRelsAtts()
{
    map<string,TableStats*>::iterator relitr=dbStats.begin();
    for(;relitr!=dbStats.end();relitr++)
    {
        cout<<"\n"<<relitr->first<<" "<<relitr->second->GetTupleCount()<<" "<<relitr->second->GetGrpName();
        map<string,int>::iterator tableitr=relitr->second->GetTableAtts()->begin();
        for(;tableitr!=relitr->second->GetTableAtts()->end();tableitr++)
        {
            cout<<"\n"<<tableitr->first<<":"<<tableitr->second;
        }
    }
}
#endif

void Statistics::CopyRel(char *oldName, char *newName)
{
  /*Logic:
  If the HashMap contains the old relation, copy it over to the new Relation and insert new relation into dbStats
  Else report error*/
  string oldRel=string(oldName);
  string newRel=string(newName);
  if(strcmp(oldName,newName)==0)  return;

  map<string,TableStats*>::iterator itr2;
  itr2 = dbStats.find(newRel);
  if(itr2!=dbStats.end())
  {
      delete itr2->second;
      string temp=itr2->first;
      itr2++;
      dbStats.erase(temp);
      
  }

  map<string,TableStats*>::iterator itr;

  itr = dbStats.find(oldRel);
  TableStats *tbptr;
  
  if(itr!=dbStats.end())
  {
      TableStats* newTable=new TableStats(dbStats[string(oldName)]->GetTupleCount(),newRel);
      tbptr=dbStats[oldRel];
      map<string,int>::iterator tableiter=tbptr->GetTableAtts()->begin();
      for(;tableiter!=tbptr->GetTableAtts()->end();tableiter++)
      {
          string temp=newRel+"."+tableiter->first;
          newTable->UpdateData(temp,tableiter->second);
      }
      dbStats[string(newName)] = newTable;
  }
  else
  {
      cout<<"\n Class:Statistics Method:CopyRel Msg: invalid relation name:"<<oldName<<endl;
      exit(1);
  }
}

void Statistics::Read(char *fromWhere)
{
     /*Logic:
      Open the File, Fill int the inmemory HashMaps , by scanning from BEGIN to END for
      each Relation

      If the File doesnt exist or if file is empty quit
     */
    FILE *fptr=NULL;
    fptr = fopen(fromWhere,"r");
    char strRead[200];    
    while(fptr!=NULL && fscanf(fptr,"%s",strRead)!=EOF)
    {
        if(strcmp(strRead,"BEGIN")==0)
        {
            int tuplecnt=0;
            char relname[200];
            int grpcnt=0;
            char groupname[200];
            fscanf(fptr,"%s %ld %s %d",relname,&tuplecnt,groupname,&grpcnt);                   
            AddRel(relname,tuplecnt);
            dbStats[string(relname)]->SetGroupDetails(groupname,grpcnt);
            char attname[200];
            int distcnt=0;
            fscanf(fptr,"%s",attname);            
            while(strcmp(attname,"END")!=0)
            {
                fscanf(fptr,"%d",&distcnt);
                AddAtt(relname,attname,distcnt);                
                fscanf(fptr,"%s",attname);                                                                
            }                                                
        }                
    }
}

void Statistics::Write(char *fromWhere)
{
    /*Logic:
     Iterate over the dBStats HashMaps, for each entry(relation) iterate over
     attribs HashMaps to print the numOfTuples, and numOfDistinctValues respectivily
     */
    
     map<string,TableStats*>::iterator dbitr;
     map<string,int>::iterator tbitr;
     map<string,int> *attrptr;

     FILE *fptr;
     fptr = fopen(fromWhere,"w");
     dbitr = dbStats.begin();
     
     for(;dbitr!=dbStats.end();dbitr++)
     {
         fprintf(fptr,"BEGIN\n");
         fprintf(fptr,"%s %ld %s %d\n",dbitr->first.c_str(),dbitr->second->GetTupleCount(),dbitr->second->GetGrpName().c_str(),dbitr->second->GetGrpSize());
         attrptr = dbitr->second->GetTableAtts();
         tbitr = attrptr->begin();
         
         for(;tbitr!=attrptr->end();tbitr++)
         {
            fprintf(fptr,"%s %d\n",tbitr->first.c_str(),tbitr->second);
         }         
         fprintf(fptr,"END\n");      
     }
     fclose(fptr);     
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
 /*
  Call Estimate , round the result and store in the statistics object;
  */
  double r = Estimate(parseTree,relNames,numToJoin);

  long result =(long)((r-floor(r))>=0.5?ceil(r):floor(r));
  string grpName="";
  int grpSize = numToJoin;
  for(int i=0;i<grpSize;i++)
  {
      grpName = grpName + "," + relNames[i];
  }
  map<string,TableStats*>::iterator itr = dbStats.begin();
  for(int i=0;i<numToJoin;i++)
  {
      dbStats[relNames[i]]->SetGroupDetails(grpName,grpSize);
      dbStats[relNames[i]]->UpdateData(result);
  }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    /* Logic:
     * Check validity of input parameters
     * Loop throught the AND LIST.
     * For each node of AND LIST which is OR LIST evaluate the Selectivity.
     * Do Product of all Selectivities and multiply result with the Tuple product of relations
     */
    
    double estTuples=1;
    map<string,long> uniqvallist;
    if(!ErrorCheck(parseTree,relNames,numToJoin,uniqvallist))
    {
      cout<<"\nClass:Statistics Method:Estimate Msg:Input Parameters invalid for Estimation";
      return -1.0;
  }
  else
  {
      string grpName="";
      map<string,long>::iterator tupitr;
      map<string,long> tuplevals;
      int grpSize = numToJoin;
      for(int i=0;i<grpSize;i++)
      {
          grpName = grpName + "," + relNames[i];
      }
      for(int i=0;i<numToJoin;i++)
      {
          tuplevals[dbStats[relNames[i]]->GetGrpName()]=dbStats[relNames[i]]->GetTupleCount();
      }
      
      estTuples = 1000.0; //Safety purpose so that we dont go out of Double precision
      /*long long int tupleproduct=1;
      for(;tupitr!=tuplevals.end();tupitr++)
      {
          tupleproduct*=tupitr->second;
      }*/
     while(parseTree!=NULL)
     {
         estTuples*=Evaluate(parseTree->left,uniqvallist);
         parseTree=parseTree->rightAnd;
     }
      tupitr=tuplevals.begin();
      for(;tupitr!=tuplevals.end();tupitr++)
      {
          estTuples*=tupitr->second;
      }
      //estTuples = estTuples*tupleproduct;
  }
    estTuples = estTuples/1000.0; //Safety purpose so that we dont go out of Double precision-revert
    return estTuples;
}

double Statistics::Evaluate(struct OrList *orList, map<string,long> &uniqvallist)
{
    /*Logic:
     * Rules:
     * </> 1/3
     * = 1/Max(noOfDistinct(R1,A1),noOfDistinct(R2,A2)) , where A1,A2 are join attribs
     */
    struct ComparisonOp *comp;
    map<string,double> attribSelectivity;

    while(orList!=NULL)
    {
        comp=orList->left;
        string key = string(comp->left->value);
        if(attribSelectivity.find(key)==attribSelectivity.end())
        {
            attribSelectivity[key]=0.0;
        }
        if(comp->code==1 || comp->code==2)
        {
            attribSelectivity[key] = attribSelectivity[key]+1.0/3;
        }
        else
        {
            string ulkey = string(comp->left->value);            
            long max=uniqvallist[ulkey];
            if(comp->right->code==4)
            {
               string urkey = string(comp->right->value);
               if(max<uniqvallist[urkey])
                   max = uniqvallist[urkey];
            }
            attribSelectivity[key] =attribSelectivity[key] + 1.0/max;
        }
        orList=orList->rightOr;
    }

    double selectivity=1.0;
    map<string,double>::iterator itr = attribSelectivity.begin();
    for(;itr!=attribSelectivity.end();itr++)
        selectivity*=(1.0-itr->second);
   // cout<<"\n selectivity"<<1.0-selectivity;
    return (1.0-selectivity);
}

bool Statistics::ErrorCheck(struct AndList *parseTree, char *relNames[], int numToJoin,map<string,long> &uniqvallist)
{
    /*
     Logic:
     * 1.Check if all the attributes in parse tree are part of the
     * relations in RelNames.
     * 2.Check if the all Relnames of partitions used, are tin relNames.
     */
  bool result=true;
  while(parseTree!=NULL && result)
  {
      struct OrList *head=parseTree->left;
      while(head!=NULL && result)
      {
          struct ComparisonOp *ptr = head->left;
          if(ptr->left->code==4 && ptr->code==3 && !ContainsAttrib(ptr->left->value,relNames,numToJoin,uniqvallist))
          {
              cout<<"\n"<< ptr->left->value<<" Does Not Exist";
              result=false;
          }          
         if(ptr->right->code==4 && ptr->code==3 && !ContainsAttrib(ptr->right->value,relNames,numToJoin,uniqvallist))
              result=false;
       head=head->rightOr;
      }
      parseTree=parseTree->rightAnd;
  }
  if(!result) return result;

  map<string,int> tmpTable;
  for(int i=0;i<numToJoin;i++)
  {
      string grpname = dbStats[string(relNames[i])]->GetGrpName();
      if(tmpTable.find(grpname)!=tmpTable.end())
          tmpTable[grpname]--;
      else
          tmpTable[grpname] = dbStats[string(relNames[i])]->GetGrpSize()-1;
  }

  map<string,int>::iterator tmpTableItr = tmpTable.begin();
  for(;tmpTableItr!=tmpTable.end();tmpTableItr++)
      if(tmpTableItr->second!=0)
         {
          result=false;
          break;
        }
  return result;
}

bool Statistics::ContainsAttrib(char *value,char *relNames[], int numToJoin,map<string,long> &uniqvallist)
{
    int i=0;    
    while(i<numToJoin)
    {
    map<string,TableStats*>::iterator itr=dbStats.find(relNames[i]);    
    if(itr!=dbStats.end())
     {
        string key = string(value);
        if(itr->second->GetTableAtts()->find(key)!=itr->second->GetTableAtts()->end())
        {
            uniqvallist[key]=itr->second->GetTableAtts()->find(key)->second;
            return true;
        }
     }
    else
        return false;
    i++;
    }
    return false;
}

