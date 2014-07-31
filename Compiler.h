#ifndef COMPILER_H
#define	COMPILER_H

#include "CompilerInterfaces.h"
#include "Statistics.h"
#include <algorithm>
#include <iostream>
#include <stack>
#include <stdio.h>
#include <map>
#include <string>
#include <vector>
#include "Statistics.h"
#define QOPSSIZE 10
#include "Defs.h"
#include "Schema.h"
#include "Function.h"
#include "ParseTree.h"
#include "Comparison.h"
#include "Pipe.h"
#include "RelOp.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern	struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern	struct TableList *tables; // the list of tables and aliases in the query
extern	struct AndList *boolean; // the predicate in the WHERE clause
extern	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern char* droptablename; //Which table to drop
extern char* outputfile; // which file to write into
extern int outputmode; //op mode
extern struct Insert *insertinto; // contains insert paramters
extern struct CreateTable *createTable;;//stores the attributes and its types if Create Table Command is called or NULL
extern bool DDL;


enum QueryOps { ProjectOp, GroupByOp, SumOp, JoinOp,DistinctOp,SelectFileOp,SelectPipeOp,CreateOp,DropOp,InsertOp,SetOp};

class ProjectParams
{
public:
    int *attsToKeep;
    int numAttsInput;
    int numAttsOut;
 ProjectParams(int *a,int ip,int op)
 {
     attsToKeep=a;
     numAttsInput = ip;
     numAttsOut = op;
 }
};
class QueryExecStatsNode
{
public:
    string expr;
    double estimateTuples;
    double costEstimate;
    QueryExecStatsNode(string ex,double tplcnt,double cst)
    {
        expr = ex;
        estimateTuples = tplcnt;
        costEstimate=cst;
    }
};
class CatalogTables
{

public:
    CatalogTables *next;
    string tableName;
    CatalogTables(string tbName)
    {
        tableName = tbName;
        next=NULL;
    }

};
//Query Plan Tree Structure;
class QueryPlanNode
{
    //data part
public:
    int inputPipe1;
    int inputPipe2;
    int outPipe;
    Schema *outSchema;
    CNF *cnf;
    QueryOps opType;
    string tableName;
    string aliasName;
    Record *lit;
    Function *func;
    OrderMaker *om;
    QueryPlanNode *leftptr;
    QueryPlanNode *rightptr;
    ProjectParams *prp;

    QueryPlanNode(QueryOps op,Schema *s,CNF *c,Function *f,OrderMaker *o,ProjectParams *pp,Record *l,string tabName,string alName)
    {
        prp=pp;
        lit = l;
        func=f;
        om=o;
        outSchema=s;
        cnf=c;
        tableName = tabName;
        aliasName = alName;
        leftptr=NULL;
        rightptr=NULL;
        inputPipe1=-1;
        inputPipe2=-1;
        outPipe=-1;
        opType=op;
    }
};

class CatAttribs
{
public:
    string attrib;
    string type;
CatAttribs(string a,string t)
    {
    attrib=a;
    type=t;
    }
};

class CatalogSingleton
{

private:
    static CatalogSingleton *obj;
    CatalogSingleton()
        {
            initializeCatalogDS();
            stats = new Statistics();
            stats->Read("Statistics.txt");
        }
    

   public:
    map<string,CatalogTables*> catalogAttribHash;
    map<string,string> catalogTableHash;
    map<string,vector<CatAttribs*> > catalogTableAttribHash;
    Statistics *stats;
    static CatalogSingleton* getInstance();
    void  initializeCatalogDS();
};
class MyOptimizer:public OptimizerInterface
{
    
    bool *queryOps;
    int size;
    int tableCnt;
    string resultJoinExpr;
    map<struct AndList*,vector<string> > andRelHash;
    map<string,string> tableAliasHashDS;
    map<string,string> aliasTableHashDS;
    map<string,string> idTableHashDS;
    map<string,string> tableIdHashDS;
    map<string,vector<struct AndList *> > queryExecHash;
    map<string,vector<struct AndList *> > queryExecSelPipeHash;
    //map<string,vector<struct AndList *> > estSelPipeHash;
    map<string,QueryExecStatsNode*> queryEstimateHash;
    CatalogSingleton *myCatalog;
    string alias;
    string attr;
    QueryPlanNode **qproot;
public:
    MyOptimizer(CatalogSingleton *myCat)
    {
        myCatalog=myCat;
    }
    void Preprocess();
    void Split(string str);
    string FindTable();
    bool isAnagram(string first,string second);
    void InsertAnd(string expr,string newtabl,vector<struct AndList *> &vec);
    string getAnagram(string newrelList);
    void delFrmEstHash(int len);
    Schema *ConstructJSchema(Schema *s1,Schema *s2);
    struct AndList* createAndList(string str);
    virtual void DetermineJoinOrder();
    virtual void GenerateQueryPlan();
    void ChangeLists();
    void ChangeFunctionList(struct FuncOperator *func );
    void CreateGroupByOm(OrderMaker *o,Schema *s);

    virtual void SetQueryOps(bool *a,int s,QueryPlanNode ** x)
    {
        queryOps = a;
        size = s;
        qproot=x;
    }    
};

class MyQueryRunner:public QueryRunnerInterface
{
    QueryPlanNode *root;    
    int noofPipes;        

public:
    MyQueryRunner()
    {
        //db = NULL;
    }
    virtual void RunQuery();
    virtual void setroot(QueryPlanNode *_root)
    {
        root=_root;
    }
    void InorderPrint(QueryPlanNode *root);
    void performoperation(QueryPlanNode *node);    
    void clearpipe(QueryPlanNode *r);
};





class MyParser:public ParserInterface
{
    CatalogSingleton *myCatalog;
    map<string,string> tableAliasHashDS;
    map<string,string> aliasTableHashDS;    
    string alias;
    string attr;
public:
    MyParser(CatalogSingleton *myCat)
    {
        myCatalog=myCat;
    }
    virtual bool ParseQuery();
    virtual bool ErrorCheckQuery();    
    void Split(string str);
    bool isAttValid(string attr);
};

class Compiler
{
    ParserInterface *pi;
    OptimizerInterface *oi;
    QueryRunnerInterface *qi;
    CatalogSingleton *myCatalog;
    bool queryOperations[QOPSSIZE];        
    QueryPlanNode *root;
    
    
    
public:    
    static bool runQueryFlag;
    static char *outFile;
    
    Compiler(ParserInterface *_pi, OptimizerInterface *_oi,QueryRunnerInterface *_qi,CatalogSingleton *_myCatalog):pi(_pi),oi(_oi),qi(_qi),myCatalog(_myCatalog)
    {
                  
    }  
    bool Parse();
    void Optimze();
    void Runquery();
    void Compile();
    void RunDDLquery();   
};


#endif	/* COMPILER_H */

