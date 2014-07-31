#include <iostream>
#include "Compiler.h"

using namespace std;
CatalogSingleton * CatalogSingleton::obj = NULL;
//bool Compiler::DDLQueryFlag=false;
char* Compiler::outFile=NULL;
bool Compiler::runQueryFlag=true;
int main(int argc, char *argv[]) {

    CatalogSingleton* myCatalog=CatalogSingleton::getInstance();
    while(true)
    {
        cout<<"\nSql>";
    clock_t start;
  double diff;
  start = clock();


    ParserInterface *_PI = new MyParser(myCatalog);
    OptimizerInterface *_OI = new MyOptimizer(myCatalog) ;
    QueryRunnerInterface *_QRI = new MyQueryRunner();
    Compiler *qCompiler = new Compiler(_PI,_OI,_QRI,myCatalog);
    qCompiler->Compile();    
    delete _PI;
    delete _OI;
    delete _QRI;
    delete qCompiler;
    diff = ( clock() - start ) / (double)CLOCKS_PER_SEC;
    cout<<"execution time: "<< diff <<'\n';
    cout<<"\n\n";
    }
    return 0;
    
}