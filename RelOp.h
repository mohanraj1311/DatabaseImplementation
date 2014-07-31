#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#define MIN_RUNLEN 10

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
	private:
	 pthread_t op_thread;
         //Pipe *inPipe;
         Pipe *outPipe;
         CNF *selOp;
         Record *literal;
         DBFile *inFile;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
        static void* doOpHelper(void*);
        void* doOperation();
        void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
        private:
	 pthread_t op_thread;
         Pipe *inPipe;
         Pipe *outPipe;
         CNF *selOp;
         Record *literal;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
        static void* doOpHelper(void*);
        void* doOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp {
        private:
	 pthread_t op_thread;
         Pipe *inPipe;
         Pipe *outPipe;
         int *keepMe;
         int numAttsInput;
         int numAttsOutput;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
        static void* doOpHelper(void*);
        void* doOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp { 
	private:
	 pthread_t op_thread;
         pthread_t lbp_thread;
         pthread_t rbp_thread;
         Pipe *inPipeL;
         Pipe *inPipeR;
         Pipe *outPipe;
         Pipe *lsrtdoutpipe;
         Pipe *rsrtdoutpipe;
         bool BypassSrtLeft;
         bool BypassSrtright;
         CNF *selOp;
         Record *literal;
         int runlen;
        public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal,bool leftflag,bool rightflag);
        static void* doOpHelper(void*);
        void* doOperation();
        void deleteandclear(vector<Record*> &vec);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        static void* LeftBypasserhelper(void *context);
        void* LeftBypasser();
        static void* RightBypasserhelper(void *context);
        void* RightBypasser();
};
class DuplicateRemoval : public RelationalOp {
        private:
	 pthread_t op_thread;
         Pipe *inPipe;
         Pipe *outPipe;
         Schema *mySchema;
         int runlen;
    
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
        static void* doOpHelper(void*);
        void* doOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Sum : public RelationalOp {
	private:
	 pthread_t op_thread;
         Pipe *inPipe;
         Pipe *outPipe;
         Function *computeMe;
        public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
        static void* doOpHelper(void*);
        void* doOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class GroupBy : public RelationalOp {
        private:
	 pthread_t op_thread;
         Pipe *inPipe;
         Pipe *outPipe;
         OrderMaker *groupAtts;
         Function *computeMe;
         int runlen;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
        static void* doOpHelper(void*);
        void* doOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class WriteOut : public RelationalOp {
        private:
	 pthread_t op_thread;
         Pipe *inPipe;
         FILE *outFile;
         Schema *mySchema;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);

        static void* doOpHelper(void*);
        void* doOperation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif
