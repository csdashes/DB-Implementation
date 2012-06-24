#define _CRT_SECURE_NO_WARNINGS

#include "STORM.h"
#include "REM.h"
#include "INXM.h"
#include "SYSM.h"
#include "SSQLM.h"
#include <iostream>

void testStorm() {
	
	STORM_StorageManager mgr;
	STORM_FileHandle fh;
	t_rc rc;
	int nAllocPages, nResPages;
	STORM_PageHandle ph;
	char *data;
	int val;
	int pid;
	int nreqs, nreads, nwrites, npinned, nframes;
	
	// ====================== STEP 1 =======================================//
	// Create the file. 
	// Open the file.
	// Reserve 100 pages.
	// Store something in each page.
	// Close the file.
	//======================================================================//
	
	rc = mgr.CreateFile("test.dat");
	if (rc != OK) {DisplayReturnCode(rc); Pause(); exit(-1);}
	
	rc = mgr.OpenFile("test.dat", fh);
	if (rc != OK) {DisplayReturnCode(rc); Pause(); exit(-1);}
	
	nAllocPages = fh.GetNumAllocatedPages();
	nResPages = fh.GetNumReservedPages();
	
	for (int i=1; i<=100; i++)
	{
		rc = fh.ReservePage(ph);
		if (rc != OK) {DisplayReturnCode(rc); Pause(); exit(-1);}
		
		// Copy something to the page.
		rc = ph.GetDataPtr(&data);
		memcpy(data, &i, sizeof(int));  
		
		// Mark the page as dirty.
		rc = ph.GetPageID(pid);
		if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
		
		rc = fh.MarkPageDirty(pid);
		if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
		
		// Unpin the page
		rc = fh.UnpinPage(pid);
		if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
	}
	
	mgr.GetStats(nreqs, nreads, nwrites, npinned, nframes);
	printf("reqs: %d, reads: %d, writes: %d, pinned: %d, frames: %d\n", nreqs, nreads, nwrites, npinned, nframes);
	printf("allocated pages: %d, reserved pages: %d\n", fh.GetNumAllocatedPages(), fh.GetNumReservedPages());
	
	rc = mgr.CloseFile(fh);
	if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
	
	Pause();
	
	// ====================== STEP 2 =======================================//
	// Open the file again. 
	// Read every page of the file and print the page contents.
	// Release the first 50 pages.
	// Close the file.
	//======================================================================//
	
	rc = mgr.OpenFile("test.dat", fh);
	if (rc != OK){DisplayReturnCode(rc); exit(-1);}
	
	printf("allocated pages: %d, reserved pages: %d\n", fh.GetNumAllocatedPages(), fh.GetNumReservedPages());
	
	// Display page contents.
	while (fh.GetNextPage(ph) != STORM_EOF)
	{
		rc = ph.GetDataPtr(&data);
		if (rc != OK) {DisplayReturnCode(rc);exit(-1);}
		memcpy(&val, data, sizeof(int));
		
		ph.GetPageID(pid);
		printf("contents of page %d = %d\n", pid, val);
		
		// Unpin the page
		rc = fh.UnpinPage(pid);
		if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
	}
	
	// Release pages from 1 to 50.
	for (int p=1; p<=50; p++)
	{
		rc = fh.ReleasePage(p);
		if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
	}
	
	printf("allocated pages: %d, reserved pages: %d\n", fh.GetNumAllocatedPages(), fh.GetNumReservedPages());
	
	rc = mgr.CloseFile(fh);
	if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
	
	Pause();
	
	// ====================== STEP 3 =======================================//
	// Open the file again. 
	// Read every page of the file and print the page contents.
	// Close the file.
	// Finally, destroy the file.
	//
	// We expect to see only 50 pages staring from 51 up to 100, since the
	// first 50 have been released in the previous step.
	//======================================================================//
	
	rc = mgr.OpenFile("test.dat", fh);
	if (rc != OK){DisplayReturnCode(rc); exit(-1);}
	
	// Display page contents.
	while (fh.GetNextPage(ph) != STORM_EOF)
	{
		rc = ph.GetDataPtr(&data);
		if (rc != OK) {DisplayReturnCode(rc);exit(-1);}
		memcpy(&val, data, sizeof(int));
		
		ph.GetPageID(pid);
		printf("contents of page %d = %d\n", pid, val);
		
		// Unpin the page
		fh.UnpinPage(pid);
	}
	
	printf("allocated pages: %d, reserved pages: %d\n", fh.GetNumAllocatedPages(), fh.GetNumReservedPages());
	
	rc = mgr.CloseFile(fh);
	if (rc != OK) {DisplayReturnCode(rc); exit(-1);}
	
	Pause();
	
	rc = mgr.DestroyFile("test.dat");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}
	
}

void testREM() {
	STORM_StorageManager mgr;
	STORM_FileHandle fh;
	t_rc rc;
	STORM_PageHandle ph;

	
	
	
	REM_RecordFileManager *rmgr = new REM_RecordFileManager(&mgr);
	
	REM_RecordFileHandle rfh;
	
	rc = rmgr->CreateRecordFile("test.dat", 40);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = rmgr->OpenRecordFile("test.dat", rfh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	REM_RecordID rid1, rid2, rid3;
	int pageID, slot;
	
	//================================================================================	
	rc = rfh.InsertRecord("check my list yo", rid1);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = rid1.GetPageID(pageID);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = rid1.GetSlot(slot);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	printf("Page ID = %d,  Slot = %d\n", pageID, slot);
	
	rc = rfh.InsertRecord("check my list yo 2", rid2);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
		
	rc = rid2.GetPageID(pageID);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = rid2.GetSlot(slot);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	printf("Page ID = %d,  Slot = %d\n", pageID, slot);

	rc = rfh.InsertRecord("check my list yo 3", rid3);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
		
	rc = rid3.GetPageID(pageID);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = rid3.GetSlot(slot);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	printf("Page ID = %d,  Slot = %d\n", pageID, slot);
	//================================================================================	
	//================================================================================	
	REM_RecordHandle rh;
	char *pData;
	
	rc = rfh.ReadRecord(rid1, rh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rh.GetData(pData);
	printf("This is data : %s ||| from rid %d\n", pData, 1);

	rc = rfh.ReadRecord(rid2, rh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rh.GetData(pData);
	printf("This is data : %s ||| from rid %d\n", pData, 2);

	rc = rfh.ReadRecord(rid3, rh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rh.GetData(pData);
	printf("This is data : %s ||| from rid %d\n", pData, 3);	
	
	//================================================================================	
	//================================================================================	
	
	REM_RecordFileScan fs;
	
	
	rc = fs.OpenRecordScan(rfh, TYPE_STRING, 18, 0, EQ_OP, (char*)"check my list yo 2");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = fs.GetNextRecord(rh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rh.GetData(pData);
	printf("This is data : %s ||| from File Scan \n", pData);
	
	rc = fs.GetNextRecord(rh);
	if (rc != OK) {DisplayReturnCode(rc);}

	rh.GetData(pData);
	printf("This is data : %s ||| from File Scan 2\n", pData);

	
	//================================================================================	
	//================================================================================	
	
	rfh.DeleteRecord(rid1);
	
	rc = rfh.ReadRecord(rid1, rh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rh.GetData(pData);
	printf("This is data : %s ||| from rid %d\n", pData, 1);
	//================================================================================	
	//================================================================================	
	memcpy(pData, "koka kolas!", 40);
	
	rfh.UpdateRecord(rh);
	
	rc = rfh.ReadRecord(rid1, rh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rh.GetData(pData);
	printf("This is data : %s ||| from rid %d\n", pData, 1);	
	//================================================================================	
	
	rc = rmgr->CloseRecordFile(rfh);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = rmgr->DestroyRecordFile("test.dat");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
}

void testINXM() {
	STORM_StorageManager mgr;
	STORM_FileHandle fh;
	t_rc rc;
	STORM_PageHandle ph;
	
	
	
	
	INXM_IndexManager *imgr = new INXM_IndexManager(&mgr);
	
	INXM_IndexHandle ih;
	
	rc = imgr->CreateIndex("test.dat", 1, TYPE_INT, 4);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = imgr->OpenIndex("test.dat", 1, ih);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	REM_RecordID rid1, rid2, rid3;
	rid1.SetPageID(1);
	rid1.SetSlot(1);
	rid2.SetPageID(2);
	rid2.SetSlot(2);
	rid3.SetPageID(3);
	rid3.SetSlot(3);
	
	//================================================================================	
	
	int *key1 = new int(1);
	int *key2 = new int(4);
	int *key3 = new int(6);
	int *key4 = new int(10);
	
	rc = ih.InsertEntry(key1, rid1);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = ih.InsertEntry(key2, rid2);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = ih.InsertEntry(key3, rid3);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = ih.InsertEntry(key4, rid1);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = ih.InsertEntry(key4, rid2);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}		
	
	rc = ih.InsertEntry(key4, rid3);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}		
	
//	for (int i=0; i<1000; i++) {
//		rc = ih.InsertEntry(key4, rid3);
//		if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
//	}
	
	ih.debug();
	
	//================================================================================	
	
	rc = imgr->CloseIndex(ih);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
	rc = imgr->DestroyIndex("test.dat", 1);
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	
}


void testSYSM() {

	STORM_StorageManager mgr;
	REM_RecordFileManager *rfm = new REM_RecordFileManager(&mgr);

	SYSM_DatabaseManager *sdm = new SYSM_DatabaseManager(rfm);

	// Create the folder "testingDB" which contains the files rel.met and attr.met
	//t_rc rc = sdm->CreateDatabase("testingDB");
	//if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	//// Open the database to use it
	//rc = sdm->OpenDatabase("testingDB");
	//if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	t_rc rc = sdm->DropDatabase("testingDB");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	//delete sdm;
	//delete rfm;
}

void testSSQLM() {

	STORM_StorageManager *mgr = new STORM_StorageManager();
	REM_RecordFileManager *rfm = new REM_RecordFileManager(mgr);

	SYSM_DatabaseManager *sdm = new SYSM_DatabaseManager(rfm);

	INXM_IndexManager *im = new INXM_IndexManager(mgr);
	SSQLM_DDL_Manager *ddlm = new SSQLM_DDL_Manager(rfm,im,"testingDB");

	// Open the database to use it
	t_rc rc = sdm->OpenDatabase("testingDB");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	//Get the name of the active db
	char dbName[50];
	strcpy(dbName,sdm->getdbName());

	// Create a table with name "table1"
	rc = ddlm->CreateTable("table1","id INT, name CHAR(15), age INT");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = ddlm->CreateTable("table2","age INT, born INT");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}

	rc = ddlm->CreateTable("table3","born INT, decade INT");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}

	// Create index
	rc = ddlm->CreateIndex("table1","name");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = ddlm->CreateIndex("table1","age");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = ddlm->CreateIndex("table2","age");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = ddlm->CreateIndex("table3","born");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	



	// Drop table
	//rc = ddlm->DropTable("table1");
	//if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	// TEST IF DELETE REALLY WORKS
	//rc = ddlm->PrintFirstRecordInAttrMet("table2");
	//if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	
	// Drop index
	//rc = ddlm->DropIndex("table1","name",1);
	//if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	delete ddlm;
	delete im;
	delete sdm;
	delete rfm;
	delete mgr;

	STORM_StorageManager *mgr2 = new STORM_StorageManager();
	REM_RecordFileManager *rfm2 = new REM_RecordFileManager(mgr2);

	SYSM_DatabaseManager *sdm2 = new SYSM_DatabaseManager(rfm2);

	INXM_IndexManager *im2 = new INXM_IndexManager(mgr2);


	//test DML part

	SSQLM_DML_Manager *dmlm = new SSQLM_DML_Manager(rfm2,im2,"testingDB");

	rc = dmlm->Insert("table1","1___ILIAS__________22__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}

	rc = dmlm->Insert("table1","2___TASOS__________25__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table1","3___ELENH__________30__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table1","4___PETROS_________18__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table1","5___PAYLOS_________22__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}



	rc = dmlm->Insert("table2","22__1990");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table2","25__1987");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table2","30__1980");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table2","18__1994");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	


	
	rc = dmlm->Insert("table3","199090__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table3","198780__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table3","198080__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = dmlm->Insert("table3","199490__");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}


	//delete dmlm;
	//delete im2;
	//delete sdm2;
	//delete rfm2;
	//delete mgr2;

	STORM_StorageManager *mgr3 = new STORM_StorageManager();
	REM_RecordFileManager *rfm3 = new REM_RecordFileManager(mgr3);

	SYSM_DatabaseManager *sdm3 = new SYSM_DatabaseManager(rfm3);
	STORM_StorageManager *mgr4 = new STORM_StorageManager();
	INXM_IndexManager *im3 = new INXM_IndexManager(mgr4);

	SSQLM_DML_Manager *dmlm3 = new SSQLM_DML_Manager(rfm3,im3,"testingDB");
	
	REM_RecordID rids[50];
	int slott;
	vector <char*> finalResultRecords3;
	vector <REM_RecordID> finalResultRIDs3;

	dmlm3->Where("table1","id>0",&finalResultRecords3,&finalResultRIDs3);			//************************************
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}									//**	Show all records
																					//**
	cout<<"ALL RECORDS IN THE TABLE:"<<endl;										//**	(SELECT *
	for(int interator = 0; interator < (int)finalResultRecords3.size(); interator++){//**		FROM table1
		finalResultRIDs3[interator].GetSlot(slott);									//**		WHERE id>0)
		cout<<slott<<" -> "<<finalResultRecords3[interator]<<endl;					//**
	}																				//*************************************
	cout<<endl;

	int until;																		//*************************************
	do {																			//**	TEST DELETE
		vector <char*> finalResultRecords2;											//**
		vector <REM_RecordID> finalResultRIDs2;										//**
																					//**	(DELETE
		dmlm3->Where("table1","age>25",&finalResultRecords2,&finalResultRIDs2);		//**		FROM	table1
		if (rc != OK) {DisplayReturnCode(rc);exit(-1);}								//**		WHERE	age > 25)
																					//**
		if(finalResultRecords2.size()!=0){											//**
			dmlm3->Delete("table1",finalResultRIDs2[0],finalResultRecords2[0]);		//**
			if (rc != OK) {DisplayReturnCode(rc);exit(-1);}							//**
		}																			//**
		until = finalResultRecords2.size();											//**	
	}while(until != 0);																//**
																					//**
	vector <char*> finalResultRecords;												//**
	vector <REM_RecordID> finalResultRIDs;											//**
																					//**
	dmlm3->Where("table1","id>0",&finalResultRecords,&finalResultRIDs);				//**
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}									//**
																					//**
	cout<<"TEST DELETE. ALL RECORDS IN THE TABLE (DELETE WHERE age > 25):"<<endl;	//**	Show all records after delete
	for(int interator = 0; interator < (int)finalResultRecords.size(); interator++){//**
		finalResultRIDs[interator].GetSlot(slott);									//**
		cout<<slott<<" -> "<<finalResultRecords[interator]<<endl;					//**
	}																				//**************************************
	cout<<endl;

	vector <char*> finalResultRecordsSelect;										//**************************************
	vector <char*> columns;															//**	TEST SELECT
	columns.push_back("name");														//**	kanw push ta columns pou 8elw na krathsw
	columns.push_back("age");														//**
	//columns.push_back("*");														//**	(SELECT name, age
																					//**		FROM table1
	dmlm3->Select("table1",columns,finalResultRecords,&finalResultRecordsSelect);	//**		WHERE id > 0)
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}									//**
																					//**
	cout<<"TEST SELECT. RECORDS CHOPPED BY SELECT( name, age ):"<<endl;				//**
	for(int interator = 0; interator < (int)finalResultRecordsSelect.size(); interator++){//**
		cout<<finalResultRecordsSelect[interator]<<endl;							//**
	}																				//**************************************
	cout<<endl;

	vector <char*> finalResultRecords4;												//**************************************
	vector <REM_RecordID> finalResultRIDs4;											//**	TEST UPDATE
																					//**
	dmlm3->Where("table1","id<2",&finalResultRecords4,&finalResultRIDs4);			//**		
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}									//**	(UPDATE table1
																					//**		set age = 30
	dmlm3->Update("table1",finalResultRIDs4,"age=30__");							//**		WHERE id < 2)
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}									//**
																					//**
	cout<<"TEST UPDATE. RECORDS AFTER UPDATE set age=30 WHERE id<2:"<<endl;			//**
	for(int interator = 0; interator < (int)finalResultRecords4.size(); interator++){//**
		cout<<finalResultRecords4[interator]<<endl;									//**
	}																				//**
	cout<<endl;																		//**************************************

	cout<<"TEST JOIN. table1, table2 - age:"<<endl;
	rc = dmlm3->Join("table1","table2","age");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}

	//rc = dmlm3->Join("tempTableForJoin","table3","born");
	//if (rc != OK) {DisplayReturnCode(rc);exit(-1);}

	system("pause");
}

int main()	{
	
//	testStorm();

//	testREM();
	
//	testINXM();

	testSYSM();

//	testSSQLM();

	return (0);
}