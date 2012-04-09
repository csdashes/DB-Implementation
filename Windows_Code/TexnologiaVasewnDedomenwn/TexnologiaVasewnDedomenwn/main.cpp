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
	t_rc rc = sdm->CreateDatabase("testingDB");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	// Open the database to use it
	rc = sdm->OpenDatabase("testingDB");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	
}

void testSSQLM() {

	STORM_StorageManager mgr;
	REM_RecordFileManager *rfm = new REM_RecordFileManager(&mgr);

	SYSM_DatabaseManager *sdm = new SYSM_DatabaseManager(rfm);

	INXM_IndexManager *im = new INXM_IndexManager(&mgr);
	SSQLM_DDL_Manager *ddlm = new SSQLM_DDL_Manager(rfm,im);

	// Open the database to use it
	t_rc rc = sdm->OpenDatabase("testingDB");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	//Get the name of the active db
	char dbName[50];
	strcpy(dbName,sdm->getdbName());

	// Create a table with name "table1"
	rc = ddlm->CreateTable(dbName,"table1","id INT, name CHAR(15), age INT");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	// Create index
	rc = ddlm->CreateIndex(dbName,"table1","name");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	rc = ddlm->CreateIndex(dbName,"table1","age");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	// Drop table
//	rc = ddlm->DropTable(dbName,"table1");
//	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	// Drop index
//	rc = ddlm->DropIndex(dbName,"table1",1);
//	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	

	//test DML part

	SSQLM_DML_Manager *dmlm = new SSQLM_DML_Manager(rfm,im);

	dmlm->Insert("testingDB","table1","5ILIAS22");
	if (rc != OK) {DisplayReturnCode(rc);exit(-1);}	


}

int main()	{
	
//	testStorm();

//	testREM();
	
//	testINXM();

	testSYSM();

	testSSQLM();

	return (0);
}