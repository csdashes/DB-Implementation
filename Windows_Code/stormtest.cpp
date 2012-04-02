#include "STORM.h"

int main()
{
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

	return (0);
}




