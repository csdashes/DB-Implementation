#include "REM_RecordFileManager.h"

REM_RecordFileManager::REM_RecordFileManager(STORM_StorageManager *sm)	{
	this->sm = sm;
}

REM_RecordFileManager::~REM_RecordFileManager()	{
	// We have only one storm manager and we can access it only through REM should we call delete this->sm ???
}

t_rc REM_RecordFileManager::CreateRecordFile(const char *fname, int rs)	{
	
	// We need to find the number of records that fit in a page.
	int recordsPerPage = (PAGE_SIZE - REM_PAGEHEADER_SIZE)/rs;	// We need only the integer part or we can "floor" the number.
	
	if (recordsPerPage < 1) {
		return (REM_INVALIDRECSIZE);
	}
	
	
	/* Create file by using STORM */
	t_rc rc = this->sm->CreateFile(fname);
	if (rc != OK) { return rc; }
	
	
	/* Open the created file */
	STORM_FileHandle stormFileHandle;

	rc = this->sm->OpenFile(fname, stormFileHandle);
	if (rc != OK) { return rc; }	
	
	
	/* Allocate a new page for the REM header page. */
	STORM_PageHandle stormPageHandle;

	rc = stormFileHandle.ReservePage(stormPageHandle);
	if (rc != OK) { return rc; }

	
	/* Get pointer to the contents (data) of the REM header page */
	char *pData;
	int pageID;

	rc = stormPageHandle.GetDataPtr(&pData);
	if (rc != OK) { return rc; }

	/* Get the page number of the REM header page */
	rc = stormPageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	/* Construct REM File Header */
	REM_FileHeader 
	fileHeader = { 0,	// # of records currently stored in file.
		rs, // record size
		recordsPerPage,
		true
	};
	
	/* Copy	the REM File Header to the first page */
	memcpy(pData, &fileHeader, REM_FILEHEADER_SIZE);
		
	
	/* Because we modified the REM header page, we write it to disk */
	rc = stormFileHandle.MarkPageDirty(pageID);
	if (rc != OK) { return rc; }
	rc = stormFileHandle.FlushPage(pageID);
	if (rc != OK) { return rc; }

	
	/* We now unpin the header page because we are done modifying it. (We really need this?) */
	rc = stormFileHandle.UnpinPage(pageID);
	if (rc != OK) { return rc; }	
	
	/* We now close the file because we are done modifying it. (We really need this too?) */
	rc = this->sm->CloseFile(stormFileHandle);
	if (rc != OK) { return rc; }	
	
	return(OK);
}

t_rc REM_RecordFileManager::DestroyRecordFile(const char *fname)	{
	return this->sm->DestroyFile(fname);
}

t_rc REM_RecordFileManager::OpenRecordFile(const char *fname, REM_RecordFileHandle &rfh)	{

	/* Close record file handler if it's already open */
	if (rfh.isOpen) {
		CloseRecordFile(rfh);
	}
	
	/* Open file with STORM and assign a STORM File Handler to REM Record File Handler given in arguments */
	t_rc rc = this->sm->OpenFile(fname, rfh.sfh);
	if (rc != OK) { return rc; }
	
	
	/* Use the STORM File Handler from REM Record File Handler given in arguments to get the first page. */
	STORM_PageHandle pageHandle;
	
	rc = rfh.sfh.GetFirstPage(pageHandle);
	if (rc != OK) { return rc; }
	
	/* Get data from first page. Those first data are the REM File Header with FULL/NOT_FULL index. */
	rc = pageHandle.GetDataPtr(&rfh.pData_FileHeader);
	if (rc != OK) { return rc; }
	
	rc = pageHandle.GetPageID(rfh.pageNum_FileHeader);
	if (rc != OK) { return rc; }
	
	/* We keep File Header as a seperated copy inside the REM Record File Handler. 
	 * Using just a pointer in a sequence of dytes would be difficult.
	 * The FULL/NOT_FULL index on the other hand, is easier to handle because
	 * we only need to change a byte from 0 to 1 or the opposite.
	 */
	memcpy(&rfh.remFileHeader, &rfh.pData_FileHeader[0], REM_FILEHEADER_SIZE);
	
	printf ("# records in file = %d, record size = %d\n",
			rfh.remFileHeader.nRecords, rfh.remFileHeader.recordSize);
	printf ("records per page = %d \n",
			rfh.remFileHeader.recordsPerPage);
	
	/* Successfully opened a record file handler */ 
	rfh.isOpen = true;
	
	return (OK);
}

t_rc REM_RecordFileManager::CloseRecordFile(REM_RecordFileHandle &rfh)	{
	
	/* If record file handler is already closed, return error */
	if (!rfh.isOpen) { return REM_FHCLOSED; }

	t_rc rc = rfh.sfh.FlushAllPages();
	if (rc != OK) { return rc; }

	rc = rfh.sfh.UnpinPage(rfh.pageNum_FileHeader);
	if (rc != OK) { return rc; }
	
	
	rc = this->sm->CloseFile(rfh.sfh);
	if (rc != OK) { return rc; }
	
	/* Successfully closed a record file handler */ 
	rfh.isOpen = false;
	
	return (OK);
}