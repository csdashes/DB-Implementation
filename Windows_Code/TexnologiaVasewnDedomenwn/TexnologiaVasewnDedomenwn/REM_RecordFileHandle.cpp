#include "REM_RecordFileHandle.h"

REM_RecordFileHandle::REM_RecordFileHandle()	{
	this->pData_FileHeader = NULL;
	this->isOpen = false;
}

REM_RecordFileHandle::~REM_RecordFileHandle()	{
	FlushPages();
}

/* We have a const problem with the method. We call GetPage from STORM which is not const!!! */
t_rc REM_RecordFileHandle::ReadRecord(const REM_RecordID &rid, REM_RecordHandle &rh)	{
	/* Check is file is open. */ 
	if (!this->isOpen) { return REM_FHCLOSED; }
	t_rc rc;
	
	/* Get pageID and slot number to find the record. */
	int pageID, slot;
	
	rc = rid.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	rc = rid.GetSlot(slot);
	if (rc != OK) { return rc; }
	
	/* Get the STORM page that contains the record we need. */
	STORM_PageHandle pageHandle;
	
	rc = this->sfh.GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	/* Check if REM Record Handler has data. If it doesn't, allocate the needed space. */
	if (rh.pData == NULL) {
		rh.pData = new char[this->remFileHeader.recordSize];
	}
	else {
		delete rh.pData;
		rh.pData = new char[this->remFileHeader.recordSize];
	}
	
	char *pData;
	REM_PageHeader pageHeader;
	
	pageHandle.GetDataPtr(&pData);
	
	/* Check if slot number is not correct. */
	memcpy(&pageHeader, pData, REM_PAGEHEADER_SIZE);	
	if (slot > pageHeader.nRecords) { return REM_INVALIDRID; }
	
	/* Find the record we want and copy the data to the record handler.  */
	memcpy(rh.pData, &pData[REM_PAGEHEADER_SIZE+((slot-1)*this->remFileHeader.recordSize)], this->remFileHeader.recordSize);
	
	/* Add the recordID to the record handler. */
	rh.rid = rid;
	
	/* Make Record Handler valid. */
	rh.isValid = true;
	
	return (OK);
}

t_rc REM_RecordFileHandle::InsertRecord(const char *pData, REM_RecordID &rid)	{
	/* Check is file is open. */ 
	if (!this->isOpen) { return REM_FHCLOSED; }
	t_rc rc;
	
	/* Find an empty page to write the new record. */
	STORM_PageHandle targetPage;
	REM_PageHeader pageHeader = {0}; // The default number of records in a page is 0.
	char *pageData;
	
	/*	If there is no free space (lastPageID=-1) then we reserve a new page. */
	if (this->remFileHeader.isLastPageFull) {
		
		rc = sfh.ReservePage(targetPage);	// Reserve the new page.
		if (rc != OK) { return rc; }
		
		/* We mark the last page as not full. */
		this->remFileHeader.isLastPageFull = false;
		
		/* We update the file header and mark as dirty. */
		memcpy(this->pData_FileHeader, &this->remFileHeader, REM_FILEHEADER_SIZE);

		rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
		if (rc != OK) { return rc; }
		
		/* Get a pointer to the data of the new page to write the new data. */		
		rc = targetPage.GetDataPtr(&pageData);
		if (rc != OK) { return rc; }
				
	}
	/* If we have free space in a page, we just get the page. */
	else {
		rc = this->sfh.GetLastPage(targetPage);
		if (rc != OK) { return rc; }
		
		/* Get a pointer to the data of the the page to read the REM Page Header. */
		rc = targetPage.GetDataPtr(&pageData);
		if (rc != OK) { return rc; }
		
		memcpy(&pageHeader, pageData, REM_PAGEHEADER_SIZE);
		
	}
	
	/* We write the data to the proper position. The new record offset must be the page header size + the offset of each record there
	 * is inside the page. We always place the new record at the end of the page. 
	 */
	memcpy(&pageData[REM_PAGEHEADER_SIZE+(pageHeader.nRecords*this->remFileHeader.recordSize)], pData, this->remFileHeader.recordSize);
	
	/* Increase the number of records in the page header. This number is also the slot number of the record. */
	int slotNum = ++pageHeader.nRecords; 
	/* Declare page number, which is the pageID we are currently working on. */
	int pageNum;
	rc = targetPage.GetPageID(pageNum);
	if (rc != OK) { return rc; }
	
	/* Check if page is full. */
	if (pageHeader.nRecords == this->remFileHeader.recordsPerPage) {
		this->remFileHeader.isLastPageFull = true;
		
		/* We update the file header and mark as dirty. */
		memcpy(this->pData_FileHeader, &this->remFileHeader, REM_FILEHEADER_SIZE);
		
		rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
		if (rc != OK) { return rc; }
		
		/* Get a pointer to the data of the new page to write the new data. */		
		rc = targetPage.GetDataPtr(&pageData);
		if (rc != OK) { return rc; }		
	}
	
	/* Write the new REM Page Header. */
	memcpy(pageData, &pageHeader, REM_PAGEHEADER_SIZE);
	
	
	/* Mark the page as dirty because we modified it */
	rc = this->sfh.MarkPageDirty(pageNum);
	if (rc != OK) { return rc; }
	
	this->sfh.FlushPage(pageNum);	// We should reconsider when we are flushing pages!!!
	
	/* Unpin the page */
	rc = this->sfh.UnpinPage (pageNum);
	if (rc != OK) { return rc; }
	
	/* Set the passed RID's page number and slot number */
	rc = rid.SetPageID(pageNum);
	if (rc != OK) { return rc; }
	rc = rid.SetSlot(slotNum);
	if (rc != OK) { return rc; }

	/* Successfuly inserted record */	
	return (OK);
}

t_rc REM_RecordFileHandle::DeleteRecord(const REM_RecordID &rid)	{
	/* Check is file is open. */ 
	if (!this->isOpen) { return REM_FHCLOSED; }
	t_rc rc;
	bool pageReleasedBool=false;
	
	/* Read page and slot we want to delete. */
	int pageID, slot;
	
	rc = rid.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	rc = rid.GetSlot(slot);
	if (rc != OK) { return rc; }
	
	/* Get wanted page. */
	STORM_PageHandle wantedPageHandle;
	rc = this->sfh.GetPage(pageID, wantedPageHandle);
	if (rc != OK) { return rc; }
	
	/* Get the wanted page header. */
	char *wantedData;
	REM_PageHeader wantedPageHeader;
	
	rc = wantedPageHandle.GetDataPtr(&wantedData);
	if (rc != OK) { return rc; }
	
	memcpy(&wantedPageHeader, wantedData, REM_PAGEHEADER_SIZE);
	if (slot > wantedPageHeader.nRecords) { return REM_INVALIDRID; }
	
	STORM_PageHandle lastPageHandle;
	int lastPageID;
	
	rc = this->sfh.GetLastPage(lastPageHandle);
	if (rc != OK) { return rc; }
	
	rc = lastPageHandle.GetPageID(lastPageID);
	if (rc != OK) { return rc; }
	
	/* Determine if we need to retrive an other page to replace the record. */
	if (lastPageID == pageID){
		
		/* Determine if our record is the last. */
		if (wantedPageHeader.nRecords == slot) {
			
			/* Check if we need to release the page. */
			if (wantedPageHeader.nRecords == 1) {
				rc = this->sfh.ReleasePage(pageID);
				pageReleasedBool = true;
				if (rc != OK) { return rc; }
				
				/* We must change the lastPageID in REM File Header. */
				this->remFileHeader.isLastPageFull = true;
				/* Copy new file header at memory */
				memcpy(this->pData_FileHeader, &this->remFileHeader, REM_FILEHEADER_SIZE);
				/* Mark REM File Header frame as dirty */
				rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
				if (rc != OK) { return rc; }

			}
			else {
				/* If our record is the last one in the row but more data exist, we just decrease the number of records the page has. */
				wantedPageHeader.nRecords--;
				
				/* Copy header back. */
				memcpy(wantedData, &wantedPageHeader, REM_PAGEHEADER_SIZE);				
			}			
		}
		else {
			/* Copy the last record of this page (last page) in the position of the record we want to erase. */
			memcpy(&wantedData[REM_PAGEHEADER_SIZE+((slot-1)*this->remFileHeader.recordSize)], 
				   &wantedData[REM_PAGEHEADER_SIZE+((wantedPageHeader.nRecords-1)*this->remFileHeader.recordSize)], 
				   this->remFileHeader.recordSize);			
			
			/* Decrease the number of records the pages has. */
			wantedPageHeader.nRecords--;

			/* Copy header back. */
			memcpy(wantedData, &wantedPageHeader, REM_PAGEHEADER_SIZE);
		}
	}
	else {
		/* Get last record from last page to replace the asked record. */
		char *lastData;
		
		rc = lastPageHandle.GetDataPtr(&lastData);
		if (rc != OK) { return rc; }
		
		/* Get last's page header. */
		REM_PageHeader lastPageHeader;
		
		memcpy(&lastPageHeader, lastData, REM_PAGEHEADER_SIZE);		
		
		/* Copy the data between the two pages */
		memcpy(&wantedData[REM_PAGEHEADER_SIZE+((slot-1)*this->remFileHeader.recordSize)], 
			   &lastData[REM_PAGEHEADER_SIZE+((lastPageHeader.nRecords-1)*this->remFileHeader.recordSize)], 
			   this->remFileHeader.recordSize);
		
		/* Check if last page has no more records. */
		if (lastPageHeader.nRecords == 1) {
			/* If we copied the last record we can safely release the last page. */
			rc = this->sfh.ReleasePage(lastPageID);
			if (rc != OK) { return rc; }
			
			/* We must change the lastPageID in REM File Header. */
			this->remFileHeader.isLastPageFull = true;
			/* Copy new file header at memory */
			memcpy(this->pData_FileHeader, &this->remFileHeader, REM_FILEHEADER_SIZE);
			/* Mark REM File Header frame as dirty */
			rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
			if (rc != OK) { return rc; }
		}
		else {
			/* Decrease last page record number in its page header. */		
			lastPageHeader.nRecords--;
			
			memcpy(lastData, &lastPageHeader, REM_PAGEHEADER_SIZE);
			
			/* Mark last page as dirty due to last record removal. */			
			rc = sfh.MarkPageDirty(lastPageID);
			if (rc != OK) { return rc; }
			
			/* Unpin last page. */
			rc = sfh.UnpinPage (lastPageID);
			if (rc != OK) { return rc; }
		}
	}

	
	/* Mark the page as dirty because we modified it */
	if(!pageReleasedBool) {
		rc = sfh.MarkPageDirty(pageID); //STORM_PAGENOTINBUFFER
		if (rc != OK) { return rc; }
		/* Unpin the page */
		rc = sfh.UnpinPage (pageID);
		if (rc != OK) { return rc; }
	
		rc = sfh.FlushPage(pageID);
		if (rc != OK) { return rc; }
	}

	/* Record successfully deleted */
	return (OK);
}

t_rc REM_RecordFileHandle::UpdateRecord(const REM_RecordHandle &rh)	{
	/* Check is file is open. */ 
	if (!this->isOpen) { return REM_FHCLOSED; }
	t_rc rc;
	
	/* Exctract RID and get pageID and slot numbers. */
	REM_RecordID rid;
	int pageID, slot;
	
	rc = rh.GetRecordID(rid);
	if (rc != OK) { return rc; }
	
	rc = rid.GetPageID(pageID);
	if (rc != OK) { return rc; }
	rc = rid.GetSlot(slot);
	if (rc != OK) { return rc; }
	
	/* Get the record and a pointer to the data inside. */
	STORM_PageHandle pageHandle;
	char *pData;
	
	rc = this->sfh.GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	rc = pageHandle.GetDataPtr(&pData);
	if (rc != OK) { return rc; }
	
	/* Copy the new data. */
	memcpy(&pData[REM_PAGEHEADER_SIZE+((slot-1)*this->remFileHeader.recordSize)], rh.pData, this->remFileHeader.recordSize);
	
	/* Mark the page as dirty because we modified it */
	rc = this->sfh.MarkPageDirty(pageID);
	if (rc != OK) { return rc; }
	/* Unpin the page */
	rc = this->sfh.UnpinPage(pageID);
	if (rc != OK) { return rc; }
	
	return (OK);
}

t_rc REM_RecordFileHandle::FlushPages() const	{
	if (this->isOpen) {
		t_rc rc = this->sfh.FlushAllPages();
		if (rc != OK) { return rc; }
	}
	
	return (OK);
}
