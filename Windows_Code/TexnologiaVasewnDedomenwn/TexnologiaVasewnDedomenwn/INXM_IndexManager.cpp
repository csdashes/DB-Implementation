#include "INXM_IndexManager.h"

INXM_IndexManager::INXM_IndexManager(STORM_StorageManager *sm)	{
	this->sm = sm;
}

INXM_IndexManager::~INXM_IndexManager() {
}

t_rc INXM_IndexManager::CreateIndex(const char *fname, int indexNo, t_attrType attrType, int attrLength) {
	
	// We need to find the number of records that fit in a page.
	int recordsPerPage = (PAGE_SIZE - INXM_INITPAGEHEADER_SIZE + INXM_NODEPAGEHEADER_SIZE)/(INXM_NODE_SIZE + attrLength);	// We need only the integer part or we can "floor" the number.
	
	if (recordsPerPage < 1) {
		return (INXM_INVALIDRECSIZE);
	}
	
	char buffer[128];
	_snprintf(buffer, sizeof(buffer), "%s.%d", fname, indexNo);

	/* Create file by using STORM */
	t_rc rc = this->sm->CreateFile(buffer);
	if (rc != OK) { return rc; }

	/* Open the created file */
	STORM_FileHandle stormFileHandle;
	
	rc = this->sm->OpenFile(buffer, stormFileHandle);
	if (rc != OK) { return rc; }	
	
	/* Allocate a new page for the INXM file header page. */
	STORM_PageHandle stormPageHandle;
	
	rc = stormFileHandle.ReservePage(stormPageHandle);
	if (rc != OK) { return rc; }

	/* Get pointer to the contents (data) of the INXM file header page */
	char *pData;
	int pageID;
	
	rc = stormPageHandle.GetDataPtr(&pData);
	if (rc != OK) { return rc; }
	
	/* Get the page number of the INXM file header page */
	rc = stormPageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	/* Construct INXM File Header */
	INXM_FileHeader 
	fileHeader = {
		attrType,
		attrLength,
		0,
		0
	};
	
	/* Copy	the INXM File Header to the first page */
	memcpy(pData, &fileHeader, INXM_FILEHEADER_SIZE);

	/* Because we modified the INXM file header page, we write it to disk */
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

t_rc INXM_IndexManager::DestroyIndex(const char *fname, int indexNo) {
	char buffer[128];
	_snprintf(buffer, sizeof(buffer), "%s.%d", fname, indexNo);

	return this->sm->DestroyFile(buffer);
	return(OK);
}

t_rc INXM_IndexManager::OpenIndex(const char *fname, int indexNo, INXM_IndexHandle &ih) {
	/* Close index file handler if it's already open */
	if (ih.isOpen) {
		CloseIndex(ih);
	}
	
	char buffer[128];
	_snprintf(buffer, sizeof(buffer), "%s.%d", fname, indexNo);
	/* Open file with STORM and assign a STORM File Handler to INXM Handler given in arguments */
	t_rc rc = this->sm->OpenFile(buffer, ih.sfh);
	if (rc != OK) { return rc; }
	
	
	/* Use the STORM File Handler from INXM Handler given in arguments to get the first page. */
	STORM_PageHandle pageHandle;
	
	rc = ih.sfh.GetFirstPage(pageHandle);
	if (rc != OK) { return rc; }
	
	/* Get data from first page. Those first data are the INXM File Header. */
	rc = pageHandle.GetDataPtr(&ih.pData_FileHeader);
	if (rc != OK) { return rc; }
	
	rc = pageHandle.GetPageID(ih.pageNum_FileHeader);
	if (rc != OK) { return rc; }
	
	/* We keep File Header as a seperated copy inside the INXM Handler. 
	 * Using just a pointer in a sequence of dytes would be difficult.
	 */
	memcpy(&ih.inxmFileHeader, &ih.pData_FileHeader[0], INXM_FILEHEADER_SIZE);
		
	/* Successfully opened a record file handler */ 
	ih.isOpen = true;
	
	return(OK);
}

t_rc INXM_IndexManager::CloseIndex(INXM_IndexHandle &ih) {
	/* If record file handler is already closed, return error */
	if (!ih.isOpen) { 
	//	return INXM_IHCLOSED;															NA TO VGALW APO SXOLIA
	}
	
	t_rc rc = ih.sfh.FlushAllPages();
	if (rc != OK) { return rc; }
	
	rc = ih.sfh.UnpinPage(ih.pageNum_FileHeader);
	if (rc != OK) { return rc; }
	
	
	rc = this->sm->CloseFile(ih.sfh);
	if (rc != OK) { return rc; }
	
	/* Successfully closed a record file handler */ 
	ih.isOpen = false;
	
	return(OK);
}