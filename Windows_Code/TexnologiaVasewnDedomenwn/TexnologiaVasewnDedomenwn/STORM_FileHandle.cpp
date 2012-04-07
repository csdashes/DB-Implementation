/* ============================================================================================== */
// STORM_FileHandle.cpp
//
// 
// 
// 
// 
//
//
//
//
//
// -----------------------
// Last update: 10/4/2009
/* ============================================================================================== */

#include "STORM_Parameters.h"
#include "STORM_FileHandle.h"
#include "STORM_PageHandle.h"
#include "STORM_BufferManager.h"
#include <io.h>
#include <fcntl.h>

//-----------------------------------------------------------------------------------------------
// Constructor
//
//
//-----------------------------------------------------------------------------------------------
STORM_FileHandle::STORM_FileHandle()
{
	InitFileHandle();
}


//-----------------------------------------------------------------------------------------------
// Destructor
//
//
//-----------------------------------------------------------------------------------------------
STORM_FileHandle::~STORM_FileHandle()
{
	if (m_isOpened)
		Close();
}


//-----------------------------------------------------------------------------------------------
// InitFileHandle
//
// Initializes fh values.
//-----------------------------------------------------------------------------------------------
void STORM_FileHandle::InitFileHandle()
{
	m_pBfrMgr = NULL;       // invalid ptr to buffer manager
	m_fileID = INVALID_FILEID;  // invalid file identifier
	m_filePos = INVALID_NUMBER; // invalid file position
	m_fileSubHeader.numAllocatedPages = INVALID_NUMBER; // invalid number of allocated pages
	m_fileSubHeader.numReservedPages = INVALID_NUMBER;  // invalid number of reserved pages
	strcpy(m_fileName, ""); // empty filename at this point
	m_isOpened = false; // file is not opened
	m_pHeaderFrame = NULL;  // initially ptr to header frame is NULL
}


//-----------------------------------------------------------------------------------------------
// Open
//
// Opens the file, and initializes the filehandle variables.
// The first page of the file contains the file header.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::Open(const char *fname, STORM_BufferManager *pBfrMgr)
{
	// Check if file is already opened.
	if (m_isOpened)
		return (STORM_FILEALREADYOPENED);

	m_fileID = open (fname, O_BINARY|O_RDWR);

	if (m_fileID == -1)
		return (STORM_FILEOPENERROR);

	strcpy(m_fileName, fname);
	m_pBfrMgr = pBfrMgr;

	STORM_Frame *pFrame;
	t_rc rc;
	int pageID = 0;

	rc = m_pBfrMgr->ReserveFrame(m_fileID, pageID, &pFrame);
	if (rc != OK)
		return (rc);

	// Pin the page to the buffer
	//rc = m_pBfrMgr->PinPage(m_fileID, pageID);
	//if (rc != OK)
	//	return (rc);

	rc = m_pBfrMgr->ReadPage(pFrame);
	if (rc != OK)
		return (rc);

	// Update the pointer to the frame containing the file header. 
	m_pHeaderFrame = pFrame;

	// Read the file subheader and update variables of file handle. 
	memcpy (&m_fileSubHeader, pFrame->GetPageDataPtr(), sizeof(t_fileSubHeader));

	m_isOpened = true;

	m_filePos = 0;
	
	return (OK);
}


//-----------------------------------------------------------------------------------------------
// Close
//
// Closes the filehandle and takes care of flushing pages to the disk.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::Close()
{
	t_rc rc;

	// Check if file is not opened.
	if (!m_isOpened)
		return (STORM_FILEALREADYCLOSED);

	// Otherwise close the file and return OK.
	// Let the buffer manager deallocate space reserved for the file.
	rc = m_pBfrMgr->ReleaseFileFrames(m_fileID, true);
	if (rc != OK) return (rc);

	// Close the file (OS)
	int r;
	r = close(m_fileID);
	if (r) return (STORM_FILECLOSEERROR);

	// Initialize the file handle variables.
	InitFileHandle();
	
	return (OK);
}



//-----------------------------------------------------------------------------------------------
// ReOpen
//
// Closes and opens the file again.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::ReOpen()
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	t_rc rc;
	STORM_BufferManager *ptr;
	char fname[MAX_FILENAME_LENGTH];

	strcpy(fname, m_fileName);  // keep the filename
	ptr = m_pBfrMgr;            // keep the pointer to the buffer manager

	rc = Close();
	if (rc != OK) return (rc);

	rc = Open(fname, ptr);
	if (rc != OK) return (rc);

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// IsPageReserved
//
// Returns 0 if page is free, 1 otherwise
//-----------------------------------------------------------------------------------------------
bool STORM_FileHandle::IsPageReserved(int pageID)
{
	char *pPageBitmap;
	int pageByte, pageBit, bitVal;

	pPageBitmap = &m_pHeaderFrame->GetPageDataPtr()[sizeof(t_fileSubHeader)];  // get ptr to bitmap
	pageByte = pageID/8;           // determine the byte containing the pageID
	pageBit = pageID - pageByte*8; // determine the bit containing the pageID
	
	bitVal = (pPageBitmap[pageByte] & (1<<pageBit)) >> pageBit;  // get the value of the page bit
	
	if (!bitVal) 
		return (false);
	else 
		return (true);
}

//-----------------------------------------------------------------------------------------------
// SetPageReservationFlag
//
// 0 page free, 1 page reserved
//-----------------------------------------------------------------------------------------------
void STORM_FileHandle::SetPageReservationFlag (int pageID, bool flag)
{
	char *pPageBitmap;
	int pageByte, pageBit, bitVal;

	pPageBitmap = &m_pHeaderFrame->GetPageDataPtr()[sizeof(t_fileSubHeader)];  // get ptr to bitmap
	pageByte = pageID/8;           // determine the byte containing the pageID
	pageBit = pageID - pageByte*8; // determine the bit containing the pageID
	
	bitVal = (pPageBitmap[pageByte] & (1<<pageBit)) >> pageBit;  // get the value of the page bit
	
	if (bitVal == (int)flag)
	{
		DisplayMessage("Critical Error. Page is already in this condition.");
		exit(-1); 
	}
	else
		pPageBitmap[pageByte] ^= (1<<pageBit);

	// Mark the frame as dirty.
	m_pHeaderFrame->SetDirty();
}

//-----------------------------------------------------------------------------------------------
// GetFirstPage
//
// Updates the page handle with the first reserved page of the file.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::GetFirstPage(STORM_PageHandle &pageHandle) 
{
	t_rc rc;

	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	rc = GetNextPage(0, pageHandle);
	if (rc != OK) return (rc);

	int curPageID;

	pageHandle.GetPageID(curPageID);
	m_filePos = curPageID;

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// GetLastPage
//
// Updates the page handle with the last reserved page of the file.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::GetLastPage(STORM_PageHandle &pageHandle)
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	int lastPageID = INVALID_PAGEID;

	for (int pageCounter = 1; pageCounter <= m_fileSubHeader.numAllocatedPages; pageCounter++) 
	{
		if (IsPageReserved(pageCounter)) 
		{
			lastPageID = pageCounter;
			m_filePos = lastPageID;
		}
	}

	if (lastPageID == INVALID_PAGEID)
		return (STORM_EOF);

	return (GetPage(lastPageID, pageHandle));
}


//-----------------------------------------------------------------------------------------------
// GetNextPage
//
// Updates the page handle with the next reserved page (the one after the current pageID)
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::GetNextPage(int curPageID, STORM_PageHandle &pageHandle) 
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	for (int pageCounter = curPageID+1; pageCounter <= m_fileSubHeader.numAllocatedPages; pageCounter++) 
	{
		if (IsPageReserved(pageCounter))
		{
			m_filePos = pageCounter;
			return GetPage(pageCounter, pageHandle);
		}
	}
	
	// If we are here we have reached the end of file and no reserved page has been found.
	return (STORM_EOF);
}


//-----------------------------------------------------------------------------------------------
// GetNextPage
//
// Updates the page handle with the next reserved page (m_filePos is being used)
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::GetNextPage(STORM_PageHandle &pageHandle) 
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	for (int pageCounter = m_filePos+1; pageCounter <= m_fileSubHeader.numAllocatedPages; pageCounter++) 
	{
		if (IsPageReserved(pageCounter))
		{
			m_filePos = pageCounter;
			return GetPage(pageCounter, pageHandle);
		}
	}
	
	// If we are here we have reached the end of file and no reserved page has been found.
	return (STORM_EOF);
}

//-----------------------------------------------------------------------------------------------
// GetPreviousPage
//
// Updates the page handle with the previous reserved page (the one before the current pageID)
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::GetPreviousPage(int curPageID, STORM_PageHandle &pageHandle)
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	for (int pageCounter = curPageID-1; pageCounter > 0; pageCounter--) 
	{
		if (IsPageReserved(pageCounter)) 
		{
			m_filePos = pageCounter;
			return GetPage(pageCounter, pageHandle);
		}
	}
	
	// If we are here we have reached the end of file and no reserved page has been found.
	return (STORM_EOF);
}


//-----------------------------------------------------------------------------------------------
// GetPage
//
// Updates the page handle with the page with the given pageID
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::GetPage(int pageID, STORM_PageHandle &pageHandle)
{
	STORM_Frame *pFrame;
	t_rc rc;

	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	// Check if page is not valid.
	if (pageID < 1)
		return (STORM_INVALIDPAGE);
		
	if (pageID > m_fileSubHeader.numAllocatedPages)
		return (STORM_INVALIDPAGE); 
		
	if (!IsPageReserved(pageID)) 
		return (STORM_INVALIDPAGE);

	rc = m_pBfrMgr->NeedPage(m_fileID, pageID, &pFrame);
	if (rc != OK) return (rc);

	rc = pageHandle.Open(pFrame);
	m_filePos = pageID;

	return (rc);
}


//-----------------------------------------------------------------------------------------------
// Reset
//
// Sets the filePos to 0.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::Reset()
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	m_filePos = 0;
	return (OK);
}

/*
//-----------------------------------------------------------------------------------------------
// Clear
//
// Clears the contents of the file. All pages are set to free.
// No dirty write takes place.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::Clear()
{
	t_rc rc;

	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	// Mark all pages as free.
	for (int pageCounter = 1; pageCounter <= m_fileSubHeader.numAllocatedPages; pageCounter++) 
	{
		if (IsPageReserved(pageCounter))
			SetPageReservationFlag(pageCounter,0);
	}

	// Release all frames of the file
	rc = m_pBfrMgr->ReleaseFileFrames(m_fileID, false);  // no dirty write takes place (false parameter)
	if (rc != OK) return (rc);

	m_fileSubHeader.numReservedPages = 0;
	m_filePos = 0;

	UpdateHeaderFrame();

	return (OK);
}
*/

//-----------------------------------------------------------------------------------------------
// ReservePage
//
// Reserves a new page for the file.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::ReservePage(STORM_PageHandle &pageHandle)
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	t_rc rc;
	int newPageID = INVALID_PAGEID; 

	for (int pageCounter=1; pageCounter<=MAX_PAGES_PER_FILE; pageCounter++)
	{
		if (!IsPageReserved(pageCounter))
		{
			newPageID = pageCounter;
			break;
		}
	}

	STORM_Frame *pFrame;

	if (newPageID == INVALID_PAGEID)  // all pages are reserved
	{
		if (m_fileSubHeader.numAllocatedPages == MAX_PAGES_PER_FILE)
			return STORM_FILELIMITREACHED;

		// Reserve frame in buffer.
		rc = m_pBfrMgr->ReserveFrame(m_fileID, newPageID, &pFrame);
		if (rc != OK) return (rc);
		m_fileSubHeader.numAllocatedPages++;
		m_fileSubHeader.numReservedPages++;
		UpdateHeaderFrame();

		rc = m_pBfrMgr->WritePage(pFrame);
		if (rc != OK) return (rc);
	}
	else  // a free page has been found
	{
		// Reserve frame in buffer.
		rc = m_pBfrMgr->ReserveFrame(m_fileID, newPageID, &pFrame);
		if (rc != OK) return (rc);
		m_fileSubHeader.numReservedPages++;
		UpdateHeaderFrame();

		if (newPageID > m_fileSubHeader.numAllocatedPages)
		{
			rc = m_pBfrMgr->WritePage(pFrame);
			if (rc != OK) return (rc);
			m_fileSubHeader.numAllocatedPages++;
			UpdateHeaderFrame();
		}
	}

	SetPageReservationFlag(newPageID, true);  // page is reserved now

	rc = pageHandle.Open(pFrame);
	if (rc != OK) return (rc);

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// ReleasePage
//
// Disposes the page from the file. The page is no longer required.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::ReleasePage(int pageID)
{
	if (!m_isOpened)
		return (STORM_FILENOTOPENED);

	t_rc rc;

	if (pageID < 1)
		return (STORM_INVALIDPAGE);
		
	if (pageID > m_fileSubHeader.numAllocatedPages)
		return (STORM_INVALIDPAGE); 
		
	if (!IsPageReserved(pageID)) 
		return (STORM_INVALIDPAGE);


	STORM_Frame *pFrame;

	// Check if the page is in the buffer pool.
	pFrame = m_pBfrMgr->LookupFrame(m_fileID, pageID); 

	if (pFrame)  // page in buffer pool
	{
		if (pFrame->IsPinned())
			return (STORM_PAGEISPINNED);

		// Release the page from the buffer to free space in memory.
		rc = m_pBfrMgr->ReleaseFrame(pFrame, false);
		if (rc != OK) return (rc);
	}
	
	m_fileSubHeader.numReservedPages--;      // update number of reserved pages
	SetPageReservationFlag(pageID, false);   // mark the page as free (not reserved)

	UpdateHeaderFrame();

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// UpdateHeaderFrame
//
// Copies subheader into the header frame
//-----------------------------------------------------------------------------------------------
void STORM_FileHandle::UpdateHeaderFrame()
{
	if (!m_pHeaderFrame)  // critical error
	{
		DisplayMessage("Critical Error. Pointer to header frame is NULL.");
		exit(-1);
	}

	// Update the subheader.
	memcpy(m_pHeaderFrame->GetPageDataPtr(), &m_fileSubHeader, sizeof(t_fileSubHeader));

	// Mark the header frame as dirty.
	m_pHeaderFrame->SetDirty();

}


//-----------------------------------------------------------------------------------------------
// MarkPageDirty
//
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::MarkPageDirty (int page_id) const
{
	return (m_pBfrMgr->MarkPageDirty(m_fileID,page_id));
}


//-----------------------------------------------------------------------------------------------
// MarkPageClean
//
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::MarkPageClean (int page_id)
{
	return (m_pBfrMgr->MarkPageClean(m_fileID,page_id));
}


//-----------------------------------------------------------------------------------------------
// IsPageDirty
//
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::IsPageDirty (int page_id, bool &dirtyBit) const
{
	return (m_pBfrMgr->IsPageDirty(m_fileID,page_id,dirtyBit));
}


//-----------------------------------------------------------------------------------------------
// UnpinPage
//
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::UnpinPage (int page_id) const
{
	return (m_pBfrMgr->UnpinPage(m_fileID,page_id));
}


//-----------------------------------------------------------------------------------------------
// FlushPage
//
// Forces a page to be written to disk if it is dirty.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::FlushPage(int pageID) const
{
	return (m_pBfrMgr->FlushPage(m_fileID, pageID));
}


//-----------------------------------------------------------------------------------------------
// FlushAllPages
//
// Forces all file pages to be written to disk if they are dirty.
//-----------------------------------------------------------------------------------------------
t_rc STORM_FileHandle::FlushAllPages() const
{
	return (m_pBfrMgr->FlushFilePages(m_fileID));  
}


//-----------------------------------------------------------------------------------------------
// PinPage
//
// 
//-----------------------------------------------------------------------------------------------
//t_rc STORM_FileHandle::PinPage (int page_id)
//{
//	return (m_pBfrMgr->PinPage(m_fileID,page_id));
//}