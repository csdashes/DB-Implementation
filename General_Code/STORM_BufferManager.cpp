/* ============================================================================================== */
// STORM_BufferManager.cpp
//
// Implementation of the buffer. The page replacement policy is based on LRU. 
// A linked list is used to hold the frames. Each time a frame is referenced it is
// send at the back of the list. Therefore, the least recently page is always at the top.
// For fast page lookup, a hashtable is being used. The method selects as a victim the
// first unpinned page. 
//
//
//
//
//
// -----------------------
// Last update: 8/4/2009 
/* ============================================================================================== */

#include "retcodes.h"
#include "STORM_Parameters.h"
#include "STORM_BufferManager.h"
#include "UTILS_misc.h"
#include <cassert>
#include <fcntl.h>
#include <stdio.h>

//-----------------------------------------------------------------------------------------------
// Constructor
//
//
//-----------------------------------------------------------------------------------------------
STORM_BufferManager::STORM_BufferManager ()
{
	m_numFrames = BUFFER_CAPACITY;
	m_numReservedFrames = 0;

	m_numHits = 0; 
	m_numMisses = 0; 
	m_numRequests = 0;
	m_numReads = 0;
	m_numWrites = 0;
	m_numPinned = 0;

	// create the hash table 
	m_p_hash_table = NULL;
	m_p_hash_table = new t_hash_table (m_numFrames);
	
	if (!m_p_hash_table)
	{
		DisplayMessage((char *)"Critical Error. Memory allocation error.");
		exit(-1);
	}

	m_p_head = NULL;
	m_p_tail = NULL;
}


//-----------------------------------------------------------------------------------------------
// Destructor
//
//
//-----------------------------------------------------------------------------------------------
STORM_BufferManager::~STORM_BufferManager()
{
	// Take care of hash table
	m_p_hash_table->clear();
	delete m_p_hash_table;

	// Take care of page queue
	ClearFrameQueue();
}

//-----------------------------------------------------------------------------------------------
// ClearFrameQueue
//
// Clear the frame priority queue
//-----------------------------------------------------------------------------------------------
void STORM_BufferManager::ClearFrameQueue()
{
	// take care of page queue
	STORM_Frame *elem, *next;
	elem = m_p_head;
	while (elem != NULL)
	{
		// If page is dirty write it to disk.
		if (elem->IsDirty())
		{
			t_rc rc;

			rc = WritePage(elem);
			if (rc != OK)
			{
				DisplayMessage((char *)"Critical Error. Cannot write page.");
				exit(-1);
			}
			//m_numWrites++;
		}

		next = elem->GetNext();
		delete elem;
		elem = next;
	}

	m_p_head = NULL;
	m_p_tail = NULL;

	m_numReservedFrames = 0;
}



//-----------------------------------------------------------------------------------------------
// Insert a page by page_id
//
//
//-----------------------------------------------------------------------------------------------
/*
void STORM_BufferManager::InsertPage(int page_id)
{
	CPage *newpage=NULL;

	newpage = new CPage(m_page_size);
	assert (newpage);

	newpage->SetPageID(page_id);
	newpage->SetDirtyBit(false);
	newpage->SetPinBit(false);

	InsertPage(newpage);
}
*/

//-----------------------------------------------------------------------------------------------
// Insert a page by pointer to page
//
//
//-----------------------------------------------------------------------------------------------
/*
void STORM_BufferManager::InsertPage (CPage *p_newpage)
{
	if (m_n_reserved_pages == m_n_pages) // buffer is full
	{
		CPage *old_head, *victim;

		// delete the head of the queue and determine new head
		
		old_head = m_p_head;

		if (m_p_head)
		{
			victim = m_p_head;

			while ((victim != NULL) && (victim->IsPinned()))
				victim = victim->GetNext();

			// Here either we have reached the end of the buffer and all pages are pinned,
			// or we have found an unpinned page.
			// ++ count the number of pinned pages and avoid pinning the whole buffer
			
			if (victim == NULL)
			{
				printf("Buffer: all pages are pinned!");
				exit(-1);
			}
			
			if (victim == m_p_head)  // head is the victim
			{
				// Set the new head if the head is the victim.
				if (m_p_head = m_p_head->GetNext())
					m_p_head->SetPrevious(NULL);
			
				if (victim->IsDirty())
					m_n_dirty_writes++;

				// delete page from hash structure
				m_p_hash_table->erase (victim->GetPageID());
				delete victim;

				// insert new page in hash structure
				m_p_hash_table->insert (std::make_pair(p_newpage->GetPageID(), p_newpage));
			}
			else // the victim is NOT the head
			{

				// delete victim from linked list
				victim->GetPrevious()->SetNext(victim->GetNext());

				if (victim->IsDirty())
					m_n_dirty_writes++;

				// delete page from hash structure
				m_p_hash_table->erase (victim->GetPageID());
				delete victim;

				// insert new page in hash structure
				m_p_hash_table->insert (std::make_pair(p_newpage->GetPageID(), p_newpage));

			}

		}
	}
	else
	{
		m_p_hash_table->insert(std::make_pair(p_newpage->GetPageID(), p_newpage));
		m_n_reserved_pages++;

		if (m_n_reserved_pages == 1)
		{
			m_p_head = p_newpage;
			m_p_tail = p_newpage;
			p_newpage->SetNext(NULL);
			p_newpage->SetPrevious(NULL);
		}
	}

	if (m_n_reserved_pages > 1)
	{
		// put the inserted page in the tail of the page queue
		p_newpage->SetNext(NULL);
		p_newpage->SetPrevious(m_p_tail);
		if (m_p_tail)
			m_p_tail->SetNext(p_newpage);
	}

	m_p_tail = p_newpage;
}
*/


//-----------------------------------------------------------------------------------------------
// Delete a page
//
//
//-----------------------------------------------------------------------------------------------
/*
bool STORM_BufferManager::DeletePage (int page_id)
{
	bool deleteOK;

	deleteOK = m_p_hash_table->erase (page_id);

	return (deleteOK);
}
*/

//-----------------------------------------------------------------------------------------------
// Clear the buffer.
// Just empty hash table and priority queue without deleting the pointers.
//
//-----------------------------------------------------------------------------------------------
void STORM_BufferManager::ClearBuffer()
{
	// Clear hash table
	m_p_hash_table->clear();

	// Clear page priority queue
	ClearFrameQueue();

	m_numPinned = 0;
}



//-----------------------------------------------------------------------------------------------
// Mark the page as dirty.
// 
//
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::MarkPageDirty(int file_id, int page_id)
{
	STORM_Frame *pFrame;

	pFrame = LookupFrame(file_id, page_id);

	if (!pFrame)
		return STORM_PAGENOTINBUFFER;

	pFrame->SetDirty();

	return OK;
}


//-----------------------------------------------------------------------------------------------
// Mark the page as clean.
// 
//
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::MarkPageClean(int file_id, int page_id)
{
	STORM_Frame *pFrame;

	pFrame = LookupFrame(file_id, page_id);

	if (!pFrame)
		return STORM_PAGENOTINBUFFER;

	pFrame->UnsetDirty();

	return OK;
}


//-----------------------------------------------------------------------------------------------
// Check if page is dirty.
// 
//
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::IsPageDirty(int file_id, int page_id, bool &dirtyBit)
{
	STORM_Frame *pFrame;

	pFrame = LookupFrame(file_id,page_id);

	if (!pFrame)
		return (STORM_PAGENOTINBUFFER);

	dirtyBit = pFrame->IsDirty();

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// PinPage
// 
// Increases the pinCount of the corresponding frame.
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::PinPage (int file_id, int page_id)
{
	STORM_Frame *p_frame;

	p_frame = LookupFrame(file_id, page_id);
	
	if (!p_frame)
		return STORM_PAGENOTINBUFFER;

	p_frame->IncreasePinCount();

	// Update number of pinned pages.
	if (p_frame->GetPinCount() == 1)
		m_numPinned++;

	return OK;
}



//-----------------------------------------------------------------------------------------------
// UnpinPage
// 
// Reduces the pinCount of the corresponding frame.
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::UnpinPage (int file_id, int page_id)
{
	STORM_Frame *p_frame;

	p_frame = LookupFrame(file_id, page_id);

	if (!p_frame)
		return STORM_PAGENOTINBUFFER;

	if (p_frame->IsPinned())
	{
		p_frame->DecreasePinCount();
		if (p_frame->GetPinCount() == 0)
			m_numPinned--;
	}

	return OK;
}


//-----------------------------------------------------------------------------------------------
// IsPagePinned
// 
// Tells us if the page is pinned in the buffer.
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::IsPagePinned (int fileID, int pageID, bool &flag)
{
	STORM_Frame *p_frame;

	p_frame = LookupFrame(fileID, pageID);

	if (!p_frame)
		return STORM_PAGENOTINBUFFER;

	if (p_frame->IsPinned())
		flag = true;
	else
		flag = false;

	return (OK);
}



//-----------------------------------------------------------------------------------------------
// CalculateTargetPageID
// 
// Given the file_id and page_id returns the effective target page_id.
// It is assumed that each file can contain MAX_PAGES_PER_FILE pages.
//-----------------------------------------------------------------------------------------------
int STORM_BufferManager::CalculateTargetPageID (int file_id, int page_id)
{
	return (file_id * MAX_PAGES_PER_FILE + page_id);
}

//-----------------------------------------------------------------------------------------------
// ReserveFrame
// 
// Reserve a frame for the specified file and page.
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::ReserveFrame(int fileID, int pageID, STORM_Frame **ppFrame)
{
	m_numRequests++;

	t_rc rc;

	if (m_numReservedFrames == m_numFrames)  // buffer is full, go for victim selection
	{
		STORM_Frame *old_head, *victimFrame;

		old_head = m_p_head;
		victimFrame = m_p_head;

		// Find an unpinned frame

		while ((victimFrame != NULL) && (victimFrame->IsPinned()))
		{
			victimFrame = victimFrame->GetNext();
		}

		// Here either we have reached the end of the buffer and all pages are pinned,
		// or we have found an unpinned page. 
		// This should not happen. Check number of pinned pages and avoid pinning all.
						
		if (!victimFrame)  // reached end of frame queue
		{
			//DisplayMessage("Critical Error. All pages are pinned!");
			//Pause();
			//exit(-1);
			return (STORM_ALLPAGESPINNED);
		}

		if (victimFrame == m_p_head)  // head is the victim
		{
			// Set the new head if the victim is the head.
			if (m_p_head->GetNext())
			{
				m_p_head = m_p_head->GetNext();
				m_p_head->SetPrevious(NULL);
			}
		}
		else if (victimFrame == m_p_tail)
		{
			// Set the new tail if the victim is the tail.
			if (m_p_tail->GetPrevious())
			{
				m_p_tail = m_p_tail->GetPrevious();
				m_p_tail->SetNext(NULL);
			}
		}
		else   
		{
			victimFrame->GetPrevious()->SetNext(victimFrame->GetNext());
			victimFrame->GetNext()->SetPrevious(victimFrame->GetPrevious());
		}

		if (victimFrame->IsDirty())
		{
			// Write dirty page to disk.
			rc = WritePage(victimFrame);
			if (rc != OK) return (rc);
			//m_numWrites++;
		}

		// Delete victim frame from hash structure
		int target_page_id = CalculateTargetPageID(victimFrame->GetFileID(), victimFrame->GetPageID());
		m_p_hash_table->erase(target_page_id);
			
		// Insert frame in hash structure
		victimFrame->SetFileID(fileID);
		victimFrame->SetPageID(pageID);
		victimFrame->SetReserved();
		victimFrame->ClearPinCount();
		target_page_id = CalculateTargetPageID(fileID, pageID);
		m_p_hash_table->insert (std::make_pair(target_page_id, victimFrame));
		
		*ppFrame = victimFrame; // update the pFrame pointer
	}
	else  // buffer is NOT full (there are free frames available)
	{
		// create a new frame
		STORM_Frame *newFrame;
		newFrame = new STORM_Frame();

		if (!newFrame)
			return STORM_MEMALLOCERROR;

		newFrame->SetFileID(fileID);
		newFrame->SetPageID(pageID);
		newFrame->SetReserved();
		newFrame->ClearPinCount();

		int target_page_id = CalculateTargetPageID(newFrame->GetFileID(), newFrame->GetPageID());
		m_p_hash_table->insert (std::make_pair(target_page_id, newFrame));

		m_numReservedFrames++;  // a new frame has been reserved

		if (m_numReservedFrames == 1)
		{
			m_p_head = newFrame;
			m_p_tail = newFrame;
			newFrame->SetNext(NULL);
			newFrame->SetPrevious(NULL);
		}

		*ppFrame = newFrame;
	}

	// put the frame in the tail of the queue
	if (m_numReservedFrames > 1)
	{
		(*ppFrame)->SetNext(NULL);
		(*ppFrame)->SetPrevious(m_p_tail);

		if (m_p_tail)
			m_p_tail->SetNext(*ppFrame);
		
		m_p_tail = *ppFrame;
	}

	// Pin the page.
	(*ppFrame)->IncreasePinCount();
	m_numPinned++;

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// ReleaseFrame
// 
// Release the frame.
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::ReleaseFrame(STORM_Frame *pFrame, bool dirtyWrite)
{
	t_rc rc;

	bool pinned;
	pinned = pFrame->IsPinned();

	// Delete frame from queue
	if (pFrame == m_p_head)
	{
		m_p_head = pFrame->GetNext();
		
		if (m_p_head)
			m_p_head->SetPrevious(NULL);
	}
	else
	if (pFrame == m_p_tail)
	{
		m_p_tail = pFrame->GetPrevious();

		if (m_p_tail)
			m_p_tail->SetNext(NULL);
	}
	else
	{
		pFrame->GetPrevious()->SetNext(pFrame->GetNext());
		pFrame->GetNext()->SetPrevious(pFrame->GetPrevious());
	}

	int pageID, fileID;
	pageID = pFrame->GetPageID();
	fileID = pFrame->GetFileID();

	if (dirtyWrite)
	{
		if (pFrame->IsDirty())
		{
			rc = WritePage(pFrame);
			if (rc != OK) return (rc);		
		}
	}
	delete (pFrame);

	// Delete frame from hash table
	int target_page_id = CalculateTargetPageID(fileID, pageID);
	m_p_hash_table->erase(target_page_id);

	m_numReservedFrames--;
	if (pinned)
		m_numPinned--;

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// ReleaseFileFrame
// 
// Release all frames assigned to the specified file.
// 
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::ReleaseFileFrames (int fid, bool dirtyWrite)
{
	STORM_Frame *pFrame, *nextFrame;
	t_rc rc;

	pFrame = m_p_head;

	while (pFrame != NULL)
	{
		nextFrame = pFrame->GetNext();

		if (fid == pFrame->GetFileID())
		{
			rc = ReleaseFrame(pFrame,dirtyWrite);
			
			if (rc != OK) return (rc);
		}

		pFrame = nextFrame;
	}

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// LookupFrame
// 
// Locates the frame given the file_id and the page_id.
// 
// Returns:
// - pointer to frame if the page is found
// - NULL is returned if the frame is not found.
//-----------------------------------------------------------------------------------------------
STORM_Frame *STORM_BufferManager::LookupFrame (int file_id, int page_id)
{
	int	target_page_id = CalculateTargetPageID (file_id, page_id);

	STORM_Frame *pFrame;
	t_hash_table::iterator x;

	// Search page in hash structure.
	x = m_p_hash_table->find(target_page_id);

	if (x == m_p_hash_table->end())  // page not found
		pFrame = NULL;
	else  // page found
		pFrame = (*x).second;

	return pFrame;
}


//-----------------------------------------------------------------------------------------------
// NeedPage
//
// Requests a page. 
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::NeedPage (int file_id, int page_id, STORM_Frame **ppFrame)
{
	t_rc rc;

	*ppFrame = LookupFrame(file_id, page_id);

	if (!*ppFrame)  // page not in buffer pool
	{
		if (ReserveFrame(file_id, page_id, ppFrame) == OK)
		{
			rc = ReadPage(*ppFrame);
			if (rc != OK) return (rc);
			m_numMisses++;
		}
		else
			return (STORM_MEMALLOCERROR);
	}
	else // page is in buffer
	{
		m_numHits++;

		// Place the frame at the end of the queue

		// Check if frame is already at end of page queue
		if (*ppFrame == m_p_tail)
			return (OK);

		// Remove page from current position
		if ((*ppFrame)->GetPrevious() != NULL)
		{
			(*ppFrame)->GetPrevious()->SetNext((*ppFrame)->GetNext());
			(*ppFrame)->GetNext()->SetPrevious((*ppFrame)->GetPrevious());

			// Make pFrame the new tail
			(*ppFrame)->SetNext(NULL);
			(*ppFrame)->SetPrevious(m_p_tail);
			m_p_tail->SetNext(*ppFrame);
			m_p_tail = *ppFrame;
		}
		else // pFrame is in head of queue
		{
			m_p_head = m_p_head->GetNext(); // update head
			m_p_head->SetPrevious(NULL);

			// Make pFrame the new tail
			(*ppFrame)->SetNext(NULL);
			(*ppFrame)->SetPrevious(m_p_tail);
			m_p_tail->SetNext(*ppFrame);
			m_p_tail = *ppFrame;
		}

	}

	return (OK);
}




//-----------------------------------------------------------------------------------------------
// ReadPage
//
// Performs a disk read. 
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::ReadPage(STORM_Frame *pFrame)
{
	int file_id, page_id, file_pos;

	file_id = pFrame->GetFileID();
	page_id = pFrame->GetPageID();
	file_pos = page_id * PAGE_SIZE;

	// Move to the correct place in the file.
	int ret;
	ret = lseek (file_id, (long)file_pos, SEEK_SET);

	if (ret == -1L)
		return (STORM_IOERROR);

	// Read the page from the file.
	int bytes;
	int pageID;
	
	bytes = read (file_id, &pageID, INT_SIZE);
	if (bytes < INT_SIZE) 
		return (STORM_IOERROR);

	pFrame->SetPageID(pageID);

	bytes = read (file_id, pFrame->GetPageDataPtr(), PAGE_DATA_SIZE);
	if (bytes < PAGE_DATA_SIZE) 
		return (STORM_IOERROR);

	m_numReads++;

	return OK;
}

//-----------------------------------------------------------------------------------------------
// WritePage
//
// Performs a disk write. 
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::WritePage(STORM_Frame *pFrame)
{
	int file_id, page_id, file_pos;

	file_id = pFrame->GetFileID();
	page_id = pFrame->GetPageID();
	file_pos = page_id * PAGE_SIZE;

	// move to the correct place in the file
	int ret;
	ret = lseek (file_id, (long)file_pos, SEEK_SET);

	if (ret == -1L)
		return STORM_IOERROR;

	// write the page to the file
	int bytes;
	int pageID;
	
	pageID = pFrame->GetPageID();  // +++ boroume na to glytwsoume +++
	bytes = write (file_id, &pageID, INT_SIZE);
	if (bytes < INT_SIZE) 
		return STORM_IOERROR;

	bytes = write (file_id, pFrame->GetPageDataPtr(), PAGE_DATA_SIZE);
	if (bytes < PAGE_DATA_SIZE) 
		return STORM_IOERROR;

	m_numWrites++;

	return OK;
}



//-----------------------------------------------------------------------------------------------
// DisplayBufferContents
//
// Iterate thtough all frames in the buffer and display some info. 
//-----------------------------------------------------------------------------------------------
void STORM_BufferManager::DisplayBufferContents()
{
	STORM_Frame *nextFrame;

	nextFrame = m_p_head;

	while (nextFrame != NULL)
	{
		printf("file: %d, page: %d\n", nextFrame->GetFileID(), nextFrame->GetPageID());
		nextFrame = nextFrame->GetNext();
	}

	printf("\n\n");
}


//-----------------------------------------------------------------------------------------------
// FlushPage
//
// Writes the page to the disk if it is dirty.
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::FlushPage(int fileID, int pageID)
{
	t_rc rc;
	STORM_Frame *pFrame;

	pFrame = LookupFrame(fileID, pageID);

	if (!pFrame)   // page not in buffer 
	{
		rc = STORM_PAGENOTINBUFFER;
		DisplayReturnCode(rc);
		return (rc);
	}

	if (pFrame->IsDirty()) // check if page is dirty
	{
		// Write dirty page to disk.
		rc = WritePage(pFrame);
		if (rc != OK) return (rc);
		//m_numWrites++;

		pFrame->UnsetDirty();
	}

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// FlushFilePages
//
// Writes the pages of a file to the disk if they are dirty.
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::FlushFilePages(int fileID)
{
	STORM_Frame *elem;
	elem = m_p_head;
	while (elem != NULL)  // while there are more frames
	{
		if (elem->GetFileID() == fileID)  // if it is our file
		{
			if (elem->IsDirty()) // if page is dirty
			{
				t_rc rc;

				rc = WritePage(elem);
				if (rc != OK)
				{
					DisplayMessage((char *)"Critical Error. Cannot write page.");
					exit(-1);
				}
				//m_numWrites++;

				elem->UnsetDirty();
			}
		}

		elem = elem->GetNext();
	}

	return (OK);
}



//-----------------------------------------------------------------------------------------------
// FlushAllPages
//
// Writes all pages to the disk if they are dirty.
//-----------------------------------------------------------------------------------------------
t_rc STORM_BufferManager::FlushAllPages()
{
	STORM_Frame *elem;
	elem = m_p_head;
	while (elem != NULL)  // while there are more frames
	{
		if (elem->IsDirty()) // if page is dirty
		{
			t_rc rc;

			rc = WritePage(elem);
			if (rc != OK)
			{
				DisplayMessage((char *)"Critical Error. Cannot write page.");
				exit(-1);
			}
			//m_numWrites++;

			elem->UnsetDirty();
		}
		
		elem = elem->GetNext();
	}

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// DisplayBufferStatistics
//
// Iterate thtough all frames in the buffer and display some info. 
//-----------------------------------------------------------------------------------------------
void STORM_BufferManager::DisplayBufferStatistics()
{
	printf("\nBuffer Statistics:\n");
	printf("requests: %d\n", m_numRequests);
	printf("reads: %d\n", m_numReads);
	printf("writes: %d\n", m_numWrites);
	printf("disk accesses: %d\n",m_numReads+m_numWrites);
	printf("\n");
}


