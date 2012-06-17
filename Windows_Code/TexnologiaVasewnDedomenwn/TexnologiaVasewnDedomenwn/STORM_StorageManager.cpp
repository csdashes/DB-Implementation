/* ============================================================================================== */
// STORM_Manager.cpp
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
// Last update: 9/3/2009 
/* ============================================================================================== */

#include "STORM_StorageManager.h"
#include "STORM_Frame.h"
#include "STORM_BufferManager.h"
#include "STORM_FileHandle.h"
#include <fcntl.h>
#include <io.h>
#include <iostream>
#include <cstdlib>
#include <cerrno>

//-----------------------------------------------------------------------------------------------
// Constructor
//
//
//-----------------------------------------------------------------------------------------------
STORM_StorageManager::STORM_StorageManager()
{
	m_numOpenedFiles = 0;

	for (int i=0; i<MAX_OPENED_FILES; i++)
	{
		m_files[i].fileID = INVALID_FILEID;
		strcpy(m_files[i].fileName, "");
	}

}

//-----------------------------------------------------------------------------------------------
// Destructor
//
// Closes all opened files.
//-----------------------------------------------------------------------------------------------
STORM_StorageManager::~STORM_StorageManager()
{
	//+++
}


//-----------------------------------------------------------------------------------------------
// CreateFile
//
// Creates a new file. If file exists it is not overwritten. 
//
// Returns:
// - STORM_CANNOTCREATEFILE, if file creation fails
// - STORM_FILEEXISTS, if file already exists
// - OK, on success
//-----------------------------------------------------------------------------------------------
t_rc STORM_StorageManager::CreateFile(const char *fname)
{
	int fileID;
	t_rc rc;
	
	// Try to create the file. 
	fileID = open (fname, O_EXCL|O_CREAT|O_WRONLY, 00600);

	// Check if file exists
	if (errno == EEXIST)
		return (STORM_FILEEXISTS);

	// Check if open has failed.
	if (fileID == -1) 
		return (STORM_CANNOTCREATEFILE);

	// Request a frame from the buffer.
	int pageID = 0; // page 0 is reserved.

	STORM_Frame *pFrame;
	rc = m_BfrMgr.ReserveFrame(fileID, pageID, &pFrame);

	if (rc != OK)
	{
		close(fileID);
		return (rc);
	}
	
	// Write subheader to the header page
	STORM_FileHandle::t_fileSubHeader fsh;

	fsh.numAllocatedPages = 0;
	fsh.numReservedPages = 0;

	memcpy(pFrame->GetPageDataPtr(), &fsh, sizeof(STORM_FileHandle::t_fileSubHeader));
	
	// The page bitmap is placed right after the file subheader.
	char *pBitmap;
	pBitmap = &pFrame->GetPageDataPtr()[sizeof(STORM_FileHandle::t_fileSubHeader)];

	// Mark all pages as free.
	int bytes_to_write = PAGE_DATA_SIZE-sizeof(STORM_FileHandle::t_fileSubHeader);
	memset(pBitmap,0,bytes_to_write);

	// Declare that the first page of the file is reserved (00000001).
	pBitmap[0] = 1;
	
	// Write the page associated to pFrame.
	rc = m_BfrMgr.WritePage(pFrame);

	// Release the frame.
	m_BfrMgr.ReleaseFrame(pFrame,true);

	if (rc != OK)
	{
		close(fileID);
		return (rc);
	}

	close(fileID);
	return (OK);
}

//-----------------------------------------------------------------------------------------------
// DestroyFile
//
// Destroys an existing file
//-----------------------------------------------------------------------------------------------
t_rc STORM_StorageManager::DestroyFile(const char *fname)
{
	//m_BfrMgr.DisplayBufferContents();

	t_rc rc;
	int fid;
	//int pos = INVALID_NUMBER;

	if (IsOpened(fname))
	{
		// Find the position in the info array
		for (int i=0; i<MAX_OPENED_FILES; i++)
		{
			if (!stricmp(m_files[i].fileName, fname)) // found
			{	
				fid = m_files[i].fileID;
				m_files[i].fileID = INVALID_FILEID;
				strcpy(m_files[i].fileName, "");
				
				// Close the file (OS)
				int r;
				r = close(fid);
				if (r) return (STORM_FILECLOSEERROR);
				
				break;
			}
		}

		// Let the buffer manager deallocate space reserved for the file.
		rc = m_BfrMgr.ReleaseFileFrames(fid, false); 
		if (rc != OK) return (rc);
		m_numOpenedFiles--;
	}

	// Remove file from directory (OS)
	if (std::remove(fname))
		return (STORM_CANNOTDESTROYFILE);
	else
		return (OK);
}


//-----------------------------------------------------------------------------------------------
// OpenFile
//
// Opens an existing file and returns a handle to it.
//-----------------------------------------------------------------------------------------------
t_rc STORM_StorageManager::OpenFile(const char *fname, STORM_FileHandle &fh)
{
	t_rc rc;

	if (m_numOpenedFiles == MAX_OPENED_FILES)
		return (STORM_OPENEDFILELIMITREACHED);

	if ((rc = fh.Open(fname, &m_BfrMgr)) == OK)
	{
		// Find an empty position and store the file info.
		for (int i=0; i<MAX_OPENED_FILES; i++)
		{
			if (m_files[i].fileID == INVALID_FILEID)
			{
				m_files[i].fileID = fh.GetFileID();
				strcpy(m_files[i].fileName,fh.GetFilename());
				break;
			}
		}

		m_numOpenedFiles++;  // one more file is opened now
		
		// Set the buffer manager ptr for this file handle.
		// fh.SetBufferManager(&m_BfrMgr); 

		return (OK);
	}
	else
		return (rc);
}


//-----------------------------------------------------------------------------------------------
// CloseFile
//
// Closes an already opened file.
//-----------------------------------------------------------------------------------------------
t_rc STORM_StorageManager::CloseFile(STORM_FileHandle &fh)
{
	t_rc rc;

	// Check if fh is not a valid file handle.
	if ((fh.GetFileID() == INVALID_FILEID) || (!strcmp(fh.GetFilename(),"")))
		return (STORM_INVALIDFILEHANDLE);

	// Search the file info.
	for (int i=0; i<MAX_OPENED_FILES; i++)
	{
		if (m_files[i].fileID == fh.GetFileID()) // found
		{
			m_files[i].fileID = INVALID_FILEID;
			strcpy(m_files[i].fileName,"");
			rc = fh.Close();
			
			if (rc == STORM_FILEALREADYCLOSED)  // critical error
			{
				DisplayReturnCode(rc);
				exit(-1);
			}

			if (rc != OK) return (rc);

			m_numOpenedFiles--;
			break;
		}
	}

	return (OK);
}


//-----------------------------------------------------------------------------------------------
// CloseAllFiles
//
// Closes all opened files.
//-----------------------------------------------------------------------------------------------
t_rc STORM_StorageManager::CloseAllFiles()
{
	t_rc rc;

	if (!m_numOpenedFiles)
		return (OK);

	for (int i=0; i<MAX_OPENED_FILES; i++)
	{
		if (m_files[i].fileID != INVALID_FILEID) // this is a valid file
		{
			rc = m_BfrMgr.ReleaseFileFrames(m_files[i].fileID, true);
			if (rc!=OK) return (rc);
			close(m_files[i].fileID);
			m_files[i].fileID = INVALID_FILEID;
			strcpy(m_files[i].fileName,"");
			m_numOpenedFiles--;
		}
	}

	return (OK);
}



//-----------------------------------------------------------------------------------------------
// IsOpened
//
// Checks if the file with the given filename is opened.
//-----------------------------------------------------------------------------------------------
bool STORM_StorageManager::IsOpened(const char *fname)
{
	for (int i=0; i<MAX_OPENED_FILES; i++)
	{
		if (!stricmp(m_files[i].fileName, fname))
			return (true);
	}

	return (false);
}


//-----------------------------------------------------------------------------------------------
// DisplayBufferContents
//
// Iterate thtough all frames in the buffer and display some info. 
//-----------------------------------------------------------------------------------------------
void STORM_StorageManager::DisplayBufferContents()
{
	m_BfrMgr.DisplayBufferContents();
}


//-----------------------------------------------------------------------------------------------
// GetStats
//
// Reads the current statistics (requests, reads, and writes)
//-----------------------------------------------------------------------------------------------
void STORM_StorageManager::GetStats(int &nrequests, int &nreads, int &nwrites, int &npinned, int &nframes)
{
	nrequests = m_BfrMgr.GetNumRequests();
	nreads = m_BfrMgr.GetNumreads();
	nwrites = m_BfrMgr.GetNumWrites();
	npinned = m_BfrMgr.GetNumPinned();
	nframes = m_BfrMgr.GetNumFrames();
}


//-----------------------------------------------------------------------------------------------
// InitStats
//
// Initializes the stats counters.
//-----------------------------------------------------------------------------------------------
void STORM_StorageManager::InitStats()
{
	m_BfrMgr.InitCounters();
}


//-----------------------------------------------------------------------------------------------
// FileExists
//
// Checks if file exists. Tries to open the file, and if all is OK then the file exists.
//-----------------------------------------------------------------------------------------------
bool STORM_StorageManager::FileExists(const char *fname)
{
	STORM_FileHandle fh;

	if (fh.Open(fname, &m_BfrMgr) == OK)
	{
		fh.Close();
		return (true);
	}
	else
		return (false);
}