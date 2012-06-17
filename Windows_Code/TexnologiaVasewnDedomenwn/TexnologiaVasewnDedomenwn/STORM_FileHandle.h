#ifndef _STORM_FileHandle_h
#define _STORM_FileHandle_h

#include "retcodes.h"
#include "STORM_Parameters.h"
#include "STORM_Frame.h"
#include "STORM_PageHandle.h"
#include "STORM_BufferManager.h"
#include <string.h>

class STORM_FileHandle
{
	// This information is stored in the first page (header page) of every file.
	typedef struct 
	{
		int numAllocatedPages;   // number of pages allocated to the file
		int numReservedPages;    // number of pages containing data

	} t_fileSubHeader;

	STORM_BufferManager *m_pBfrMgr;            // ptr to buffer manager
	int m_fileID;                              // file identifier (descriptor)
	char m_fileName[MAX_FILENAME_LENGTH];      // filename
	int m_filePos;                             // pageID that was last read
	t_fileSubHeader m_fileSubHeader;           // info stored in first file page
	bool m_isOpened;                           // if set then the file has been opened before
	STORM_Frame *m_pHeaderFrame;               // ptr to the frame containing the header page 
	
	// Private methods
	bool IsPageReserved(int pageID);           // checks if the specified page is reserved or free. 
	void SetPageReservationFlag (int pageID, bool flag);  // sets or unsets the page flag (free,reserved)
	void UpdateHeaderFrame();  // updates the file sub header info in the frame
	t_rc Open(const char *fname, STORM_BufferManager *pBfrMgr); // Opens the file with specified fname
	void InitFileHandle();
	t_rc Close(); // Closes the file

public:
	STORM_FileHandle();
	~STORM_FileHandle();

	int GetFileID() const {return m_fileID;}
	const char *GetFilename() const {return m_fileName;}
	int GetFilePos() const {return m_filePos;}
	
	int GetNumAllocatedPages() const {return m_fileSubHeader.numAllocatedPages;}
	int GetNumReservedPages() const {return m_fileSubHeader.numReservedPages;}
	bool IsOpened() const {return m_isOpened;}

	t_rc GetFirstPage(STORM_PageHandle &pageHandle);
	t_rc GetLastPage(STORM_PageHandle &pageHandle);
	t_rc GetNextPage(int curPageID, STORM_PageHandle &pageHandle);
	t_rc GetNextPage(STORM_PageHandle &pageHandle);
	t_rc GetPreviousPage(int curPageID, STORM_PageHandle &pageHandle);
	t_rc GetPage(int pageID, STORM_PageHandle &pageHandle);

	t_rc ReservePage(STORM_PageHandle &pageHandle); // reserves a new page 
	t_rc ReleasePage(int pageID);                   // releases a specific page

	t_rc MarkPageDirty (int pageID) const;
	t_rc MarkPageClean (int pageID);
	t_rc IsPageDirty (int pageID, bool &dirtyBit) const;
	t_rc UnpinPage (int pageID) const;

	t_rc FlushPage(int pageID) const ; 
	t_rc FlushAllPages() const; 

	//t_rc Clear();           // Clears the contents of the file (an empty file is produced)
	t_rc Reset();           // Sets the file pos to the begining of the file
	t_rc ReOpen();          // First closes the file and then opens it again

	//void SetBufferManager (STORM_BufferManager *ptr) {m_pBfrMgr = ptr;}
	//void SetFileID(int fid) {m_fileID = fid;}
	//void SetFilename(char *fname) {strcpy(m_fileName,fname);}
	//void SetOpenedFlag(bool flag) {m_isOpened = flag;}
	//t_rc Open(char *fname, STORM_BufferManager *pBfrMgr); // Opens the file with specified fname
	//t_rc PinPage (int pageID);

	friend class STORM_StorageManager;
};

#endif
