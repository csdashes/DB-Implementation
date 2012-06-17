/* ============================================================================================== */
// STORM_BufferManager.h
//
//
//
//
// -----------------------
// Last update: 8/4/2009 
/* ============================================================================================== */

#ifndef _STORM_BufferManager_h
#define _STORM_BufferManager_h

#include "retcodes.h"
#include "STORM_Parameters.h"
#include "STORM_Frame.h"
#include "UTILS_hashmap.h"

typedef HashMap<int,STORM_Frame*> t_hash_table;

class STORM_BufferManager
{
	int m_numFrames;          // number of frames (capacity)
	int m_numReservedFrames;  // number of reserved frames <= m_numFrames
	int m_numPinned;          // number of pinned pages
	
	int m_numHits;            // number of hits
	int m_numMisses;          // number of misses (page faults)
	int m_numRequests;        // number of total page requests
	int m_numWrites;          // number of disk writes
	int m_numReads;           // number of disk reads

	t_hash_table *m_p_hash_table;  // for fast page lookup

	// used for victim selection
	STORM_Frame *m_p_head; // head of frame queue
	STORM_Frame *m_p_tail; // tail of frame queue

public:

	STORM_BufferManager();
	~STORM_BufferManager ();

	void InitCounters() {m_numHits = 0; m_numMisses = 0; m_numRequests = 0; 
	                     m_numReads = 0; m_numWrites = 0;}
	void ClearBuffer();

	int GetNumFrames() {return (m_numFrames);}
	int GetNumHits() {return m_numHits;}
	int GetNumMisses() {return m_numMisses;}
	int GetNumRequests() {return m_numRequests;}
	int GetNumreads() {return m_numReads;}
	int GetNumWrites() {return m_numWrites;}
	int GetNumDiskAccesses() {return (m_numReads+m_numWrites);}
	int GetNumPinned() {return (m_numPinned);}

	t_rc NeedPage (int fileID, int page_id, STORM_Frame **ppframe);
	
	t_rc MarkPageDirty (int fileID, int pageID);
	t_rc MarkPageClean (int fileID, int pageID);
	t_rc IsPageDirty (int fileID, int pageID, bool &dirtyBit);
	t_rc IsPagePinned (int fileID, int pageID, bool &flag);
	t_rc PinPage (int fileID, int pageID);
	t_rc UnpinPage (int fileID, int pageID);

	t_rc FlushAllPages();                      // flush all pages of buffer  
	t_rc FlushFilePages (int fileID);          // flush all pages of a file 
	t_rc FlushPage (int fileID, int pageID);   // flush specific page

	t_rc ReserveFrame(int fileID, int pageID, STORM_Frame **ppFrame);  // reserve a frame  
	t_rc ReleaseFrame(STORM_Frame *pFrame, bool dirtyWrite); // release specified frame
	t_rc ReleaseFrame(int fileID, int pageID); // release specified frame (file and page are given)
	t_rc ReleaseFileFrames (int fid, bool dirtyWrite); // release all frames reserved for fid

	t_rc ReadPage(STORM_Frame *pFrame);
	t_rc WritePage(STORM_Frame *pFrame);

	STORM_Frame *LookupFrame (int fileID, int pageID);
	void DisplayBufferContents();
	void DisplayBufferStatistics();

	//void InsertPage (int fileID, int pageID);
	//bool DeletePage (int fileID, int pageID);


private:
	void ClearFrameQueue();
	void InsertPage (t_Page *p_page);
	void PageReplacement (t_Page *p_page);
	int CalculateTargetPageID (int fileID, int pageID);
};

#endif