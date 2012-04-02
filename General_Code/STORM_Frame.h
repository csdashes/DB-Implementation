#ifndef _STORM_Frame_h
#define _STORM_Frame_h

#include "STORM_Parameters.h"

typedef struct
{
	int pageID;	// the id of the page
	char pageData[PAGE_DATA_SIZE];	// the page contents
} t_Page;

class STORM_Frame
{
	bool    m_reservedBit; // if set the frame is reserved
	int		m_fileID;	   // id of file occupying this frame
	bool	m_dirtyBit;	   // if set, page is dirty
	t_Page	m_page;		   // page associated to the frame
	int		m_pinCount;	   // number of pin operations applied to page (0 means unpinned)
	
	STORM_Frame *m_p_previous; // previous frame
	STORM_Frame *m_p_next;     // next frame

public:

	STORM_Frame();
	~STORM_Frame();

	bool IsReserved() const {return m_reservedBit;}
	bool IsPinned() const {if (m_pinCount) return true; else return false;}
	bool IsDirty() const {return m_dirtyBit;}
	int GetPinCount() const {return m_pinCount;}

	void SetDirty() {m_dirtyBit = true;}
	void UnsetDirty() {m_dirtyBit = false;}
	void SetReserved() {m_reservedBit = true;}
	void UnsetReserved() {m_reservedBit = false;}
	void IncreasePinCount() {m_pinCount++;}
	void DecreasePinCount() {m_pinCount--; if (m_pinCount<0) m_pinCount=0;}
	void ClearPinCount() {m_pinCount=0;}

	int GetFileID() const {return m_fileID;}
	int GetPageID() const {return m_page.pageID;}
	char *GetPageDataPtr() {return m_page.pageData;}
	void SetFileID(int fid) {m_fileID = fid;}
	void SetPageID(int pageID) {m_page.pageID = pageID;}
	
	void SetNext (STORM_Frame *pframe) {m_p_next = pframe;}
	void SetPrevious (STORM_Frame *pframe) {m_p_previous = pframe;}

	STORM_Frame *GetPrevious () {return m_p_previous;}
	STORM_Frame *GetNext () {return m_p_next;}
};

#endif
