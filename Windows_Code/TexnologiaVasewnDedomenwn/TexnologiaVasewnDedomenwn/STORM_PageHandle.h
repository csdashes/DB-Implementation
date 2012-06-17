#ifndef _STORM_PageHandle_h
#define _STORM_PageHandle_h

#include "retcodes.h"
#include "STORM_Frame.h"

class STORM_PageHandle
{
	bool m_opened;          // is the handle opened?
	STORM_Frame *m_pFrame;  // ptr to associated frame

	// Private methods
	t_rc Open(STORM_Frame *pFrame);    // open handle

public:

	STORM_PageHandle();
	~STORM_PageHandle();

	//STORM_PageHandle (const STORM_PageHandle &ph);            // copy constructor
    //STORM_PageHandle& operator= (const STORM_PageHandle &ph); // overload operator =

	t_rc Close();                            // close file handle  
	t_rc GetPageID(int &pageID) const;       // get the page id
	t_rc GetDataPtr(char **ppData) const;    // get the ptr to the data area

	friend class STORM_FileHandle;
};

#endif