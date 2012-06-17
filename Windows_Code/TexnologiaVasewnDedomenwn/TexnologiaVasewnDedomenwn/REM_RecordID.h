#ifndef _REM_RecordID_h
#define _REM_RecordID_h

#include "retcodes.h"

class REM_RecordID { 
  private:
	
	int pageID;
	int slot;
	
	bool isValid;
	
  public:
	
	REM_RecordID(); 
	REM_RecordID(int pageID, int slot); 
	~REM_RecordID();
	
	t_rc GetPageID(int &pageID) const; 
	t_rc GetSlot(int &slot) const; 
	t_rc SetPageID (int pageID); 
	t_rc SetSlot (int slot);
	
};
#endif