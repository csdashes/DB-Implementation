#ifndef _REM_RecordHandle_h
#define _REM_RecordHandle_h

#include "retcodes.h"
#include "REM_RecordID.h"
#include "STORM.h"

class REM_RecordHandle { 
  private:
	
	REM_RecordID rid;
	
	char *pData;
	
	bool isValid;
	
  public:
		
	REM_RecordHandle(); 
	~REM_RecordHandle();
	
	t_rc GetData(char *&pData) const; 
	t_rc GetRecordID(REM_RecordID &rid) const;
	
	friend class REM_RecordFileHandle;
};
#endif