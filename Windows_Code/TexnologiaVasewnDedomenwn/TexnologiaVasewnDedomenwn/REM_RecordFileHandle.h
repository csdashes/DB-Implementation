#ifndef _REM_RecordFileHandle_h
#define _REM_RecordFileHandle_h

#include "retcodes.h"
#include "STORM.h"
#include "REM_Components.h"
#include "REM_RecordID.h"
#include "REM_RecordHandle.h"

class REM_RecordFileHandle { 
  private:
	
	bool isOpen; // Is this file handler open?
	
	STORM_FileHandle sfh;
	
	int pageNum_FileHeader;
	
	REM_FileHeader remFileHeader;
	
	char *pData_FileHeader;
	
  public:
	
	REM_RecordFileHandle(); 
	~REM_RecordFileHandle();
	
	t_rc ReadRecord (const REM_RecordID &rid, REM_RecordHandle &rh); // cont at method problem!!!
	t_rc InsertRecord (const char *pData, REM_RecordID &rid); 
	t_rc DeleteRecord (const REM_RecordID &rid); 
	t_rc UpdateRecord (const REM_RecordHandle &rh);
	t_rc FlushPages () const;
	
	friend class REM_RecordFileManager;
	friend class REM_RecordFileScan;
};
#endif