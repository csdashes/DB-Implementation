#ifndef _REM_RecordFileManager_h
#define _REM_RecordFileManager_h

#include "retcodes.h"
#include "STORM_StorageManager.h"
#include "REM_Components.h"
#include "REM_RecordFileHandle.h"

class REM_RecordFileManager {
	
private:
	
	STORM_StorageManager *sm;
	
public:
	
	REM_RecordFileManager  (STORM_StorageManager *sm);							// Constructor
	~REM_RecordFileManager ();													// Destructor
	
    t_rc CreateRecordFile  (const char *fname, int rs);							// Create a new file
    t_rc DestroyRecordFile (const char *fname);									// Destroy a file
    t_rc OpenRecordFile    (const char *fname, REM_RecordFileHandle &rfh);		// Open a file
    t_rc CloseRecordFile   (REM_RecordFileHandle &rfh);							// Close a file
	
};
#endif