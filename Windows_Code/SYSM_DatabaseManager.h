#ifndef _SYSM_DatabaseManager_h
#define _SYSM_DatabaseManager_h

#include <fstream>
//#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "REM.h"
#include "retcodes.h"
#include "SYSM_Metadata.h"

#include <direct.h>

class SYSM_DatabaseManager  {
	
private:

	bool open;
	const char *activedb;
	
	REM_RecordFileManager *rfm;
	
public:
	SYSM_DatabaseManager(REM_RecordFileManager *rfm);
	~SYSM_DatabaseManager();
	t_rc CreateDatabase(const char *dbName);
	t_rc DropDatabase (const char *dbName);
	t_rc OpenDatabase (const char *dbName);
	t_rc CloseDatabase ();
	const char *getdbName();
	bool isOpen();	
};
#endif


