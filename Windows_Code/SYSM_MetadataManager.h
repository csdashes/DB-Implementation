#ifndef _SYSM_MetadataManager_h
#define _SYSM_MetadataManager_h

#include "REM.h"
#include "retcodes.h"
#include "SYSM_DatabaseManager.h"
#include "SYSM_Metadata.h"
#include <fstream>

class SYSM_MetadataManager {
	
private:
	SYSM_DatabaseManager *dbm;

public:
	SYSM_MetadataManager(SYSM_DatabaseManager *dbm);
	~SYSM_MetadataManager();
	t_rc RecordRelationMeta(SYSM_relmet *relation);
	t_rc RecordAttributeMeta(SYSM_attrmet *attribute);
};

#endif