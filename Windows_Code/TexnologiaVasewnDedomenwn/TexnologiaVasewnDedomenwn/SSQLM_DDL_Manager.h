#ifndef _SSQLM_DDL_Manager_h
#define _SSQLM_DDL_Manager_h

#include "REM.h"
#include "INXM.h"
#include "retcodes.h"

class SSQLM_DDL_Manager {

private:

	REM_RecordFileManager *rfm;
	INXM_IndexManager *im;
	REM_RecordFileHandle *attrmet;
	REM_RecordFileHandle *relmet;
	char *dbName;

	t_rc OpenRelmet(char *dbName);
	t_rc OpenAttrmet(char *dbName);

	t_rc EditAttrMet(char *str, const char *tName, int &recSize);
	t_rc EditRelMet(const char *tName, int recSize, int numOfColumns);
	t_rc DeleteTableMeta(const char *tName);
	t_rc FindRecordInRelMet(const char *tName, REM_RecordHandle &rh);
	t_rc FindRecordInAttrMet(const char *tName, REM_RecordHandle &rh);
	t_rc FindRecordInAttrMet(const char *tName, const char *attrName, REM_RecordHandle &rh);

public:

	SSQLM_DDL_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im, char *dbName);
	~SSQLM_DDL_Manager();

	t_rc CreateTable(const char *tname, const char *attributes);
	t_rc DropTable(const char *tname);
	t_rc CreateIndex(const char *tname, const char *attrName);
	t_rc DropIndex(const char *tname, int indexNo);
};

#endif