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
	t_rc FindRecordInAttrMet(char *tName, const char *attrName, REM_RecordHandle &rh);
	t_rc UpdateRelmetIndexes(const char *tName, REM_RecordHandle &rh, int &indexNo, bool increase);
	t_rc UpdateAttrmetIndexNo(char *tName, const char *attrName, REM_RecordHandle &rh, t_attrType &attrType, int &attrLength, int indexNo);
	t_rc GetIndexNo(char *pData, int &indexNo);

public:

	SSQLM_DDL_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im, char *dbName);
	~SSQLM_DDL_Manager();

	

	t_rc CreateTable(const char *tname, const char *attributes);
	t_rc DropTable(char *tname);
	t_rc CreateIndex(char *tname, const char *attrName);
	t_rc DropIndex(char *tname, const char *attrName, int indexNo);

	// TESTING FUNCTION
	t_rc PrintFirstRecordInAttrMet( const char *tname );
};

#endif