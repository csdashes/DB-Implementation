#ifndef _SSQLM_DDL_Manager_h
#define _SSQLM_DDL_Manager_h

#include "REM.h"
#include "INXM.h"
#include "retcodes.h"

class SSQLM_DDL_Manager {

private:

	REM_RecordFileManager *rfm;
	INXM_IndexManager *im;

public:

	SSQLM_DDL_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im);
	~SSQLM_DDL_Manager();

	t_rc CreateTable(char *dbName, const char *tname, const char *attributes);
	t_rc DropTable(char *dbName, const char *tname);
	t_rc CreateIndex(char *dbName, const char *tname, int indexNo, t_attrType attrType, int attrLength);
	t_rc DropIndex(char *dbName, const char *tname, int indexNo);
};

#endif