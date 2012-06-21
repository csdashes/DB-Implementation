#ifndef _SSQLM_DML_Manager_h
#define _SSQLM_DML_Manager_h

#include "REM.h"
#include "INXM.h"
#include "retcodes.h"

class SSQLM_DML_Manager {

private:

	REM_RecordFileManager *rfm;
	INXM_IndexManager *im;
	REM_RecordFileHandle *attrmet;
	REM_RecordFileHandle *relmet;
	char *dbName;

	t_rc OpenRelmet(char *dbName);
	t_rc OpenAttrmet(char *dbName);
	//char *cacheAttr;
	//char *cacheRel;

	t_rc GetAttrInfo(char *rec, int &offset, char *&type, int &size, int &indexID);	//Retrieve attribute info
	t_rc GetConditionInfo(char *condition, char *&conditionAttribute, t_compOp &comp, char *&conditionValue);

public:

	SSQLM_DML_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im, char *dbName);
	~SSQLM_DML_Manager();

	t_rc Select();
	t_rc From();
	t_rc Where(const char *tName, char *conditions, REM_RecordID *ridsArray);
	t_rc Insert(const char *tName, const char *record);
	t_rc Delete();
	t_rc Update();

};

#endif