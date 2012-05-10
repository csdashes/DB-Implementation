#ifndef _SSQLM_DML_Manager_h
#define _SSQLM_DML_Manager_h

#include "REM.h"
#include "INXM.h"
#include "retcodes.h"

class SSQLM_DML_Manager {

private:

	REM_RecordFileManager *rfm;
	INXM_IndexManager *im;

	t_rc GetAttrInfo(char *rec, int &offset, char *type, int &size, int &indexID);	//Retrieve attribute info
public:

	SSQLM_DML_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im);
	~SSQLM_DML_Manager();

	t_rc Select();
	t_rc From();
	t_rc Where(const char *dbName, const char *tName, char *conditions, REM_RecordID *ridsArray);
	t_rc Insert(const char *dbName, const char *tName, const char *record);
	t_rc Delete();
	t_rc Update();

};

#endif