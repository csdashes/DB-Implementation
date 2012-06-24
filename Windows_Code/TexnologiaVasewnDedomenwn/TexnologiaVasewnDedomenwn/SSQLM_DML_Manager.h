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

	t_rc GetAttrInfo(char *rec, int &offset, char *&type, int &size, int &indexID);	//Retrieve attribute info
	t_rc GetConditionInfo(char *condition, char *&conditionAttribute, t_compOp &comp, char *&conditionValue);
	t_rc FindAttributeInAttrmet(const char *tName, char *attributeName, int &offset, char *&type, int &size, int &indexID);
	t_rc CheckIfTableHasIndexes(const char *tName,bool &hasIndexes);
	t_rc GetTableRecordSize(char *tName, int &recordSize);

public:

	SSQLM_DML_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im, char *dbName);
	~SSQLM_DML_Manager();

	t_rc Select(const char *tName, vector<char *> columns, vector<char *> recordsFromWhereFunction, vector<char *> *finalResults);
	t_rc Join(char *table1, char* table2, char *connectionAttribute);
	t_rc Where(const char *tName, char *conditions, vector<char *> *finalResultRecords, vector<REM_RecordID> *finalResultsRIDs);
	t_rc Insert(const char *tName, const char *record);
	t_rc Delete(const char *tName, REM_RecordID ridsToDelete, char *recordsToDelete);
	t_rc Update(const char *tName, vector<REM_RecordID> ridsFromWhere, char *setAttributes);

};

#endif