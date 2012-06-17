#include "REM_RecordHandle.h"

REM_RecordHandle::REM_RecordHandle()	{
	this->pData	  = NULL;
	this->isValid = false;	// This boolean can be changed only from REM Record File Handler.
}

REM_RecordHandle::~REM_RecordHandle()	{
	if (this->pData != NULL) {
		delete this->pData;
	}
}

t_rc REM_RecordHandle::GetData(char *&pData) const	{
	if (!this->isValid) { return REM_INVALIDREC; }
	
	pData = this->pData;
	
	return (OK);
}

t_rc REM_RecordHandle::GetRecordID(REM_RecordID &rid) const	{
	if (!this->isValid) { return REM_INVALIDREC; }
	
	rid = this->rid;
	
	return (OK);
}
