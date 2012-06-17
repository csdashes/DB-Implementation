#include "REM_RecordID.h"

REM_RecordID::REM_RecordID()	{
	this->pageID = -1;
	this->slot   = -1;	
	this->isValid = false;
}

REM_RecordID::REM_RecordID(int pageID, int slot)	{
	this->pageID  = pageID;
	this->slot    = slot;
	this->isValid = true;
	
	if (pageID <= 0 || slot <= 0) {
		this->isValid = false;
	}
}

REM_RecordID::~REM_RecordID()	{
	
}

t_rc REM_RecordID::GetPageID(int &pageID) const	{
	if (!this->isValid) { return REM_INVALIDRID; }
	
	pageID = this->pageID;
	return (OK);
}

t_rc REM_RecordID::GetSlot(int &slot) const	{
	if (!this->isValid) { return REM_INVALIDRID; }
	
	slot = this->slot;
	return (OK);
}

t_rc REM_RecordID::SetPageID (int pageID)	{
	if (pageID <= 0) {
		return REM_INVALIDRID;
	}
	
	this->pageID = pageID;
	this->isValid = true;
	return (OK);
}

t_rc REM_RecordID::SetSlot (int slot)	{
	if (slot <= 0) {
		return REM_INVALIDRID;
	}
	
	this->slot = slot;
	this->isValid = true;
	return (OK);
}
