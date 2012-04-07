#include "REM_RecordFileScan.h"

REM_RecordFileScan::REM_RecordFileScan()	{
	this->value  = NULL;
	this->isOpen = false;
}

REM_RecordFileScan::~REM_RecordFileScan()	{
	
}

t_rc REM_RecordFileScan::OpenRecordScan ( REM_RecordFileHandle &rfh, 
					 t_attrType attrType,
					 int	attrLength, 
					 int	attrOffset, 
					 t_compOp	compOp, 
					 void *value)
{
	/* Check if file scan is open. */
	if (this->isOpen) { return REM_FSOPEN; }
	
	/* Initialise local copies of passed parameters. */
	this->rfh		 = &rfh;
	this->sfh		 = &rfh.sfh;
	this->attrType   = attrType;
	this->attrLength = attrLength;
	this->attrOffset = attrOffset;
	this->compOp	 = compOp;
	if (compOp != NO_OP) {
		this->value = malloc(attrLength);
		memcpy(this->value, value, attrLength);
	}
	
	/* Store a copy of the REM File Header for convenience. */
	this->remFileHeader = this->rfh->remFileHeader;
	
	/* Keep an iterator at first page to restore/continue scanning. Move it at the first record page. */
	t_rc rc = this->sfh->GetFirstPage(this->iterPageHandle);
	if (rc != OK) { return rc; }
	rc = this->sfh->GetNextPage(this->iterPageHandle);
	if (rc != OK) { return rc; }
	
	this->iterSlot = 0;
	
	/* Opened file scan successfully. */
	this->isOpen = true;
	return (OK);
}

t_rc REM_RecordFileScan::GetNextRecord(REM_RecordHandle &rh)	{
	/* Check if file scan is not open. */
	if (!this->isOpen) { return REM_FSCLOSED; }
	t_rc rc;
	
	
	do {
		char *pData;
		int pageID;
		REM_PageHeader iterPageHeader;
		
		rc = this->iterPageHandle.GetDataPtr(&pData);
		if (rc != OK) { return rc; }
		
		memcpy(&iterPageHeader, pData, REM_PAGEHEADER_SIZE);
		
		rc = iterPageHandle.GetPageID(pageID);
		if (rc != OK) { return rc; }
		
		for (; this->iterSlot < iterPageHeader.nRecords; this->iterSlot++) {
			
			if (SatisfiesConditions(&pData[REM_PAGEHEADER_SIZE+(this->iterSlot*this->remFileHeader.recordSize)])) {
				
				REM_RecordID rid(pageID, iterSlot+1);
								
				rc = this->rfh->ReadRecord(rid, rh);
				if (rc != OK) { return rc; }
				
				rc = this->sfh->UnpinPage(pageID);
				if (rc != OK) { return rc; }
				
				this->iterSlot++;
				
				return OK;
			}
		}
		
		/* Unpin the page we just searched. */
		rc = this->sfh->UnpinPage(pageID);
		if (rc != OK) { return rc; }
		
	} while (this->sfh->GetNextPage(this->iterPageHandle) != STORM_EOF);
	
	/* No more records. */
	return REM_FSEOF;
}

t_rc REM_RecordFileScan::CloseRecordScan()	{
	/* Check if file scan is not open. */
	if (!this->isOpen) { return REM_FSCLOSED; }
	
	/* Reset all values. */
	this->rfh		 = NULL;
	this->sfh		 = NULL;
	this->attrLength = NULL;
	this->attrOffset = NULL;
	
	/* Free value memory. */
	free(this->value);
	
	/* Reset slot iterator */
	this->iterSlot = 0;
	
	/* Closed file scan successfully. */
	this->isOpen = false;
	return OK;
}

bool REM_RecordFileScan::SatisfiesConditions(const char *pData) const {
	/* If operation is NO_OP return true */
	if (this->compOp == NO_OP) {
		return true;
	}
	
	int   i1 = 0, i2 = 0;
	const char *c1 = 0, *c2 = 0;
	/* Get value of attribute at offset attrOffset */
	switch (this->attrType) {
		case TYPE_INT:
			memcpy (&i1, &pData[this->attrOffset], 4);
			i2 = *(int *)this->value;
			break;
		case TYPE_STRING:
			c1 = &pData[this->attrOffset];
			c2 = (char *)this->value;	
			break;
	}
	
	bool isSatisfied = false;
	/* Check if data satisifies condition */
	switch (this->compOp) {
		case EQ_OP:
			switch (this->attrType) {
				case TYPE_INT:
					isSatisfied = (i1 == i2);
					break;
				case TYPE_STRING:
					isSatisfied = (strncmp(c1,c2,this->attrLength)==0);
					break;
			}
			break;
		case LT_OP:
			switch (this->attrType) {
				case TYPE_INT:
					isSatisfied = (i1 < i2);
					break;
				case TYPE_STRING:
					isSatisfied = (strncmp(c1,c2,this->attrLength)<0);
					break;
			}
			break;
		case GT_OP:
			switch (this->attrType) {
				case TYPE_INT:
					isSatisfied = (i1 > i2);
					break;
				case TYPE_STRING:
					isSatisfied = (strncmp(c1,c2,this->attrLength)>0);
					break;
			}
			break;
		case LE_OP:
			switch (this->attrType) {
				case TYPE_INT:
					isSatisfied = (i1 <= i2);
					break;
				case TYPE_STRING:
					isSatisfied = (strncmp(c1,c2,this->attrLength)<=0);
					break;
			}
			break;
		case GE_OP:
			switch (this->attrType) {
				case TYPE_INT:
					isSatisfied = (i1 >= i2);
					break;
				case TYPE_STRING:
					isSatisfied = (strncmp(c1,c2,this->attrLength)>=0);
					break;
			}
			break;
		case NE_OP:
			switch (this->attrType) {
				case TYPE_INT:
					isSatisfied = (i1 != i2);
					break;
				case TYPE_STRING:
					isSatisfied = (strncmp(c1,c2,this->attrLength)!=0);
					break;
			}
			break;
	}
	
	return isSatisfied;
}
