#ifndef _REM_RecordFileScan_h
#define _REM_RecordFileScan_h

#include "retcodes.h"
#include "REM_Components.h"
#include "REM_RecordFileHandle.h"
#include "REM_RecordHandle.h"

class REM_RecordFileScan { 
  private:
	
	REM_RecordFileHandle *rfh;
	STORM_FileHandle *sfh;
	
	t_attrType attrType;
	int	attrLength;
	int	attrOffset; 
	t_compOp	compOp;
	void	*value;
	
	REM_FileHeader remFileHeader;
	
	STORM_PageHandle iterPageHandle;
	int iterSlot;
	
	bool isOpen;
	
	bool SatisfiesConditions(const char *pData) const;
	
  public:
	
	REM_RecordFileScan(); 
	~REM_RecordFileScan();
	
	t_rc OpenRecordScan (REM_RecordFileHandle &rfh, 
						 t_attrType attrType,
						 int	attrLength, 
						 int	attrOffset, 
						 t_compOp	compOp, 
						 void	*value);
	t_rc GetNextRecord(REM_RecordHandle &rh); 
	t_rc CloseRecordScan();
	
};
#endif