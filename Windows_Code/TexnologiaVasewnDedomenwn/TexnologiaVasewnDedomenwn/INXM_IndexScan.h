#ifndef _INXM_IndexScan_h
#define _INXM_IndexScan_h

#include "retcodes.h"
#include "components.h"
#include "INXM_IndexHandle.h"

class INXM_IndexScan { 
private:
	bool flag;
	
	STORM_FileHandle *sfh;
	
	STORM_PageHandle rootPageHandle;
	
	bool isOpen;
	
	t_compOp compOp;
	void *key;
	
	INXM_FileHeader inxmFileHeader;
	
	STORM_PageHandle *lastLeafPageHandle;
	int lastKeySlot;
	
	int nextPageID;
	int nextSlot;
	
	int KeyCmp(void *key,void *key2);
	t_rc ReadData(int page, int slot, INXM_Data &node);
	t_rc ReadData(STORM_PageHandle pageHandle, int slot, INXM_Data &data);
	t_rc ReadNode(int page, int slot, INXM_Node &node);
	t_rc ReadNode(STORM_PageHandle pageHandle, int slot, INXM_Node &node);
	t_rc LoadNodeHeaders(int pageID, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader);
	t_rc LoadNodeHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader);
	t_rc FindLeaf(STORM_PageHandle rootPage,void *pData, STORM_PageHandle &leafPageHandle);
	t_rc FollowList(REM_RecordID &rid);
public:
	INXM_IndexScan(); 
	~INXM_IndexScan();
	
	t_rc OpenIndexScan(const INXM_IndexHandle &ih,
					   t_compOp compOp,
					   void *value);
	t_rc GetNextEntry(REM_RecordID &rid);
	t_rc CloseIndexScan();
};

#endif