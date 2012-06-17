#ifndef _INXM_IndexHandle_h
#define _INXM_IndexHandle_h

#include "retcodes.h"
#include "REM_RecordID.h"
#include "INXM_Components.h"
#include "STORM.h"

class INXM_IndexHandle {
private:
	
	bool isOpen; // Is this file handler open?
	
	STORM_FileHandle sfh;
		
	int pageNum_FileHeader;
	
	INXM_FileHeader inxmFileHeader;
	
	char *pData_FileHeader;
	
	bool LeafHasRoom(STORM_PageHandle pageHandle);
	int Cut(int length);
	int KeyCmp(void *key,void *key2);

    t_rc FindLeaf(int rootPageID, void *key, STORM_PageHandle &leafPageHandle);
	t_rc FindAndAppend(int rootPageID, void *key, const REM_RecordID &rid);
    t_rc FindLeafWithParent(int rootPageID, void *key, STORM_PageHandle &leafPageHandle, STORM_PageHandle &parentPageHandle);

    t_rc LoadInitHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader &initPageHeader);
	t_rc LoadNodeHeaders(int pageID, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader);
	t_rc LoadNodeHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader);
	t_rc UpdateNodeHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader initPageHeader, INXM_NodePageHeader nodePageHeader);
	t_rc ReadNode(int page, int slot, INXM_Node &node);
	t_rc ReadNode(STORM_PageHandle pageHandle, int slot, INXM_Node &node);
	t_rc ReadData(STORM_PageHandle pageHandle, int slot, INXM_Data &data);
    t_rc EditData(STORM_PageHandle pageHandle, int slot, INXM_Data data);
	t_rc EditNode(STORM_PageHandle pageHandle, int slot, INXM_Node node);
	t_rc WriteNode(STORM_PageHandle pageHandle, int insertPoint, void *key, int left, int slot);
	t_rc WriteNode(STORM_PageHandle pageHandle, void *key, int left, int slot);
	t_rc WriteData(STORM_PageHandle pageHandle, const REM_RecordID &rid, int &slot);
	t_rc CreateNodePage(STORM_PageHandle &pageHandle, int &pageID, int parent, int next, int previous, bool isLeaf);
	t_rc CreateLastDataPage(STORM_PageHandle &pageHandle);
	t_rc InsertIntoParent(int rootID, STORM_PageHandle leftPage, INXM_Node &keyNode, STORM_PageHandle rightPage);
	t_rc InsertIntoLeaf(STORM_PageHandle leafPageHandle, void *key, const REM_RecordID &rid);
	t_rc InsertIntoNoLeaf(STORM_PageHandle parentPage, int left_index, void* key, int rightPageID);
	t_rc StartNewTree(void *pData, const REM_RecordID &rid);
	t_rc InsertIntoLeafAfterSplitting(int rootID, STORM_PageHandle leafPageHandle, void *key, const REM_RecordID &rid);
	
public:
	INXM_IndexHandle(); 
	~INXM_IndexHandle();
	
	t_rc InsertEntry(void *key, const REM_RecordID &rid); 
	t_rc DeleteEntry(void *key, const REM_RecordID &rid); 
	t_rc FlushPages();
	
	void debug();
	
	friend class INXM_IndexManager;
	friend class INXM_IndexScan;
};

#endif