#include "INXM_IndexScan.h"

INXM_IndexScan::INXM_IndexScan() {
	this->isOpen = false;
	this->key    = NULL;
	this->nextPageID = 0;
	this->nextSlot = 0;
	this->lastLeafPageHandle = NULL;
	this->flag = false;
}

INXM_IndexScan::~INXM_IndexScan() {
}

int INXM_IndexScan::KeyCmp(void *key,void *key2) {
	
	if (this->inxmFileHeader.attrType == TYPE_STRING) {
		return strcmp((char *)key, (char *)key2);
	} else {
		if ((*(int *)key) < (*(int *)key2)) {
			return -1;
		} else if ((*(int *)key) > (*(int *)key2)) {
			return 1;
		}
	}
	
	return 0;
}

t_rc INXM_IndexScan::ReadData(int pageID, int slot, INXM_Data &node) {
	t_rc rc;
	STORM_PageHandle pageHandle;
	
	rc = this->sfh->GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	rc = ReadData(pageHandle, slot, node);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexScan::ReadData(STORM_PageHandle pageHandle, int slot, INXM_Data &data) {
	t_rc rc;
	
	/* Read headers. */
	char *dataPage;
	
	rc = pageHandle.GetDataPtr(&dataPage);
	if (rc != OK) { return rc; }
	
	/* Read data. */
	memcpy(&data, &dataPage[INXM_INITPAGEHEADER_SIZE+slot*INXM_DATA_SIZE], INXM_DATA_SIZE);
	
	return(OK);
}

t_rc INXM_IndexScan::ReadNode(int pageID, int slot, INXM_Node &node) {
	t_rc rc;
	STORM_PageHandle pageHandle;
	
	rc = this->sfh->GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	rc = ReadNode(pageHandle, slot, node);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexScan::ReadNode(STORM_PageHandle pageHandle, int slot, INXM_Node &node) {
	t_rc rc;
	
	/* Read headers. */
	char *nodePage;
	
	rc = pageHandle.GetDataPtr(&nodePage);
	if (rc != OK) { return rc; }
	
	/* Read node. */
	memcpy(&node, &nodePage[INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+slot*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength)], INXM_NODE_SIZE);
	
	/* Read key. */
	void *key = malloc(this->inxmFileHeader.attrLength);
	
	memcpy(key, &nodePage[INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+slot*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength)+INXM_NODE_SIZE], this->inxmFileHeader.attrLength);
	
	/* Update key pointer. */
	node.key = key;
	
	return(OK);
}

t_rc INXM_IndexScan::LoadNodeHeaders(int pageID, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader) {
	t_rc rc;
	STORM_PageHandle pageHandle;
	
	rc = this->sfh->GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	LoadNodeHeaders(pageHandle, initPageHeader, nodePageHeader);
	
	return(OK);
}

t_rc INXM_IndexScan::LoadNodeHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader) {
	t_rc rc;
	
	/* Read headers. */
	char *nodePage;
	
	rc = pageHandle.GetDataPtr(&nodePage);
	if (rc != OK) { return rc; }
	
	/* init page header. */	
	memcpy(&initPageHeader, nodePage, INXM_INITPAGEHEADER_SIZE);
	
	/* node page header. */	
	memcpy(&nodePageHeader, &nodePage[INXM_INITPAGEHEADER_SIZE], INXM_NODEPAGEHEADER_SIZE);
	return(OK);
}

t_rc INXM_IndexScan::FindLeaf(STORM_PageHandle rootPage, void *key, STORM_PageHandle &leafPageHandle) {
	t_rc rc;
	
	int rootPageID;
	rc = rootPage.GetPageID(rootPageID);
	if (rc != OK) { return rc; }
	
	int nodeRunner = rootPageID;
	INXM_Node node;
	
	INXM_InitPageHeader initPageHeader;
	INXM_NodePageHeader nodePageHeader;
	
	LoadNodeHeaders(nodeRunner, initPageHeader, nodePageHeader);
	
	while (!nodePageHeader.isLeaf) {
		int keyRunner = 0;
		
		while (keyRunner < initPageHeader.nItems) {
			rc = ReadNode(nodeRunner, keyRunner, node);
			if (rc != OK) { return rc; }
			
			if (KeyCmp(key,node.key) > 0) {
				keyRunner++;
			} else {
				break;
			}
		}
		
		nodeRunner = node.left;
		if (keyRunner == initPageHeader.nItems) {
			nodeRunner = nodePageHeader.next;
		}
		rc = LoadNodeHeaders(nodeRunner, initPageHeader, nodePageHeader);
		if (rc != OK) { return rc; }
	}
	
	rc = this->sfh->GetPage(nodeRunner, leafPageHandle);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexScan::FollowList(REM_RecordID &rid) {
	INXM_Data data;
	ReadData(this->nextPageID, this->nextSlot, data);
	
	rid.SetPageID(data.pageID);
	rid.SetSlot(data.slot);
	
	if (data.nextPageID != 0) {
		this->nextPageID = data.nextPageID;
		this->nextSlot = data.nextSlot;
	}
	
	return(OK);
}

t_rc INXM_IndexScan::OpenIndexScan(const INXM_IndexHandle &ih, t_compOp compOp, void *value) {
	
	/* Check if file scan is open. */
	if (this->isOpen) { return INXM_FSOPEN; }
	
	this->compOp = compOp;
	memcpy(&this->inxmFileHeader, &ih.inxmFileHeader, INXM_FILEHEADER_SIZE);
	this->key = malloc(this->inxmFileHeader.attrLength);
	memcpy(this->key, value, this->inxmFileHeader.attrLength);
	
	this->sfh = (STORM_FileHandle*)&ih.sfh;
	
	/* Check if the index is initialized and have records inside. */
	if (this->inxmFileHeader.treeRoot == 0) {
		return(INXM_INVALIDINDEX);
	}

	/* Load root page. */
	this->sfh->GetPage(this->inxmFileHeader.treeRoot, this->rootPageHandle);
	
	/* Opened file scan successfully. */
	this->isOpen = true;
	return(OK);
}

t_rc INXM_IndexScan::GetNextEntry(REM_RecordID &rid) {
	/* Check if file scan is not open. */
	if (!this->isOpen) { return(INXM_FSCLOSED); }
	t_rc rc;
	
	if (this->nextPageID != 0) {
		return FollowList(rid);
	}
	
	if (this->flag) {
		/* No more records. */
		return(INXM_FSEOF);
	}
	
	int runner = 0;
	INXM_Node node;
	INXM_InitPageHeader leafInitPageHeader;
	INXM_NodePageHeader leafNodePageHeader;
	
	/* Get key node and the leaf page. */
	if (this->lastLeafPageHandle == NULL) {
		STORM_PageHandle leafPageHandle;
		rc = FindLeaf(this->rootPageHandle, this->key, leafPageHandle);
		if (rc != OK) { return rc; }
		
		rc = LoadNodeHeaders(leafPageHandle, leafInitPageHeader, leafNodePageHeader);
		if (rc != OK) { return rc; }
		
		do {
			ReadNode(leafPageHandle, runner, node);
			runner++;
			printf("%s\n",(char*)node.key);
			printf("%s\n",(char*)this->key);
		} while (KeyCmp(node.key, this->key) != 0 && runner < leafInitPageHeader.nItems);
		
		if (runner >= leafInitPageHeader.nItems) {
			this->flag = true;
			return(INXM_FSEOF);
		}

		if (this->compOp == LE_OP || this->compOp == GE_OP) {
			runner--;
		}
		
		this->lastLeafPageHandle = &leafPageHandle;
		this->lastKeySlot = runner;		
	}
		
	switch (this->compOp) {
		case EQ_OP:
			runner = 0;
						
			INXM_Data data;
			ReadData(node.left, node.slot, data);
			
			rid.SetPageID(data.pageID);
			rid.SetSlot(data.slot);
			
			this->nextPageID = data.nextPageID;
			this->nextSlot = data.nextSlot;
			
			this->flag = true;
			
			return(OK);
		case LT_OP:
		case LE_OP:
			
			LoadNodeHeaders(*this->lastLeafPageHandle, leafInitPageHeader, leafNodePageHeader);
			
			if (this->lastKeySlot == leafInitPageHeader.nItems) {
				this->sfh->GetPage(leafNodePageHeader.next, *this->lastLeafPageHandle);
				this->lastKeySlot = 0;
			}
			
			ReadNode(*this->lastLeafPageHandle, this->lastKeySlot, node);
			
			ReadData(node.left, node.slot, data);
				
			rid.SetPageID(data.pageID);
			rid.SetSlot(data.slot);
			
			this->lastKeySlot++;
			this->nextPageID = data.nextPageID;
			this->nextSlot = data.nextSlot;
				
			if (leafNodePageHeader.next == 0) {
				this->flag = true;
			}
			
			return(OK);
				
		case GT_OP:
		case GE_OP:
			
			LoadNodeHeaders(*this->lastLeafPageHandle, leafInitPageHeader, leafNodePageHeader);
			
			if (this->lastKeySlot == 0) {
				this->sfh->GetPage(leafNodePageHeader.next, *this->lastLeafPageHandle);
				this->lastKeySlot = leafInitPageHeader.nItems;
				this->lastKeySlot--;
			}
			
			ReadNode(*this->lastLeafPageHandle, this->lastKeySlot, node);
			
			ReadData(node.left, node.slot, data);
			
			rid.SetPageID(data.pageID);
			rid.SetSlot(data.slot);
			
			this->lastKeySlot--;
			this->nextPageID = data.nextPageID;
			this->nextSlot = data.nextSlot;
			
			if (leafNodePageHeader.previous == 0) {
				this->flag = true;
			}
			
			return(OK);
		case NE_OP:
			
			break;
		case NO_OP:
			
			break;
	}
	
	return(OK);
}

t_rc INXM_IndexScan::CloseIndexScan() {
	/* Check if file scan is not open. */
	if (!this->isOpen) { return INXM_FSCLOSED; }
	
	free(this->key);
	
	/* Closed file scan successfully. */
	this->isOpen = false;
	return(OK);
}
