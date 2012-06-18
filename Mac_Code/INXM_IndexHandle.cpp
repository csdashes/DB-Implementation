#include "INXM_IndexHandle.h"

INXM_IndexHandle::INXM_IndexHandle() {
	this->isOpen=false;
}

INXM_IndexHandle::~INXM_IndexHandle() {
}

int INXM_IndexHandle::Cut(int length) {
	if (length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}

bool INXM_IndexHandle::LeafHasRoom(STORM_PageHandle pageHandle) {
	t_rc rc;
	INXM_InitPageHeader initPageHeader;
	
	rc = LoadInitHeaders(pageHandle, initPageHeader);
	if (rc != OK) { return rc; }
	
	int room = INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+(initPageHeader.nItems+1)*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength);
	
	if (PAGE_DATA_SIZE-room <=0 ) {
		return false;
	} else {
		return true;
	}

}

int INXM_IndexHandle::KeyCmp(void *key,void *key2) {
	
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

t_rc INXM_IndexHandle::LoadInitHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader &initPageHeader) {
	t_rc rc;
	
	/* Read headers. */
	char *nodePage;
	
	rc = pageHandle.GetDataPtr(&nodePage);
	if (rc != OK) { return rc; }
	
	/* init page header. */	
	memcpy(&initPageHeader, nodePage, INXM_INITPAGEHEADER_SIZE);
	
	return(OK);
}

t_rc INXM_IndexHandle::LoadNodeHeaders(int pageID, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader) {
	t_rc rc;
	STORM_PageHandle pageHandle;
	
	rc = this->sfh.GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	LoadNodeHeaders(pageHandle, initPageHeader, nodePageHeader);
	
	return(OK);
}

t_rc INXM_IndexHandle::LoadNodeHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader &initPageHeader, INXM_NodePageHeader &nodePageHeader) {
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

t_rc INXM_IndexHandle::UpdateNodeHeaders(STORM_PageHandle pageHandle, INXM_InitPageHeader initPageHeader, INXM_NodePageHeader nodePageHeader) {
	t_rc rc;
	
	/* Get page data pointer. */
	char *nodePage;
	
	rc = pageHandle.GetDataPtr(&nodePage);
	if (rc != OK) { return rc; }
	
	/* init page header. */	
	memcpy(nodePage, &initPageHeader, INXM_INITPAGEHEADER_SIZE);
	
	/* node page header. */	
	memcpy(&nodePage[INXM_INITPAGEHEADER_SIZE], &nodePageHeader, INXM_NODEPAGEHEADER_SIZE);
	return(OK);
	
}

t_rc INXM_IndexHandle::ReadNode(int pageID, int slot, INXM_Node &node) {
	t_rc rc;
	STORM_PageHandle pageHandle;
	
	rc = this->sfh.GetPage(pageID, pageHandle);
	if (rc != OK) { return rc; }
	
	rc = ReadNode(pageHandle, slot, node);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::ReadData(STORM_PageHandle pageHandle, int slot, INXM_Data &data) {
	t_rc rc;
	
	/* Read headers. */
	char *dataPage;
	
	rc = pageHandle.GetDataPtr(&dataPage);
	if (rc != OK) { return rc; }
	
	/* Read data. */
	memcpy(&data, &dataPage[INXM_INITPAGEHEADER_SIZE+slot*INXM_DATA_SIZE], INXM_DATA_SIZE);
		
	return(OK);
}

t_rc INXM_IndexHandle::ReadNode(STORM_PageHandle pageHandle, int slot, INXM_Node &node) {
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

t_rc INXM_IndexHandle::FindLeaf(int rootPageID, void *key, STORM_PageHandle &leafPageHandle) {
	t_rc rc;
	
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
	
	rc = this->sfh.GetPage(nodeRunner, leafPageHandle);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::FindLeafWithParent(int rootPageID, void *key, STORM_PageHandle &leafPageHandle, STORM_PageHandle &parentPageHandle) {
	t_rc rc;
	
	int nodeRunner = rootPageID;
    int parentRunner = rootPageID;
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
            parentRunner = nodeRunner;
			nodeRunner   = nodePageHeader.next;
		}
		rc = LoadNodeHeaders(nodeRunner, initPageHeader, nodePageHeader);
		if (rc != OK) { return rc; }
	}
	
	rc = this->sfh.GetPage(nodeRunner, leafPageHandle);
	if (rc != OK) { return rc; }
    
    rc = this->sfh.GetPage(parentRunner, parentPageHandle);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::FindAndAppend(int rootPageID, void *key, const REM_RecordID &rid) {
	t_rc rc;
	
	STORM_PageHandle leafPageHandle;
	rc = FindLeaf(rootPageID, key, leafPageHandle);
	if (rc != OK) { return rc; }
	
	INXM_InitPageHeader initPageHeader;
	rc = LoadInitHeaders(leafPageHandle, initPageHeader);
	if (rc != OK) { return rc; }
	
	INXM_Node node;
	int i;
	for (i=0; i < initPageHeader.nItems; i++) {
		rc = ReadNode(leafPageHandle, i, node);
		if (rc != OK) { return rc; }
		
		if (KeyCmp(key, node.key) == 0) {
			break;
		}
	}
	
	if (i == initPageHeader.nItems) {		
		return(INXM_KEY_NOT_FOUND);
	} else {
		
		int slotOfNewData;
		STORM_PageHandle lastDataPageHandle;
		
		rc = this->sfh.GetPage(this->inxmFileHeader.lastDataPage, lastDataPageHandle);
		if (rc != OK) { return rc; }
		
		/* Check if last data page has room. */
		
		INXM_InitPageHeader initPageHeader;
		
		rc = LoadInitHeaders(lastDataPageHandle, initPageHeader);
		if (rc != OK) { return rc; }
		
		int room = PAGE_DATA_SIZE-(INXM_INITPAGEHEADER_SIZE+(initPageHeader.nItems+1)*INXM_DATA_SIZE);
		
		/* If no room. Reserve new last data page. */
		if (room < 0 ) {
			CreateLastDataPage(lastDataPageHandle);
			
			/* Write last data. */
			WriteData(lastDataPageHandle, rid, slotOfNewData);
		} else if (room == 0) {
			/* Write last data. */
			WriteData(lastDataPageHandle, rid, slotOfNewData);
			
			CreateLastDataPage(lastDataPageHandle);
		} else {
			/* Write last data. */
			WriteData(lastDataPageHandle, rid, slotOfNewData);
		}

		
		/* Update last-1 link data. */
		STORM_PageHandle listDataPageHandle;
		this->sfh.GetPage(node.left, listDataPageHandle);
		
		INXM_Data data;
		ReadData(listDataPageHandle, node.slot, data);
		
		int fromWhere = node.slot;
		
		while (data.nextPageID != 0) {
			this->sfh.GetPage(data.nextPageID, listDataPageHandle);
			fromWhere = data.nextSlot;
			ReadData(listDataPageHandle, data.nextSlot, data);
		}
		
		data.nextPageID=this->inxmFileHeader.lastDataPage;
		data.nextSlot=slotOfNewData;
		
		/* Write updated last-1 data of list. */
		
		EditData(listDataPageHandle, fromWhere, data);
		
		return(OK);
	}
	
}

t_rc INXM_IndexHandle::EditData(STORM_PageHandle pageHandle, int slot, INXM_Data data) {
	t_rc rc;
	
	char *dataPageData;
	
	rc = pageHandle.GetDataPtr(&dataPageData);
	if (rc != OK) { return rc; }
	
	memcpy(&dataPageData[INXM_INITPAGEHEADER_SIZE+(slot*INXM_DATA_SIZE)], &data, INXM_DATA_SIZE);
	
	return(OK);
}

t_rc INXM_IndexHandle::EditNode(STORM_PageHandle pageHandle, int slot, INXM_Node node) {
	t_rc rc;
	
	char *dataPageData;
	
	rc = pageHandle.GetDataPtr(&dataPageData);
	if (rc != OK) { return rc; }
	
	memcpy(&dataPageData[INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+(slot*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength))], &node, INXM_NODE_SIZE+this->inxmFileHeader.attrLength);
    memcpy(&dataPageData[INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+(slot*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength))+INXM_NODE_SIZE], node.key, this->inxmFileHeader.attrLength);
	
	return(OK);
}

t_rc INXM_IndexHandle::WriteNode(STORM_PageHandle pageHandle, int insertPoint, void *key, int left, int slot) {
	t_rc rc;
	INXM_Node *node = new INXM_Node();
	
	node->left = left;
	node->slot = slot;
	
	char *nodePageData;
	
	rc = pageHandle.GetDataPtr(&nodePageData);
	if (rc != OK) { return rc; }
	
	/* Write data to correct place. */
	memcpy(&nodePageData[INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+insertPoint*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength)], node, INXM_NODE_SIZE);
	memcpy(&nodePageData[INXM_INITPAGEHEADER_SIZE+INXM_NODEPAGEHEADER_SIZE+insertPoint*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength)+INXM_NODE_SIZE], key, this->inxmFileHeader.attrLength);
	
	/* Update page header. */
	INXM_InitPageHeader pageHeader;
	
	rc = LoadInitHeaders(pageHandle, pageHeader);
	if (rc != OK) { return rc; }
	
	pageHeader.nItems++;
	
	memcpy(nodePageData, &pageHeader, INXM_INITPAGEHEADER_SIZE);
	
	int pageID;
	rc = pageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	/* Mark the page as dirty because we modified it */
	rc = this->sfh.MarkPageDirty(pageID);
	if (rc != OK) { return rc; }
	
	rc = this->sfh.FlushPage(pageID);
	if (rc != OK) { return rc; }
	
	/* Unpin the page */
	rc = this->sfh.UnpinPage (pageID);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::WriteNode(STORM_PageHandle pageHandle, void *key, int left, int slot) {
	t_rc rc;
	
	/* Read node header. */
	INXM_InitPageHeader pageHeader;
	
	rc = LoadInitHeaders(pageHandle, pageHeader);
	if (rc != OK) { return rc; }
	
	rc = WriteNode(pageHandle, pageHeader.nItems, key, left, slot);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::WriteData(STORM_PageHandle pageHandle, const REM_RecordID &rid, int &slot) {
	t_rc rc;
	
	INXM_Data *inxmData = new INXM_Data();
	
	rid.GetPageID(inxmData->pageID);
	rid.GetSlot(inxmData->slot);
	
	char *leafPageData;
	
	rc = pageHandle.GetDataPtr(&leafPageData);
	if (rc != OK) { return rc; }
	
	/* Read leaf header. */
	INXM_InitPageHeader pageHeader;
	
	memcpy(&pageHeader, leafPageData, INXM_INITPAGEHEADER_SIZE);
	
	/* Write data to correct place. */
	memcpy(&leafPageData[INXM_INITPAGEHEADER_SIZE+pageHeader.nItems*INXM_DATA_SIZE], inxmData, INXM_DATA_SIZE);
	
	/* Update page header. */
	slot = pageHeader.nItems;
	pageHeader.nItems++;
	
	memcpy(leafPageData, &pageHeader, INXM_INITPAGEHEADER_SIZE);
	
	int pageID;
	rc = pageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	/* Mark the page as dirty because we modified it */
	rc = this->sfh.MarkPageDirty(pageID);
	if (rc != OK) { return rc; }
	
	rc = this->sfh.FlushPage(pageID);
	if (rc != OK) { return rc; }
	
	/* Unpin the page */
	rc = this->sfh.UnpinPage (pageID);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::CreateNodePage(STORM_PageHandle &pageHandle, int &pageID, int parent, int next, int previous, bool isLeaf) {
	t_rc rc;
	
	rc = this->sfh.ReservePage(pageHandle);
	if (rc != OK) { return rc; }
	
	rc = pageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	char *pageData;
	
	rc = pageHandle.GetDataPtr(&pageData);
	if (rc != OK) { return rc; }
	
	/* Write index init page header */
	INXM_InitPageHeader pageHeader = { 0, 0 };
	
	memcpy(pageData, &pageHeader, INXM_INITPAGEHEADER_SIZE);
	
	/* Write index node page header */
	INXM_NodePageHeader nPageHeader = { isLeaf, parent, next, previous };
	
	memcpy(&pageData[INXM_INITPAGEHEADER_SIZE], &nPageHeader, INXM_NODEPAGEHEADER_SIZE);
	
	return(OK);
}

t_rc INXM_IndexHandle::CreateLastDataPage(STORM_PageHandle &pageHandle) {
	t_rc rc;
	int pageID;
	
	rc = this->sfh.ReservePage(pageHandle);
	if (rc != OK) { return rc; }
	
	rc = pageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	char *pageData;
	
	rc = pageHandle.GetDataPtr(&pageData);
	if (rc != OK) { return rc; }
	
	/* Write index init page header */
	INXM_InitPageHeader pageHeader = { 1, 0 };
	
	memcpy(pageData, &pageHeader, INXM_INITPAGEHEADER_SIZE);
	
	/* Update file header (new lastDataPage). */
	this->inxmFileHeader.lastDataPage = pageID;
	memcpy(this->pData_FileHeader, &this->inxmFileHeader, INXM_FILEHEADER_SIZE);
	
    rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
    if (rc != OK) { return rc; }
    
	rc = this->sfh.FlushPage(this->pageNum_FileHeader);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::InsertIntoLeaf(STORM_PageHandle leafPageHandle, void *key, const REM_RecordID &rid) {
	t_rc rc;
	/* Find correct point for the new key. */
	INXM_InitPageHeader initPageHeader;
	
	LoadInitHeaders(leafPageHandle, initPageHeader);
	
	INXM_Node node;
	int insertPoint = 0;

	rc = ReadNode(leafPageHandle, insertPoint, node);
	if (rc != OK) { return rc; }
	
	while (insertPoint < initPageHeader.nItems && KeyCmp(node.key, key) < 0) {
		insertPoint++;
		
		rc = ReadNode(leafPageHandle, insertPoint, node);
		if (rc != OK) { return rc; }
	}
		
	/* Open needed space at correct point. */
	
	int point = INXM_INITPAGEHEADER_SIZE + INXM_NODEPAGEHEADER_SIZE + insertPoint*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength);
	
	char *leafData;
	
	rc = leafPageHandle.GetDataPtr(&leafData);
	if (rc != OK) { return rc; }
	
	memcpy(&leafData[point + INXM_NODE_SIZE+this->inxmFileHeader.attrLength],
		   &leafData[point],
		   (initPageHeader.nItems-insertPoint)*(INXM_NODE_SIZE+this->inxmFileHeader.attrLength));
	
	/* Write new data. */
	int slot;
	STORM_PageHandle lastDataPageHandle;

	rc = this->sfh.GetPage(this->inxmFileHeader.lastDataPage, lastDataPageHandle);
	if (rc != OK) { return rc; }
	
 	rc = WriteData(lastDataPageHandle, rid, slot);
	if (rc != OK) { return rc; }
	
	/* Write new node. */
	rc = WriteNode(leafPageHandle, insertPoint, key, this->inxmFileHeader.lastDataPage, slot);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::InsertIntoNoLeaf(STORM_PageHandle parentPage, int left_index, void* key, int rightPageID) {
	
	INXM_InitPageHeader parentInitPageHeader;
	INXM_NodePageHeader parentNodePageHeader;
	LoadNodeHeaders(parentPage, parentInitPageHeader, parentNodePageHeader);
	
	if (left_index == parentInitPageHeader.nItems) {
		parentNodePageHeader.next = rightPageID;
		UpdateNodeHeaders(parentPage, parentInitPageHeader, parentNodePageHeader);
		return(OK);
	}
	
	WriteNode(parentPage, key, 0, 0);
	
	INXM_Node runNode;
	for (int i = parentInitPageHeader.nItems; i > left_index; i--) {	//Hack! we use the old nItems. We didnt update the header.
		ReadNode(parentPage, i-1, runNode);
		EditNode(parentPage, i, runNode);
	}
	
	runNode.key = key;
	runNode.left = rightPageID;
	runNode.slot = 0;
	
	EditNode(parentPage, left_index, runNode);
		
	return(OK);
}

t_rc INXM_IndexHandle::StartNewTree(void *pData, const REM_RecordID &rid) {
	t_rc rc;
	STORM_PageHandle rootPageHandle, dataPageHandle;
	
	/* Make root page. */
	rc = CreateNodePage(rootPageHandle, this->inxmFileHeader.treeRoot, 0, 0, 0, 1);
	if (rc != OK) { return rc; }
	
	/* Make first data page. */
	rc = CreateLastDataPage(dataPageHandle);
	if (rc != OK) { return rc; }
	
	/* Write node and data. And connect to each other. */
	int slot;
	rc = WriteData(dataPageHandle, rid, slot);
	if (rc != OK) { return rc; }
	
	
	rc = WriteNode(rootPageHandle, pData, this->inxmFileHeader.lastDataPage, slot);
	if (rc != OK) { return rc; }
	
	/* Update File Header. (New root) */
	memcpy(this->pData_FileHeader, &this->inxmFileHeader, INXM_FILEHEADER_SIZE);
	
    rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
    if (rc != OK) { return rc; }
    
	rc = this->sfh.FlushPage(this->pageNum_FileHeader);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::InsertIntoParent(int rootID, STORM_PageHandle leftPage, INXM_Node &keyNode, STORM_PageHandle rightPage) {
	t_rc rc;
	INXM_InitPageHeader leftInitPageHeader;
	INXM_NodePageHeader leftNodePageHeader;
	LoadNodeHeaders(leftPage, leftInitPageHeader, leftNodePageHeader);
	
	/* Case: new root. */
	if (leftNodePageHeader.parent == 0) {
		int rightPageID;
		rc = rightPage.GetPageID(rightPageID);
		if (rc != OK) { return rc; }
		
		STORM_PageHandle newRootPageHandle;
		CreateNodePage(newRootPageHandle, this->inxmFileHeader.treeRoot, 0, rightPageID, 0, 0);
		
		int leftPageID;
		rc = leftPage.GetPageID(leftPageID);
		if (rc != OK) { return rc; }
		
		rc = WriteNode(newRootPageHandle, keyNode.key, leftPageID, 0);
		if (rc != OK) { return rc; }
		
		/* Update inxm file header. */
		memcpy(this->pData_FileHeader, &this->inxmFileHeader, INXM_FILEHEADER_SIZE);
		
        rc = this->sfh.MarkPageDirty(this->pageNum_FileHeader);
        if (rc != OK) { return rc; }
        
		rc = this->sfh.FlushPage(this->pageNum_FileHeader);
		if (rc != OK) { return rc; }
		
		/* Write all. */
		rc = this->sfh.FlushAllPages();
		if (rc != OK) { return rc; }
		
		return(OK);
	}
		
	/* Case: leaf or node. (Remainder of
	 * function body.)  
	 */
	
	/* Find the parent's node index to the left 
	 * page.
	 */
	
	int leftPageID;
	rc = leftPage.GetPageID(leftPageID);
	if (rc != OK) { return rc; }
	
	int rightPageID;
	rc = rightPage.GetPageID(rightPageID);
	if (rc != OK) { return rc; }
	
	int left_index = 0;
	INXM_Node node;
	INXM_InitPageHeader parentInitPageHeader;
	INXM_NodePageHeader parentNodePageHeader;
	LoadNodeHeaders(leftNodePageHeader.parent, parentInitPageHeader, parentNodePageHeader);
	
	while (left_index < parentInitPageHeader.nItems) {
		ReadNode(leftNodePageHeader.parent, left_index, node);
		if (node.left == leftPageID) {
			break;
		}
		left_index++;
	}
	
	/* Simple case: the new key fits into the page. */
	STORM_PageHandle parentPageHandle;
	
	rc = this->sfh.GetPage(leftNodePageHeader.parent, parentPageHandle);
	if (rc != OK) { return rc; }
	
	if (LeafHasRoom(parentPageHandle)) {
		return InsertIntoNoLeaf(parentPageHandle, left_index, keyNode.key, rightPageID);
	}
	
	/* Harder case:  split a not leaf page, in order 
	 * to preserve the B+ tree properties.
	 */
	
//	return insert_into_node_after_splitting(root, parent, left_index, key, right);
	
	return(OK);
}

t_rc INXM_IndexHandle::InsertIntoLeafAfterSplitting(int rootID, STORM_PageHandle leafPageHandle, void *key, const REM_RecordID &rid) {
	t_rc rc;
	STORM_PageHandle newLeafPageHandle;
	int newLeafPageID;
	rc = CreateNodePage(newLeafPageHandle, newLeafPageID, 0, 0, 0, true);
	if (rc != OK) { return rc; }
	
	/* Read leaf page headers. */
	INXM_InitPageHeader leafPageHeader;
	rc = LoadInitHeaders(leafPageHandle, leafPageHeader);
	if (rc != OK) { return rc; }
	
	int insertPoint = 0;
	INXM_Node node;
	ReadNode(leafPageHandle, insertPoint, node);
	while (insertPoint < leafPageHeader.nItems  && KeyCmp(node.key, key) < 0) {
		ReadNode(leafPageHandle, insertPoint, node);
		insertPoint++;
	}
	
	INXM_Node **temp_nodes = (INXM_Node **)malloc(INXM_NODE_SIZE*leafPageHeader.nItems);
	
	int i,j;
	for (i=0, j=0; i < leafPageHeader.nItems; i++, j++) {
		if (j == insertPoint) j++;
		ReadNode(leafPageHandle, i, *(temp_nodes[j]));
	}
	
	/* Write new data to last page. */
	int newDataSlot;
	STORM_PageHandle lastDataPageHandle;
	rc = this->sfh.GetPage(this->inxmFileHeader.lastDataPage, lastDataPageHandle);
	if (rc != OK) { return rc; }
	
	rc = WriteData(lastDataPageHandle, rid, newDataSlot);
	if (rc != OK) { return rc; }
	
	/* Place new node to temp array. */
	INXM_Node *newNode = new INXM_Node();
	newNode->key = key;
	newNode->left = this->inxmFileHeader.lastDataPage;
	newNode->slot = newDataSlot;
	temp_nodes[insertPoint] = newNode;
	
	/* Reset leaf's nItems */
	char *tmpData;
	leafPageHandle.GetDataPtr(&tmpData);
	leafPageHeader.nItems = 0;
	memcpy(tmpData, &leafPageHeader, INXM_INITPAGEHEADER_SIZE);
	
	int maxNodeRoom = (PAGE_DATA_SIZE-INXM_INITPAGEHEADER_SIZE-INXM_NODEPAGEHEADER_SIZE)/(INXM_NODE_SIZE+this->inxmFileHeader.attrLength);
	int split = Cut(maxNodeRoom);
	
	/* Write new data. */
	for (i = 0; i < split; i++) {
		WriteNode(leafPageHandle, i, temp_nodes[i]->key, temp_nodes[i]->left, temp_nodes[i]->slot);
	}
	
	for (i = split, j = 0; i < maxNodeRoom; i++, j++) {
		WriteNode(newLeafPageHandle, j, temp_nodes[i]->key, temp_nodes[i]->left, temp_nodes[i]->slot);
	}

	free(temp_nodes); // This is not enough!
	
	INXM_NodePageHeader leafNodePageHeader;
	LoadNodeHeaders(leafPageHandle, leafPageHeader, leafNodePageHeader);

	INXM_NodePageHeader newLeafNodePageHeader;
	INXM_InitPageHeader newLeafPageHeader;
	rc = LoadNodeHeaders(newLeafPageHandle, newLeafPageHeader, newLeafNodePageHeader);
	if (rc != OK) { return rc; }
	
	int leafPageID;
	rc = leafPageHandle.GetPageID(leafPageID);
	if (rc != OK) { return rc; }
	
	newLeafNodePageHeader.next = leafNodePageHeader.next;
	newLeafNodePageHeader.previous = leafPageID;
	leafNodePageHeader.next = newLeafPageID;
	
	if (newLeafNodePageHeader.next != 0) {
		STORM_PageHandle nextLeafPageHandle;
		rc = this->sfh.GetPage(newLeafNodePageHeader.next, nextLeafPageHandle);
		if (rc != OK) { return rc; }
		
		INXM_InitPageHeader nextInitPageHeader;
		INXM_NodePageHeader nextNodePageHeader;
		LoadNodeHeaders(nextLeafPageHandle, nextInitPageHeader, nextNodePageHeader);
		
		nextNodePageHeader.previous = newLeafPageID;
		
		UpdateNodeHeaders(nextLeafPageHandle, nextInitPageHeader, nextNodePageHeader);
	}
	
	rc = UpdateNodeHeaders(leafPageHandle, leafPageHeader, leafNodePageHeader);
	if (rc != OK) { return rc; }
	rc = UpdateNodeHeaders(newLeafPageHandle, newLeafPageHeader, newLeafNodePageHeader);
	if (rc != OK) { return rc; }
	
	INXM_Node keyNode;
	rc = ReadNode(newLeafPageHandle, 0, keyNode);
	if (rc != OK) { return rc; }
	
	rc = InsertIntoParent(this->inxmFileHeader.treeRoot, leafPageHandle, keyNode, newLeafPageHandle);
	if (rc != OK) { return rc; }
	
	return(OK);
}

t_rc INXM_IndexHandle::InsertEntry(void *key, const REM_RecordID &rid) {
	t_rc rc;
		
	/* Tree does not exist yet.
	 * Start a new tree.
	 */	
	if (this->inxmFileHeader.treeRoot == 0) {
		return StartNewTree(key, rid);
	}
	
	/* If tree already exists. */
	
	/* If key exists, append. */
	rc = FindAndAppend(this->inxmFileHeader.treeRoot, key, rid);
	if (rc == OK) { return rc; }

	/* Else add new node and data to b+ tree. */
	STORM_PageHandle leafPageHandle;
	rc = FindLeaf(this->inxmFileHeader.treeRoot, key, leafPageHandle);
	if (rc != OK) { return rc; }
	
	/* Case: leaf has room for key and pointer.
	 */
	
	if (LeafHasRoom(leafPageHandle)) {
		return InsertIntoLeaf(leafPageHandle, key, rid);
	}
	
	/* Case: leaf must be split.
	 */
	
	return InsertIntoLeafAfterSplitting(this->inxmFileHeader.treeRoot, leafPageHandle, key, rid);
	
	return(OK);
}

t_rc INXM_IndexHandle::DeleteLeafEntry(STORM_PageHandle leafPageHandle, const REM_RecordID &rid) {
    
    return(OK);
}

t_rc INXM_IndexHandle::DeleteNodeEntry(STORM_PageHandle parentPageHandle, void *key) {
    t_rc rc;
    
    int keyRunner = 0;
    
    INXM_Node node;
    
	INXM_InitPageHeader initPageHeader;
	INXM_NodePageHeader nodePageHeader;
	
	LoadNodeHeaders(parentPageHandle, initPageHeader, nodePageHeader);

    while (keyRunner < initPageHeader.nItems) {
        rc = ReadNode(parentPageHandle, keyRunner, node);
        if (rc != OK) { return rc; }
        
        if (KeyCmp(key,node.key) > 0) {
            keyRunner++;
        } else {
            break;
        }
    }
    
    INXM_Node runNode;
	for (int i = keyRunner; i < (initPageHeader.nItems-1); i++) {
		ReadNode(parentPageHandle, i+1, runNode);
		EditNode(parentPageHandle, i, runNode);
	}
    
    /* Reduce number of nodes and update headers. */
    char *nodePageData;
	
	rc = parentPageHandle.GetDataPtr(&nodePageData);
	if (rc != OK) { return rc; }
    
    /* Update page header. */
	initPageHeader.nItems--;
	
	memcpy(nodePageData, &initPageHeader, INXM_INITPAGEHEADER_SIZE);
	
	int pageID;
	rc = parentPageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	/* Mark the page as dirty because we modified it */
	rc = this->sfh.MarkPageDirty(pageID);
	if (rc != OK) { return rc; }
	
	rc = this->sfh.FlushPage(pageID);
	if (rc != OK) { return rc; }
	
	/* Unpin the page */
	rc = this->sfh.UnpinPage (pageID);
	if (rc != OK) { return rc; }

    return(OK);
}

t_rc INXM_IndexHandle::DeleteEntry(void *key, const REM_RecordID &rid) {
    t_rc rc;
    
	/* Tree does not exist yet.
	 * Just exit.
	 */	
	if (this->inxmFileHeader.treeRoot == 0) {
		return(INXM_ISEMPTY);
	}
	
	/* If tree already exists. */
    /* Find leaf and parent node pages. */
    STORM_PageHandle leafPageHandle, parentPageHandle;
    
    rc = FindLeafWithParent(this->inxmFileHeader.treeRoot, key, leafPageHandle, parentPageHandle);
    if (rc != OK) { return rc; }
    
    int parentPageID;
    rc = parentPageHandle.GetPageID(parentPageID);
    if (rc != OK) { return rc; }
    
    /* If parent node is the root. */
    if (parentPageID == this->inxmFileHeader.treeRoot) {
        /* Got to leafs and check if there is a linked list. */
        rc = DeleteLeafEntry(leafPageHandle, rid);
        if (rc != OK) { return rc; }
        
        /* Delete key node. */
        rc = DeleteNodeEntry(parentPageHandle, key);
        if (rc != OK) { return rc; }
    }
    
	return(OK);
}

t_rc INXM_IndexHandle::FlushPages() {
	t_rc rc = this->sfh.FlushAllPages();
	if (rc != OK) { return rc; }
	
	return(OK);
}

void INXM_IndexHandle::debug() {
		
	/* Read root page. */
	INXM_InitPageHeader initPageHeader;
	INXM_NodePageHeader nodePageHeader;
	LoadNodeHeaders(this->inxmFileHeader.treeRoot, initPageHeader, nodePageHeader);
	printf("Root stats\n");
	
	printf("initHeader: is_data:%d, nItems: %d \n", initPageHeader.isData, initPageHeader.nItems);
	printf("nodeHeader: isLeaf:%d, next: %d, parent: %d \n", nodePageHeader.isLeaf, nodePageHeader.next, nodePageHeader.parent);
	
	INXM_Node node;
	int runner = 0;
	ReadNode(this->inxmFileHeader.treeRoot, runner, node);
	
	while (runner < initPageHeader.nItems) {
		printf("node %d::: key:%d, left:%d, slot:%d\n", runner, *(int *)node.key, node.left, node.slot);
		runner++;
		ReadNode(this->inxmFileHeader.treeRoot, runner, node);
	}
	
	/* Read lastData page. */
	runner = 0;
	
	STORM_PageHandle lastDataPageHandle;
	this->sfh.GetPage(this->inxmFileHeader.lastDataPage, lastDataPageHandle);
	
	INXM_Data data;
	INXM_InitPageHeader lastDataInitPageHeader;
	LoadInitHeaders(lastDataPageHandle, lastDataInitPageHeader);
	
	printf("Last Data stats\n");
	printf("initHeader: is_data:%d, nItems: %d \n", lastDataInitPageHeader.isData, lastDataInitPageHeader.nItems);
	
	ReadData(lastDataPageHandle, runner, data);
	while (runner < lastDataInitPageHeader.nItems) {
		printf("Data %d::: pageID:%d, slot:%d, nextPageID:%d, nextSlot:%d\n", runner, data.pageID, data.slot, data.nextPageID, data.nextSlot);
		runner++;
		ReadData(lastDataPageHandle, runner, data);
	}
	
}
















