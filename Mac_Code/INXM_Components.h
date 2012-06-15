#ifndef _INXM_Components_h
#define _INXM_Components_h

#include "components.h"
#include "REM_RecordID.h"

typedef struct INXM_FileHeader {
	
	t_attrType attrType;
	
	int attrLength;
	
	int treeRoot;
	
	int lastDataPage;
		
} INXM_FileHeader;
#define INXM_FILEHEADER_SIZE sizeof(INXM_FileHeader)

typedef struct INXM_InitPageHeader {
	bool isData;
	
	int nItems;	// Number of items a page currently keeps 
	
} INXM_InitPageHeader;
#define INXM_INITPAGEHEADER_SIZE sizeof(INXM_InitPageHeader)

typedef struct INXM_NodePageHeader {
	bool isLeaf;
	
	int parent; // Parent page
	
	int next;   // Next page (neighbour)
	
	int previous;
} INXM_NodePageHeader;
#define INXM_NODEPAGEHEADER_SIZE sizeof(INXM_NodePageHeader)


#define REM_RECORDID_SIZE sizeof(REM_RecordID)

//template <class ItemType> struct INXM_Node {
//	ItemType key;
//	int left;
//	int right;
//	int next; // Used for queue.
//};
//#define INXM_NODE_SIZE(ItemType) sizeof(INXM_Node<ItemType>)

typedef struct INXM_Node {
	void *key;
	int left;
	int slot; // Used for leafs.
} INXM_Node;
#define INXM_NODE_SIZE sizeof(INXM_Node)

typedef struct INXM_Data {
	int pageID;
	int slot;
	int nextPageID;
	int nextSlot;
} INXM_Data;
#define INXM_DATA_SIZE sizeof(INXM_Data)

#endif