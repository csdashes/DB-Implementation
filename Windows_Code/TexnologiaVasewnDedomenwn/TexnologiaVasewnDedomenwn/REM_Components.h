#ifndef _REM_Components_h
#define _REM_Components_h

#include "components.h"

typedef enum {
	NOT_FULL_PAGE,  /* A page that can hold additional records */
	FULL_PAGE		/* A page that cannot hold any more records */
} RM_PageType;

typedef struct REM_FileHeader {

	int nRecords;	// Number of records in the file.

	int recordSize;	// Size of each record.

	int recordsPerPage;	// Number of records a page can hold.
	
	bool isLastPageFull; // Stores the last STORM page ID that has free space for records. If lastPageID=-1 we can reserve a new page instantly.
	
} REM_FileHeader;
#define REM_FILEHEADER_SIZE sizeof(REM_FileHeader)

typedef struct REM_PageHeader {

	int nRecords;	// Number of records a page currently keeps
	
} REM_PageHeader;
#define REM_PAGEHEADER_SIZE sizeof(REM_PageHeader)

#endif