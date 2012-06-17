#ifndef _SYSM_Metadata_h
#define _SYSM_Metadata_h

#include "components.h"

//metadata for each table
typedef struct {
	const char *relName;
	int recSize;
	int attrNo;
	int indexNo;
}SYSM_relmet;

//metadata fore each record
typedef struct {
	const char *relName;
	const char *attrName;
	t_attrType attrType;
	int attrLength;
	int offset;
	int indexcode;
}SYSM_attrmet;


#endif