#include "SSQLM_DDL_Manager.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

SSQLM_DDL_Manager::SSQLM_DDL_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im, char *dbName){
	this->rfm = rfm;
	this->im = im;
	this->dbName = (char*)malloc(sizeof(char)*strlen(dbName));
	strcpy(this->dbName,dbName);
	t_rc rc = OpenRelmet(dbName);
	rc = OpenAttrmet(dbName);
}

SSQLM_DDL_Manager::~SSQLM_DDL_Manager(){
}

t_rc SSQLM_DDL_Manager::CreateTable(const char *tName, const char *attributes){
	
	
	char pathname[255];

	// Count the attributes
	int recSize = 0;
	int numOfColumns=0;
	char *str;
	t_rc rc;

	int attrlength = strlen(attributes);
	str = (char*)malloc(attrlength);
	strcpy(str,attributes);
	char *tokens;
	char *nextToken;

	tokens = strtok_s (str,",", &nextToken);					//split the attributes
	while (tokens != NULL)
	{
		numOfColumns++;
		tokens = strtok_s (NULL, ",", &nextToken);
	}

	strcpy(str,attributes);
	// Calculate record size and insert the attributes in attr.met
	rc = EditAttrMet(str,tName,recSize);
	if (rc != OK) { return rc; }

	// Create and insert the record for the rel.met
	rc = EditRelMet(tName,recSize,numOfColumns);
	if (rc != OK) { return rc; }

	// Create the database file
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = rfm->CreateRecordFile(pathname,recSize);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DropTable(char *tName){

	t_rc rc;
	char pathname[50];
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = rfm->DestroyRecordFile(pathname);
	if (rc != OK) { return rc; }

	rc = DeleteTableMeta(tName);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::CreateIndex(char *tName, const char *attrName){

	t_attrType attrType;
	int attrLength;
	int indexNo;
	REM_RecordHandle rh;
	REM_RecordHandle *rh2 = new REM_RecordHandle();
	char pathname[50];
	t_rc rc;

	// UPDATE THE REL.MET FILE
	rc = UpdateRelmetIndexes(tName,rh,indexNo,true); //true means that it will increase the number of indexes
	if (rc != OK) { return rc; }

	// UPDATE THE ATTR.MET FILE
	rc = UpdateAttrmetIndexNo(tName,attrName,*rh2,attrType,attrLength,indexNo);
	if (rc != OK) {return rc; }

	// Create the index file
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = im->CreateIndex(pathname,indexNo,attrType,attrLength);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DropIndex(char *tName, const char *attrName, int indexNo){
	REM_RecordHandle rh;
	REM_RecordHandle rh2;
	INXM_IndexHandle ih;
	t_attrType attrType;
	int attrLength;

	char pathname[50];

	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = im->DestroyIndex(pathname,indexNo);
	if (rc != OK) { return rc; }

	// Decrease the number of indexes in rel.met
	rc = UpdateRelmetIndexes(tName,rh,indexNo,false);
	if (rc != OK) { return rc; }

	// Change the code of the index -1
	rc = UpdateAttrmetIndexNo(tName,attrName,rh2,attrType,attrLength,-1); 
	if (rc != OK) { return rc; }

	return OK;
}


// PRIVATE FUNCTIONS

t_rc SSQLM_DDL_Manager::OpenRelmet(char *dbName){
	char pathname[255];
	this->relmet = new REM_RecordFileHandle();
	_snprintf_s(pathname,sizeof(pathname),"%s/rel.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*relmet);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::OpenAttrmet(char *dbName){
	char pathname[255];
	this->attrmet = new REM_RecordFileHandle();
	_snprintf_s(pathname,sizeof(pathname),"%s/attr.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*attrmet);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::EditAttrMet(char *str, const char *tName, int &recSize){

	char *tokens;
	char *nextToken;

	tokens = strtok_s (str, ", ()", &nextToken);
	char *char_token;
	char *attrName;
	char buffer[5];
	char attr_string[200];
	REM_RecordID rid;
	t_rc rc;

	while (tokens != NULL)
	 {
		 if(strcmp(tokens,"INT") == 0){

			 // Create the attribute record
			 strcpy(attr_string,tName);
			 strcat(attr_string,";");
			 strcat(attr_string,attrName);
			 strcat(attr_string,";");
			 strcat(attr_string,itoa(recSize,buffer,10));
			 strcat(attr_string,";");
			 strcat(attr_string,"TYPE_INT");
			 strcat(attr_string,";");
			 strcat(attr_string,"4");
			 strcat(attr_string,";");
			 strcat(attr_string,"-1;;");

			 // Insert the record
			 rc = attrmet->InsertRecord(attr_string,rid);
			 if (rc != OK) { return rc; }

			 recSize+=4;
		 }
		 else if(strcmp(tokens,"CHAR") == 0){

			 char_token = strtok_s (NULL, ", ()", &nextToken);
			 
			 // Create the attribute record
			 strcpy(attr_string,tName);
			 strcat(attr_string,";");
			 strcat(attr_string,attrName);
			 strcat(attr_string,";");
			 strcat(attr_string,itoa(recSize,buffer,10));
			 strcat(attr_string,";");
			 strcat(attr_string,"TYPE_STRING");
			 strcat(attr_string,";");
			 strcat(attr_string,char_token);
			 strcat(attr_string,";");
			 strcat(attr_string,"-1;;");

			 // Insert the record
			 rc = attrmet->InsertRecord(attr_string,rid);
			 if (rc != OK) { return rc; }

			 recSize += atoi(char_token);
		 }
		 else{
			 attrName = tokens;
		 }
		 tokens = strtok_s (NULL, ", ()", &nextToken);
	 }

	rc = attrmet->FlushPages();
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::EditRelMet(const char *tName, int recSize, int numOfColumns){

	char buffer[5];
	REM_RecordID rid;
	t_rc rc;
	char record_string[256];

	strcpy(record_string,tName);					//table name
	strcat(record_string,";");
	strcat(record_string,itoa(recSize,buffer,10));	//record size
	strcat(record_string,";");
	strcat(record_string,itoa(numOfColumns,buffer,10));		//amount of columns. (To 10 shmainei 10diko systhma ari8mishs)
	strcat(record_string,";");
	strcat(record_string,"0;;");					//amount of indexes (init:0)

	// Insert the record
	rc = relmet->InsertRecord(record_string,rid);
	if (rc != OK) { return rc; }

	rc = relmet->FlushPages();
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::FindRecordInRelMet(const char *tName, REM_RecordHandle &rh){
	t_rc rc;
	REM_RecordFileScan *rfs = new REM_RecordFileScan();

	int tNameLength = strlen(tName);
	rc = rfs->OpenRecordScan(*relmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	// Close FileScan
	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::FindRecordInAttrMet(const char *tName, REM_RecordHandle &rh){
	t_rc rc;
	REM_RecordFileScan *rfs = new REM_RecordFileScan();

	int tNameLength = strlen(tName);
	rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	// Close FileScan
	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::FindRecordInAttrMet(char *tName,const char *attrName, REM_RecordHandle &rh){

	t_rc rc;
	REM_RecordFileScan *rfs = new REM_RecordFileScan();

	int attributeLength = strlen(attrName);
	int tableLength = strlen(tName);
	int tableNattributeLength = attributeLength + tableLength + 1;

	char *tableANDattribute = (char *)malloc(tableNattributeLength);
	strcpy(tableANDattribute,tName);
	strcat(tableANDattribute,";");
	strcat(tableANDattribute,attrName);

	rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,tableNattributeLength, 0, EQ_OP, tableANDattribute);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	// Close FileScan
	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DeleteTableMeta(const char *tName){
	
	char *pData;
	int indexNo;
	t_rc rc;
	REM_RecordHandle rh;
	REM_RecordID rid;

	// Delete metadata from rel.met
	rc = FindRecordInRelMet(tName,rh);
	if (rc != OK) {return rc; }

	rc = rh.GetRecordID(rid);
	if (rc != OK) {return rc; }

	rc = relmet->DeleteRecord(rid);
	if (rc != OK) {return rc; }

	// NOT SURE
	rc = relmet->FlushPages();
	if (rc != OK) {return rc; }

	// Delete metadata from attr.met
	while(FindRecordInAttrMet(tName,rh) != STORM_EOF){
		rc = rh.GetRecordID(rid);
		if (rc != OK) {return rc; }

		rc = rh.GetData(pData);
		if (rc != OK) {return rc; }

		rc = GetIndexNo(pData,indexNo);
		if (rc != OK) {return rc; }

		if(indexNo!=-1){
			// DELETE THE INDEX FILE
			char pathname[50];

			_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
			t_rc rc = im->DestroyIndex(pathname,indexNo);
			if (rc != OK) { return rc; }
		}
		rc = attrmet->DeleteRecord(rid);
		if (rc != OK) {return rc; }

		// NOT SURE
		rc = attrmet->FlushPages();
		if (rc != OK) {return rc; }
	}


	return OK;
}

t_rc SSQLM_DDL_Manager::UpdateRelmetIndexes(const char *tName, REM_RecordHandle &rh, int &indexNo, bool increase){

	// UPDATE THE REL.MET FILE
	char new_relmet_record[256];							
	int i = 1;					
	char buffer[5];
	t_rc rc;
	char *pData;

	// Find the record with table name "tName"
	rc = FindRecordInRelMet(tName,rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }


	// Raise the number of indexes
	indexNo = 0;										//number of indexes
	char *nextToken;
	char *tokens = strtok_s (pData,";", &nextToken);	//split the recordData
	while (tokens != NULL)
	{
		 if( i == 4 ){									//the field of the indexes number
			 indexNo = atoi(tokens);
			 if(increase)
				 indexNo++;								//increase the number of indexes
			 else
				 indexNo--;								//decrease the number of indexes
			 tokens = itoa(indexNo,buffer,10);
		 }
		 if( i == 1 ){
			strcpy(new_relmet_record,tokens);
			strcat(new_relmet_record,";");
		 }
		 else{
			strcat(new_relmet_record,tokens);
			strcat(new_relmet_record,";");
		 }
		 i++;
		 tokens = strtok_s (NULL, ";", &nextToken);
	}
	strcat(new_relmet_record,";");
	memcpy(pData, new_relmet_record, 256);

	// Update the record
	rc = relmet->UpdateRecord(rh);
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::UpdateAttrmetIndexNo(char *tName, const char *attrName, REM_RecordHandle &rh, t_attrType &attrType, int &attrLength, int indexNo){
	int i = 1;
	char *new_attrmet_record;						
	char buffer[5];
	t_rc rc;
	char *pData;
	//Find the record with attribute name "attrName"
	rc = FindRecordInAttrMet(tName,attrName,rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }

	char *data = (char *)malloc(strlen(pData));
	strcpy(data,pData);

	new_attrmet_record = (char*)malloc(strlen(pData));

	char *nextToken;
	// Retrieve attrType and attrLength and create the new attribute record raising the index number.
	char *tokens = strtok_s (data,";", &nextToken);	//split the recordData
	i = 1;
	while (tokens != NULL)
	{
		 if( i == 4 ){	//the field of the indexes number
			 if( strcmp(tokens,"TYPE_STRING") == 0 )
				 attrType = TYPE_STRING;
			 else
				 attrType = TYPE_INT;
		 }
		 if( i == 5 ){
			 attrLength = atoi(tokens);
		 }
		 if( i == 6 ){
			 tokens = itoa(indexNo,buffer,10);
		 }
		 if( i == 1 ){
			strcpy(new_attrmet_record,tokens);
			strcat(new_attrmet_record,";");
		 }
		 else{
			strcat(new_attrmet_record,tokens);
			strcat(new_attrmet_record,";");
		 }
		 i++;
		 tokens = strtok_s (NULL, ";", &nextToken);
	}
	strcat(new_attrmet_record,";");
	memcpy(pData, new_attrmet_record, 256);

	// Update the record
	rc = attrmet->UpdateRecord(rh);
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::GetIndexNo(char *pData, int &indexNo){

	char *nextToken;
	char *tokens = strtok_s (pData,";", &nextToken);	//split the recordData
	char buffer[5];
	int i = 1;

	while (tokens != NULL)
	{
		 if( i == 6 ){
			 indexNo = atoi(tokens);
		 }
		 i++;
		 tokens = strtok_s (NULL, ";", &nextToken);
	}

	return OK;
}




// TESTING FUNCTION
t_rc SSQLM_DDL_Manager::PrintFirstRecordInAttrMet(const char *tName){
	REM_RecordHandle rh;
	char *pData;
	FindRecordInAttrMet(tName,rh);
	rh.GetData(pData);
	cout<<pData<<endl;
	return OK;
}