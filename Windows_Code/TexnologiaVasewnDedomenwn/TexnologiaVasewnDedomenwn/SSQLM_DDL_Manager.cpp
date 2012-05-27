#include "SSQLM_DDL_Manager.h"

#include <string.h>
#include <stdio.h>

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
	tokens = strtok (str,",");					//split the attributes
	while (tokens != NULL)
	{
		numOfColumns++;
		tokens = strtok (NULL, ",");
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

t_rc SSQLM_DDL_Manager::DropTable(const char *tName){

	t_rc rc;
	char pathname[50];
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = rfm->DestroyRecordFile(pathname);
	if (rc != OK) { return rc; }

	rc = DeleteTableMeta(tName);	// NEEDS TO BE IMPLEMENTED
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::CreateIndex(const char *tName, const char *attrName){

	t_attrType attrType;
	int attrLength;
	int indexNo;

	
	REM_RecordID rid;
	REM_RecordHandle rh;
	char *pData;
	char pathname[50];
	t_rc rc;

	// UPDATE THE REL.MET FILE
	char new_relmet_record[256];							
	int i = 1;
	char new_attrmet_record[256];						
	char buffer[5];

	// Find the record with table name "tName"
	rc = FindRecordInRelMet(tName,rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }

	// Raise the number of indexes
	int NoIndexes = 0;										//number of indexes
	char *tokens = strtok (pData,";");					//split the recordData
	while (tokens != NULL)
	{
		 if( i == 4 ){									//the field of the indexes number
			 NoIndexes = atoi(tokens);
			 NoIndexes++;								//raise the number of indexes
			 tokens = itoa(NoIndexes,buffer,10);
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
		 tokens = strtok (NULL, ";");
	}
	strcat(new_relmet_record,";");
	memcpy(pData, new_relmet_record, 256);

	// Update the record
	rc = relmet->UpdateRecord(rh);
	if (rc != OK) {return rc; }

	// UPDATE THE ATTR.MET FILE
	// Find the record with attribute name "attrName"
	rc = FindRecordInAttrMet(tName,attrName,rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }

	// Retrieve attrType and attrLength and create the new attribute record raising the index number.
	tokens = strtok (pData,";");	//split the recordData
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
			 tokens = itoa(NoIndexes,buffer,10);
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
		 tokens = strtok (NULL, ";");
	}
	strcat(new_attrmet_record,";");
	memcpy(pData, new_attrmet_record, 256);

	// Update the record
	rc = attrmet->UpdateRecord(rh);
	if (rc != OK) {return rc; }

	// Create the index file
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = im->CreateIndex(pathname,NoIndexes,attrType,attrLength);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DropIndex(const char *tName, int indexNo){

	char pathname[50];
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = im->DestroyIndex(pathname,indexNo);
	if (rc != OK) { return rc; }

	// CODE HERE: na meiwsw ton ari8mo twn indexes apo rel.met, attr.met

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

	tokens = strtok (str, ", ()");
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

			 char_token = strtok (NULL, ", ()");
			 
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
		 tokens = strtok (NULL, ", ()");
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

t_rc SSQLM_DDL_Manager::FindRecordInAttrMet(const char *tName,const char *attrName, REM_RecordHandle &rh){

	t_rc rc;
	REM_RecordFileScan *rfs = new REM_RecordFileScan();

	int tNameLength = strlen(tName);
	int attrNameLength = strlen(attrName);
	rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,attrNameLength, tNameLength+1, EQ_OP, (char*)attrName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	// Close FileScan
	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DeleteTableMeta(const char *tName){
	
	t_rc rc;
	REM_RecordHandle rh;
	REM_RecordID rid;

	// Delete metadata from rel.met
	rc = FindRecordInRelMet(tName,rh);
	if (rc != OK) {return rc; }

	rc = rh.GetRecordID(rid);
	if (rc != OK) {return rc; }

	rc = relmet->DeleteRecord(rid);	// RETURNS ERROR: STORM PAGENOTINBUFFER
	if (rc != OK) {return rc; }

	// Delete metadata from attr.met
	while(FindRecordInAttrMet(tName,rh) != INXM_FSEOF){
		rc = rh.GetRecordID(rid);
		if (rc != OK) {return rc; }

		rc = attrmet->DeleteRecord(rid);
		if (rc != OK) {return rc; }
	}

	// Delete the indexes
	// CODE HERE

	return OK;
}