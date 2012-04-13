#include "SSQLM_DDL_Manager.h"

#include <string.h>
#include <stdio.h>

SSQLM_DDL_Manager::SSQLM_DDL_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im){
	this->rfm = rfm;
	this->im = im;
}

SSQLM_DDL_Manager::~SSQLM_DDL_Manager(){
}

t_rc SSQLM_DDL_Manager::CreateTable(char *dbName, const char *tName, const char *attributes){
	
	// Count the attributes
	int i=0;
	char *str;
	int attrlength = strlen(attributes);
	str = (char*)malloc(attrlength);
	strcpy(str,attributes);
	char *tokens;
	tokens = strtok (str,",");					//split the attributes
	while (tokens != NULL)
	 {
	  i++;
	  tokens = strtok (NULL, ",");
	 }
	
	// Open the record file attr.met
	char pathname[50];
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();
	REM_RecordID rid;
	_snprintf(pathname,sizeof(pathname),"%s/attr.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }

	// Calculate record size and insert the attributes in attr.met
	int recSize = 0;
	strcpy(str,attributes);
	tokens = strtok (str, ", ()");
	char *char_token;
	char *attrName;
	char buffer[5];
	char attr_string[200];
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
			 rc = rfh->InsertRecord(attr_string,rid);
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
			 rc = rfh->InsertRecord(attr_string,rid);
			 if (rc != OK) { return rc; }

			 recSize += atoi(char_token);
		 }
		 else{
			 attrName = tokens;
		 }
		 tokens = strtok (NULL, ", ()");
	 }

	// Close the record file attr.met
	rc = rfm->CloseRecordFile(*rfh);
	if (rc != OK) { return rc; }

	// Create the record for the rel.met
	char record_string[256];
	strcpy(record_string,tName);					//table name
	strcat(record_string,";");
	strcat(record_string,itoa(recSize,buffer,10));	//record size
	strcat(record_string,";");
	strcat(record_string,itoa(i,buffer,10));		//amount of columns. (To 10 shmainei 10diko systhma ari8mishs)
	strcat(record_string,";");
	strcat(record_string,"0;;");					//amount of indexes (init:0)

	// Open the record file rel.met
	_snprintf(pathname,sizeof(pathname),"%s/rel.met",dbName);
	rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }
	
	// Insert the record
	rc = rfh->InsertRecord(record_string,rid);
	if (rc != OK) { return rc; }

	// Close the record file
	rc = rfm->CloseRecordFile(*rfh);
	if (rc != OK) { return rc; }

	// Create the database file
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = rfm->CreateRecordFile(pathname,recSize);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DropTable(char *dbName, const char *tName){

	char pathname[50];
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = rfm->DestroyRecordFile(pathname);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::CreateIndex(char *dbName, const char *tName, const char *attrName){

	t_attrType attrType;
	int attrLength;
	int indexNo;

	REM_RecordFileHandle rfh;
	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	REM_RecordID rid;
	REM_RecordHandle rh;
	char *pData;
	char pathname[50];

	// UPDATE THE REL.MET FILE
	char new_relmet_record[256];							//ISWS PREPEI NA ALLAKSOUME TO MEGE8OS
	int i = 1;
	char new_attrmet_record[256];							//ISWS PREPEI NA ALLAKSOUME TO MEGE8OS
	char buffer[5];
	int attrNameLength = strlen(attrName);
	int tNameLength = strlen(tName);

	// Open the record file rel.met
	_snprintf(pathname,sizeof(pathname),"%s/rel.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,rfh);
	if (rc != OK) {return rc; }

	// Find the record with table name "tName"
	rc = rfs->OpenRecordScan(rfh,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
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
	rc = rfh.UpdateRecord(rh);
	if (rc != OK) {return rc; }

	// Close FileScan
	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	// Close RecordFile
	rc = rfm->CloseRecordFile(rfh);
	if (rc != OK) {return rc; }

	//UPDATE THE ATTR.MET FILE
	// Open the record file attr.met to retrieve the attrType and attrLength of the attribute "attrName"
	_snprintf(pathname,sizeof(pathname),"%s/attr.met",dbName);
	rc = rfm->OpenRecordFile(pathname,rfh);
	if (rc != OK) {return rc; }

	// Find the record with attribute name "attrName"
	rc = rfs->OpenRecordScan(rfh,TYPE_STRING,attrNameLength, tNameLength+1, EQ_OP, (char*)attrName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }

	// Retrieve attrType and attrLength and create the new attribute record raising the index number.
	tokens = strtok (pData,";");					//split the recordData
	i = 1;
	while (tokens != NULL)
	{
		 if( i == 4 ){									//the field of the indexes number
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
	rc = rfh.UpdateRecord(rh);
	if (rc != OK) {return rc; }
	
	// Close FileScan
	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	// Close RecordFile
	rc = rfm->CloseRecordFile(rfh);
	if (rc != OK) {return rc; }

	// Create the index file
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = im->CreateIndex(pathname,NoIndexes,attrType,attrLength);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DDL_Manager::DropIndex(char *dbName, const char *tName, int indexNo){

	char pathname[50];
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = im->DestroyIndex(pathname,indexNo);
	if (rc != OK) { return rc; }

	return OK;
}