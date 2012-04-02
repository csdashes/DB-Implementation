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
	printf("Number of attributes: %d\n",i);
	
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
	char attr_string[256];
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
			 strcat(attr_string,tokens);
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
			 strcat(attr_string,tokens);
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

t_rc SSQLM_DDL_Manager::CreateIndex(char *dbName, const char *tName, int indexNo, t_attrType attrType, int attrLength){

	char pathname[50];
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = im->CreateIndex(pathname,indexNo,attrType,attrLength);
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