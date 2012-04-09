#include "SSQLM_DML_Manager.h"

SSQLM_DML_Manager::SSQLM_DML_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im){
	this->rfm = rfm;
	this->im = im;
}

SSQLM_DML_Manager::~SSQLM_DML_Manager(){
}

t_rc SSQLM_DML_Manager::Insert(const char *dbName, const char *tName,const char *record){
	
	char pathname[50];
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();
	REM_RecordID rid;
	char *tokens;

	// Open the table file
	_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }

	// Insert the new record							//PREPEI NA DOUME PWS AKRIVWS 8A MPAINOUN TA DEDOMENA STON INDEX
	rc = rfh->InsertRecord(record,rid);					// We will use this rid to insert the new index entry (if exists)
	if (rc != OK) { return rc; }

	// Close the table file
	rc = rfm->CloseRecordFile(*rfh);
	if (rc != OK) { return rc; }

	// CHECK IF THERE ARE INDEXES
	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	int tNameLength = strlen(tName);
	REM_RecordHandle rh;
	char *pData;

	// Open rel.met file
	_snprintf(pathname,sizeof(pathname),"%s/rel.met",dbName);
	rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }

	// Find the record with table name "tName"
	rc = rfs->OpenRecordScan(*rfh,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }

	// Check if the indexes field is not 0
	tokens = strtok (pData,";");					//split the recordData
	int i = 1;
	bool has_indexes = false;

	while (tokens != NULL){
		if( i == 4 ){
			if( strcmp(tokens,"0") != 0 )
				has_indexes = true;
			break;
		}
		i++;
		tokens = strtok (NULL, ";");
	}

	rc = rfs->CloseRecordScan();

	// In case the number of indexes is different than 0, we have to insert entry in every index file
	// First we find the ids of the index files by looking into the attr.met file
	if( has_indexes ){

		_snprintf(pathname,sizeof(pathname),"%s/attr.met",dbName);
		rc = rfm->OpenRecordFile(pathname,*rfh);
		if (rc != OK) { return rc; }

		rc = rfs->OpenRecordScan(*rfh,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
		if (rc != OK) {return rc; }

		while( rfs->GetNextRecord(rh) != STORM_EOF ){

			rc = rh.GetData(pData);
			if (rc != OK) {return rc; }

			tokens = strtok (pData,";");					//split the recordData
			i = 1;
			int indexNo = 0;

			// Retrieve the index id
			while (tokens != NULL){
				if( i == 6 ){
					if( strcmp(tokens,"-1") != 0 )
						indexNo = atoi(tokens);
					break;
				}
				i++;
				tokens = strtok (NULL, ";");
			}

			// Insert the entry into index file with id = indexNo
			INXM_IndexHandle ih;
			_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);

			rc = im->OpenIndex(pathname,indexNo,ih);
			if (rc != OK) { return rc; }

			ih.InsertEntry( (char*)record, rid);
			if (rc != OK) { return rc; }

			rc = im->CloseIndex(ih);
			if (rc != OK) { return rc; }

		}

		rc = rfs->CloseRecordScan();
	}
	

	return OK;
}