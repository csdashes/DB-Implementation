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

	// Insert the new record							
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

		int offset = 0;
		int size = 0;
		char type[12];
		int indexNo = 0;
		while( rfs->GetNextRecord(rh) != REM_FSEOF ){
		
			char*pp;
			rh.GetData(pp);

			printf("%s\n",pp);
			//Get the index id, offset, type and size of the attribute
			rc = GetAttrInfo(pp,offset,type,size,indexNo);	//6 because we want the 6th token

			//if there is index for this attribute
			if( indexNo != -1 ){

				// Retrieve the index entry from the attribute data
				INXM_IndexHandle ih;
				_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);

				char *indexEntry;
				indexEntry = (char*)malloc(size);

				int j;
				for( j=0; j<size; j++){
					indexEntry[j] = record[j+offset];
				}
				indexEntry[j] = '\0';
				rc = im->OpenIndex(pathname,indexNo,ih);
				if (rc != OK) { return rc; }

			
				// Insert the entry into index file with id = indexNo
				if(strcmp(type,"TYPE_INT") == 0){
					int coca = atoi(indexEntry);
					int *key = new int(coca);

					ih.InsertEntry( key, rid);
					if (rc != OK) { return rc; }
				}
				else{
					ih.InsertEntry( indexEntry, rid);
					if (rc != OK) { return rc; }
				}

				rc = im->CloseIndex(ih);
				if (rc != OK) { return rc; }

			}
		}

		rc = rfs->CloseRecordScan();
	}	
	
	
	return OK;
}

/*t_rc SSQLM_DML_Manager::Where(const char *dbName, const char *tName, const char *attrName, t_compOp compOp, void *value, REM_RecordID *ridsArray){

	
	char pathname[50];
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();
	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	REM_RecordHandle rh;
	REM_RecordID rid;

	// Open attr.met file
	_snprintf(pathname,sizeof(pathname),"%s/attr.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }

	// Calculate the length of "tName;attrName"
	int tNameLength = strlen(tName);
	int attrNameLength = strlen(attrName);
	int finalLength = tNameLength + attrNameLength + 1;

	// Build the "tName;attrName"
	char *tableNattr;
	strcpy(tableNattr,tName);
	strcat(tableNattr,";");
	strcat(tableNattr,attrName);

	// Find the record
	rc = rfs->OpenRecordScan(*rfh,TYPE_STRING,finalLength, 0, EQ_OP, tableNattr);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	int indexNo;
	//Get the index id
	rc = GetAttrInfo(rh,indexNo,6);	// 6 is the position of the index field
	if (rc != OK) {return rc; }

	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	if(indexNo != -1){	//IN CASE OF INDEX
		// Open index file
		INXM_IndexHandle ih;
		_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);

		rc = im->OpenIndex(pathname,indexNo,ih);
		if (rc != OK) { return rc; }

		INXM_IndexScan *is = new INXM_IndexScan();
		rc = is->OpenIndexScan(ih, compOp, value);
		if (rc != OK) { return rc; }

		int i = 0;
		while( is->GetNextEntry(rid) != STORM_EOF ){
			ridsArray[i] = rid;
		}
	}
	else{	//IN CASE THERE IS NO INDEX
		// Open the table file
		_snprintf(pathname,sizeof(pathname),"%s/%s",dbName,tName);
		rc = rfm->OpenRecordFile(pathname,*rfh);
		if (rc != OK) { return rc; }
																				//FIXXING: NA PAIRNEI SWSTES PARAMETROUS H OpenRecordScan
		// Find the record
//		rc = rfs->OpenRecordScan(*rfh,TYPE_STRING,finalLength, 0, EQ_OP, tableNattr);
		if (rc != OK) {return rc; }

		rfs->GetNextRecord(rh);
		int i = 0;
		while( rh.GetRecordID(rid) != STORM_EOF ){
			ridsArray[i] = rid;
		}

		rc = rfm->CloseRecordFile(*rfh);
		if (rc != OK) {return rc; }
	}
	return OK;
}*/


//PRIVATE FUNCTIONS
t_rc SSQLM_DML_Manager::GetAttrInfo(char *rec, int &offset, char *type, int &size, int &indexID){

	char *tokens;
	int i = 1;
	
	tokens = strtok (rec,";");					//split the recordData

	// Retrieve the index id
	while (tokens != NULL){
		if( i == 3 ){
			offset = atoi(tokens);
		}
		else if( i == 4 ){
			strcpy(type,tokens);
		}
		else if( i == 5 ){
			size = atoi(tokens);
		}
		else if( i == 6 ){
			indexID = atoi(tokens);
		}
		i++;
		tokens = strtok (NULL, ";");
	}

	return OK;
}