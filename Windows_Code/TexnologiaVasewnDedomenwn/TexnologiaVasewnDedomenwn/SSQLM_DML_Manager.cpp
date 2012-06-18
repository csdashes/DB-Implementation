#include "SSQLM_DML_Manager.h"
#include <string>
#include <iostream>
#include <cstring>
#include <vector>
using namespace std;

SSQLM_DML_Manager::SSQLM_DML_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im,char *dbName){
	this->rfm = rfm;
	this->im = im;
	this->dbName = dbName;
	t_rc rc = OpenRelmet(dbName);
	rc = OpenAttrmet(dbName);
}

SSQLM_DML_Manager::~SSQLM_DML_Manager(){
}

t_rc SSQLM_DML_Manager::Insert(const char *tName,const char *record){
	
	char pathname[50];
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();
	REM_RecordID rid;
	char *tokens;
	
	// Open the table file
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }

	// Insert the new record							
	rc = rfh->InsertRecord(record,rid);					// We will use this rid to insert the new index entry (if exists)
	if (rc != OK) { return rc; }

	// Close the table file
	rc = rfm->CloseRecordFile(*rfh);
	if (rc != OK) { return rc; }

	// CHECK IF THERE ARE INDEXES							na to kanw synarthsh
	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	int tNameLength = strlen(tName);
	REM_RecordHandle rh;
	char *pData;

	// Open rel.met file
	//_snprintf_s(pathname,sizeof(pathname),"%s/rel.met",dbName);
	//rc = rfm->OpenRecordFile(pathname,*relmet);
	//if (rc != OK) { return rc; }

	// Find the record with table name "tName"
	rc = rfs->OpenRecordScan(*relmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
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

		//_snprintf_s(pathname,sizeof(pathname),"%s/attr.met",dbName);
		//rc = rfm->OpenRecordFile(pathname,*rfh);
		//if (rc != OK) { return rc; }

		rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
		if (rc != OK) {return rc; }

		int offset = 0;
		int size = 0;
		char type[12];
		int indexNo = 0;
		while( rfs->GetNextRecord(rh) != REM_FSEOF ){
		
			char*pp;
			rh.GetData(pp);

			//Get the index id, offset, type and size of the attribute
			rc = GetAttrInfo(pp,offset,type,size,indexNo);

			//if there is index for this attribute
			if( indexNo != -1 ){

				// Retrieve the index entry from the attribute data
				INXM_IndexHandle ih;
				_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);

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

					rc = ih.InsertEntry( key, rid);
					if (rc != OK) { return rc; }
				}
				else{
					rc = ih.InsertEntry( indexEntry, rid);
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

t_rc SSQLM_DML_Manager::Where(const char *tName, char *conditions, REM_RecordID *ridsArray){

	
	
	char pathname[50];
	char pathname2[50];
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();
	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	REM_RecordHandle rh;
	REM_RecordID rid;

	vector <char*> conditionsList; // empty

	int condLength = strlen(conditions);
	char *str = (char*)malloc(condLength);
	strcpy(str,conditions);

	char *pointer = str;
	int offset = 0;
	char condition[50]; //mia syn8hkh
	int i = 0;
	char *conditionAttribute, *conditionValue;
	t_compOp comp;
	char *rdata;
	char * token;
	int index_no;

	// Open attr.met file
	//_snprintf_s(pathname,sizeof(pathname),"%s/attr.met",dbName);
	//t_rc rc = rfm->OpenRecordFile(pathname,*rfh);
	//if (rc != OK) { return rc; }

	// Split with delimeter "AND"
	while(strstr(pointer,"AND")){					//***********************************************
													//**	Spaw thn eisodo tou where sta conditions.
		pointer = strstr(pointer,"AND");			//**	To conditionsList periexei ola ta 
		i = 0;										//**	conditions.
		while(&str[offset] != pointer){				//**
			condition[i] = str[offset];				//**
			i++;									//**
			offset++;								//**
		}											//**
		condition[i] = '\0';						//**
													//**
		conditionsList.push_back(condition);		//**
													//**
		offset+=4;									//**
		pointer+=4;									//**
	}												//**
	//add the last condition in the list			//**
	conditionsList.push_back(pointer);				//************************************************

	for(int iii=0; iii< conditionsList.size(); iii++){					//*****************************************
		
		// Get the comparison											//**	kai ton telesth sygkrishs
			if(strstr(conditionsList[iii],"<="))		//case of <=	//**
				comp = LE_OP;											//**
			else if(strstr(conditionsList[iii],">="))	//case of >=	//**
				comp = GE_OP;											//**
			else if(strstr(conditionsList[iii],"!="))	//case of !=	//**
				comp = NE_OP;											//**
			else if(strstr(conditionsList[iii],"<"))	//case of <		//**
				comp = LT_OP;											//**
			else if(strstr(conditionsList[iii],">"))	//case of >		//**
				comp = GT_OP;											//**
			else if(strstr(conditionsList[iii],"="))	//case of =		//**
				comp = EQ_OP;											//*******************************************

		//**	Gia ka8e condition, pairnw to 
		conditionAttribute = strtok (conditionsList[iii],"><=!");		//**	attribute,
		conditionValue = strtok (NULL, "><=!");							//**	to value
																		//**
			

		// Calculate the length of "tName;attrName"											//***************************************
		int tNameLength = strlen(tName);													//**	Sto arxeio attr.met psaxnw na dw
		int attrNameLength = strlen(conditionAttribute);									//**	an to sygkekrimeno attribute
		int finalLength = tNameLength + attrNameLength + 1;									//**	exei index. Gia na vrw to attribute
																							//**	psaxnw thn symvoloseira: "tablename;attrname"
		// Build the "tName;attrName". We use it to find the record in the file attr.met	//**
		char *tableNattr = (char*)malloc(finalLength);										//**
		strcpy(tableNattr,tName);															//**
		strcat(tableNattr,";");																//**
		strcat(tableNattr,conditionAttribute);												//**
																							//**
		// Find the record																	//**
		t_rc rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,finalLength, 0, EQ_OP, tableNattr);		//**
		if (rc != OK) {return rc; }															//**
																							//**
		rc = rfs->GetNextRecord(rh);														//**
		if (rc != OK) {return rc; }	
																							//**
		int indexNo;																		//**
		//Get the index id																	//**
		rc = rh.GetData(rdata);																	//**
		
		token = strtok (rdata,";");					//split the recordData					//**
																							//**
		// Retrieve the index id															//**
		int jj = 1;																			//**	pairnw to indexID
		while (token != NULL){																//**
			if( jj == 6 ){																	//**	
				index_no = atoi(token);														//**
			}																				//**
			jj++;																			//**
			token = strtok (NULL, ";");														//**
		}																					//********************************************

		rc = rfs->CloseRecordScan();
		if (rc != OK) {return rc; }

		// MEXRI EDW EXOUME VREI OTI 8ELOUME NAME=ILIAS KAI OTI YPARXEI
		// INDEXER GIA TO NAME. ETSI ANOIGOUME TO INDEX ME ID=1 KAI PAME
		// NA DIAVASOUME. TO ih, comp KAI conditionValue PAIRNOUN SWSTES
		// TIMES EPOMENWS TO KEY(conditionValue) EINAI SWSTO.
		if(index_no != -1){	//IN CASE OF INDEX									//***********************************
																				//**	ean exei index to attribute, 
			// Open index file													//**	anoigw to arxeio tou index
			INXM_IndexHandle ih;												//**
			_snprintf_s(pathname2,sizeof(pathname),"%s/%s",dbName,tName);			//**
																				//**
			rc = im->OpenIndex(pathname2,index_no,ih);							//**
			if (rc != OK) { return rc; }										//**
																				//**
			INXM_IndexScan *is = new INXM_IndexScan();							//**

			rc = is->OpenIndexScan(ih, comp, conditionValue);					//**
			if (rc != OK) { return rc; }										//**
																				//**
			int i = 0;	
			int slott;
			// TO PARAKATW PETAEI TO EXCEPTION
			while( is->GetNextEntry(rid) != INXM_FSEOF ){						//**	kai ka8e record pou epistrefei
				rid.GetSlot(slott);
				cout<<slott<<", "<<i<<endl;
				ridsArray[i] = rid;												//**	to apo8hkeyw ston pinaka
				i++;															//**	PETAEI EXCEPTION
			}																	//**
		}																		//************************************
//	else{	//IN CASE THERE IS NO INDEX
		
		//cout<<conditionsList[iii]<<endl;
		break;
	}


//	// Calculate the length of "tName;attrName"
//	int tNameLength = strlen(tName);
//	int attrNameLength = strlen(conditionAttribute);
//	int finalLength = tNameLength + attrNameLength + 1;
//
//	// Build the "tName;attrName"
//	char *tableNattr;
//	strcpy(tableNattr,tName);
//	strcat(tableNattr,";");
//	strcat(tableNattr,conditionAttribute);
//
//	// Find the record
//	rc = rfs->OpenRecordScan(*rfh,TYPE_STRING,finalLength, 0, EQ_OP, tableNattr);
//	if (rc != OK) {return rc; }
//
//	rc = rfs->GetNextRecord(rh);
//	if (rc != OK) {return rc; }
//
//	int indexNo;
//	//Get the index id
//	rc = GetAttrInfo(rh,indexNo,6);	// 6 is the position of the index field
//	if (rc != OK) {return rc; }
//
//	rc = rfs->CloseRecordScan();
//	if (rc != OK) {return rc; }
//
//	if(indexNo != -1){	//IN CASE OF INDEX
//		// Open index file
//		INXM_IndexHandle ih;
//		_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
//
//		rc = im->OpenIndex(pathname,indexNo,ih);
//		if (rc != OK) { return rc; }
//
//		INXM_IndexScan *is = new INXM_IndexScan();
//		rc = is->OpenIndexScan(ih, compOp, value);
//		if (rc != OK) { return rc; }
//
//		int i = 0;
//		while( is->GetNextEntry(rid) != STORM_EOF ){
//			ridsArray[i] = rid;
//		}
//	}
//	else{	//IN CASE THERE IS NO INDEX
//		// Open the table file
//		_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
//		rc = rfm->OpenRecordFile(pathname,*rfh);
//		if (rc != OK) { return rc; }
//																				//FIXXING: NA PAIRNEI SWSTES PARAMETROUS H OpenRecordScan
//		// Find the record
////		rc = rfs->OpenRecordScan(*rfh,TYPE_STRING,finalLength, 0, EQ_OP, tableNattr);
//		if (rc != OK) {return rc; }
//
//		rfs->GetNextRecord(rh);
//		int i = 0;
//		while( rh.GetRecordID(rid) != STORM_EOF ){
//			ridsArray[i] = rid;
//		}
//
//		rc = rfm->CloseRecordFile(*rfh);
//		if (rc != OK) {return rc; }
//	}
//	
	return OK;
}


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

t_rc SSQLM_DML_Manager::OpenRelmet(char *dbName){
	char pathname[255];
	this->relmet = new REM_RecordFileHandle();
	_snprintf_s(pathname,sizeof(pathname),"%s/rel.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*relmet);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SSQLM_DML_Manager::OpenAttrmet(char *dbName){
	char pathname[255];
	this->attrmet = new REM_RecordFileHandle();
	_snprintf_s(pathname,sizeof(pathname),"%s/attr.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*attrmet);
	if (rc != OK) { return rc; }

	return OK;
}
