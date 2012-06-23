#define _CRT_SECURE_NO_WARNINGS

#include "SSQLM_DML_Manager.h"
#include <string>
#include <iostream>
#include <cstring>
#include <vector>
#include <hash_map>
using namespace std;

SSQLM_DML_Manager::SSQLM_DML_Manager(REM_RecordFileManager *rfm, INXM_IndexManager *im,char *dbName){
	this->rfm = rfm;
	this->im = im;
	this->dbName = dbName;
	t_rc rc = OpenRelmet(dbName);	//	Anoigei to arxeio rel.met kai arxikopoieitai o deikths *relmet
	rc = OpenAttrmet(dbName);		//	Anoigei to arxeio attr.met kai arxikopoieitai o deikths *attrmet
}

SSQLM_DML_Manager::~SSQLM_DML_Manager(){

	rfm->CloseRecordFile(*attrmet);
	rfm->CloseRecordFile(*relmet);
	delete(attrmet);
	delete(relmet);
	delete(rfm);
	delete(im);
}

t_rc SSQLM_DML_Manager::Insert(const char *tName,const char *record){
	
	char pathname[50];												//	Gia na eisagoume ena record ston pinaka
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();			//	anoigoume to arxeio tou pinaka,
	REM_RecordID rid;												//	kanoume thn eisagwgh, kai epeita prepei na
																	//	elegksoume ean yparxoun indexes tous opoious
																	//	prepei na enhmerwsoume.
	// Open the table file
	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = rfm->OpenRecordFile(pathname,*rfh);
	if (rc != OK) { return rc; }

	// Insert the new record							
	rc = rfh->InsertRecord(record,rid);								//	8a xrhsimopoihsoume auto to rid gia na kanoume
	if (rc != OK) { return rc; }									//	thn eisagwgh stous indexers (ean yparxoun)

	// Close the table file
	rc = rfm->CloseRecordFile(*rfh);
	if (rc != OK) { return rc; }

	// CHECK IF THERE ARE INDEXES									//	Gia na elegksoume ean yparxoun indexes,
	bool has_indexes;

	rc = CheckIfTableHasIndexes(tName,has_indexes);
	if (rc != OK) { return rc; }

	// In case the number of indexes is different than 0, we have to insert entry in every index file
	// First we find the ids of the index files by looking into the attr.met file
	if( has_indexes ){

		REM_RecordFileScan *rfs = new REM_RecordFileScan();
		int tNameLength = strlen(tName);
		REM_RecordHandle rh;

		rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
		if (rc != OK) {return rc; }

		int offset = 0;
		int size = 0;
		char *type;
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

t_rc SSQLM_DML_Manager::Where(const char *tName, char *conditions, vector<char *> *finalResultRecords, vector<REM_RecordID> *finalResultRIDs){
		
	char pathname[50];
	char pathname2[50];
	REM_RecordFileHandle *rfh = new REM_RecordFileHandle();
	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	REM_RecordHandle rh;
	REM_RecordID rid;

	vector <char*> conditionsList;
	vector <char*> conditionsListWithoutIndex;

	int condLength = strlen(conditions);
	char *str = (char*)malloc(condLength);
	strcpy(str,conditions);

	char *pointer = str;
	int offset = 0;
	int size;
	char *type;
	char condition[50]; // one condition
	int i = 0;
	char *conditionAttribute = NULL;
	char *conditionValue = NULL;
	t_compOp comp = NO_OP;
	int index_no;
	t_rc rc;

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

	hash_map<int, REM_RecordID> rids;
	bool isFirstCondition = true;

	for(int iii=0; iii< (int)conditionsList.size(); iii++){		// Gia ka8e condition, 8a elegksw ean yparxei index.  
															// Gia na to kanw auto spaw to condition sta attribute, comperator kai value, kai anoigw to attrmet.
		int condLength = strlen(conditionsList[iii]);		
		char *condit;
		condit = (char *)malloc(condLength);
		strcpy(condit,conditionsList[iii]);					// keep a back up from the condition string because strtok_s destroys it.

		rc = GetConditionInfo(conditionsList[iii],conditionAttribute,comp,conditionValue);		// get the attribute, the comperator and the value of the condition
		if (rc != OK) {return rc; }	
		
		rc = FindAttributeInAttrmet(tName,conditionAttribute,offset,type,size,index_no);
		if (rc != OK) {return rc; }	

		if(index_no != -1){	//IN CASE OF INDEX									//***********************************
																				//**	ean exei index, 
			// Open index file													//**	anoigw to arxeio tou index
			INXM_IndexHandle ih;												//**	kai psaxnw mesa ta rids pou epalh8eyoun thn syn8hkh
			_snprintf_s(pathname2,sizeof(pathname),"%s/%s",dbName,tName);		//**
																				//**
			rc = im->OpenIndex(pathname2,index_no,ih);							//**
			if (rc != OK) { return rc; }										//**
																				//**
			INXM_IndexScan *is = new INXM_IndexScan();							//**
																				//**
			if(type){	// case of Integer										//**
				int cndV = atoi(conditionValue);								//**
				int *key1 = new int(cndV);										//**
				rc = is->OpenIndexScan(ih, comp, key1);							//**			
				if (rc != OK) { return rc; }									//**
			}																	//**
			else{		//case of string										//**
				rc = is->OpenIndexScan(ih, comp, conditionValue);				//**
				if (rc != OK) { return rc; }									//**	
			}																	//***********************************
			
			
			// Prepei twra na vroume thn tomh olwn twn rid pou epistrefoun oi indexers																	
			int i = 0;	
			int page;
			int slott;

			if(isFirstCondition){									//***********************************
																	//**	Edw dhmiourgeitai o hashmap gia to prwto condition.
				while( is->GetNextEntry(rid) != INXM_FSEOF ){		//**	Gia ka8e record pou epistrefei
																	//**
					rid.GetPageID(page);							//**
					rid.GetSlot(slott);								//**			
					rids[page*10000+slott] = rid;					//**	ypologizw ena monadiko kleidi					
					i++;											//**	kai pros8etw to recordid ston hash.		
				}													//**
				isFirstCondition = false;							//**	Gia thn epomenh syn8hkh, 8a vrei thn tomh tou hash me ta epomena rids.
			}														//***********************************
			else{
				hash_map<int, REM_RecordID> intersectionRIDs;

				while( is->GetNextEntry(rid) != INXM_FSEOF ){
				
					rid.GetPageID(page);
					rid.GetSlot(slott);
					if(rids.count(page*10000+slott)){				//	ean yparxei hdh ston prohgoumeno hash
						intersectionRIDs[page*10000+slott] = rid;	//	pros8ese to ston hash me thn tomh.
					}								
					i++;															
				}
				rids.swap(intersectionRIDs);						//	antallakse tous hashes wste na leitourghsei anadromika kai gia ta epomena conditions
			}

			rc = is->CloseIndexScan();
			if (rc != OK) { return rc; }
		}
		else{	// IN CASE THERE IS NO INDEX						//	Se periptwsh pou to attribute tou condition den exei index
			conditionsListWithoutIndex.push_back(condit);			//	pros8ese to sthn lista me ta conditions pou den exoun index
		}
	}

	STORM_StorageManager *stormgr3 = new STORM_StorageManager();				//	Epomeno vhma einai na paroume apo to REM ta records sta opoia
	REM_RecordFileManager *remrfm = new REM_RecordFileManager(stormgr3);		//	antistoixoun ta recordids pou mazepsame apo tous indexers.
	REM_RecordFileHandle *remrfh = new REM_RecordFileHandle();					//	Ean den mazepsame kanena rid apo indexer, tote anazhtoume sto
	REM_RecordHandle *remrh = new REM_RecordHandle();							//	table ta records pou epalh8euoun thn syn8hkh (ths opoias to
	vector <char*> recordsFromIndexes;											//	attribute den eixe index)
	vector <REM_RecordID> ridsFromIndexes;		
	hash_map <int, REM_RecordID>::iterator rids_Iter;
	char *pdata;

	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = remrfm->OpenRecordFile(pathname,*remrfh);
	if (rc != OK) { return rc; }

	for ( rids_Iter = rids.begin( ); rids_Iter != rids.end( ); rids_Iter++ ){	// Vres ta records sta opoia antistoixoun h tomh twn rids twn indexes.
		
		REM_RecordID recordid = rids_Iter->second;
		rc = remrfh->ReadRecord(recordid,*remrh);
		if (rc != OK) {return rc; }	

		remrh->GetData(pdata);

		int pDataLength = strlen(pdata);
		char * newInput;
		newInput = (char *)malloc(pDataLength);
		strcpy(newInput,pdata);

		ridsFromIndexes.push_back(recordid);
		recordsFromIndexes.push_back(newInput);									// Apo8hkeyse ta se mia lista.
	}

	rc = remrfm->CloseRecordFile(*remrfh);
	if (rc != OK) {return rc; }

	if(!conditionsListWithoutIndex.empty()){																//	Ean yparxoun conditions xwris indexes,
																											//	8a psaksoume to attribute tou ka8e condition sto arxeio attrmet
//																											//	wste na paroume plhrofories gia to attribute (offset,type,...)
		int offset2,size2,indexID2;																			//	me teliko stoxo na elegksoume poia apo ta records pou epestrepsan
		char *type2;																							//	oi indexers, epalh8euoun ta conditions xwris index.

		for(int i=0; i< (int) conditionsListWithoutIndex.size(); i++){											//	Gia ka8e tetoio condition loipon,

			rc = GetConditionInfo(conditionsListWithoutIndex[i],conditionAttribute,comp,conditionValue);	//	vres ta systatika tou merh (attribute, comperator, value)
			if (rc != OK) {return rc; }	

			rc = FindAttributeInAttrmet(tName,conditionAttribute,offset2,type2,size2,indexID2);						//	kai psakse mesa sto attrmet to record gia to sygkekrimeno attribute. Pare tis plhrofories gia to attribute.
			if (rc != OK) {return rc; }	

			int j = 0;
			if(!recordsFromIndexes.empty()){
				for(int j=0; j<(int)recordsFromIndexes.size(); j++){											//	Sygkrine an isxyei h syn8hkh me ola ta records pou epestrepsan oi indexes.
				//for ( ridsToRecords_Iter = ridsToRecords.begin( ); ridsToRecords_Iter != ridsToRecords.end( ); ridsToRecords_Iter++ ){	
					
					
					char *value;																			//	Ean h syn8hkh epalh8euetai, pros8ese to record sthn lista eksodou finalResultRecords.
					value = (char *)malloc(size);
					int z;
					for(z=0; z<size; z++){
						value[z] = recordsFromIndexes[j][offset+z];
						//value[z] = ridsToRecords_Iter->second[offset+z];
					}
					value[z] = '\0';
					if(strstr(type,"TYPE_INT")){
						int val = atoi(value);
						int condValue = atoi(conditionValue);
						if(comp == EQ_OP){
							if(val == condValue){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						else if(comp == GT_OP){
							if(val > condValue){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						else if(comp == LT_OP){
							if(val < condValue){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						else if(comp == NE_OP){
							if(val != condValue){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						else if(comp == GE_OP){
							if(val >= condValue){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						else if(comp == LE_OP){
							if(val <= condValue){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
					}
					else{ // the type is TYPE_STRING
						if(comp == EQ_OP){
							if(strstr(value,conditionValue)){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						if(comp == GT_OP || comp == GE_OP){
							if(strcmp(value,conditionValue) == 1){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
						if(comp == LT_OP || comp == LE_OP){
							if(strcmp(value,conditionValue) == -1){
								finalResultRecords->push_back(recordsFromIndexes[j]);
								finalResultRIDs->push_back(ridsFromIndexes[j]);
							}
						}
					}
					j++;
				}
			}
			else{	// PERIPTWSH POU DEN EIXAME KANENA CONDITION ME INDEX									//	Sthn periptwsh pou den exoume kamia syn8hkh me index
				REM_RecordFileScan *remrfs = new REM_RecordFileScan();										//	psaxnoume mesa ston pinaka ta records pou epalh8euoun
				REM_RecordHandle *remrh2 = new REM_RecordHandle();											//	thn syn8hkh kai ta apouhkeyoume sthn lista eksodou.
				STORM_StorageManager *stormgr4 = new STORM_StorageManager();				
				REM_RecordFileManager *remrfm2 = new REM_RecordFileManager(stormgr4);		
				REM_RecordFileHandle *remrfh2 = new REM_RecordFileHandle();					
				char *data;

				_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
				rc = remrfm2->OpenRecordFile(pathname,*remrfh2);
				if (rc != OK) { return rc; }
				
				if(strcmp(type,"TYPE_INT") == 0){

					int atoiCondition = atoi(conditionValue);
					int *key = new int(atoiCondition);

					rc = remrfs->OpenRecordScan(*remrfh2,TYPE_INT,size,offset,comp,key);	// DEN EPISTREFEI SWSTA APOTELESMATA. PX id<2 TA EPISTREFEI OLA.
					if (rc != OK) {return rc; }
				}
				else{
					rc = remrfs->OpenRecordScan(*remrfh2,TYPE_STRING,size,offset,comp,conditionValue);
					if (rc != OK) {return rc; }
				}
				while( remrfs->GetNextRecord(*remrh2) != REM_FSEOF ){

					REM_RecordID recordIDFromNoIndex;
					rc = remrh2->GetRecordID(recordIDFromNoIndex);
					if (rc != OK) {return rc; }

					rc = remrh2->GetData(data);
					if (rc != OK) {return rc; }

					int dataLength = strlen(data);
					char *input;
					input = (char *)malloc(dataLength);
					strcpy(input,data);
					finalResultRecords->push_back(input);	
					finalResultRIDs->push_back(recordIDFromNoIndex);
				}

				rc = remrfs->CloseRecordScan();
				if (rc != OK) {return rc; }

				rc = remrfm2->CloseRecordFile(*remrfh2);
				if (rc != OK) {return rc; }
				
				delete remrfs;
				delete remrh2;
				delete remrfh2;
				delete remrfm2;
				delete stormgr4;
			}
		}
	}
	else{	// PERIPTWH POU EIXAME MONO CONDITIONS ME INDEXES			//	Se auth thn periptwsh apla vgazoume sthn eksodo
		for( int i=0; i<(int)recordsFromIndexes.size(); i++){			//	ta records pou epestrepsan oi indexes.
			finalResultRecords->push_back(recordsFromIndexes[i]);
			finalResultRIDs->push_back(ridsFromIndexes[i]);
		}
	}

	delete remrh;
	delete remrfh;
	delete remrfm;
	delete stormgr3;

	return OK;
}

t_rc SSQLM_DML_Manager::Select(const char *tName, vector<char *> columns, vector<char *> recordsFromWhereFunction, vector<char *> *finalResults){

	//	gia ka8e column, psaxnoume sto attrmet to offset
	//	gia ka8e record apo auta pou epestrepe h WHERE,
	//	pairnoume ta dedomena analoga me to offset kai ta apo8hkeyoume se mia lista
	//	telos, dhmiourgoyme ton teliko pinaka ton opoio kai epistrefoume.

	if(strcmp(columns[0],"*") != 0){												//	Se periptwsh poy anti gia column exw *,
																					//	to teliko apotelesma einai idio me auto pou epestrepse h WHERE
		vector<vector<char *>> listsToCombine;
		int offset, size, indexID;
		char *type;

		for(int n = 0; n < (int)columns.size(); n++) {									//	Gia ka8e column
			vector<char *> tempList;												//	ftiaxnw thn lista me ta dedomena apo to ka8e record.

			t_rc rc = FindAttributeInAttrmet(tName,columns[n],offset,type,size,indexID);	//	Vriskw to offset tou column
			if (rc != OK) { return rc; }

			for(int i = 0; i < (int)recordsFromWhereFunction.size(); i++) {

				char *value;
				value = (char *)malloc(size);
				int z;
				for(z=0; z<size; z++){
					value[z] = recordsFromWhereFunction[i][offset+z];				//	Pairnw ta dedomena pou xreiazomai.
				}
				value[z] = '\0';

				tempList.push_back(value);
			}
			listsToCombine.push_back(tempList);										//	Pros8etw thn lista tou column sthn lista me ta columns
		}

		for(int h = 0; h <(int) recordsFromWhereFunction.size(); h++) {					//	Ayto 8a to ekshghsw me sxhma kalytera.
			char *tempRecord;														//	Pairnw to idio stoixeio ka8e listas
			int tempRecordlength = strlen(recordsFromWhereFunction[0]);				//	kai ftiaxnw tis telikes pleiades eksodou.
			tempRecord = (char *)malloc(tempRecordlength);

			strcpy(tempRecord,listsToCombine[0][h]);
			for(int t = 1; t < (int)listsToCombine.size(); t ++) {
				strcat(tempRecord,listsToCombine[t][h]);
			}
			finalResults->push_back(tempRecord);
		}
	}
	else {
		finalResults->swap(recordsFromWhereFunction);
	}
	return OK;
}

t_rc SSQLM_DML_Manager::Delete(const char *tName, REM_RecordID ridsToDelete, char *recordsToDelete){

	STORM_StorageManager *stormgr = new STORM_StorageManager();				
	REM_RecordFileManager *remrfm = new REM_RecordFileManager(stormgr);		
	REM_RecordFileHandle *remrfh = new REM_RecordFileHandle();					
//	char *data;
	char pathname[50];

	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	t_rc rc = remrfm->OpenRecordFile(pathname,*remrfh);
	if (rc != OK) { return rc; }

	//for(int i = 0; i < ridsToDelete.size(); i++){

		rc = remrfh->DeleteRecord(ridsToDelete);
		if (rc != OK) { return rc; }
	//}

	rc = remrfh->FlushPages();
	if (rc != OK) { return rc; }

	bool hasIndexes;

	rc = CheckIfTableHasIndexes(tName,hasIndexes);
	if (rc != OK) { return rc; }

	if(hasIndexes){

		REM_RecordFileScan *rfs = new REM_RecordFileScan();
		int tNameLength = strlen(tName);
		REM_RecordHandle rh;

		rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
		if (rc != OK) {return rc; }

		int offset = 0;
		int size = 0;
		char *type;
		int indexNo = 0;
		while( rfs->GetNextRecord(rh) != REM_FSEOF ){
		
			char *pp;
			rh.GetData(pp);

			//Get the index id, offset, type and size of the attribute
			rc = GetAttrInfo(pp,offset,type,size,indexNo);

			//if there is index for this attribute
			if( indexNo != -1 ){

				// Retrieve the index entry from the attribute data
				INXM_IndexHandle ih;
				REM_RecordID rid;
				_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);

				rc = im->OpenIndex(pathname,indexNo,ih);
				if (rc != OK) { return rc; }

				char *indexEntry;
				indexEntry = (char*)malloc(size);

				int j;
				for( j=0; j<size; j++){
					indexEntry[j] = recordsToDelete[j+offset];
				}
				indexEntry[j] = '\0';
			
				// Delete the entry from the index file with id = indexNo
				if(strcmp(type,"TYPE_INT") == 0){
					int coca = atoi(indexEntry);
					int *key = new int(coca);

					rc = ih.DeleteEntry( key, rid );
					if (rc != OK) { return rc; }
				}
				else{
					rc = ih.DeleteEntry( indexEntry, rid);
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

t_rc SSQLM_DML_Manager::Update(const char *tName, vector<REM_RecordID> ridsFromWhere, char *setAttribute){

	STORM_StorageManager *stormgr = new STORM_StorageManager();				
	REM_RecordFileManager *remrfm = new REM_RecordFileManager(stormgr);	
	REM_RecordFileHandle *remrfh = new REM_RecordFileHandle();	
	REM_RecordHandle remrh;
	char pathname[50];
	char *pData;
	int offset,size,indexID;
	char *type;
	char *nextToken;

	int setAttrLength = strlen(setAttribute);
	char *setAttributeCondition = (char*)malloc(setAttrLength);
	strcpy(setAttributeCondition,setAttribute);

	char *setAttributeKey;
	char *setValue;
	setAttributeKey = strtok_s(setAttributeCondition,"= ", &nextToken);
	setValue = strtok_s(NULL,"= ", &nextToken);

	t_rc rc = FindAttributeInAttrmet(tName,setAttributeKey,offset,type,size,indexID);
	if (rc != OK) { return rc; }

	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,tName);
	rc = remrfm->OpenRecordFile(pathname,*remrfh);
	if (rc != OK) { return rc; }

	for(int i = 0; i < (int)ridsFromWhere.size(); i++){

		rc = remrfh->ReadRecord(ridsFromWhere[i],remrh);
		if (rc != OK) { return rc; }

		rc = remrh.GetData(pData);
		if (rc != OK) { return rc; }

		for(int j = 0; j < size; j++){
			pData[j+offset] = setValue[j];
		}

		rc = remrfh->UpdateRecord(remrh);
		if (rc != OK) { return rc; }

		rc = remrfh->FlushPages();
		if (rc != OK) { return rc; }
	}

	rc = remrfm->CloseRecordFile(*remrfh);
	if (rc != OK) { return rc; }

	delete remrfh;
	delete remrfm;
	delete stormgr;

	return OK;
}

t_rc SSQLM_DML_Manager::Join(const char *table1, const char* table2, char *connectionAttribute){

	STORM_StorageManager *stormgr = new STORM_StorageManager();				
	REM_RecordFileManager *remrfm = new REM_RecordFileManager(stormgr);	
	REM_RecordFileHandle *remrfh = new REM_RecordFileHandle();

	STORM_StorageManager *stormgr2 = new STORM_StorageManager();				
	REM_RecordFileManager *remrfm2 = new REM_RecordFileManager(stormgr2);	
	REM_RecordFileHandle *remrfh2 = new REM_RecordFileHandle();

	char pathname[50];
	int offset,
		offset2,
		size,
		size2,
		indexID,
		indexID2;
	char *type,
		*type2;

	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,table1);
	t_rc rc = remrfm->OpenRecordFile(pathname,*remrfh);
	if (rc != OK) { return rc; }

	_snprintf_s(pathname,sizeof(pathname),"%s/%s",dbName,table2);
	rc = remrfm2->OpenRecordFile(pathname,*remrfh2);
	if (rc != OK) { return rc; }

	rc = FindAttributeInAttrmet(table1,connectionAttribute,offset,type,size,indexID);
	if (rc != OK) { return rc; }

	rc = FindAttributeInAttrmet(table2,connectionAttribute,offset,type,size,indexID);
	if (rc != OK) { return rc; }








	rc = remrfm->CloseRecordFile(*remrfh);
	if (rc != OK) { return rc; }

	rc = remrfm2->CloseRecordFile(*remrfh2);
	if (rc != OK) { return rc; }

	return OK;
}



//PRIVATE FUNCTIONS

//	Synarthsh pou spaei ena record apo to attrmet arxeio sta systatika tou
t_rc SSQLM_DML_Manager::GetAttrInfo(char *rec, int &offset, char *&type, int &size, int &indexID){

	char *tokens;
	char *nextToken;
	int i = 1;
	
	tokens = strtok_s (rec,";", &nextToken);					

	while (tokens != NULL){
		if( i == 3 ){
			offset = atoi(tokens);
		}
		else if( i == 4 ){
			int len = strlen(tokens);
			type = (char*)malloc(len);
			strcpy(type,tokens);
		}
		else if( i == 5 ){
			size = atoi(tokens);
		}
		else if( i == 6 ){
			indexID = atoi(tokens);
		}
		i++;
		tokens = strtok_s (NULL, ";", &nextToken);
	}

	return OK;
}

//	Synarthsh pou anoigei to arxeio relmet, dinontas timh ston deikth *relmet
t_rc SSQLM_DML_Manager::OpenRelmet(char *dbName){
	char pathname[255];
	this->relmet = new REM_RecordFileHandle();
	_snprintf_s(pathname,sizeof(pathname),"%s/rel.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*relmet);
	if (rc != OK) { return rc; }

	return OK;
}
//	Synarthsh pou anoigei to arxeio attrmet dinontas timh ston deikth *attrmet
t_rc SSQLM_DML_Manager::OpenAttrmet(char *dbName){
	char pathname[255];
	this->attrmet = new REM_RecordFileHandle();
	_snprintf_s(pathname,sizeof(pathname),"%s/attr.met",dbName);
	t_rc rc = rfm->OpenRecordFile(pathname,*attrmet);
	if (rc != OK) { return rc; }

	return OK;
}

//	Synarthsh h opoia spaei to string mias syn8hkhs sta systatika thw (attribute, comperator, value)
t_rc SSQLM_DML_Manager::GetConditionInfo(char *condition, char *&conditionAttribute, t_compOp &comp, char *&conditionValue){
	
	// Get the comparison											
	if(strstr(condition,"<="))		//case of <=	
		comp = LE_OP;											
	else if(strstr(condition,">="))	//case of >=	
		comp = GE_OP;											
	else if(strstr(condition,"!="))	//case of !=	
		comp = NE_OP;											
	else if(strstr(condition,"<"))	//case of <		
		comp = LT_OP;											
	else if(strstr(condition,">"))	//case of >		
		comp = GT_OP;											
	else if(strstr(condition,"="))	//case of =		
		comp = EQ_OP;											

	char *nextToken;
	// Get the attribute
	conditionAttribute = strtok_s (condition,"><=!", &nextToken);
	// Get the value
	conditionValue = strtok_s (NULL, "><=!", &nextToken);							

	return OK;
}

t_rc SSQLM_DML_Manager::FindAttributeInAttrmet(const char *tName, char *attributeName, int &offset, char *&type, int &size, int &indexID){

	int attributeLength = strlen(attributeName);
	int tableLength = strlen(tName);
	int tableNattributeLength = attributeLength + tableLength + 1;

	char *tableANDattribute = (char *)malloc(tableNattributeLength);
	strcpy(tableANDattribute,tName);
	strcat(tableANDattribute,";");
	strcat(tableANDattribute,attributeName);

	REM_RecordFileScan *rfs = new REM_RecordFileScan();
	REM_RecordHandle rh;
	char *pData;

	t_rc rc = rfs->OpenRecordScan(*attrmet,TYPE_STRING,tableNattributeLength, 0, EQ_OP, tableANDattribute);	//	kai psakse mesa sto attrmet to record gia to sygkekrimeno attribute tou sygkekrimenou table
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }
	
	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }
	
	rc = GetAttrInfo(pData,offset,type,size,indexID);												//	Pare tis plhrofories gia to attribute.
	if (rc != OK) {return rc; }

	rc = rfs->CloseRecordScan();
	if (rc != OK) {return rc; }

	return OK;
}

t_rc SSQLM_DML_Manager::CheckIfTableHasIndexes(const char *tName, bool &hasIndexes){

	char *tokens;
	char *nextToken;
	REM_RecordFileScan *rfs = new REM_RecordFileScan();				//	psaxnoume sto relmet arxeio na vroume thn eggrafh
	int tNameLength = strlen(tName);								//	pou anaferete sto diko mas table kai na elegksoume
	REM_RecordHandle rh;											//	ean o ari8mos twn indexes einai diaforos tou 0.
	char *pData;

	// Find the record with table name "tName"
	t_rc rc = rfs->OpenRecordScan(*relmet,TYPE_STRING,tNameLength, 0, EQ_OP, (char*)tName);
	if (rc != OK) {return rc; }

	rc = rfs->GetNextRecord(rh);
	if (rc != OK) {return rc; }

	rc = rh.GetData(pData);
	if (rc != OK) {return rc; }

	// Check if the indexes field is not 0
	tokens = strtok_s (pData,";", &nextToken);
	int i = 1;
	hasIndexes = false;

	while (tokens != NULL){
		if( i == 4 ){
			if( strcmp(tokens,"0") != 0 )
				hasIndexes = true;
			break;
		}
		i++;
		tokens = strtok_s (NULL, ";", &nextToken);
	}

	rc = rfs->CloseRecordScan();

	return OK;
}