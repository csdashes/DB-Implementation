#include "SYSM_MetadataManager.h"

SYSM_MetadataManager::SYSM_MetadataManager(SYSM_DatabaseManager *dbm){
	this->dbm=dbm;
}

SYSM_MetadataManager::~SYSM_MetadataManager(){
}

t_rc SYSM_MetadataManager::RecordRelationMeta(SYSM_relmet *relation){
	ofstream myfile;
	const char *activedb;
	char pathname[260];
	activedb=this->dbm->getdbName();
	_snprintf(pathname,sizeof(pathname),"%s/%s",activedb,"rel.met");
	myfile.open(pathname,ios::app);
	myfile<<relation->relName<<" "<<relation->recSize<<" "<<relation->attrNo<<" "<<relation->indexNo<<endl;
	myfile.flush();
	myfile.close();
	return OK;
}

t_rc SYSM_MetadataManager::RecordAttributeMeta(SYSM_attrmet *attribute){
	ofstream myfile;
	const char *activedb;
	const char *attrType;
	char pathname[260];
	if (attribute->attrType==TYPE_INT) {
		attrType="INT";
	}
	else {
		attrType="STRING";
	}
	activedb=this->dbm->getdbName();
	_snprintf(pathname,sizeof(pathname),"%s/%s",activedb,"attr.met");
	myfile.open(pathname);
	myfile<<attribute->relName<<" "<<attribute->attrName<<" "<<attrType<<" "<<attribute->attrLength<<" "<<attribute->offset<<" "<<attribute->indexcode<<endl;
	return OK;
}