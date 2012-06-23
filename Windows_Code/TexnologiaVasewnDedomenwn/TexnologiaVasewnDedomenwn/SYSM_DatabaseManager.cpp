#include "SYSM_DatabaseManager.h"

SYSM_DatabaseManager::SYSM_DatabaseManager(REM_RecordFileManager *rfm){
	this->rfm = rfm;
	open = false;
	this->activedb = (char*)malloc(sizeof(char)*255);
}

SYSM_DatabaseManager::~SYSM_DatabaseManager(){
	//n kalei thn close database, free tous pointers.
}

t_rc SYSM_DatabaseManager::CreateDatabase (const char *dbName){
	
	// Create the directory (database)
	char dirname[260];
	strcpy(dirname,dbName);											//(to apo katw to egrapse o nikos k den douleue se mena, auto douleuei)
	//_snprintf_s(dirname,sizeof(dirname),"./data/%s",dbName);		//create the path for the directory (database)
	_mkdir(dirname);												//create the directory given the path			
	
	// Create the rel.met file
	char pathname[260];
	_snprintf_s(pathname,sizeof(pathname),"%s/rel.met",dirname);
	t_rc rc = this->rfm->CreateRecordFile(pathname,256);
	if (rc != OK) { return rc; }

	// Create the attr.met file
	_snprintf_s(pathname,sizeof(pathname),"%s/attr.met",dirname);
	rc = this->rfm->CreateRecordFile(pathname,256);
	if (rc != OK) { return rc; }

	return OK;
}

t_rc SYSM_DatabaseManager::OpenDatabase (const char *dbName) {

	// Check if there is another open database
	if(open){
		return (SYSM_OTHERDBISOPEN);
	}
													//prepei na mpei elegxos ean YPARXEI db me onoma dbName, prin thn energopoihsei
	activedb = (char*)malloc(strlen(dbName));
	strcpy(activedb,dbName);
	open = true;
	return OK;
}

t_rc SYSM_DatabaseManager::CloseDatabase(){

	if (!open) { return SYSM_DBCLOSED; }
	//close relmet, attrmet
	delete activedb;
	open=false;
	return OK;
}

const char *SYSM_DatabaseManager::getdbName(){

	return activedb;
}

bool SYSM_DatabaseManager::isOpen(){
	if (open){
		return true;
	}
	return false;	
}


t_rc SYSM_DatabaseManager::DropDatabase (const char *dbName){
	ifstream myfile;
	int i=0;
	char relpath[260];
	char attrpath[260];
	char tablename[255];
	char deletename[255];
	const char *name;
	_snprintf_s(relpath,sizeof(relpath),"%s/rel.met",dbName);
	_snprintf_s(attrpath, sizeof(attrpath), "%s/attr.met",dbName);
	myfile.open(relpath);
	while (myfile.getline(tablename,255)){
		while (tablename[i]!=' ' && tablename[i]!='\0') {
			i++;
		}
		tablename[i]='\0';
		name=tablename;
		_snprintf_s(deletename,sizeof(deletename),"%s/%s",dbName,name);
		remove(deletename);
		remove(relpath);
		remove(attrpath);
		getchar();
		rmdir(dbName);
	}
	return OK;
}
