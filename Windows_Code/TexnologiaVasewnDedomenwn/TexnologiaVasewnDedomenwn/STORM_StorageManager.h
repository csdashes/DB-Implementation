#ifndef _STORM_Storage_Manager_h
#define _STORM_Storage_Manager_h

#include "retcodes.h"
#include "STORM_FileHandle.h"
#include "STORM_BufferManager.h"

class STORM_StorageManager
{
	typedef struct
	{
		int fileID;
		char fileName[MAX_FILENAME_LENGTH];
	} t_fileInfo; 

	t_fileInfo m_files[MAX_OPENED_FILES]; // info for all opened files 
	int m_numOpenedFiles;                 // number of files opened so far

	STORM_BufferManager m_BfrMgr;         // each storage manager has its own buffer manager

	t_rc CloseAllFiles();
	
public:
	STORM_StorageManager();
	~STORM_StorageManager();
	
	t_rc CreateFile(const char *fname); // creates a new file
	t_rc DestroyFile(const char *fname); // file destruction (all contents+file deleted)
	t_rc OpenFile(const char *fname, STORM_FileHandle &fh); // file open
	t_rc CloseFile(STORM_FileHandle &fh); // file close
	void GetStats(int &nrequests, int &nreads, int &nwrites, int &npinned, int &nframes);  // read the stats
	void InitStats(); // initializes the statistics
	bool FileExists(const char *fname);  // checks if file exists
	bool IsOpened(const char *fname);    // returns true if file is opened, false otherwise

	void DisplayBufferContents();
};

#endif