#ifndef _STORM_Parameters_h
#define _STORM_Parameters_h

#define PAGE_SIZE 4096								// page capacity (in bytes)
#define INT_SIZE (sizeof(int))						// size in bytes of an int
#define PAGE_DATA_SIZE  (PAGE_SIZE-INT_SIZE)		// pure space for data
#define MAX_PAGES_PER_FILE ((PAGE_DATA_SIZE-8)*8)	// maximum number of pages in a single file 
#define MAX_FILENAME_LENGTH 100						// maximum number of characters in a filename
#define MAX_OPENED_FILES 20							// maximum number of opened files
#define INVALID_NUMBER (-1)							// invalid number
#define INVALID_PAGEID (-1)							// invalid page ID
#define INVALID_FILEID (-1)							// invalid file ID
#define BUFFER_CAPACITY 50							// number of frames in buffer (default 50)

#endif