#include "retcodes.h"
#include <cstdio>

void DisplayReturnCode(t_rc rc)
{
	char const *msg;

	switch(rc)
	{
	case OK: msg = "OK"; break;
	case STORM_FILENOTFOUND: msg = "Specified file is not found."; break;
	case STORM_FILEEXISTS: msg = "File already exists."; break;
	case STORM_FILEDOESNOTEXIST: msg="File does not exist."; break;
	case STORM_FILEALREADYOPENED: msg = "File already opened."; break;
	case STORM_EOF: msg = "End Of File (EOF) has been reached."; break;
	case STORM_FILEFULL: msg = "File has reached its maximum capacity."; break;
	case STORM_FILEOPENERROR: msg = "File open error."; break;
	case STORM_FILECLOSEERROR: msg = "File close error."; break;
	case STORM_FILENOTOPENED: msg = "File is not opened."; break;
	case STORM_OPENEDFILELIMITREACHED: msg = "Limit of opened files reached."; break;
	case STORM_INVALIDFILEHANDLE: msg = "Invalid File Handle."; break;
	case STORM_INVALIDPAGE: msg = "Page is not valid."; break;
	case STORM_CANNOTCREATEFILE: msg = "Cannot create file."; break;
	case STORM_CANNOTDESTROYFILE: msg = "Cannot destroy file."; break;
	case STORM_PAGENOTINBUFFER: msg = "Page is not in buffer."; break;
	case STORM_PAGEISPINNED: msg = "Page is pinned."; break;
	case STORM_ALLPAGESPINNED: msg = "All pages are pinned."; break;
	case STORM_IOERROR: msg = "Input/Output error."; break;
	case STORM_MEMALLOCERROR: msg = "Memory allocation error."; break;
	
	/* return codes for RECORD MODULE (REM) */
			
	case REM_INVALIDRID:		msg = "Invalid RID"; break;
	case REM_INVALIDREC:		msg = "Invalid record"; break;
	case REM_INVALIDRECSIZE:	msg = "Invalid record size"; break;
	case REM_FHCLOSED:			msg = "REM File-handle closed"; break;
	case REM_FSOPEN:			msg = "REM FileScan is already open"; break;
	case REM_FSCLOSED:			msg = "REM FileScan is closed"; break;
	case REM_FSEOF:				msg = "REM FileScan End Of File (EOF) has been reached."; break;
			
	/* return codes for INDEX MODULE (INXM) */
		
	case INXM_INVALIDINDEX:			msg = "Invalid index, maybe no records inside?"; break;
	case INXM_INVALIDRECSIZE:		msg = "Invalid record size"; break;
	case INXM_IHCLOSED:				msg = "INXM handle closed"; break;
	case INXM_KEY_NOT_FOUND:		msg = "Key not found in b+ tree"; break;
	case INXM_FSOPEN:				msg = "INXM FileScan is already open"; break;
	case INXM_FSCLOSED:				msg = "INXM FileScan is closed"; break;
	case INXM_FSEOF:				msg = "INXM FileScan End Of File (EOF) has been reached."; break;

	/* return codes for SYSTEM MODULE (SYSM) */

	case SYSM_OTHERDBISOPEN:		msg = "Another database is already open"; break;
	case SYSM_DBCLOSED:				msg = "The database is already closed"; break;
		

		
	default: msg = "Unknown return code."; break;

	}

	fprintf (stderr, "ERROR: %s\n", msg);
}

void DisplayMessage(char *msg)
{
	fprintf (stderr, "ERROR: %s\n", msg); 
}
