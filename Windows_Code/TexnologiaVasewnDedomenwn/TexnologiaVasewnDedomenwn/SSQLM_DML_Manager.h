#ifndef _SSQLM_DML_Manager_h
#define _SSQLM_DML_Manager_h

#include "REM.h"
#include "retcodes.h"

class SSQLM_DML_Manager {

private:

	REM_RecordFileManager *rfm;

public:

	SSQLM_DML_Manager(REM_RecordFileManager *rfm);
	~SSQLM_DML_Manager();

	t_rc Select();
	t_rc From();
	t_rc Where();
	t_rc Insert();
	t_rc Delete();
	t_rc Update();

};

#endif