/* ============================================================================================== */
// UTILS_misc.cpp
//
// Misc utility functions. You can add more to this file.
// 
// 
// 
//
//
//
//
//
// -----------------------
// Last update: 9/3/2009
/* ============================================================================================== */

#include "UTILS_misc.h"
#include <cstdio>
#include <cstring>

void Pause()
{
	char s[3];
	printf("Pause (press ENTER to continue) ...");
	gets(s);
}
