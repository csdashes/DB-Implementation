#ifndef _components_h
#define _components_h

typedef enum {
	TYPE_INT,	/* for integers */
	TYPE_STRING /* character string up to 255 chars */ 
} t_attrType;

typedef enum {
	EQ_OP, // equal
	LT_OP, // less than
	GT_OP, // greater than
	NE_OP, // not equal
	LE_OP, // less than or equal
	GE_OP, // greater than or equal
	NO_OP  // No comparison. Should be used when file scan value is NULL.
} t_compOp;

#endif