/*
 * plr - PostgreSQL support for R as a
 *	     procedural language (PL)
 *
 * Copyright (c) 2003 by Joseph E. Conway
 * ALL RIGHTS RESERVED;
 * 
 * Joe Conway <mail@joeconway.com>
 *
 * Heavily based on pltcl by Jan Wieck
 * and
 * on REmbeddedPostgres by
 * Duncan Temple Lang <duncan@research.bell-labs.com>
 * http://www.omegahat.org/RSPostgres/
 *
 * License: GPL version 2 or newer. http://www.gnu.org/copyleft/gpl.html
 *
 * pg_userfuncs.c - User visible PostgreSQL functions
 */
#include "plr.h"

static ArrayType *array_create(FunctionCallInfo fcinfo, int numelems, int elem_start);


/*-----------------------------------------------------------------------------
 * install_rcmd :
 *		interface to allow user defined R functions to be called from other
 *		R functions
 *----------------------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(install_rcmd);
Datum
install_rcmd(PG_FUNCTION_ARGS)
{
	char *cmd = PG_TEXT_GET_STR(PG_GETARG_TEXT_P(0));

	load_r_cmd(cmd);

	PG_RETURN_TEXT_P(PG_STR_GET_TEXT("OK"));
}

/*-----------------------------------------------------------------------------
 * array :
 *		form a one-dimensional array given starting elements
 *----------------------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(array);
Datum
array(PG_FUNCTION_ARGS)
{
	ArrayType  *result;

	result = array_create(fcinfo, PG_NARGS(), 0);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*-----------------------------------------------------------------------------
 * array_push :
 *		push an element onto the end of a one-dimensional array
 *----------------------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(array_push);
Datum
array_push(PG_FUNCTION_ARGS)
{
	ArrayType  *v;
	Datum		newelem;
	int		   *dimv,
			   *lb, ub;
	ArrayType  *result;
	int			indx;
	bool		isNull;
	Oid			element_type;
	int			typlen;
	bool		typbyval;
	char		typdelim;
	Oid			typinput,
				typelem;
	char		typalign;

	/* return NULL if first argument is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* return the first argument if the second is NULL */
	if (PG_ARGISNULL(1))
		PG_RETURN_ARRAYTYPE_P(PG_GETARG_ARRAYTYPE_P_COPY(0));

	v = PG_GETARG_ARRAYTYPE_P(0);
	newelem = PG_GETARG_DATUM(1);

	/* Sanity check: do we have a one-dimensional array */
	if (ARR_NDIM(v) != 1)
		elog(ERROR, "array_push: only one-dimensional arrays are supported");

	lb = ARR_LBOUND(v);
	dimv = ARR_DIMS(v);
	ub = dimv[0] + lb[0] - 1;
	indx = ub + 1;

	element_type = ARR_ELEMTYPE(v);
	/* Sanity check: do we have a non-zero element type */
	if (element_type == 0)
		elog(ERROR, "array_push: invalid array element type");

	system_cache_lookup(element_type, true, &typlen, &typbyval,
						&typdelim, &typelem, &typinput, &typalign);

	result = array_set(v, 1, &indx, newelem, -1,
						typlen, typbyval, typalign, &isNull);

	PG_RETURN_ARRAYTYPE_P(result);
}

/*-----------------------------------------------------------------------------
 * array_accum :
 *		accumulator to build an array from input values -- when used in
 *		conjunction with plr functions that accept an array, and output
 *		a statistic, this can be used to create custom aggregates.
 *----------------------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(array_accum);
Datum
array_accum(PG_FUNCTION_ARGS)
{
	Datum		v;
	Datum		newelem;
	ArrayType  *result;

	/* return NULL if both arguments are NULL */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	/* create a new array from the second argument if first is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_ARRAYTYPE_P(array_create(fcinfo, 1, 1));

	/* return the first argument if the second is NULL */
	if (PG_ARGISNULL(1))
		PG_RETURN_ARRAYTYPE_P(PG_GETARG_ARRAYTYPE_P_COPY(0));

	v = PG_GETARG_DATUM(0);
	newelem = PG_GETARG_DATUM(1);

	result = DatumGetArrayTypeP(DirectFunctionCall2(array_push, v, newelem));

	PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * actually does the work for array(), and array_accum() if it is given a null
 * input array.
 *
 * numelems and elem_start allow the function to be shared given the differing
 * arguments accepted by array() and array_accum(). With array(), all function
 * arguments are used for array construction -- therefore elem_start is 0 and
 * numelems is the number of function arguments. With array_accum(), we are
 * always initializing the array with a single element given to us as argument
 * number 1 (i.e. the second argument).
 */
static ArrayType *
array_create(FunctionCallInfo fcinfo, int numelems, int elem_start)
{
	Oid			funcid = fcinfo->flinfo->fn_oid;
	Datum	   *dvalues = (Datum *) palloc(numelems * sizeof(Datum));
	int			typlen;
	bool		typbyval;
	char		typdelim;
	Oid			typinput,
				typelem,
				element_type;
	char		typalign;
	int			i;
	HeapTuple	tp;
	Oid			functypeid;
	Oid		   *funcargtypes;
	ArrayType  *result;

	if (numelems == 0)
		elog(ERROR, "array: at least one value required to construct an array");

	/*
	 * Get the type metadata for the array return type and its elements
	 */
	tp = SearchSysCache(PROCOID,
						ObjectIdGetDatum(funcid),
						0, 0, 0);
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "Function OID %u does not exist", funcid);

	functypeid = ((Form_pg_proc) GETSTRUCT(tp))->prorettype;
	getTypeInputInfo(functypeid, &typinput, &element_type);

	system_cache_lookup(element_type, true, &typlen, &typbyval,
						&typdelim, &typelem, &typinput, &typalign);

	funcargtypes = ((Form_pg_proc) GETSTRUCT(tp))->proargtypes;

	/*
	 * the first function argument(s) may not be one of our array elements,
	 * but the caller is responsible to ensure we get nothing but array
	 * elements once they start coming
	 */
	for (i = elem_start; i < elem_start + numelems; i++)
		if (funcargtypes[i] != element_type)
			elog(ERROR, "array_create: argument %d datatype not " \
						"compatible with return data type", i + 1);
	ReleaseSysCache(tp);

	for (i = 0; i < numelems; i++)
		dvalues[i] = PG_GETARG_DATUM(elem_start + i);

	result = construct_array(dvalues, numelems, element_type,
							 typlen, typbyval, typalign);

	return result;
}

