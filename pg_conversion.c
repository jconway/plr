/*
 * PL/R - PostgreSQL support for R as a
 *	      procedural language (PL)
 *
 * Copyright (c) 2003 by Joseph E. Conway
 * ALL RIGHTS RESERVED;
 * 
 * Joe Conway <mail@joeconway.com>
 * 
 * Based on pltcl by Jan Wieck
 * and inspired by REmbeddedPostgres by
 * Duncan Temple Lang <duncan@research.bell-labs.com>
 * http://www.omegahat.org/RSPostgres/
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 * pg_conversion.c - functions for converting arguments from pg types to
 *                   R types, and for converting return values from R types
 *                   to pg types
 */
#include "plr.h"
#include "funcapi.h"
#include "miscadmin.h"

static void pg_get_one_r(char *value, Oid arg_out_fn_oid, SEXP *obj, int elnum);
static SEXP get_r_vector(Oid typtype, int numels);
static Datum get_tuplestore(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo, bool *isnull);
static Datum get_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull);
static Datum get_frame_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull);
static Datum get_matrix_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull);
static Datum get_generic_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull);
static Tuplestorestate *get_frame_tuplestore(SEXP rval,
											 plr_proc_desc *prodesc,
											 AttInMetadata *attinmeta,
											 MemoryContext per_query_ctx);
static Tuplestorestate *get_matrix_tuplestore(SEXP rval,
											 plr_proc_desc *prodesc,
											 AttInMetadata *attinmeta,
											 MemoryContext per_query_ctx);
static Tuplestorestate *get_generic_tuplestore(SEXP rval,
											 plr_proc_desc *prodesc,
											 AttInMetadata *attinmeta,
											 MemoryContext per_query_ctx);

/*
 * given a scalar pg value, convert to a one row R vector
 */
SEXP
pg_scalar_get_r(Datum dvalue, Oid arg_typid, FmgrInfo arg_out_func)
{
	SEXP		result;
	char	   *value;

	if (dvalue == (Datum) 0)
	{
		/* fast track for null arguments */
		PROTECT(result = NEW_CHARACTER(1));
		SET_STRING_ELT(result, 0, NA_STRING);
		UNPROTECT(1);

		return result;
	}

	value = DatumGetCString(FunctionCall3(&arg_out_func,
										  dvalue,
							 			  (Datum) 0,
										  Int32GetDatum(-1)));

	if (value != NULL)
	{
		/* get new vector of the appropriate type, length 1 */
		PROTECT(result = get_r_vector(arg_typid, 1));

		/* add our value to it */
		pg_get_one_r(value, arg_typid, &result, 0);
	}
	else
	{
		PROTECT(result = NEW_CHARACTER(1));
		SET_STRING_ELT(result, 0, NA_STRING);
	}

	UNPROTECT(1);

	return result;
}


/*
 * given an array pg value, convert to a multi-row R vector
 */
SEXP
pg_array_get_r(Datum dvalue, FmgrInfo out_func, int typlen, bool typbyval, char typalign)
{
	/*
	 * Loop through and convert each scalar value.
	 * Use the converted values to build an R vector.
	 */
	SEXP		result;
	char	   *value;
	ArrayType  *v = (ArrayType *) dvalue;
	Oid			element_type;
	int			i, j,
				nitems,
				nr = 0,
				nc = 0,
				ndim,
			   *dim;
	char	   *p;

	/* only support one-dim arrays for the moment */
	ndim = ARR_NDIM(v);
	element_type = ARR_ELEMTYPE(v);
	dim = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dim);

	/* pass an NA if the array is empty */
	if (nitems == 0)
	{
		PROTECT(result = NEW_CHARACTER(1));
		SET_STRING_ELT(result, 0, NA_STRING);
		UNPROTECT(1);

		return result;
	}

	if (ndim < 2)
	{
		nr = nitems;
		nc = 1;
	}
	else if (ndim == 2)
	{
		nr = dim[0];
		nc = dim[1];
	}
	else
		elog(ERROR, "plr: 3 (or more) dimension arrays are not yet supported as function arguments");

	/* get new vector of the appropriate type and length */
	PROTECT(result = get_r_vector(element_type, nitems));

	/* Convert all values to their R form and build the vector */
	p = ARR_DATA_PTR(v);
	for (i = 0; i < nr; i++)
	{
		for (j = 0; j < nc; j++)
		{
			Datum		itemvalue;
			int			idx = (j * nr) + i;

			itemvalue = fetch_att(p, typbyval, typlen);
			value = DatumGetCString(FunctionCall3(&out_func,
													  itemvalue,
													  (Datum) 0,
													  Int32GetDatum(-1)));
			p = att_addlength(p, typlen, PointerGetDatum(p));
			p = (char *) att_align(p, typalign);

			if (value != NULL)
				pg_get_one_r(value, element_type, &result, idx);
			else
				SET_STRING_ELT(result, idx, NA_STRING);
		}
	}
	UNPROTECT(1);

	if (ndim == 2)
	{
		SEXP	matrix_dims;

		/* attach dimensions */
		PROTECT(matrix_dims = allocVector(INTSXP, 2));
		INTEGER_DATA(matrix_dims)[0] = dim[0];
		INTEGER_DATA(matrix_dims)[1] = dim[1];
		setAttrib(result, R_DimSymbol, matrix_dims);
		UNPROTECT(1);
	}

	return result;
}

/*
 * given an array of pg tuples, convert to an R data.frame
 */
SEXP
pg_tuple_get_r_frame(int ntuples, HeapTuple *tuples, TupleDesc tupdesc)
{
	int			nr = ntuples;
	int			nc = tupdesc->natts;
	int			i = 0;
	int			j = 0;
	Oid			element_type;
	Oid			typelem;
	SEXP		names = NULL;
	SEXP		row_names = NULL;
	char		buf[256];
	SEXP		result;
	SEXP		fun, rargs;
	SEXP		fldvec;

	if (tuples == NULL || ntuples < 1)
		return(R_NilValue);

	/* get a reference to the R "factor" function */
	PROTECT(fun = Rf_findFun(Rf_install("factor"), R_GlobalEnv));

	/*
	 * create an array of R objects with the number of elements
	 * equal to the number of arguments needed by "factor".
	 */
	PROTECT(rargs = allocVector(VECSXP, 2));

	/* the first arg to "factor" is NIL */
	SET_VECTOR_ELT(rargs, 0, R_NilValue);

	/*
	 * Allocate the data.frame initially as a list,
	 * and also allocate a names vector for the column names
	 */
	PROTECT(result = allocVector(VECSXP, nc));
    PROTECT(names = allocVector(STRSXP, nc));

	/*
	 * Loop by columns
	 */
	for (j = 0; j < nc; j++)		
	{
		int			typlen;
		bool		typbyval;
		char		typdelim;
		Oid			typoutput,
					elemtypelem;
		FmgrInfo	outputproc;
		char		typalign;

		/* set column name */
		SET_STRING_ELT(names, j,  mkChar(SPI_fname(tupdesc, j + 1)));

		/* get column datatype oid */
		element_type = SPI_gettypeid(tupdesc, j + 1);

		/* special case -- NAME looks like an array, but treat as a scalar */
		if (element_type == NAMEOID)
			typelem = 0;
		else
			/* check to see it it is an array type */
			typelem = get_typelem(element_type);

		/* get new vector of the appropriate type and length */
		if (typelem == 0)
			PROTECT(fldvec = get_r_vector(element_type, nr));
		else
		{
			PROTECT(fldvec = NEW_LIST(nr));
			system_cache_lookup(typelem, false, &typlen, &typbyval,
								&typdelim, &elemtypelem, &typoutput, &typalign);
			fmgr_info(typoutput, &outputproc);
		}

		/* loop rows for this column */
		for (i = 0; i < nr; i++)
		{
			if (typelem == 0)
			{
				/* not an array type */
				char	   *value;

				value = SPI_getvalue(tuples[i], tupdesc, j + 1);
				if (value != NULL)
					pg_get_one_r(value, element_type, &fldvec, i);
				else
					SET_STRING_ELT(fldvec, i, NA_STRING);
			}
			else
			{
				/* array type */
				Datum		dvalue;
				bool		isnull;
				SEXP		fldvec_elem;

				dvalue = SPI_getbinval(tuples[i], tupdesc, j + 1, &isnull);
				if (!isnull)
					PROTECT(fldvec_elem = pg_array_get_r(dvalue, outputproc, typlen, typbyval, typalign));
				else
				{
					PROTECT(fldvec_elem = NEW_CHARACTER(1));
					SET_STRING_ELT(fldvec_elem, 0, NA_STRING);
				}

				SET_VECTOR_ELT(fldvec, i, fldvec_elem);
				UNPROTECT(1);
			}
		}

		/* convert column into a factor column if needed */
		if (typelem == 0)
		{
			switch(element_type)
			{
				case OIDOID:
				case INT2OID:
				case INT4OID:
				case INT8OID:
				case FLOAT4OID:
				case FLOAT8OID:
				case CASHOID:
				case NUMERICOID:
				case BOOLOID:
					/* do nothing */
				    break;
				default:
					/* the second arg to "factor" is our character vector */
					SET_VECTOR_ELT(rargs, 1, fldvec);

					/* convert to a factor */
					PROTECT(fldvec = call_r_func(fun, rargs));
					UNPROTECT(1);
			}
		}

		SET_VECTOR_ELT(result, j, fldvec);
		UNPROTECT(1);
	}

	/* attach the column names */
    setAttrib(result, R_NamesSymbol, names);
	UNPROTECT(3);

	/* attach row names - basically just the row number, zero based */
	PROTECT(row_names = allocVector(STRSXP, nr));
	for (i=0; i<nr; i++)
	{
	    sprintf(buf, "%d", i+1);
	    SET_STRING_ELT(row_names, i, mkChar(buf));
	}
	setAttrib(result, R_RowNamesSymbol, row_names);

	/* finally, tell R we are a "data.frame" */
    setAttrib(result, R_ClassSymbol, mkString("data.frame"));

	UNPROTECT(2);
	return result;
}

/*
 * create an R vector of a given type and size based on pg output function oid
 */
static SEXP
get_r_vector(Oid typtype, int numels)
{
	SEXP	result;

	switch (typtype)
	{
		case INT2OID:
		case INT4OID:
			/* 2 and 4 byte integer pgsql datatype => use R INTEGER */
			PROTECT(result = NEW_INTEGER(numels));
		    break;
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case CASHOID:
		case NUMERICOID:
			/*
			 * Other numeric types => use R REAL
			 * Note pgsql int8 is mapped to R REAL
			 * because R INTEGER is only 4 byte
			 */
			PROTECT(result = NEW_NUMERIC(numels));
		    break;
		case BOOLOID:
			PROTECT(result = NEW_LOGICAL(numels));
		    break;
		default:
			/* Everything else is defaulted to string */
			PROTECT(result = NEW_CHARACTER(numels));
	}
	UNPROTECT(1);

	return result;
}

/*
 * given a single non-array pg value, convert to its R value representation
 */
static void
pg_get_one_r(char *value, Oid typtype, SEXP *obj, int elnum)
{
	switch (typtype)
	{
		case INT2OID:
		case INT4OID:
			/* 2 and 4 byte integer pgsql datatype => use R INTEGER */
			INTEGER_DATA(*obj)[elnum] = atoi(value);
		    break;
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case CASHOID:
		case NUMERICOID:
			/*
			 * Other numeric types => use R REAL
			 * Note pgsql int8 is mapped to R REAL
			 * because R INTEGER is only 4 byte
			 */
			NUMERIC_DATA(*obj)[elnum] = atof(value);
		    break;
		case BOOLOID:
			LOGICAL_DATA(*obj)[elnum] = ((*value == 't') ? 1 : 0);
		    break;
		default:
			/* Everything else is defaulted to string */
			SET_STRING_ELT(*obj, elnum, COPY_TO_USER_STRING(value));
	}
}

/*
 * given an R value, convert to its pg representation
 */
Datum
r_get_pg(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo)
{
	bool	isnull = false;
	Datum	result;

	/* short circuit if return value is Null */
	if (isNull(rval) || length(rval) == 0)
	{
		fcinfo->isnull = true;
		return (Datum) 0;
	}

	if (prodesc->result_istuple)
		result = get_tuplestore(rval, prodesc, fcinfo, &isnull);
	else
	{
		if (prodesc->result_elem == 0)
			result = get_scalar_datum(rval, prodesc->result_in_func, prodesc->result_elem, &isnull);
		else
			result = get_array_datum(rval, prodesc, &isnull);
	}

	if (isnull)
		fcinfo->isnull = true;

	return result;
}

static Datum
get_tuplestore(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo, bool *isnull)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	AttInMetadata  *attinmeta;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	int				nc;

	/* check to see if caller supports us returning a tuplestore */
	if (!rsinfo || (!rsinfo->allowedModes & SFRM_Materialize))
		elog(ERROR, "plr: Materialize mode required, but it is not "
			 "allowed in this context");

	if (isFrame(rval))
		nc = length(rval);
	else if (isMatrix(rval))
		nc = ncols(rval);
	else
		nc = 1;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* get the requested return tuple description */
	tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);

	/*
	 * Check to make sure we have the same number of columns
	 * to return as there are attributes in the return tuple.
	 *
	 * Note we will attempt to coerce the R values into whatever
	 * the return attribute type is and depend on the "in"
	 * function to complain if needed.
	 */
	if (nc != tupdesc->natts)
		elog(ERROR, "plr: Query-specified return tuple and " \
					"function returned data.frame are not compatible");

	attinmeta = TupleDescGetAttInMetadata(tupdesc);

	/* OK, go to work */
	rsinfo->returnMode = SFRM_Materialize;

	if (isFrame(rval))
		rsinfo->setResult = get_frame_tuplestore(rval, prodesc, attinmeta, per_query_ctx);
	else if (isMatrix(rval))
		rsinfo->setResult = get_matrix_tuplestore(rval, prodesc, attinmeta, per_query_ctx);
	else
		rsinfo->setResult = get_generic_tuplestore(rval, prodesc, attinmeta, per_query_ctx);

	/*
	 * SFRM_Materialize mode expects us to return a NULL Datum. The actual
	 * tuples are in our tuplestore and passed back through
	 * rsinfo->setResult. rsinfo->setDesc is set to the tuple description
	 * that we actually used to build our tuples with, so the caller can
	 * verify we did what it was expecting.
	 */
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	*isnull = true;
	return (Datum) 0;
}

Datum
get_scalar_datum(SEXP rval, FmgrInfo result_in_func, Oid result_elem, bool *isnull)
{
	Datum		dvalue;
	SEXP		obj;
	char	   *value;

	/*
	 * if the element type is zero, we don't have an array,
	 * so coerce to string and take the first element as a scalar
	 */
	PROTECT(obj = AS_CHARACTER(rval));
	value = CHAR(STRING_ELT(obj, 0));

	if (STRING_ELT(obj, 0) == NA_STRING)
	{
		*isnull = true;
		dvalue = (Datum) 0;
	}
	else if (value != NULL)
	{
		dvalue = FunctionCall3(&result_in_func,
								CStringGetDatum(value),
								ObjectIdGetDatum(result_elem),
								Int32GetDatum(-1));
	}
	else
	{
		*isnull = true;
		dvalue = (Datum) 0;
	}

	UNPROTECT(1);

	return dvalue;
}

static Datum
get_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull)
{
	if (isFrame(rval))
		return get_frame_array_datum(rval, prodesc, isnull);
	else if (isMatrix(rval))
		return get_matrix_array_datum(rval, prodesc, isnull);
	else
		return get_generic_array_datum(rval, prodesc, isnull);
}

static Datum
get_frame_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull)
{
	Datum		dvalue;
	SEXP		obj;
	char	   *value;
	Oid			result_elem = prodesc->result_elem;
	FmgrInfo	in_func = prodesc->result_elem_in_func;
	int			typlen = prodesc->result_elem_typlen;
	bool		typbyval = prodesc->result_elem_typbyval;
	char		typalign = prodesc->result_elem_typalign;
	int			i;
	Datum	   *dvalues = NULL;
	ArrayType  *array;
	int			nr = 0;
	int			nc = length(rval);
	int			ndims = 2;
	int			ndatabytes;
	int			nbytes;
	ArrayType  *tmparray;
	int			dims[ndims];
	int			lbs[ndims];
	int			idx;
	SEXP		dfcol = NULL;
	int			j;

	for (j = 0; j < nc; j++)
	{
		if (TYPEOF(rval) == VECSXP)
			PROTECT(dfcol = VECTOR_ELT(rval, j));
		else if (TYPEOF(rval) == LISTSXP)
		{
			PROTECT(dfcol = CAR(rval));
			rval = CDR(rval);
		}
		else
			elog(ERROR, "plr: bad internal representation of data.frame");

		if (ATTRIB(dfcol) == R_NilValue)
			PROTECT(obj = AS_CHARACTER(dfcol));
		else
			PROTECT(obj = AS_CHARACTER(CAR(ATTRIB(dfcol))));

		if (j == 0)
		{
			nr = length(obj);
			dvalues = (Datum *) palloc(nr * nc * sizeof(Datum));
		}

		for(i = 0; i < nr; i++)
		{
			value = CHAR(STRING_ELT(obj, i));
			idx = ((i * nc) + j);

			if (STRING_ELT(obj, i) == NA_STRING || value == NULL)
				elog(ERROR, "plr: cannot return array with NULL elements");
			else
				dvalues[idx] = FunctionCall3(&in_func,
										CStringGetDatum(value),
										(Datum) 0,
										Int32GetDatum(-1));
	    }
		UNPROTECT(2);
	}

	dims[0] = nr;
	dims[1] = nc;
	lbs[0] = 1;
	lbs[1] = 1;

	/* build up 1d array */
	array = construct_array(dvalues, nr * nc, result_elem, typlen, typbyval, typalign);

	/* convert it to a 2d array */
	ndatabytes = ARR_SIZE(array) - ARR_OVERHEAD(1);
	nbytes = ndatabytes + ARR_OVERHEAD(2);
	tmparray = (ArrayType *) palloc(nbytes);

	tmparray->size = nbytes;
	tmparray->ndim = ndims;
	tmparray->flags = 0;
	tmparray->elemtype = result_elem;
	memcpy(ARR_DIMS(tmparray), dims, ndims * sizeof(int));
	memcpy(ARR_LBOUND(tmparray), lbs, ndims * sizeof(int));
	memcpy(ARR_DATA_PTR(tmparray), ARR_DATA_PTR(array), ndatabytes);

	dvalue = PointerGetDatum(tmparray);

	return dvalue;
}

static Datum
get_matrix_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull)
{
	Datum		dvalue;
	SEXP		obj;
	char	   *value;
	Oid			result_elem = prodesc->result_elem;
	FmgrInfo	in_func = prodesc->result_elem_in_func;
	int			typlen = prodesc->result_elem_typlen;
	bool		typbyval = prodesc->result_elem_typbyval;
	char		typalign = prodesc->result_elem_typalign;
	int			i;
	Datum	   *dvalues = NULL;
	ArrayType  *array;

	int			nr = nrows(rval);
	int			nc = ncols(rval);
	int			ndims = 2;
	int			ndatabytes;
	int			nbytes;
	ArrayType  *tmparray;
	int			dims[ndims];
	int			lbs[ndims];
	int			idx;

	dims[0] = nr;
	dims[1] = nc;
	lbs[0] = 1;
	lbs[1] = 1;

	dvalues = (Datum *) palloc(nr * nc * sizeof(Datum));
	PROTECT(obj =  AS_CHARACTER(rval));

	/* Loop is needed here as result value might be of length > 1 */
	for(i = 0; i < nr * nc; i++)
	{
		value = CHAR(STRING_ELT(obj, i));
		idx = ((i % nr) * nc) + (i / nr);

		if (STRING_ELT(obj, i) == NA_STRING || value == NULL)
			elog(ERROR, "plr: cannot return array with NULL elements");
		else
			dvalues[idx] = FunctionCall3(&in_func,
									CStringGetDatum(value),
									(Datum) 0,
									Int32GetDatum(-1));
    }
	UNPROTECT(1);

	/* build up 1d array */
	array = construct_array(dvalues, nr * nc, result_elem, typlen, typbyval, typalign);

	/* convert it to a 2d array */
	ndatabytes = ARR_SIZE(array) - ARR_OVERHEAD(1);
	nbytes = ndatabytes + ARR_OVERHEAD(2);
	tmparray = (ArrayType *) palloc(nbytes);

	tmparray->size = nbytes;
	tmparray->ndim = ndims;
	tmparray->flags = 0;
	tmparray->elemtype = result_elem;
	memcpy(ARR_DIMS(tmparray), dims, ndims * sizeof(int));
	memcpy(ARR_LBOUND(tmparray), lbs, ndims * sizeof(int));
	memcpy(ARR_DATA_PTR(tmparray), ARR_DATA_PTR(array), ndatabytes);

	dvalue = PointerGetDatum(tmparray);

	return dvalue;
}

static Datum
get_generic_array_datum(SEXP rval, plr_proc_desc *prodesc, bool *isnull)
{
	int			objlen = length(rval);
	Datum		dvalue;
	SEXP		obj;
	char	   *value;
	Oid			result_elem = prodesc->result_elem;
	FmgrInfo	in_func = prodesc->result_elem_in_func;
	int			typlen = prodesc->result_elem_typlen;
	bool		typbyval = prodesc->result_elem_typbyval;
	char		typalign = prodesc->result_elem_typalign;
	int			i;
	Datum	   *dvalues = NULL;
	ArrayType  *array;

	dvalues = (Datum *) palloc(objlen * sizeof(Datum));
	PROTECT(obj =  AS_CHARACTER(rval));

	/* Loop is needed here as result value might be of length > 1 */
	for(i = 0; i < objlen; i++)
	{
		value = CHAR(STRING_ELT(obj, i));

		if (STRING_ELT(obj, i) == NA_STRING || value == NULL)
			elog(ERROR, "plr: cannot return array with NULL elements");
		else
			dvalues[i] = FunctionCall3(&in_func,
									CStringGetDatum(value),
									(Datum) 0,
									Int32GetDatum(-1));
    }
	UNPROTECT(1);

	/* build up array */
	array = construct_array(dvalues, objlen, result_elem, typlen, typbyval, typalign);

	dvalue = PointerGetDatum(array);

	return dvalue;
}

static Tuplestorestate *
get_frame_tuplestore(SEXP rval,
					 plr_proc_desc *prodesc,
					 AttInMetadata *attinmeta,
					 MemoryContext per_query_ctx)
{
	Tuplestorestate	   *tupstore;
	char			  **values;
	HeapTuple			tuple;
	MemoryContext		oldcontext;
	SEXP				obj;
	int					i, j;
	int					nr = 0;
	int					nc = length(rval);
	SEXP				dfcol = NULL,
						orig_rval;

	/* switch to appropriate context to create the tuple store */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* initialize our tuplestore */
	tupstore = tuplestore_begin_heap(true, SortMem);

	MemoryContextSwitchTo(oldcontext);

	/* get number of rows by examining the first column */
	if (TYPEOF(rval) == VECSXP)
		PROTECT(dfcol = VECTOR_ELT(rval, 0));
	else if (TYPEOF(rval) == LISTSXP)
		PROTECT(dfcol = CAR(rval));
	else
		elog(ERROR, "plr: bad internal representation of data.frame");

	if (ATTRIB(dfcol) != R_NilValue)
	{
		UNPROTECT(1);
		PROTECT(dfcol = CAR(ATTRIB(dfcol)));
	}

	nr = length(dfcol);
	UNPROTECT(1);

	values = (char **) palloc(nc * sizeof(char *));

	orig_rval = rval;
	for(i = 0; i < nr; i++)
	{
		rval = orig_rval;
		for (j = 0; j < nc; j++)
		{
			if (TYPEOF(rval) == VECSXP)
				PROTECT(dfcol = VECTOR_ELT(rval, j));
			else if (TYPEOF(rval) == LISTSXP)
			{
				PROTECT(dfcol = CAR(rval));
				rval = CDR(rval);
			}
			else
				elog(ERROR, "plr: bad internal representation of data.frame");

			if (ATTRIB(dfcol) == R_NilValue)
				PROTECT(obj = AS_CHARACTER(dfcol));
			else
				PROTECT(obj = AS_CHARACTER(CAR(ATTRIB(dfcol))));

			if (STRING_ELT(obj, 0) != NA_STRING)
				values[j] = pstrdup(CHAR(STRING_ELT(obj, i)));
			else
				values[j] = NULL;

			UNPROTECT(2);
		}

		/* construct the tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* switch to appropriate context while storing the tuple */
		oldcontext = MemoryContextSwitchTo(per_query_ctx);

		/* now store it */
		tuplestore_puttuple(tupstore, tuple);

		/* now reset the context */
		MemoryContextSwitchTo(oldcontext);

		for (j = 0; j < nc; j++)
			if (values[j] != NULL)
				pfree(values[j]);
    }

	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	return tupstore;
}

static Tuplestorestate *
get_matrix_tuplestore(SEXP rval,
					 plr_proc_desc *prodesc,
					 AttInMetadata *attinmeta,
					 MemoryContext per_query_ctx)
{
	Tuplestorestate	   *tupstore;
	char			  **values;
	HeapTuple			tuple;
	MemoryContext		oldcontext;
	SEXP				obj;
	int					i, j;
	int					nr = nrows(rval);
	int					nc = ncols(rval);

	/* switch to appropriate context to create the tuple store */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* initialize our tuplestore */
	tupstore = tuplestore_begin_heap(true, SortMem);

	MemoryContextSwitchTo(oldcontext);

	values = (char **) palloc(nc * sizeof(char *));

	PROTECT(obj =  AS_CHARACTER(rval));
	for(i = 0; i < nr; i++)
	{
		for (j = 0; j < nc; j++)
			values[j] = CHAR(STRING_ELT(obj, (j * nr) + i));

		/* construct the tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* switch to appropriate context while storing the tuple */
		oldcontext = MemoryContextSwitchTo(per_query_ctx);

		/* now store it */
		tuplestore_puttuple(tupstore, tuple);

		/* now reset the context */
		MemoryContextSwitchTo(oldcontext);
    }
	UNPROTECT(1);

	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	return tupstore;
}

static Tuplestorestate *
get_generic_tuplestore(SEXP rval,
					 plr_proc_desc *prodesc,
					 AttInMetadata *attinmeta,
					 MemoryContext per_query_ctx)
{
	Tuplestorestate	   *tupstore;
	char			  **values;
	HeapTuple			tuple;
	MemoryContext		oldcontext;
	int					nr = length(rval);
	int					nc = 1;
	SEXP				obj;
	int					i;

	/* switch to appropriate context to create the tuple store */
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* initialize our tuplestore */
	tupstore = tuplestore_begin_heap(true, SortMem);

	MemoryContextSwitchTo(oldcontext);

	values = (char **) palloc(nc * sizeof(char *));

	PROTECT(obj =  AS_CHARACTER(rval));

	for(i = 0; i < nr; i++)
	{
		values[0] = CHAR(STRING_ELT(obj, i));

		/* construct the tuple */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* switch to appropriate context while storing the tuple */
		oldcontext = MemoryContextSwitchTo(per_query_ctx);

		/* now store it */
		tuplestore_puttuple(tupstore, tuple);

		/* now reset the context */
		MemoryContextSwitchTo(oldcontext);
    }
	UNPROTECT(1);

	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	return tupstore;
}

