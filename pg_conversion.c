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
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * pg_conversion.c - functions for converting arguments from pg types to
 *                   R types, and for converting return values from R types
 *                   to pg types
 */
#include "plr.h"
#include "funcapi.h"
#include "miscadmin.h"

static void pg_get_one_r(char *value, Oid arg_out_fn_oid, SEXP *obj, int elnum);
static void get_r_vector(Oid typtype, SEXP *obj, int numels);
static Datum get_tuplestore(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
static Datum get_array_datum(SEXP rval, plr_proc_desc *prodesc);
static Datum get_frame_array_datum(SEXP rval, plr_proc_desc *prodesc);
static Datum get_matrix_array_datum(SEXP rval, plr_proc_desc *prodesc);
static Datum get_generic_array_datum(SEXP rval, plr_proc_desc *prodesc);
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
 * given a pg value, convert to its R value representation
 */
SEXP
pg_get_r(plr_proc_desc *prodesc, FunctionCallInfo fcinfo, int idx)
{
	Datum		dvalue = fcinfo->arg[idx];
	Oid			arg_typid = prodesc->arg_typid[idx];
	FmgrInfo	arg_out_func = prodesc->arg_out_func[idx];
	Oid			arg_elem = prodesc->arg_elem[idx];
	SEXP		obj = NULL;
	char	   *value;

	PROTECT(obj);

	if (arg_elem == 0)
	{
		/*
		 * if the element type is zero, we don't have an array,
		 * so just convert as a scalar value
		 */
		value = DatumGetCString(FunctionCall3(&arg_out_func,
											  dvalue,
								 			  ObjectIdGetDatum(arg_elem),
											  Int32GetDatum(-1)));

		if (value != NULL)
		{
			/* get new vector of the appropriate type, length 1 */
			get_r_vector(arg_typid, &obj, 1);

			/* add our value to it */
			pg_get_one_r(value, arg_typid, &obj, 0);
		}
		else
		{
			obj = NEW_CHARACTER(1);
			SET_STRING_ELT(obj, 0, NA_STRING);
		}
	}
	else
	{
		/*
		 * We do have an array, so loop through and convert each scalar value.
		 * Use the converted values to build an R vector.
		 */
		ArrayType  *v = (ArrayType *) dvalue;
		Oid			element_type;
		int			i,
					nitems,
					ndim,
				   *dim;
		char	   *p;
		FmgrInfo	out_func = prodesc->arg_elem_out_func[idx];
		int			typlen = prodesc->arg_elem_typlen[idx];
		bool		typbyval = prodesc->arg_elem_typbyval[idx];
		char		typalign = prodesc->arg_elem_typalign[idx];

		/* only support one-dim arrays for the moment */
		ndim = ARR_NDIM(v);
		if (ndim > 1)
			elog(ERROR, "plr: multiple dimension arrays are not yet supported");

		element_type = ARR_ELEMTYPE(v);
		/* sanity check */
		if (element_type != arg_elem)
			elog(ERROR, "plr: declared and actual types for array elements do not match");

		dim = ARR_DIMS(v);
		nitems = ArrayGetNItems(ndim, dim);

		/* pass an NA if the array is empty */
		if (nitems == 0)
		{
			obj = NEW_CHARACTER(1);
			SET_STRING_ELT(obj, 0, NA_STRING);

			UNPROTECT(1);
			return(obj);
		}

		/* get new vector of the appropriate type and length */
		get_r_vector(element_type, &obj, nitems);

		/* Convert all values to their R form and build the vector */
		p = ARR_DATA_PTR(v);
		for (i = 0; i < nitems; i++)
		{
			Datum		itemvalue;

			itemvalue = fetch_att(p, typbyval, typlen);
			value = DatumGetCString(FunctionCall3(&out_func,
													  itemvalue,
													  (Datum) 0,
													  Int32GetDatum(-1)));
			p = att_addlength(p, typlen, PointerGetDatum(p));
			p = (char *) att_align(p, typalign);

			if (value != NULL)
				pg_get_one_r(value, arg_elem, &obj, i);
			else
				SET_STRING_ELT(obj, i, NA_STRING);
		}
	}

	UNPROTECT(1);
	return(obj);
}

/*
 * create an R vector of a given type and size based on pg output function oid
 */
static void
get_r_vector(Oid typtype, SEXP *obj, int numels)
{
	switch (typtype)
	{
		case INT2OID:
		case INT4OID:
			/* 2 and 4 byte integer pgsql datatype => use R INTEGER */
			*obj = NEW_INTEGER(numels);
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
			*obj = NEW_NUMERIC(numels);
		    break;
		case BOOLOID:
			*obj = NEW_LOGICAL(numels);
		    break;
		default:
			/* Everything else is defaulted to string */
			*obj = NEW_CHARACTER(numels);
	}
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
	/* short circuit if return value is Null */
	if (isNull(rval))
		return (Datum) 0;

	if (prodesc->result_istuple)
		return get_tuplestore(rval, prodesc, fcinfo);
	else
	{
		if (prodesc->result_elem == 0)
			return get_scalar_datum(rval, prodesc->result_in_func, prodesc->result_elem);
		else
			return get_array_datum(rval, prodesc);
	}
}

static Datum
get_tuplestore(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	AttInMetadata  *attinmeta;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	int				nc;

	/* short circuit if there is nothing to return */
	if (length(rval) == 0)
		return (Datum) 0;

	/* check to see if caller supports us returning a tuplestore */
	if (!rsinfo->allowedModes & SFRM_Materialize)
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

	return (Datum) 0;
}

Datum
get_scalar_datum(SEXP rval, FmgrInfo result_in_func, Oid result_elem)
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

	if (value != NULL)
		dvalue = FunctionCall3(&result_in_func,
								CStringGetDatum(value),
								ObjectIdGetDatum(result_elem),
								Int32GetDatum(-1));
	else
		dvalue = (Datum) 0;

	UNPROTECT(1);

	return dvalue;
}

static Datum
get_array_datum(SEXP rval, plr_proc_desc *prodesc)
{
	/* short circuit if there is nothing to return */
	if (length(rval) == 0)
		return (Datum) 0;

	if (isFrame(rval))
		return get_frame_array_datum(rval, prodesc);
	else if (isMatrix(rval))
		return get_matrix_array_datum(rval, prodesc);
	else
		return get_generic_array_datum(rval, prodesc);
}

static Datum
get_frame_array_datum(SEXP rval, plr_proc_desc *prodesc)
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

			if (value != NULL)
				dvalues[idx] = FunctionCall3(&in_func,
										CStringGetDatum(value),
										(Datum) 0,
										Int32GetDatum(-1));
			else
				dvalues[idx] = (Datum) 0;
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
get_matrix_array_datum(SEXP rval, plr_proc_desc *prodesc)
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

		if (value != NULL)
			dvalues[idx] = FunctionCall3(&in_func,
									CStringGetDatum(value),
									(Datum) 0,
									Int32GetDatum(-1));
		else
			dvalues[idx] = (Datum) 0;
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
get_generic_array_datum(SEXP rval, plr_proc_desc *prodesc)
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

		if (value != NULL)
			dvalues[i] = FunctionCall3(&in_func,
									CStringGetDatum(value),
									(Datum) 0,
									Int32GetDatum(-1));
		else
			dvalues[i] = (Datum) 0;
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

			values[j] = pstrdup(CHAR(STRING_ELT(obj, i)));

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

