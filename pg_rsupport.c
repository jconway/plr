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
 * pg_rsupport.c - Postgres support for use within plr functions
 */
#include "plr.h"

static SEXP rpgsql_get_results(int ntuples, SPITupleTable *tuptable);


/*
 * Functions used in R
 *****************************************************************************/
void
throw_pg_error(const char **msg)
{
	elog(NOTICE, "%s", *msg);
}

/*
 * plr_quote_literal() - quote literal strings that are to
 *			  be used in SPI_exec query strings
 */
SEXP
plr_quote_literal(SEXP rval)
{
	char	   *value;
	text	   *value_text;
	text	   *result_text;
	SEXP		result;

	/* extract the C string */
	PROTECT(rval =  AS_CHARACTER(rval));
	value = CHAR(STRING_ELT(rval, 0));

	/* convert using the pgsql quote_literal function */
	value_text = PG_STR_GET_TEXT(value);
	result_text = DatumGetTextP(DirectFunctionCall1(quote_literal, PointerGetDatum(value_text)));

	/* copy result back into an R object */
	PROTECT(result = NEW_CHARACTER(1));
	SET_STRING_ELT(result, 0, COPY_TO_USER_STRING(PG_TEXT_GET_STR(result_text)));
	UNPROTECT(2);

	return result;
}

/*
 * plr_quote_literal() - quote identifiers that are to
 *			  be used in SPI_exec query strings
 */
SEXP
plr_quote_ident(SEXP rval)
{
	char	   *value;
	text	   *value_text;
	text	   *result_text;
	SEXP		result;

	/* extract the C string */
	PROTECT(rval =  AS_CHARACTER(rval));
	value = CHAR(STRING_ELT(rval, 0));

	/* convert using the pgsql quote_literal function */
	value_text = PG_STR_GET_TEXT(value);
	result_text = DatumGetTextP(DirectFunctionCall1(quote_ident, PointerGetDatum(value_text)));

	/* copy result back into an R object */
	PROTECT(result = NEW_CHARACTER(1));
	SET_STRING_ELT(result, 0, COPY_TO_USER_STRING(PG_TEXT_GET_STR(result_text)));
	UNPROTECT(2);

	return result;
}

/*
 * plr_SPI_exec - The builtin SPI_exec command for the R interpreter
 */
SEXP
plr_SPI_exec(SEXP rsql)
{
	int				spi_rc;
	char			buf[64];
	char		   *sql;
	int				count = 0;
	int				ntuples;
	SEXP			result = NULL;

	PROTECT(rsql =  AS_CHARACTER(rsql));
	sql = CHAR(STRING_ELT(rsql, 0));
	if (sql == NULL)
		elog(ERROR, "plr: cannot exec empty query");

	/* Execute the query and handle return codes */
	spi_rc = SPI_exec(sql, count);

	switch (spi_rc)
	{
		case SPI_OK_UTILITY:
			snprintf(buf, sizeof(buf), "%d", 0);
			SPI_freetuptable(SPI_tuptable);

			PROTECT(result = NEW_CHARACTER(1));
			SET_STRING_ELT(result, 0, COPY_TO_USER_STRING(buf));
			UNPROTECT(1);

			return result;

		case SPI_OK_SELINTO:
		case SPI_OK_INSERT:
		case SPI_OK_DELETE:
		case SPI_OK_UPDATE:
			snprintf(buf, sizeof(buf), "%d", SPI_processed);
			SPI_freetuptable(SPI_tuptable);

			PROTECT(result = NEW_CHARACTER(1));
			SET_STRING_ELT(result, 0, COPY_TO_USER_STRING(buf));
			UNPROTECT(1);

			return result;

		case SPI_OK_SELECT:
			break;

		case SPI_ERROR_ARGUMENT:
			elog(ERROR, "plr: SPI_exec() failed - SPI_ERROR_ARGUMENT");

		case SPI_ERROR_UNCONNECTED:
			elog(ERROR, "plr: SPI_exec() failed - SPI_ERROR_UNCONNECTED");

		case SPI_ERROR_COPY:
			elog(ERROR, "plr: SPI_exec() failed - SPI_ERROR_COPY");

		case SPI_ERROR_CURSOR:
			elog(ERROR, "plr: SPI_exec() failed - SPI_ERROR_CURSOR");

		case SPI_ERROR_TRANSACTION:
			elog(ERROR, "plr: SPI_exec() failed - SPI_ERROR_TRANSACTION");

		case SPI_ERROR_OPUNKNOWN:
			elog(ERROR, "plr: SPI_exec() failed - SPI_ERROR_OPUNKNOWN");

		default:
			snprintf(buf, sizeof(buf), "%d", spi_rc);
			elog(ERROR, "plr: SPI_exec() failed - unknown RC");
	}

	/*
	 * Only SELECT queries fall through to here - remember the
	 * tuples we got
	 */
	ntuples = SPI_processed;
	if (ntuples > 0)
	{
		result = rpgsql_get_results(ntuples, SPI_tuptable);
		SPI_freetuptable(SPI_tuptable);
	}
	else
		return(R_NilValue);

	return result;
}

static SEXP
rpgsql_get_results(int ntuples, SPITupleTable *tuptable)
{
	/*
	 * Make sure we have a result
	 */
	if (tuptable != NULL)
	{
		HeapTuple	   *tuples = tuptable->vals;
		TupleDesc		tupdesc = tuptable->tupdesc;
		int				nr = ntuples;
		int				nc = tupdesc->natts;
		int				i = 0;
		int				j = 0;
		SEXP			fldvec = NULL;
		SEXP			names = NULL;
		SEXP			row_names = NULL;
		char			buf[256];
		SEXP			result;
		char		   *value;
		SEXP			fun, rargs;

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
			/*
			 * Set column name
			 */
			SET_STRING_ELT(names, j,  mkChar(SPI_fname(tupdesc, j + 1)));

			/*
			 * Loop rows, setting vector datatype based on pgsql datatype
			 */
			switch (SPI_gettypeid(tupdesc, j + 1))
			{
				case OIDOID:
				case INT2OID:
				case INT4OID:
					/*
					 * 2 and 4 byte integer pgsql datatype => use R INTEGER
					 */
					PROTECT(fldvec = NEW_INTEGER(nr));

					for (i = 0; i < nr; i++)
					{
						if ((value = SPI_getvalue(tuples[i], tupdesc, j + 1)))
							INTEGER(fldvec)[i] = atoi(value);
						else
							SET_STRING_ELT(fldvec, i, NA_STRING);
					}
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
					PROTECT(fldvec = NEW_NUMERIC(nr));

					for (i = 0; i < nr; i++)
					{
						if ((value = SPI_getvalue(tuples[i], tupdesc, j + 1)))
							REAL(fldvec)[i] = atof(value);
						else
							SET_STRING_ELT(fldvec, i, NA_STRING);
					}

				    break;
				case BOOLOID:
					PROTECT(fldvec = NEW_LOGICAL(nr));

					for (i = 0; i < nr; i++)
					{
						if ((value = SPI_getvalue(tuples[i], tupdesc, j + 1)))
							LOGICAL(fldvec)[i] = ((*value == 't') ? 1 : 0);
						else
							SET_STRING_ELT(fldvec, i, NA_STRING);
					}

				    break;
				default:
					/*
					 * Everything else is defaulted to string
					 * which should be converted into a factor column
					 */
					PROTECT(fldvec = NEW_CHARACTER(nr));

					for (i = 0; i < nr; i++)
					{
						if ((value = SPI_getvalue(tuples[i], tupdesc, j + 1)))
							SET_STRING_ELT(fldvec, i, mkChar(value));
						else
							SET_STRING_ELT(fldvec, i, NA_STRING);
					}

					/* the second arg to "factor" is our character vector */
					SET_VECTOR_ELT(rargs, 1, fldvec);

					/* convert to a factor */
					PROTECT(fldvec = callRFunction(fun, rargs));
					UNPROTECT(1);
			}

			SET_VECTOR_ELT(result, j, fldvec);
			UNPROTECT(1);
		}

		/*
		 * Attach the column names
		 */
	    setAttrib(result, R_NamesSymbol, names);
		UNPROTECT(3);

		/*
		 * Attach the row names - basically just the row number,
		 * zero based.
		 */
		PROTECT(row_names = allocVector(STRSXP, nr));
		for (i=0; i<nr; i++)
		{
		    sprintf(buf, "%d", i+1);
		    SET_STRING_ELT(row_names, i, mkChar(buf));
		}
		setAttrib(result, R_RowNamesSymbol, row_names);

		/*
		 * Finally, tell R we are a "data.frame"
		 */
	    setAttrib(result, R_ClassSymbol, mkString("data.frame"));

		UNPROTECT(2);
		return(result);
	}
	else
		/*
		 * no result, return nothing
		 */
		return(R_NilValue);

}
