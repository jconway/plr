/*
 * PL/R - PostgreSQL support for R as a
 *	      procedural language (PL)
 *
 * Copyright (c) 2003 by Joseph E. Conway
 * ALL RIGHTS RESERVED
 * 
 * Joe Conway <mail@joeconway.com>
 * 
 * Based on pltcl by Jan Wieck
 * and inspired by REmbeddedPostgres by
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

extern MemoryContext plr_SPI_context;

static SEXP rpgsql_get_results(int ntuples, SPITupleTable *tuptable);

/* The information we cache prepared plans */
typedef struct saved_plan_desc
{
	void	   *saved_plan;
	int			nargs;
	Oid		   *typeids;
	Oid		   *typelems;
	FmgrInfo   *typinfuncs;
}	saved_plan_desc;

/*
 * Functions used in R
 *****************************************************************************/
void
throw_pg_notice(const char **msg)
{
	elog(NOTICE, "%s", *msg);
}

void
throw_pg_error(const char **msg)
{
	elog(ERROR, "%s", *msg);
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
	MemoryContext	oldcontext;

	PROTECT(rsql =  AS_CHARACTER(rsql));
	sql = CHAR(STRING_ELT(rsql, 0));
	if (sql == NULL)
		elog(ERROR, "plr: cannot exec empty query");

	/* switch to SPI memory context */
	oldcontext = MemoryContextSwitchTo(plr_SPI_context);

	/* Execute the query and handle return codes */
	spi_rc = SPI_exec(sql, count);

	/* back to caller's memory context */
	MemoryContextSwitchTo(oldcontext);

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
	if (tuptable != NULL)
	{
		HeapTuple	   *tuples = tuptable->vals;
		TupleDesc		tupdesc = tuptable->tupdesc;

		return pg_tuple_get_r_frame(ntuples, tuples, tupdesc);
	}
	else
		return(R_NilValue);
}

/*
 * plr_SPI_prepare - The builtin SPI_prepare command for the R interpreter
 */
SEXP
plr_SPI_prepare(SEXP rsql, SEXP rargtypes)
{
	char			   *sql;
	int					nargs;
	int					i;
	Oid				   *typeids = NULL;
	Oid				   *typelems = NULL;
	FmgrInfo		   *typinfuncs = NULL;
	void			   *pplan;
	void			   *saved_plan;
	saved_plan_desc	   *plan_desc;
	SEXP				result;
	MemoryContext		oldcontext;

	/* switch to long lived context to create plan description */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	plan_desc = (saved_plan_desc *) palloc(sizeof(saved_plan_desc));

	MemoryContextSwitchTo(oldcontext);

	PROTECT(rsql =  AS_CHARACTER(rsql));
	sql = CHAR(STRING_ELT(rsql, 0));
	if (sql == NULL)
		elog(ERROR, "plr: cannot prepare empty query");

	PROTECT(rargtypes = AS_INTEGER(rargtypes));
	if (!isVector(rargtypes) || !isInteger(rargtypes))
		elog(ERROR, "plr: second parameter must be a vector of PostgreSQL datatypes");

	/* deal with case of no parameters for the prepared query */
	if (rargtypes == R_MissingArg || INTEGER(rargtypes)[0] == NA_INTEGER)
		nargs = 0;
	else
		nargs = length(rargtypes);

	if (nargs < 0)	/* can this even happen?? */
		elog(ERROR, "plr: second parameter must be a vector of PostgreSQL datatypes");

	if (nargs > 0)
	{
		/* switch to long lived context to create plan description elements */
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		typeids = (Oid *) palloc(nargs * sizeof(Oid));
		typelems = (Oid *) palloc(nargs * sizeof(Oid));
		typinfuncs = (FmgrInfo *) palloc(nargs * sizeof(FmgrInfo));

		MemoryContextSwitchTo(oldcontext);

		for (i = 0; i < nargs; i++)
		{
			int			typlen;
			bool		typbyval;
			char		typdelim;
			Oid			typinput,
						typelem;
			char		typalign;
			FmgrInfo	typinfunc;

			typeids[i] = (int) VECTOR_ELT(rargtypes, i);

			/* switch to long lived context to create plan description elements */
			oldcontext = MemoryContextSwitchTo(TopMemoryContext);

			system_cache_lookup(typeids[i], true, &typlen, &typbyval,
						&typdelim, &typelem, &typinput, &typalign);
			typelems[i] = typelem;

			MemoryContextSwitchTo(oldcontext);

			/* perm_fmgr_info already uses TopMemoryContext */
			perm_fmgr_info(typinput, &typinfunc);
			typinfuncs[i] = typinfunc;
		}
	}
	else
		typeids = NULL;

	/* switch to SPI memory context */
	oldcontext = MemoryContextSwitchTo(plr_SPI_context);

	/* Prepare plan for query */
	pplan = SPI_prepare(sql, nargs, typeids);

	UNPROTECT(2);

	if (pplan == NULL)
	{
		char		buf[128];
		char	   *reason;

		switch (SPI_result)
		{
			case SPI_ERROR_ARGUMENT:
				reason = "SPI_ERROR_ARGUMENT";
				break;

			case SPI_ERROR_UNCONNECTED:
				reason = "SPI_ERROR_UNCONNECTED";
				break;

			case SPI_ERROR_COPY:
				reason = "SPI_ERROR_COPY";
				break;

			case SPI_ERROR_CURSOR:
				reason = "SPI_ERROR_CURSOR";
				break;

			case SPI_ERROR_TRANSACTION:
				reason = "SPI_ERROR_TRANSACTION";
				break;

			case SPI_ERROR_OPUNKNOWN:
				reason = "SPI_ERROR_OPUNKNOWN";
				break;

			default:
				snprintf(buf, sizeof(buf), "unknown RC %d", SPI_result);
				reason = buf;
				break;

		}

		elog(ERROR, "plr: SPI_prepare failed - %s", reason);
	}

	/* SPI_saveplan already uses TopMemoryContext */
	saved_plan = SPI_saveplan(pplan);
	if (saved_plan == NULL)
	{
		char		buf[128];
		char	   *reason;

		switch (SPI_result)
		{
			case SPI_ERROR_ARGUMENT:
				reason = "SPI_ERROR_ARGUMENT";
				break;

			case SPI_ERROR_UNCONNECTED:
				reason = "SPI_ERROR_UNCONNECTED";
				break;

			default:
				snprintf(buf, sizeof(buf), "unknown RC %d", SPI_result);
				reason = buf;
				break;

		}

		elog(ERROR, "plr: SPI_saveplan failed - %s", reason);
	}

	/* back to caller's memory context */
	MemoryContextSwitchTo(oldcontext);

	/* no longer need this */
	SPI_freeplan(pplan);

	plan_desc->saved_plan = saved_plan;
	plan_desc->nargs = nargs;
	plan_desc->typeids = typeids;
	plan_desc->typelems = typelems;
	plan_desc->typinfuncs = typinfuncs;

	result = R_MakeExternalPtr(plan_desc, R_NilValue, R_NilValue);

	return result;
}

/*
 * plr_SPI_execp - The builtin SPI_execp command for the R interpreter
 */
SEXP
plr_SPI_execp(SEXP rsaved_plan, SEXP rargvalues)
{
	saved_plan_desc	   *plan_desc = (saved_plan_desc *) R_ExternalPtrAddr(rsaved_plan);
	void			   *saved_plan = plan_desc->saved_plan;
	int					nargs = plan_desc->nargs;
	Oid				   *typelems = plan_desc->typelems;
	FmgrInfo		   *typinfuncs = plan_desc->typinfuncs;
	int					i;
	Datum			   *argvalues = NULL;
	char			   *nulls = NULL;
	bool				isnull = false;
	SEXP				obj;
	int					spi_rc;
	char				buf[64];
	int					count = 0;
	int					ntuples;
	SEXP				result;
	MemoryContext		oldcontext;

	if (nargs > 0)
	{
		if (!IS_LIST(rargvalues))
			elog(ERROR, "plr: second parameter must be a list of arguments to the prepared plan");

		if (length(rargvalues) != nargs)
			elog(ERROR, "plr: list of arguments (%d) is not the same length as " \
						"that of the prepared plan (%d)", length(rargvalues), nargs);

		argvalues = (Datum *) palloc(nargs * sizeof(Datum));
		nulls = (char *) palloc(nargs * sizeof(char));
	}

	for (i = 0; i < nargs; i++)
	{
		PROTECT(obj = VECTOR_ELT(rargvalues, i));

		argvalues[i] = get_scalar_datum(obj, typinfuncs[i], typelems[i], &isnull);
		if (!isnull)
			nulls[i] = ' ';
		else
			nulls[i] = 'n';

		UNPROTECT(1);
	}

	/* switch to SPI memory context */
	oldcontext = MemoryContextSwitchTo(plr_SPI_context);

	/* Execute the plan */
	spi_rc = SPI_execp(saved_plan, argvalues, nulls, count);

	/* back to caller's memory context */
	MemoryContextSwitchTo(oldcontext);

	/* check the result */
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
			elog(ERROR, "plr: SPI_execp() failed - SPI_ERROR_ARGUMENT");

		case SPI_ERROR_UNCONNECTED:
			elog(ERROR, "plr: SPI_execp() failed - SPI_ERROR_UNCONNECTED");

		case SPI_ERROR_COPY:
			elog(ERROR, "plr: SPI_execp() failed - SPI_ERROR_COPY");

		case SPI_ERROR_CURSOR:
			elog(ERROR, "plr: SPI_execp() failed - SPI_ERROR_CURSOR");

		case SPI_ERROR_TRANSACTION:
			elog(ERROR, "plr: SPI_execp() failed - SPI_ERROR_TRANSACTION");

		case SPI_ERROR_OPUNKNOWN:
			elog(ERROR, "plr: SPI_execp() failed - SPI_ERROR_OPUNKNOWN");

		default:
			snprintf(buf, sizeof(buf), "%d", spi_rc);
			elog(ERROR, "plr: SPI_execp() failed - unknown RC");
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

/* 
 * plr_SPI_lastoid - return the last oid. To be used after insert queries.
 */
SEXP
plr_SPI_lastoid(void)
{
	SEXP	result;

	PROTECT(result = NEW_INTEGER(1));
	INTEGER_DATA(result)[0] = SPI_lastoid;
	UNPROTECT(1);

	return result;
}
