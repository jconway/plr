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
 * plr.h
 */
#ifndef PLR_H
#define PLR_H

#include <unistd.h>
#include <fcntl.h>
#include <setjmp.h>
#include <sys/stat.h>

#define ELOG_H

#include "postgres.h"
#include "access/heapam.h"
#include "catalog/catversion.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "fmgr.h"

#include "R.h"
#include "Rinternals.h"
#include "Rdefines.h"

#ifdef ERROR
#undef ERROR
#endif

#ifdef WARNING
#undef WARNING
#endif

#undef ELOG_H
#include "utils/elog.h"

/* working with postgres 7.3 compatible sources */
#if (CATALOG_VERSION_NO < 200303091)
#define PG_VERSION_73_COMPAT
#endif

#ifdef DEBUGPROTECT
#undef PROTECT
#define PROTECT(s) \
	do { \
		elog(NOTICE, "\tPROTECT\t1\t%s\t%d", __FILE__, __LINE__); \
		protect(s); \
	} while (0)
#undef UNPROTECT
#define UNPROTECT(n) \
	do { \
		elog(NOTICE, "\tUNPROTECT\t%d\t%s\t%d", n, __FILE__, __LINE__); \
		unprotect(n); \
	} while (0)
#endif /* DEBUGPROTECT */

#define xpfree(var_) \
	do { \
		if (var_ != NULL) \
		{ \
			pfree(var_); \
			var_ = NULL; \
		} \
	} while (0)

#define freeStringInfo(mystr_) \
	do { \
		xpfree((mystr_)->data); \
		xpfree(mystr_); \
	} while (0)

#define resetStringInfo(mystr_) \
	do { \
		xpfree((mystr_)->data); \
		initStringInfo(mystr_); \
	} while (0)

#define NEXT_STR_ELEMENT	" %s"

/*
 * See the no-exported header file ${R_HOME}/src/include/Parse.h
 */
extern SEXP R_ParseVector(SEXP, int, int *);
#define PARSE_NULL			0
#define PARSE_OK			1
#define PARSE_INCOMPLETE	2
#define PARSE_ERROR			3
#define PARSE_EOF			4

/* convert C string to text pointer */
#define PG_TEXT_GET_STR(textp_) \
	DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp_)))
#define PG_STR_GET_TEXT(str_) \
	DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(str_)))
#define PG_REPLACE_STR(str_, substr_, replacestr_) \
	PG_TEXT_GET_STR(DirectFunctionCall3(replace_text, \
										PG_STR_GET_TEXT(str_), \
										PG_STR_GET_TEXT(substr_), \
										PG_STR_GET_TEXT(replacestr_)))

#define CHECK_RETURN_PSEUDOTYPES \
	do { \
		if (typeStruct->typtype == 'p') \
		{ \
			if (procStruct->prorettype == VOIDOID || \
				procStruct->prorettype == RECORDOID) \
				 /* okay */ ; \
			else if (procStruct->prorettype == TRIGGEROID) \
			{ \
				xpfree(prodesc->proname); \
				xpfree(prodesc); \
				elog(ERROR, "plr functions cannot return type %s" \
					 "\n\texcept when used as triggers", \
					 format_type_be(procStruct->prorettype)); \
			} \
			else \
			{ \
				xpfree(prodesc->proname); \
				xpfree(prodesc); \
				elog(ERROR, "plr functions cannot return type %s", \
					 format_type_be(procStruct->prorettype)); \
			} \
		} \
	} while (0)

#define CHECK_ARG_PSEUDOTYPES \
	do { \
		if (typeStruct->typtype == 'p') \
		{ \
			xpfree(prodesc->proname); \
			xpfree(prodesc); \
			elog(ERROR, "plr functions cannot take type %s", \
				 format_type_be(arg_typid[i])); \
		} \
	} while (0)


#ifdef PG_VERSION_73_COMPAT
/*************************************************************************
 * working with postgres 7.3 compatible sources
 *************************************************************************/

#define INIT_FINFO_FUNCEXPR
#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, SortMem)
#define GET_PRORETTYPE(prorettype_) \
	do { \
		prorettype_ = procStruct->prorettype; \
	} while (0)

#define GET_PROARGS(pronargs_, proargtypes_) \
	do { \
		int i; \
		pronargs_ = procStruct->pronargs; \
		for (i = 0; i < pronargs_; i++) \
			proargtypes_[i] = procStruct->proargtypes[i]; \
	} while (0)

#define CHECK_POLYMORPHIC_TYPES

#define INIT_AUX_FMGR_ATTS \
	do { \
		finfo->fn_mcxt = QueryContext; \
	} while (0)

#define palloc0(sz_)	palloc(sz_)

#else
/*************************************************************************
 * working with postgres 7.4 compatible sources
 *************************************************************************/

#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, false, SortMem)
#define GET_PRORETTYPE(prorettype_) \
	do { \
		if (procStruct->prorettype == ANYARRAYOID || \
			procStruct->prorettype == ANYELEMENTOID) \
		{ \
			prorettype_ = get_fn_expr_rettype(fcinfo->flinfo); \
			if (prorettype_ == InvalidOid) \
				prorettype_ = procStruct->prorettype; \
		} \
		else \
			prorettype_ = procStruct->prorettype; \
	} while (0)

#define GET_PROARGS(pronargs_, proargtypes_) \
	do { \
		int i; \
		pronargs_ = procStruct->pronargs; \
		for (i = 0; i < pronargs_; i++) \
		{ \
			if (procStruct->proargtypes[i] == ANYARRAYOID || \
				procStruct->proargtypes[i] == ANYELEMENTOID) \
			{ \
				proargtypes_[i] = get_fn_expr_argtype(fcinfo->flinfo, i); \
				if (proargtypes_[i] == InvalidOid) \
					proargtypes_[i] = procStruct->proargtypes[i]; \
			} \
			else \
				proargtypes_[i] = procStruct->proargtypes[i]; \
		} \
	} while (0)

#define CHECK_POLYMORPHIC_TYPES \
	do { \
		if (uptodate) \
			uptodate = (result_typid == prodesc->result_typid); \
		if (uptodate) \
		{ \
			int			i; \
			for (i = 0; i < nargs; i++) \
			{ \
				if (arg_typid[i] != prodesc->arg_typid[i]) \
				{ \
					uptodate = false; \
					break; \
				} \
			} \
		} \
	} while (0)

#define INIT_AUX_FMGR_ATTS \
	do { \
		finfo->fn_mcxt = QueryContext; \
		finfo->fn_expr = (Node *) NULL; \
	} while (0)

#endif /* PG_VERSION_73_COMPAT */

/*
 * structs
 */

/* The information we cache about loaded procedures */
typedef struct plr_proc_desc
{
	char	   *proname;
	TransactionId fn_xmin;
	CommandId	fn_cmin;
	bool		lanpltrusted;
	Oid			result_typid;
	bool		result_istuple;
	FmgrInfo	result_in_func;
	Oid			result_elem;
	FmgrInfo	result_elem_in_func;
	int			result_elem_typlen;
	bool		result_elem_typbyval;
	char		result_elem_typalign;
	int			nargs;
	Oid			arg_typid[FUNC_MAX_ARGS];
	FmgrInfo	arg_out_func[FUNC_MAX_ARGS];
	Oid			arg_elem[FUNC_MAX_ARGS];
	FmgrInfo	arg_elem_out_func[FUNC_MAX_ARGS];
	int			arg_elem_typlen[FUNC_MAX_ARGS];
	bool		arg_elem_typbyval[FUNC_MAX_ARGS];
	char		arg_elem_typalign[FUNC_MAX_ARGS];
	int			arg_is_rel[FUNC_MAX_ARGS];
	SEXP		fun;	/* compiled R function */
}	plr_proc_desc;


/*
 * external declarations
 */

/* libR interpreter initialization */
extern int Rf_initEmbeddedR(int argc, char **argv);

/* PL/R language handler */
extern Datum plr_call_handler(PG_FUNCTION_ARGS);
extern void start_interp(void);
extern void plr_init_load_modules(MemoryContext	plr_SPI_context);
extern void load_r_cmd(const char *cmd);
extern SEXP call_r_func(SEXP fun, SEXP rargs);

/* argument and return value conversion functions */
extern SEXP pg_scalar_get_r(Datum dvalue, Oid arg_typid, FmgrInfo arg_out_func);
extern SEXP pg_array_get_r(Datum dvalue, FmgrInfo out_func, int typlen, bool typbyval, char typalign);
extern SEXP pg_tuple_get_r_frame(int ntuples, HeapTuple *tuples, TupleDesc tupdesc);
extern Datum r_get_pg(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
extern Datum get_scalar_datum(SEXP rval, FmgrInfo result_in_func, Oid result_elem, bool *isnull);

/* Postgres support functions installed into the R interpreter */
extern void throw_pg_notice(const char **msg);
extern void throw_pg_error(const char **msg);
extern SEXP plr_quote_literal(SEXP rawstr);
extern SEXP plr_quote_ident(SEXP rawstr);
extern SEXP plr_SPI_exec(SEXP rsql);
extern SEXP plr_SPI_prepare(SEXP rsql, SEXP rargtypes);
extern SEXP plr_SPI_execp(SEXP rsaved_plan, SEXP rargvalues);
extern SEXP plr_SPI_lastoid(void);

/* Postgres callable functions useful in conjunction with PL/R */
extern Datum reload_plr_modules(PG_FUNCTION_ARGS);
extern Datum install_rcmd(PG_FUNCTION_ARGS);
extern Datum plr_array_push(PG_FUNCTION_ARGS);
extern Datum plr_array(PG_FUNCTION_ARGS);
extern Datum plr_array_accum(PG_FUNCTION_ARGS);
extern Datum plr_environ(PG_FUNCTION_ARGS);

/* Postgres backend support functions */
extern bool isArrayTypeName(const char *typeName);
extern char *get_load_self_ref_cmd(Oid funcid);
extern void perm_fmgr_info(Oid functionId, FmgrInfo *finfo);
extern void system_cache_lookup(Oid element_type, bool input, int *typlen,
					bool *typbyval, char *typdelim, Oid *typelem,
					Oid *proc, char *typalign);
extern Oid get_typelem(Oid element_type);


/*
 * See the no-exported header file ${R_HOME}/src/include/Defn.h
 */
extern void R_PreserveObject(SEXP);
extern void R_ReleaseObject(SEXP);

#ifdef PG_VERSION_73_COMPAT
/*************************************************************************
 * working with postgres 7.3 compatible sources
 *************************************************************************/

/* these are in the backend in 7.4 sources */
extern ArrayType *construct_md_array(Datum *elems, int ndims, int *dims,
									 int *lbs, Oid elmtype, int elmlen,
									 bool elmbyval, char elmalign);
extern Oid get_fn_expr_rettype(FunctionCallInfo fcinfo);

#endif /* PG_VERSION_73_COMPAT */

#endif   /* PLR_H */
