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

/* initial number of hash table entries for compiled functions */
#define FUNCS_PER_USER		64


#ifdef PG_VERSION_73_COMPAT
/*************************************************************************
 * working with postgres 7.3 compatible sources
 *************************************************************************/

/* I/O function selector for get_type_io_data */
typedef enum IOFuncSelector
{
	IOFunc_input,
	IOFunc_output,
	IOFunc_receive,
	IOFunc_send
} IOFuncSelector;

#define ANYELEMENTOID	2283

#define INIT_FINFO_FUNCEXPR
#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, SortMem)
#define INIT_AUX_FMGR_ATTS \
	do { \
		finfo->fn_mcxt = QueryContext; \
	} while (0)

#define palloc0(sz_)	palloc(sz_)

#define ERRORCONTEXTCALLBACK
#define PUSH_PLERRCONTEXT(_error_callback_, _plr_error_funcname_)
#define POP_PLERRCONTEXT
#define SAVE_PLERRCONTEXT
#define RESTORE_PLERRCONTEXT

#else
/*************************************************************************
 * working with postgres 7.4 compatible sources
 *************************************************************************/

#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, false, SortMem)
#define INIT_AUX_FMGR_ATTS \
	do { \
		finfo->fn_mcxt = QueryContext; \
		finfo->fn_expr = (Node *) NULL; \
	} while (0)

#define ERRORCONTEXTCALLBACK \
	ErrorContextCallback	plerrcontext

#define PUSH_PLERRCONTEXT(_error_callback_, _plr_error_funcname_) \
	do { \
		plerrcontext.callback = _error_callback_; \
		plerrcontext.arg = (void *) pstrdup(_plr_error_funcname_); \
		plerrcontext.previous = error_context_stack; \
		error_context_stack = &plerrcontext; \
	} while (0)

#define POP_PLERRCONTEXT \
	do { \
		error_context_stack = plerrcontext.previous; \
	} while (0)

#define SAVE_PLERRCONTEXT \
	ErrorContextCallback *ecs_save; \
	do { \
		ecs_save = error_context_stack; \
		error_context_stack = NULL; \
	} while (0)

#define RESTORE_PLERRCONTEXT \
	do { \
		error_context_stack = ecs_save; \
	} while (0)

#endif /* PG_VERSION_73_COMPAT */

/*
 * structs
 */

typedef struct plr_func_hashkey
{								/* Hash lookup key for functions */
	Oid		funcOid;
	/*
	 * We include actual argument types in the hash key to support
	 * polymorphic PLpgSQL functions.  Be careful that extra positions
	 * are zeroed!
	 */
	Oid		argtypes[FUNC_MAX_ARGS];
} plr_func_hashkey;


/* The information we cache about loaded procedures */
typedef struct plr_function
{
	char			   *proname;
	TransactionId		fn_xmin;
	CommandId			fn_cmin;
	plr_func_hashkey   *fn_hashkey; /* back-link to hashtable key */
	bool				lanpltrusted;
	Oid					result_typid;
	bool				result_istuple;
	FmgrInfo			result_in_func;
	Oid					result_elem;
	FmgrInfo			result_elem_in_func;
	int					result_elem_typlen;
	bool				result_elem_typbyval;
	char				result_elem_typalign;
	int					nargs;
	Oid					arg_typid[FUNC_MAX_ARGS];
	FmgrInfo			arg_out_func[FUNC_MAX_ARGS];
	Oid					arg_elem[FUNC_MAX_ARGS];
	FmgrInfo			arg_elem_out_func[FUNC_MAX_ARGS];
	int					arg_elem_typlen[FUNC_MAX_ARGS];
	bool				arg_elem_typbyval[FUNC_MAX_ARGS];
	char				arg_elem_typalign[FUNC_MAX_ARGS];
	int					arg_is_rel[FUNC_MAX_ARGS];
	SEXP				fun;	/* compiled R function */
}	plr_function;

/* compiled function hash table */
typedef struct plr_hashent
{
	plr_func_hashkey key;
	plr_function   *function;
} plr_HashEnt;

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
extern Datum r_get_pg(SEXP rval, plr_function *function, FunctionCallInfo fcinfo);
extern Datum get_scalar_datum(SEXP rval, FmgrInfo result_in_func, Oid result_elem, bool *isnull);

/* Postgres support functions installed into the R interpreter */
extern void throw_pg_notice(const char **msg);
extern SEXP plr_quote_literal(SEXP rawstr);
extern SEXP plr_quote_ident(SEXP rawstr);
extern SEXP plr_SPI_exec(SEXP rsql);
extern SEXP plr_SPI_prepare(SEXP rsql, SEXP rargtypes);
extern SEXP plr_SPI_execp(SEXP rsaved_plan, SEXP rargvalues);
extern SEXP plr_SPI_lastoid(void);
extern void throw_r_error(const char **msg);

/* Postgres callable functions useful in conjunction with PL/R */
extern Datum reload_plr_modules(PG_FUNCTION_ARGS);
extern Datum install_rcmd(PG_FUNCTION_ARGS);
extern Datum plr_array_push(PG_FUNCTION_ARGS);
extern Datum plr_array(PG_FUNCTION_ARGS);
extern Datum plr_array_accum(PG_FUNCTION_ARGS);
extern Datum plr_environ(PG_FUNCTION_ARGS);

/* Postgres backend support functions */
extern void compute_function_hashkey(FmgrInfo *flinfo,
									 Form_pg_proc procStruct,
									 plr_func_hashkey *hashkey);
extern void plr_HashTableInit(void);
extern plr_function *plr_HashTableLookup(plr_func_hashkey *func_key);
extern void plr_HashTableInsert(plr_function *function,
								plr_func_hashkey *func_key);
extern void plr_HashTableDelete(plr_function *function);
extern char *get_load_self_ref_cmd(Oid funcid);
extern void perm_fmgr_info(Oid functionId, FmgrInfo *finfo);

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
extern Oid get_element_type(Oid typid);
extern Oid get_array_type(Oid typid);
extern void get_type_io_data(Oid typid, IOFuncSelector which_func,
							 int16 *typlen, bool *typbyval,
							 char *typalign, char *typdelim,
							 Oid *typelem, Oid *func);
extern Oid get_fn_expr_rettype(FmgrInfo *flinfo);
extern Oid get_fn_expr_argtype(FmgrInfo *flinfo, int argnum);

#else
/*************************************************************************
 * working with postgres 7.4 compatible sources
 *************************************************************************/


#endif /* PG_VERSION_73_COMPAT */

#endif   /* PLR_H */
