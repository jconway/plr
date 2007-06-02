/*
 * PL/R - PostgreSQL support for R as a
 *	      procedural language (PL)
 *
 * Copyright (c) 2003, 2004 by Joseph E. Conway
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

#include "fmgr.h"
#include "funcapi.h"
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

#include "R.h"
#include "Rinternals.h"
#include "Rdefines.h"
#include "Rdevices.h"
#include "Rversion.h"

#ifdef ERROR
#undef ERROR
#endif

#ifdef WARNING
#undef WARNING
#endif

#undef ELOG_H
#include "utils/elog.h"

/* working with postgres 7.3 compatible sources */
#if (CATALOG_VERSION_NO <= 200211021)
#define PG_VERSION_73_COMPAT
#elif (CATALOG_VERSION_NO <= 200310211)
#define PG_VERSION_74_COMPAT
#elif (CATALOG_VERSION_NO <= 200411041)
#define PG_VERSION_80_COMPAT
#else
#define PG_VERSION_81_COMPAT
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
 * R version is calculated thus:
 *   Maj * 65536 + Minor * 256 + Build * 1
 * So:
 * version 1.8.0 results in:
 *   (1 * 65536) + (8 * 256) + (0 * 1) == 67584
 * version 1.9.0 results in:
 *   (1 * 65536) + (9 * 256) + (0 * 1) == 67840
 */
#if (R_VERSION < 67840) /* R_VERSION < 1.9.0 */
#define SET_COLUMN_NAMES \
	do { \
		int i; \
		char *names_buf; \
		names_buf = SPI_fname(tupdesc, j + 1); \
		for (i = 0; i < strlen(names_buf); i++) { \
			if (names_buf[i] == '_') \
				names_buf[i] = '.'; \
		} \
		SET_STRING_ELT(names, df_colnum, mkChar(names_buf)); \
		pfree(names_buf); \
	} while (0)
#else /* R_VERSION >= 1.9.0 */
#define SET_COLUMN_NAMES \
	do { \
		char *names_buf; \
		names_buf = SPI_fname(tupdesc, j + 1); \
		SET_STRING_ELT(names, df_colnum, mkChar(names_buf)); \
		pfree(names_buf); \
	} while (0)
#endif

#if (R_VERSION < 67584) /* R_VERSION < 1.8.0 */
/*
 * See the non-exported header file ${R_HOME}/src/include/Parse.h
 */
extern SEXP R_ParseVector(SEXP, int, int *);
#define PARSE_NULL			0
#define PARSE_OK			1
#define PARSE_INCOMPLETE	2
#define PARSE_ERROR			3
#define PARSE_EOF			4

#define R_PARSEVECTOR(a_, b_, c_)		R_ParseVector(a_, b_, c_)

/*
 * See the non-exported header file ${R_HOME}/src/include/Defn.h
 */
extern void R_PreserveObject(SEXP);
extern void R_ReleaseObject(SEXP);

/* in main.c */
extern void R_dot_Last(void);

/* in memory.c */
extern void R_RunExitFinalizers(void);

#else /* R_VERSION >= 1.8.0 */

#include "R_ext/Parse.h"

#if (R_VERSION >= 132352) /* R_VERSION >= 2.5.0 */
#define R_PARSEVECTOR(a_, b_, c_)		R_ParseVector(a_, b_, (ParseStatus *) c_, R_NilValue)
#else /* R_VERSION < 2.5.0 */
#define R_PARSEVECTOR(a_, b_, c_)		R_ParseVector(a_, b_, (ParseStatus *) c_)
#endif /* R_VERSION >= 2.5.0 */
#endif /* R_VERSION >= 1.8.0 */

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

#ifndef TEXTARRAYOID
#define TEXTARRAYOID	1009
#endif

#define TRIGGER_NARGS	9

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

#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, SortMem)
#define INIT_AUX_FMGR_ATTS \
	do { \
		finfo->fn_mcxt = QueryContext; \
	} while (0)

#define palloc0(sz_)	palloc(sz_)

#else
/*************************************************************************
 * working with postgres 7.4 or 8.0 compatible sources
 *************************************************************************/

#ifdef PG_VERSION_74_COMPAT
#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, false, SortMem)
#else  /* 8.0 or greater */
#define TUPLESTORE_BEGIN_HEAP	tuplestore_begin_heap(true, false, work_mem)
#endif /* PG_VERSION_74_COMPAT */

#define INIT_AUX_FMGR_ATTS \
	do { \
		finfo->fn_mcxt = plr_caller_context; \
		finfo->fn_expr = (Node *) NULL; \
	} while (0)

#endif /* PG_VERSION_73_COMPAT */

#ifdef PG_VERSION_81_COMPAT
#define PROARGTYPES(i) \
		procStruct->proargtypes.values[i]
#define FUNCARGTYPES(_tup_) \
		((Form_pg_proc) GETSTRUCT(_tup_))->proargtypes.values
#else  /* 8.0 or less */
#define PROARGTYPES(i) \
		procStruct->proargtypes[i]
#define FUNCARGTYPES(_tup_) \
		((Form_pg_proc) GETSTRUCT(_tup_))->proargtypes
#endif /* PG_VERSION_81_COMPAT */

#if defined(PG_VERSION_80_COMPAT) || defined(PG_VERSION_81_COMPAT)
/*************************************************************************
 * working with postgres 8.0 compatible sources
 *************************************************************************/
#define  PLR_CLEANUP \
	plr_cleanup(int code, Datum arg)
#define TRIGGERTUPLEVARS \
	HeapTuple		tup; \
	HeapTupleHeader	dnewtup; \
	HeapTupleHeader	dtrigtup
#define SET_INSERT_ARGS_567 \
	do { \
		arg[5] = DirectFunctionCall1(textin, CStringGetDatum("INSERT")); \
		tup = trigdata->tg_trigtuple; \
		dtrigtup = (HeapTupleHeader) palloc(tup->t_len); \
		memcpy((char *) dtrigtup, (char *) tup->t_data, tup->t_len); \
		HeapTupleHeaderSetDatumLength(dtrigtup, tup->t_len); \
		HeapTupleHeaderSetTypeId(dtrigtup, tupdesc->tdtypeid); \
		HeapTupleHeaderSetTypMod(dtrigtup, tupdesc->tdtypmod); \
		arg[6] = PointerGetDatum(dtrigtup); \
		argnull[6] = false; \
		arg[7] = (Datum) 0; \
		argnull[7] = true; \
	} while (0)
#define SET_DELETE_ARGS_567 \
	do { \
		arg[5] = DirectFunctionCall1(textin, CStringGetDatum("DELETE")); \
		arg[6] = (Datum) 0; \
		argnull[6] = true; \
		tup = trigdata->tg_trigtuple; \
		dtrigtup = (HeapTupleHeader) palloc(tup->t_len); \
		memcpy((char *) dtrigtup, (char *) tup->t_data, tup->t_len); \
		HeapTupleHeaderSetDatumLength(dtrigtup, tup->t_len); \
		HeapTupleHeaderSetTypeId(dtrigtup, tupdesc->tdtypeid); \
		HeapTupleHeaderSetTypMod(dtrigtup, tupdesc->tdtypmod); \
		arg[7] = PointerGetDatum(dtrigtup); \
		argnull[7] = false; \
	} while (0)
#define SET_UPDATE_ARGS_567 \
	do { \
		arg[5] = DirectFunctionCall1(textin, CStringGetDatum("UPDATE")); \
		tup = trigdata->tg_newtuple; \
		dnewtup = (HeapTupleHeader) palloc(tup->t_len); \
		memcpy((char *) dnewtup, (char *) tup->t_data, tup->t_len); \
		HeapTupleHeaderSetDatumLength(dnewtup, tup->t_len); \
		HeapTupleHeaderSetTypeId(dnewtup, tupdesc->tdtypeid); \
		HeapTupleHeaderSetTypMod(dnewtup, tupdesc->tdtypmod); \
		arg[6] = PointerGetDatum(dnewtup); \
		argnull[6] = false; \
		tup = trigdata->tg_trigtuple; \
		dtrigtup = (HeapTupleHeader) palloc(tup->t_len); \
		memcpy((char *) dtrigtup, (char *) tup->t_data, tup->t_len); \
		HeapTupleHeaderSetDatumLength(dtrigtup, tup->t_len); \
		HeapTupleHeaderSetTypeId(dtrigtup, tupdesc->tdtypeid); \
		HeapTupleHeaderSetTypMod(dtrigtup, tupdesc->tdtypmod); \
		arg[7] = PointerGetDatum(dtrigtup); \
		argnull[7] = false; \
	} while (0)
#define CONVERT_TUPLE_TO_DATAFRAME \
	do { \
		Oid			tupType; \
		int32		tupTypmod; \
		TupleDesc	tupdesc; \
		HeapTuple	tuple = palloc(sizeof(HeapTupleData)); \
		HeapTupleHeader	tuple_hdr = DatumGetHeapTupleHeader(arg[i]); \
		tupType = HeapTupleHeaderGetTypeId(tuple_hdr); \
		tupTypmod = HeapTupleHeaderGetTypMod(tuple_hdr); \
		tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod); \
		tuple->t_len = HeapTupleHeaderGetDatumLength(tuple_hdr); \
		ItemPointerSetInvalid(&(tuple->t_self)); \
		tuple->t_tableOid = InvalidOid; \
		tuple->t_data = tuple_hdr; \
		PROTECT(el = pg_tuple_get_r_frame(1, &tuple, tupdesc)); \
		pfree(tuple); \
	} while (0)
#define GET_ARG_NAMES \
		char  **argnames; \
		argnames = fetchArgNames(procTup, procStruct->pronargs)
#define SET_ARG_NAME \
	do { \
		if (argnames && argnames[i] && argnames[i][0]) \
		{ \
			appendStringInfo(proc_internal_args, "%s", argnames[i]); \
			pfree(argnames[i]); \
		} \
		else \
			appendStringInfo(proc_internal_args, "arg%d", i + 1); \
	} while (0)
#define FREE_ARG_NAMES \
	do { \
		if (argnames) \
			pfree(argnames); \
	} while (0)
#define PREPARE_PG_TRY \
	ERRORCONTEXTCALLBACK
#define PLR_PG_CATCH() \
		PG_CATCH(); \
		{ \
			ErrorData  *edata; \
			MemoryContextSwitchTo(plr_SPI_context); \
			edata = CopyErrorData(); \
			error("error in SQL statement : %s", edata->message); \
		}
#define PLR_PG_END_TRY() \
	PG_END_TRY()
#else
/*************************************************************************
 * working with earlier than postgres 8.0 compatible sources
 *************************************************************************/
#define  PLR_CLEANUP \
	plr_cleanup(void)
#define TRIGGERTUPLEVARS \
	TupleTableSlot *slot
#define SET_INSERT_ARGS_567 \
	do { \
		arg[5] = DirectFunctionCall1(textin, CStringGetDatum("INSERT")); \
		slot = TupleDescGetSlot(tupdesc); \
		slot->val = trigdata->tg_trigtuple; \
		arg[6] = PointerGetDatum(slot); \
		argnull[6] = false; \
		arg[7] = (Datum) 0; \
		argnull[7] = true; \
	} while (0)
#define SET_DELETE_ARGS_567 \
	do { \
		arg[5] = DirectFunctionCall1(textin, CStringGetDatum("DELETE")); \
		arg[6] = (Datum) 0; \
		argnull[6] = true; \
		slot = TupleDescGetSlot(tupdesc); \
		slot->val = trigdata->tg_trigtuple; \
		arg[7] = PointerGetDatum(slot); \
		argnull[7] = false; \
	} while (0)
#define SET_UPDATE_ARGS_567 \
	do { \
		arg[5] = DirectFunctionCall1(textin, CStringGetDatum("UPDATE")); \
		slot = TupleDescGetSlot(tupdesc); \
		slot->val = trigdata->tg_newtuple; \
		arg[6] = PointerGetDatum(slot); \
		argnull[6] = false; \
		slot = TupleDescGetSlot(tupdesc); \
		slot->val = trigdata->tg_trigtuple; \
		arg[7] = PointerGetDatum(slot); \
		argnull[7] = false; \
	} while (0)
#define CONVERT_TUPLE_TO_DATAFRAME \
	do { \
		TupleTableSlot *slot = (TupleTableSlot *) arg[i]; \
		HeapTuple		tuple = slot->val; \
		TupleDesc		tupdesc = slot->ttc_tupleDescriptor; \
		PROTECT(el = pg_tuple_get_r_frame(1, &tuple, tupdesc)); \
	} while (0)
#define GET_ARG_NAMES
#define SET_ARG_NAME \
	do { \
		appendStringInfo(proc_internal_args, "arg%d", i + 1); \
	} while (0)
#define FREE_ARG_NAMES
#define PREPARE_PG_TRY \
	ERRORCONTEXTCALLBACK; \
	sigjmp_buf		save_restart
#define PG_TRY()  \
	do { \
		memcpy(&save_restart, &Warn_restart, sizeof(save_restart)); \
		if (sigsetjmp(Warn_restart, 1) != 0) \
		{ \
			InError = false; \
			memcpy(&Warn_restart, &save_restart, sizeof(Warn_restart)); \
			error("%s", "error in SQL statement"); \
		} \
	} while (0)
#define PLR_PG_CATCH()
#define PLR_PG_END_TRY() \
	memcpy(&Warn_restart, &save_restart, sizeof(Warn_restart))
#endif /* PG_VERSION_80_COMPAT || PG_VERSION_81_COMPAT */

/*
 * structs
 */

typedef struct plr_func_hashkey
{								/* Hash lookup key for functions */
	Oid		funcOid;

	/*
	 * For a trigger function, the OID of the relation triggered on is part
	 * of the hashkey --- we want to compile the trigger separately for each
	 * relation it is used with, in case the rowtype is different.  Zero if
	 * not called as a trigger.
	 */
	Oid			trigrelOid;

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
	int					result_natts;
	Oid				   *result_fld_elem_typid;
	FmgrInfo		   *result_fld_elem_in_func;
	int				   *result_fld_elem_typlen;
	bool			   *result_fld_elem_typbyval;
	char			   *result_fld_elem_typalign;
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
extern void PLR_CLEANUP;
extern void plr_init(void);
extern void plr_load_modules(MemoryContext plr_SPI_context);
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
extern void compute_function_hashkey(FunctionCallInfo fcinfo,
									 Form_pg_proc procStruct,
									 plr_func_hashkey *hashkey);
extern void plr_HashTableInit(void);
extern plr_function *plr_HashTableLookup(plr_func_hashkey *func_key);
extern void plr_HashTableInsert(plr_function *function,
								plr_func_hashkey *func_key);
extern void plr_HashTableDelete(plr_function *function);
extern char *get_load_self_ref_cmd(Oid funcid);
extern void perm_fmgr_info(Oid functionId, FmgrInfo *finfo);

#ifndef PG_VERSION_73_COMPAT
/*************************************************************************
 * working with postgres 7.4 or greater compatible sources
 *************************************************************************/

/* currently nothing */

#else

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


/*-------------------------------------------------------------------------
 *
 * errcodes.h
 *	  POSTGRES error codes
 *
 * The error code list is kept in its own source file for possible use by
 * automatic tools.  Each error code is identified by a five-character string
 * following the SQLSTATE conventions.  The exact representation of the
 * string is determined by the MAKE_SQLSTATE() macro, which is not defined
 * in this file; it can be defined by the caller for special purposes.
 *
 * Copyright (c) 2003, PostgreSQL Global Development Group
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef ERRCODES_H here */


/*
 * SQLSTATE codes for errors.
 *
 * The SQL99 code set is rather impoverished, especially in the area of
 * syntactical and semantic errors.  We have borrowed codes from IBM's DB2
 * and invented our own codes to develop a useful code set.
 *
 * When adding a new code, make sure it is placed in the most appropriate
 * class (the first two characters of the code value identify the class).
 * The listing is organized by class to make this prominent.
 *
 * The generic '000' class code should be used for an error only when there
 * is not a more-specific code defined.
 */

/* Class 00 - Successful Completion */
#define ERRCODE_SUCCESSFUL_COMPLETION		MAKE_SQLSTATE('0','0', '0','0','0')

/* Class 01 - Warning */
/* (do not use this class for failure conditions!) */
#define ERRCODE_WARNING						MAKE_SQLSTATE('0','1', '0','0','0')
#define ERRCODE_WARNING_DYNAMIC_RESULT_SETS_RETURNED		MAKE_SQLSTATE('0','1', '0','0','C')
#define ERRCODE_WARNING_IMPLICIT_ZERO_BIT_PADDING	MAKE_SQLSTATE('0','1', '0','0','8')
#define ERRCODE_WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION	MAKE_SQLSTATE('0','1', '0','0','3')
#define ERRCODE_WARNING_STRING_DATA_RIGHT_TRUNCATION	MAKE_SQLSTATE('0','1', '0','0','4')

/* Class 02 - No Data --- this is also a warning class per SQL99 */
/* (do not use this class for failure conditions!) */
#define ERRCODE_NO_DATA						MAKE_SQLSTATE('0','2', '0','0','0')
#define ERRCODE_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED	MAKE_SQLSTATE('0','2', '0','0','1')

/* Class 03 - SQL Statement Not Yet Complete */
#define ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE		MAKE_SQLSTATE('0','3', '0','0','0')

/* Class 08 - Connection Exception */
#define ERRCODE_CONNECTION_EXCEPTION		MAKE_SQLSTATE('0','8', '0','0','0')
#define ERRCODE_CONNECTION_DOES_NOT_EXIST	MAKE_SQLSTATE('0','8', '0','0','3')
#define ERRCODE_CONNECTION_FAILURE			MAKE_SQLSTATE('0','8', '0','0','6')
#define ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION		MAKE_SQLSTATE('0','8', '0','0','1')
#define ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION	MAKE_SQLSTATE('0','8', '0','0','4')
#define ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN		MAKE_SQLSTATE('0','8', '0','0','7')
#define ERRCODE_PROTOCOL_VIOLATION			MAKE_SQLSTATE('0','8', 'P','0','1')

/* Class 09 - Triggered Action Exception */
#define ERRCODE_TRIGGERED_ACTION_EXCEPTION	MAKE_SQLSTATE('0','9', '0','0','0')

/* Class 0A - Feature Not Supported */
#define ERRCODE_FEATURE_NOT_SUPPORTED		MAKE_SQLSTATE('0','A', '0','0','0')

/* Class 0B - Invalid Transaction Initiation */
#define ERRCODE_INVALID_TRANSACTION_INITIATION		MAKE_SQLSTATE('0','B', '0','0','0')

/* Class 0F - Locator Exception */
#define ERRCODE_LOCATOR_EXCEPTION			MAKE_SQLSTATE('0','F', '0','0','0')
#define ERRCODE_L_E_INVALID_SPECIFICATION	MAKE_SQLSTATE('0','F', '0','0','1')

/* Class 0L - Invalid Grantor */
#define ERRCODE_INVALID_GRANTOR				MAKE_SQLSTATE('0','L', '0','0','0')
#define ERRCODE_INVALID_GRANT_OPERATION		MAKE_SQLSTATE('0','L', 'P','0','1')

/* Class 0P - Invalid Role Specification */
#define ERRCODE_INVALID_ROLE_SPECIFICATION	MAKE_SQLSTATE('0','P', '0','0','0')

/* Class 21 - Cardinality Violation */
/* (this means something returned the wrong number of rows) */
#define ERRCODE_CARDINALITY_VIOLATION		MAKE_SQLSTATE('2','1', '0','0','0')

/* Class 22 - Data Exception */
#define ERRCODE_DATA_EXCEPTION				MAKE_SQLSTATE('2','2', '0','0','0')
#define ERRCODE_ARRAY_ELEMENT_ERROR			MAKE_SQLSTATE('2','2', '0','2','E')
/* SQL99's actual definition of "array element error" is subscript error */
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR		ERRCODE_ARRAY_ELEMENT_ERROR
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE	MAKE_SQLSTATE('2','2', '0','2','1')
#define ERRCODE_DATETIME_FIELD_OVERFLOW		MAKE_SQLSTATE('2','2', '0','0','8')
#define ERRCODE_DATETIME_VALUE_OUT_OF_RANGE	ERRCODE_DATETIME_FIELD_OVERFLOW
#define ERRCODE_DIVISION_BY_ZERO			MAKE_SQLSTATE('2','2', '0','1','2')
#define ERRCODE_ERROR_IN_ASSIGNMENT			MAKE_SQLSTATE('2','2', '0','0','5')
#define ERRCODE_ESCAPE_CHARACTER_CONFLICT	MAKE_SQLSTATE('2','2', '0','0','B')
#define ERRCODE_INDICATOR_OVERFLOW			MAKE_SQLSTATE('2','2', '0','2','2')
#define ERRCODE_INTERVAL_FIELD_OVERFLOW		MAKE_SQLSTATE('2','2', '0','1','5')
#define ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST		MAKE_SQLSTATE('2','2', '0','1','8')
#define ERRCODE_INVALID_DATETIME_FORMAT		MAKE_SQLSTATE('2','2', '0','0','7')
#define ERRCODE_INVALID_ESCAPE_CHARACTER	MAKE_SQLSTATE('2','2', '0','1','9')
#define ERRCODE_INVALID_ESCAPE_OCTET		MAKE_SQLSTATE('2','2', '0','0','D')
#define ERRCODE_INVALID_ESCAPE_SEQUENCE		MAKE_SQLSTATE('2','2', '0','2','5')
#define ERRCODE_INVALID_INDICATOR_PARAMETER_VALUE		MAKE_SQLSTATE('2','2', '0','1','0')
#define ERRCODE_INVALID_LIMIT_VALUE			MAKE_SQLSTATE('2','2', '0','2','0')
#define ERRCODE_INVALID_PARAMETER_VALUE		MAKE_SQLSTATE('2','2', '0','2','3')
#define ERRCODE_INVALID_REGULAR_EXPRESSION	MAKE_SQLSTATE('2','2', '0','1','B')
#define ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE	MAKE_SQLSTATE('2','2', '0','0','9')
#define ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER		MAKE_SQLSTATE('2','2', '0','0','C')
#define ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH	MAKE_SQLSTATE('2','2', '0','0','G')
#define ERRCODE_NULL_VALUE_NOT_ALLOWED		MAKE_SQLSTATE('2','2', '0','0','4')
#define ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER	MAKE_SQLSTATE('2','2', '0','0','2')
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE	MAKE_SQLSTATE('2','2', '0','0','3')
#define ERRCODE_STRING_DATA_LENGTH_MISMATCH	MAKE_SQLSTATE('2','2', '0','2','6')
#define ERRCODE_STRING_DATA_RIGHT_TRUNCATION		MAKE_SQLSTATE('2','2', '0','0','1')
#define ERRCODE_SUBSTRING_ERROR				MAKE_SQLSTATE('2','2', '0','1','1')
#define ERRCODE_TRIM_ERROR					MAKE_SQLSTATE('2','2', '0','2','7')
#define ERRCODE_UNTERMINATED_C_STRING		MAKE_SQLSTATE('2','2', '0','2','4')
#define ERRCODE_ZERO_LENGTH_CHARACTER_STRING		MAKE_SQLSTATE('2','2', '0','0','F')
#define ERRCODE_FLOATING_POINT_EXCEPTION	MAKE_SQLSTATE('2','2', 'P','0','1')
#define ERRCODE_INVALID_TEXT_REPRESENTATION	MAKE_SQLSTATE('2','2', 'P','0','2')
#define ERRCODE_INVALID_BINARY_REPRESENTATION	MAKE_SQLSTATE('2','2', 'P','0','3')
#define ERRCODE_BAD_COPY_FILE_FORMAT		MAKE_SQLSTATE('2','2', 'P','0','4')
#define ERRCODE_UNTRANSLATABLE_CHARACTER	MAKE_SQLSTATE('2','2', 'P','0','5')

/* Class 23 - Integrity Constraint Violation */
#define ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION		MAKE_SQLSTATE('2','3', '0','0','0')
#define ERRCODE_RESTRICT_VIOLATION			MAKE_SQLSTATE('2','3', '0','0','1')
#define ERRCODE_NOT_NULL_VIOLATION			MAKE_SQLSTATE('2','3', '5','0','2')
#define ERRCODE_FOREIGN_KEY_VIOLATION		MAKE_SQLSTATE('2','3', '5','0','3')
#define ERRCODE_UNIQUE_VIOLATION			MAKE_SQLSTATE('2','3', '5','0','5')
#define ERRCODE_CHECK_VIOLATION				MAKE_SQLSTATE('2','3', '5','1','4')

/* Class 24 - Invalid Cursor State */
#define ERRCODE_INVALID_CURSOR_STATE		MAKE_SQLSTATE('2','4', '0','0','0')

/* Class 25 - Invalid Transaction State */
#define ERRCODE_INVALID_TRANSACTION_STATE	MAKE_SQLSTATE('2','5', '0','0','0')
#define ERRCODE_ACTIVE_SQL_TRANSACTION		MAKE_SQLSTATE('2','5', '0','0','1')
#define ERRCODE_BRANCH_TRANSACTION_ALREADY_ACTIVE	MAKE_SQLSTATE('2','5', '0','0','2')
#define ERRCODE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL	MAKE_SQLSTATE('2','5', '0','0','8')
#define ERRCODE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION	MAKE_SQLSTATE('2','5', '0','0','3')
#define ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION	MAKE_SQLSTATE('2','5', '0','0','4')
#define ERRCODE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION	MAKE_SQLSTATE('2','5', '0','0','5')
#define ERRCODE_READ_ONLY_SQL_TRANSACTION	MAKE_SQLSTATE('2','5', '0','0','6')
#define ERRCODE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED	MAKE_SQLSTATE('2','5', '0','0','7')
#define ERRCODE_NO_ACTIVE_SQL_TRANSACTION	MAKE_SQLSTATE('2','5', 'P','0','1')
#define ERRCODE_IN_FAILED_SQL_TRANSACTION	MAKE_SQLSTATE('2','5', 'P','0','2')

/* Class 26 - Invalid SQL Statement Name */
/* (we take this to mean prepared statements) */
#define ERRCODE_INVALID_SQL_STATEMENT_NAME	MAKE_SQLSTATE('2','6', '0','0','0')

/* Class 27 - Triggered Data Change Violation */
#define ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION		MAKE_SQLSTATE('2','7', '0','0','0')

/* Class 28 - Invalid Authorization Specification */
#define ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION	MAKE_SQLSTATE('2','8', '0','0','0')

/* Class 2B - Dependent Privilege Descriptors Still Exist */
#define ERRCODE_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST		MAKE_SQLSTATE('2','B', '0','0','0')
#define ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST		MAKE_SQLSTATE('2','B', 'P','0','1')

/* Class 2D - Invalid Transaction Termination */
#define ERRCODE_INVALID_TRANSACTION_TERMINATION		MAKE_SQLSTATE('2','D', '0','0','0')

/* Class 2F - SQL Routine Exception */
#define ERRCODE_SQL_ROUTINE_EXCEPTION		MAKE_SQLSTATE('2','F', '0','0','0')
#define ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT	MAKE_SQLSTATE('2','F', '0','0','5')
#define ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED		MAKE_SQLSTATE('2','F', '0','0','2')
#define ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED	MAKE_SQLSTATE('2','F', '0','0','3')
#define ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED		MAKE_SQLSTATE('2','F', '0','0','4')

/* Class 34 - Invalid Cursor Name */
#define ERRCODE_INVALID_CURSOR_NAME			MAKE_SQLSTATE('3','4', '0','0','0')

/* Class 38 - External Routine Exception */
#define ERRCODE_EXTERNAL_ROUTINE_EXCEPTION	MAKE_SQLSTATE('3','8', '0','0','0')
#define ERRCODE_E_R_E_CONTAINING_SQL_NOT_PERMITTED	MAKE_SQLSTATE('3','8', '0','0','1')
#define ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED	MAKE_SQLSTATE('3','8', '0','0','2')
#define ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED	MAKE_SQLSTATE('3','8', '0','0','3')
#define ERRCODE_E_R_E_READING_SQL_DATA_NOT_PERMITTED	MAKE_SQLSTATE('3','8', '0','0','4')

/* Class 39 - External Routine Invocation Exception */
#define ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION	MAKE_SQLSTATE('3','9', '0','0','0')
#define ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED	MAKE_SQLSTATE('3','9', '0','0','1')
#define ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED	MAKE_SQLSTATE('3','9', '0','0','4')
#define ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED	MAKE_SQLSTATE('3','9', 'P','0','1')
#define ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED	MAKE_SQLSTATE('3','9', 'P','0','2')

/* Class 3D - Invalid Catalog Name */
#define ERRCODE_INVALID_CATALOG_NAME		MAKE_SQLSTATE('3','D', '0','0','0')

/* Class 3F - Invalid Schema Name */
#define ERRCODE_INVALID_SCHEMA_NAME			MAKE_SQLSTATE('3','F', '0','0','0')

/* Class 40 - Transaction Rollback */
#define ERRCODE_TRANSACTION_ROLLBACK		MAKE_SQLSTATE('4','0', '0','0','0')
#define ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION	MAKE_SQLSTATE('4','0', '0','0','2')
#define ERRCODE_T_R_SERIALIZATION_FAILURE	MAKE_SQLSTATE('4','0', '0','0','1')
#define ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN	MAKE_SQLSTATE('4','0', '0','0','3')
#define ERRCODE_T_R_DEADLOCK_DETECTED		MAKE_SQLSTATE('4','0', 'P','0','1')

/* Class 42 - Syntax Error or Access Rule Violation */
#define ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION		MAKE_SQLSTATE('4','2', '0','0','0')
/* never use the above; use one of these two if no specific code exists: */
#define ERRCODE_SYNTAX_ERROR				MAKE_SQLSTATE('4','2', '6','0','1')
#define ERRCODE_INSUFFICIENT_PRIVILEGE		MAKE_SQLSTATE('4','2', '5','0','1')
#define ERRCODE_CANNOT_COERCE				MAKE_SQLSTATE('4','2', '8','4','6')
#define ERRCODE_GROUPING_ERROR				MAKE_SQLSTATE('4','2', '8','0','3')
#define ERRCODE_INVALID_FOREIGN_KEY			MAKE_SQLSTATE('4','2', '8','3','0')
#define ERRCODE_INVALID_NAME				MAKE_SQLSTATE('4','2', '6','0','2')
#define ERRCODE_NAME_TOO_LONG				MAKE_SQLSTATE('4','2', '6','2','2')
#define ERRCODE_RESERVED_NAME				MAKE_SQLSTATE('4','2', '9','3','9')
#define ERRCODE_DATATYPE_MISMATCH			MAKE_SQLSTATE('4','2', '8','0','4')
#define ERRCODE_INDETERMINATE_DATATYPE		MAKE_SQLSTATE('4','2', 'P','1','8')
#define ERRCODE_WRONG_OBJECT_TYPE			MAKE_SQLSTATE('4','2', '8','0','9')
/*
 * Note: for ERRCODE purposes, we divide namable objects into these categories:
 * databases, schemas, prepared statements, cursors, tables, columns,
 * functions (including operators), and all else (lumped as "objects").
 * (The first four categories are mandated by the existence of separate
 * SQLSTATE classes for them in the spec; in this file, however, we group
 * the ERRCODE names with all the rest under class 42.)  Parameters are
 * sort-of-named objects and get their own ERRCODE.
 *
 * The same breakdown is used for "duplicate" and "ambiguous" complaints,
 * as well as complaints associated with incorrect declarations.
 */
#define ERRCODE_UNDEFINED_COLUMN			MAKE_SQLSTATE('4','2', '7','0','3')
#define ERRCODE_UNDEFINED_CURSOR			ERRCODE_INVALID_CURSOR_NAME
#define ERRCODE_UNDEFINED_DATABASE			ERRCODE_INVALID_CATALOG_NAME
#define ERRCODE_UNDEFINED_FUNCTION			MAKE_SQLSTATE('4','2', '8','8','3')
#define ERRCODE_UNDEFINED_PSTATEMENT		ERRCODE_INVALID_SQL_STATEMENT_NAME
#define ERRCODE_UNDEFINED_SCHEMA			ERRCODE_INVALID_SCHEMA_NAME
#define ERRCODE_UNDEFINED_TABLE				MAKE_SQLSTATE('4','2', 'P','0','1')
#define ERRCODE_UNDEFINED_PARAMETER			MAKE_SQLSTATE('4','2', 'P','0','2')
#define ERRCODE_UNDEFINED_OBJECT			MAKE_SQLSTATE('4','2', '7','0','4')
#define ERRCODE_DUPLICATE_COLUMN			MAKE_SQLSTATE('4','2', '7','0','1')
#define ERRCODE_DUPLICATE_CURSOR			MAKE_SQLSTATE('4','2', 'P','0','3')
#define ERRCODE_DUPLICATE_DATABASE			MAKE_SQLSTATE('4','2', 'P','0','4')
#define ERRCODE_DUPLICATE_FUNCTION			MAKE_SQLSTATE('4','2', '7','2','3')
#define ERRCODE_DUPLICATE_PSTATEMENT		MAKE_SQLSTATE('4','2', 'P','0','5')
#define ERRCODE_DUPLICATE_SCHEMA			MAKE_SQLSTATE('4','2', 'P','0','6')
#define ERRCODE_DUPLICATE_TABLE				MAKE_SQLSTATE('4','2', 'P','0','7')
#define ERRCODE_DUPLICATE_ALIAS				MAKE_SQLSTATE('4','2', '7','1','2')
#define ERRCODE_DUPLICATE_OBJECT			MAKE_SQLSTATE('4','2', '7','1','0')
#define ERRCODE_AMBIGUOUS_COLUMN			MAKE_SQLSTATE('4','2', '7','0','2')
#define ERRCODE_AMBIGUOUS_FUNCTION			MAKE_SQLSTATE('4','2', '7','2','5')
#define ERRCODE_AMBIGUOUS_PARAMETER			MAKE_SQLSTATE('4','2', 'P','0','8')
#define ERRCODE_AMBIGUOUS_ALIAS				MAKE_SQLSTATE('4','2', 'P','0','9')
#define ERRCODE_INVALID_COLUMN_REFERENCE	MAKE_SQLSTATE('4','2', 'P','1','0')
#define ERRCODE_INVALID_COLUMN_DEFINITION	MAKE_SQLSTATE('4','2', '6','1','1')
#define ERRCODE_INVALID_CURSOR_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','1')
#define ERRCODE_INVALID_DATABASE_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','2')
#define ERRCODE_INVALID_FUNCTION_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','3')
#define ERRCODE_INVALID_PSTATEMENT_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','4')
#define ERRCODE_INVALID_SCHEMA_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','5')
#define ERRCODE_INVALID_TABLE_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','6')
#define ERRCODE_INVALID_OBJECT_DEFINITION	MAKE_SQLSTATE('4','2', 'P','1','7')

/* Class 44 - WITH CHECK OPTION Violation */
#define ERRCODE_WITH_CHECK_OPTION_VIOLATION	MAKE_SQLSTATE('4','4', '0','0','0')

/* Class 53 - Insufficient Resources (PostgreSQL-specific error class) */
#define ERRCODE_INSUFFICIENT_RESOURCES		MAKE_SQLSTATE('5','3', '0','0','0')
#define ERRCODE_DISK_FULL					MAKE_SQLSTATE('5','3', '1','0','0')
#define ERRCODE_OUT_OF_MEMORY				MAKE_SQLSTATE('5','3', '2','0','0')
#define ERRCODE_TOO_MANY_CONNECTIONS		MAKE_SQLSTATE('5','3', '3','0','0')

/* Class 54 - Program Limit Exceeded (class borrowed from DB2) */
/* (this is for wired-in limits, not resource exhaustion problems) */
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED		MAKE_SQLSTATE('5','4', '0','0','0')
#define ERRCODE_STATEMENT_TOO_COMPLEX		MAKE_SQLSTATE('5','4', '0','0','1')
#define ERRCODE_TOO_MANY_COLUMNS			MAKE_SQLSTATE('5','4', '0','1','1')
#define ERRCODE_TOO_MANY_ARGUMENTS			MAKE_SQLSTATE('5','4', '0','2','3')

/* Class 55 - Object Not In Prerequisite State (class borrowed from DB2) */
#define ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE	MAKE_SQLSTATE('5','5', '0','0','0')
#define ERRCODE_OBJECT_IN_USE				MAKE_SQLSTATE('5','5', '0','0','6')
#define ERRCODE_INDEXES_DEACTIVATED			MAKE_SQLSTATE('5','5', 'P','0','1')
#define ERRCODE_CANT_CHANGE_RUNTIME_PARAM	MAKE_SQLSTATE('5','5', 'P','0','2')

/* Class 57 - Operator Intervention (class borrowed from DB2) */
#define ERRCODE_OPERATOR_INTERVENTION		MAKE_SQLSTATE('5','7', '0','0','0')
#define ERRCODE_QUERY_CANCELED				MAKE_SQLSTATE('5','7', '0','1','4')
#define ERRCODE_ADMIN_SHUTDOWN				MAKE_SQLSTATE('5','7', 'P','0','1')
#define ERRCODE_CRASH_SHUTDOWN				MAKE_SQLSTATE('5','7', 'P','0','2')
#define ERRCODE_CANNOT_CONNECT_NOW			MAKE_SQLSTATE('5','7', 'P','0','3')

/* Class 58 - System Error (class borrowed from DB2) */
/* (we define this as errors external to PostgreSQL itself) */
#define ERRCODE_IO_ERROR					MAKE_SQLSTATE('5','8', '0','3','0')

/* Class F0 - Configuration File Error (PostgreSQL-specific error class) */
#define ERRCODE_CONFIG_FILE_ERROR			MAKE_SQLSTATE('F','0', '0','0','0')
#define ERRCODE_LOCK_FILE_EXISTS			MAKE_SQLSTATE('F','0', '0','0','1')

/* Class XX - Internal Error (PostgreSQL-specific error class) */
/* (this is for "can't-happen" conditions and software bugs) */
#define ERRCODE_INTERNAL_ERROR				MAKE_SQLSTATE('X','X', '0','0','0')
#define ERRCODE_DATA_CORRUPTED				MAKE_SQLSTATE('X','X', '0','0','1')
#define ERRCODE_INDEX_CORRUPTED				MAKE_SQLSTATE('X','X', '0','0','2')


/* macros for representing SQLSTATE strings compactly */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val)	(((val) & 0x3F) + '0')

#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* Which __func__ symbol do we have, if any? */
#ifdef HAVE_FUNCNAME__FUNC
#define PG_FUNCNAME_MACRO	__func__
#else
#ifdef HAVE_FUNCNAME__FUNCTION
#define PG_FUNCNAME_MACRO	__FUNCTION__
#else
#define PG_FUNCNAME_MACRO	NULL
#endif
#endif

/*----------
 * New-style error reporting API: to be used in this way:
 *		ereport(ERROR,
 *				(errcode(ERRCODE_UNDEFINED_CURSOR),
 *				 errmsg("portal \"%s\" not found", stmt->portalname),
 *				 ... other errxxx() fields as needed ...));
 *
 * The error level is required, and so is a primary error message (errmsg
 * or errmsg_internal).  All else is optional.  errcode() defaults to
 * ERRCODE_INTERNAL_ERROR if elevel is ERROR or more, ERRCODE_WARNING
 * if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is
 * NOTICE or below.
 *----------
 */
#define ereport(elevel, rest)  \
	(errstart(elevel, __FILE__, __LINE__, PG_FUNCNAME_MACRO) ? \
	 (errfinish rest) : (void) 0)

extern bool errstart(int elevel, const char *filename, int lineno,
					 const char *funcname);
extern void errfinish(int dummy, ...);

extern int errcode(int sqlerrcode);

extern int errcode_for_file_access(void);
extern int errcode_for_socket_access(void);

extern int errmsg(const char *fmt, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int errmsg_internal(const char *fmt, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int errdetail(const char *fmt, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int errhint(const char *fmt, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int errcontext(const char *fmt, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int errfunction(const char *funcname);
extern int errposition(int cursorpos);


extern void
elog_finish(int elevel, const char *fmt, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 2, 3)));


/* Support for attaching context information to error reports */

typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void (*callback) (void *arg);
	void *arg;
} ErrorContextCallback;

extern DLLIMPORT ErrorContextCallback *error_context_stack;


/* GUC-configurable parameters */

typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
} PGErrorVerbosity;

extern PGErrorVerbosity Log_error_verbosity;

#endif /* not PG_VERSION_73_COMPAT */

#endif   /* PLR_H */
