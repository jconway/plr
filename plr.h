/*
 * plr - PostgreSQL support for R as a
 *	     procedural language (PL)
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
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/spi.h"
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
	SEXP		args;	/* converted args */
}	plr_proc_desc;


/*
 * external declarations
 */

/* libR interpreter initialization */
extern int Rf_initEmbeddedR(int argc, char **argv);

/* PL/R language handler */
extern Datum plr_call_handler(PG_FUNCTION_ARGS);
extern void load_r_cmd(const char *cmd);
extern SEXP call_r_func(SEXP fun, SEXP rargs);

/* argument and return value conversion functions */
extern SEXP pg_get_r(plr_proc_desc *prodesc, FunctionCallInfo fcinfo, int idx);
extern Datum r_get_pg(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
extern Datum get_scalar_datum(SEXP rval, FmgrInfo result_in_func, Oid result_elem);

/* Postgres support functions installed into the R interpreter */
extern void throw_pg_error(const char **msg);
extern SEXP plr_quote_literal(SEXP rawstr);
extern SEXP plr_quote_ident(SEXP rawstr);
extern SEXP plr_SPI_exec(SEXP rsql);
extern SEXP plr_SPI_prepare(SEXP rsql, SEXP rargtypes);
extern SEXP plr_SPI_execp(SEXP rsaved_plan, SEXP rargvalues);
extern SEXP plr_SPI_lastoid(void);

/* Postgres callable functions useful in conjunction with PL/R */
extern Datum install_rcmd(PG_FUNCTION_ARGS);
extern Datum array_push(PG_FUNCTION_ARGS);
extern Datum array(PG_FUNCTION_ARGS);
extern Datum array_accum(PG_FUNCTION_ARGS);

/* Postgres backend support functions */
extern char *get_load_self_ref_cmd(Oid funcid);
extern void perm_fmgr_info(Oid functionId, FmgrInfo *finfo);
extern void system_cache_lookup(Oid element_type, bool input, int *typlen,
					bool *typbyval, char *typdelim, Oid *typelem,
					Oid *proc, char *typalign);

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

#endif   /* PLR_H */
