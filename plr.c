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
 * plr.c - Language handler and support functions
 */
#include "plr.h"

/*
 * Global data
 */
MemoryContext plr_SPI_context;
HTAB *plr_HashTable = (HTAB *) NULL;
char *last_R_error_msg = NULL;

static bool plr_firstcall = true;
static bool	plr_interp_started = false;
static FunctionCallInfo plr_current_fcinfo = NULL;

/*
 * defines
 */

/* real max is 3 (for "PLR") plus number of characters in an Oid */
#define MAX_PRONAME_LEN		NAMEDATALEN

#define OPTIONS_NULL_CMD	"options(error = expression(NULL))"
#define THROWRERROR_CMD \
			"pg.throwrerror <-function(msg) " \
			"{" \
			"  msglen <- nchar(msg);" \
			"  if (substr(msg, msglen, msglen + 1) == \"\\n\")" \
			"    msg <- substr(msg, 1, msglen - 1);" \
			"  .C(\"throw_r_error\", as.character(msg));" \
			"}"
#define OPTIONS_THROWRERROR_CMD \
			"options(error = expression(pg.throwrerror(geterrmessage())))"
#define THROWNOTICE_CMD \
			"pg.thrownotice <-function(msg) " \
			"{.C(\"throw_pg_notice\", as.character(msg))}"
#define THROWERROR_CMD \
			"pg.throwerror <-function(msg) " \
			"{.C(\"throw_pg_error\", as.character(msg))}"
#define QUOTE_LITERAL_CMD \
			"pg.quoteliteral <-function(sql) " \
			"{.Call(\"plr_quote_literal\", sql)}"
#define QUOTE_IDENT_CMD \
			"pg.quoteident <-function(sql) " \
			"{.Call(\"plr_quote_ident\", sql)}"
#define SPI_EXEC_CMD \
			"pg.spi.exec <-function(sql) {.Call(\"plr_SPI_exec\", sql)}"
#define SPI_PREPARE_CMD \
			"pg.spi.prepare <-function(sql, argtypes = NA) " \
			"{.Call(\"plr_SPI_prepare\", sql, argtypes)}"
#define SPI_EXECP_CMD \
			"pg.spi.execp <-function(sql, argvalues = NA) " \
			"{.Call(\"plr_SPI_execp\", sql, argvalues)}"
#define SPI_LASTOID_CMD \
			"pg.spi.lastoid <-function() " \
			"{.Call(\"plr_SPI_lastoid\")}"
#define SPI_FACTOR_CMD \
			"pg.spi.factor <- function(arg1) {\n" \
			"  for (col in 1:ncol(arg1)) {\n" \
			"    if (!is.numeric(arg1[,col])) {\n" \
			"      arg1[,col] <- factor(arg1[,col])\n" \
			"    }\n" \
			"  }\n" \
			"  return(arg1)\n" \
			"}"
#define REVAL \
			"pg.reval <- function(arg1) {eval(parse(text = arg1))}"
#define PG_STATE_FIRSTPASS \
			"pg.state.firstpass <- TRUE"

#define CurrentTriggerData ((TriggerData *) fcinfo->context)


/*
 * static declarations
 */
static void plr_init_interp(Oid funcid);
static void plr_init_all(Oid funcid);
static HeapTuple plr_trigger_handler(PG_FUNCTION_ARGS);
static Datum plr_func_handler(PG_FUNCTION_ARGS);
static plr_function *compile_plr_function(FunctionCallInfo fcinfo);
static plr_function *do_compile(FunctionCallInfo fcinfo,
							    HeapTuple procTup,
							    plr_func_hashkey *hashkey);
static SEXP plr_parse_func_body(const char *body);
static SEXP plr_convertargs(plr_function *function, FunctionCallInfo fcinfo);

#ifndef PG_VERSION_73_COMPAT
static void plr_error_callback(void *arg);
#endif

/*
 * plr_call_handler -	This is the only visible function
 *						of the PL interpreter. The PostgreSQL
 *						function manager and trigger manager
 *						call this function for execution of
 *						PL/R procedures.
 */
PG_FUNCTION_INFO_V1(plr_call_handler);

Datum
plr_call_handler(PG_FUNCTION_ARGS)
{
	Datum			retval;
	MemoryContext	origcontext = CurrentMemoryContext;

	/* Connect to SPI manager */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "plr: cannot connect to SPI manager");

	/* switch from SPI back to original call memory context */
	plr_SPI_context = MemoryContextSwitchTo(origcontext);

	/* initialize R if needed */
	if(plr_firstcall)
		plr_init_all(fcinfo->flinfo->fn_oid);

	if (CALLED_AS_TRIGGER(fcinfo))
	{
		plr_current_fcinfo = NULL;
		retval = PointerGetDatum(plr_trigger_handler(fcinfo));
	}
	else
	{
		plr_current_fcinfo = fcinfo;
		retval = plr_func_handler(fcinfo);
	}

	/* switch back to SPI memory context */
	MemoryContextSwitchTo(plr_SPI_context);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "plr: SPI_finish() failed");

	return retval;
}

void
load_r_cmd(const char *cmd)
{
	SEXP		cmdSexp,
				cmdexpr,
				ans = R_NilValue;
	int			i,
				status;

	/* start EmbeddedR if not already done */
	if (plr_interp_started == false)
		start_interp();

	PROTECT(cmdSexp = NEW_CHARACTER(1));
	SET_STRING_ELT(cmdSexp, 0, COPY_TO_USER_STRING(cmd));
	PROTECT(cmdexpr = R_ParseVector(cmdSexp, -1, &status));
	if (status != PARSE_OK) {
	    UNPROTECT(2);
		if (last_R_error_msg)
			elog(ERROR, "%s", last_R_error_msg);
		else
			elog(ERROR, "R parse error: %s", cmd);
	}

	/* Loop is needed here as EXPSEXP may be of length > 1 */
	for(i = 0; i < length(cmdexpr); i++)
	{
		ans = R_tryEval(VECTOR_ELT(cmdexpr, i), R_GlobalEnv, &status);
		if(status != 0)
		{
			if (last_R_error_msg)
				elog(ERROR, "%s", last_R_error_msg);
			else
				elog(ERROR, "%s", "caught error calling R function");
		}
	}

	UNPROTECT(2);
}

/*
 * start_interp() - start embedded R
 */
void
start_interp(void)
{
	char	   *r_home;
	int			rargc;
	char	   *rargv[] = {"PL/R", "--silent", "--no-save"};

	/* refuse to start more than once */
	if (plr_interp_started == true)
		return;

	/* refuse to start if R_HOME is not defined */
	r_home = getenv("R_HOME");
	if (r_home == NULL)
		elog(ERROR, "plr: cannot start interpreter unless R_HOME " \
					"environment variable is defined");

	rargc = sizeof(rargv)/sizeof(rargv[0]);
	Rf_initEmbeddedR(rargc, rargv);
	plr_interp_started = true;
}

/*
 * plr_init_interp() - initialize an R interpreter
 */
static void
plr_init_interp(Oid funcid)
{
	int			j;
	char	   *cmd;
	char	   *cmds[] =
	{
		/* first turn off error handling by R */
		OPTIONS_NULL_CMD,

		/* set up the postgres error handler in R */
		THROWRERROR_CMD,
		OPTIONS_THROWRERROR_CMD,
		THROWNOTICE_CMD,
		THROWERROR_CMD,

		/* install the commands for SPI support in the interpreter */
		QUOTE_LITERAL_CMD,
		QUOTE_IDENT_CMD,
		SPI_EXEC_CMD,
		SPI_PREPARE_CMD,
		SPI_EXECP_CMD,
		SPI_LASTOID_CMD,
		SPI_FACTOR_CMD,

		/* handy predefined R functions */
		REVAL,

		/* terminate */
		NULL
	};

	/* start EmbeddedR if not already done */
	if (plr_interp_started == false)
		start_interp();

	/*
	 * temporarily turn off R error reporting -- it will be turned back on
	 * once the custom R error handler is installed from the plr library
	 */
	load_r_cmd(cmds[0]);

	/* next load the plr library into R */
	load_r_cmd(get_load_self_ref_cmd(funcid));

	/*
	 * run the rest of the R bootstrap commands, being careful to start
	 * at cmds[1] since we already executed cmds[0]
	 */
	for (j = 1; (cmd = cmds[j]); j++)
		load_r_cmd(cmds[j]);

	/*
	 * Try to load procedures from plr_modules
	 */
	plr_init_load_modules(plr_SPI_context);
}

/*
 * plr_init_load_modules() - Load procedures from
 *				  table plr_modules (if it exists)
 *
 * The caller is responsible to ensure SPI has already been connected
 */
void
plr_init_load_modules(MemoryContext	plr_SPI_context)
{
	int				spi_rc;
	char		   *cmd;
	int				i;
	int				fno;
	MemoryContext	oldcontext;

	/* start EmbeddedR if not already done */
	if (plr_interp_started == false)
		start_interp();

	/* switch to SPI memory context */
	oldcontext = MemoryContextSwitchTo(plr_SPI_context);

	/*
	 * Check if table plr_modules exists
	 */
	spi_rc = SPI_exec("select 1 from pg_catalog.pg_class "
					  "where relname = 'plr_modules'", 1);
	SPI_freetuptable(SPI_tuptable);

	if (spi_rc != SPI_OK_SELECT)
		elog(ERROR, "plr_init_load_modules: select from pg_class failed");
	if (SPI_processed == 0)
		return;

	/* Read all the row's from it in the order of modseq */
	spi_rc = SPI_exec("select modseq, modsrc from plr_modules " \
					  "order by modseq", 0);
	if (spi_rc != SPI_OK_SELECT)
		elog(ERROR, "plr_init_load_modules: select from plr_modules failed");

	/* If there's nothing, no modules exist */
	if (SPI_processed == 0)
	{
		SPI_freetuptable(SPI_tuptable);
		return;
	}

	/*
	 * There is at least on module to load. Get the
	 * source from the modsrc load it in the R interpreter
	 */
	fno = SPI_fnumber(SPI_tuptable->tupdesc, "modsrc");

	for (i = 0; i < SPI_processed; i++)
	{
		cmd = SPI_getvalue(SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc, fno);

		if (cmd != NULL)
		{
			load_r_cmd(cmd);
			pfree(cmd);
		}
	}
	SPI_freetuptable(SPI_tuptable);

	/* back to caller's memory context */
	MemoryContextSwitchTo(oldcontext);
}

static void
plr_init_all(Oid funcid)
{
	MemoryContext		oldcontext;

	/* everything initialized needs to live until/unless we explicitly delete it */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	/* initialize EmbeddedR */
	plr_init_interp(funcid);

	plr_firstcall = false;

	/* switch back to caller's context */
	MemoryContextSwitchTo(oldcontext);
}

static HeapTuple
plr_trigger_handler(PG_FUNCTION_ARGS)
{
	/* FIXME */
	elog(ERROR, "plr does not support triggers yet");
	return (HeapTuple) 0;
}

static Datum
plr_func_handler(PG_FUNCTION_ARGS)
{
	plr_function  *function;
	SEXP			fun;
	SEXP			rargs;
	SEXP			rvalue;
	Datum			retval;
	ERRORCONTEXTCALLBACK;

	/* Find or compile the function */
	function = compile_plr_function(fcinfo);

	/* set up error context */
	PUSH_PLERRCONTEXT(plr_error_callback, function->proname);

	PROTECT(fun = function->fun);

	/* Convert all call arguments */
	PROTECT(rargs = plr_convertargs(function, fcinfo));

	/* Call the R function */
	PROTECT(rvalue = call_r_func(fun, rargs));

	/*
	 * Convert the return value from an R object to a Datum.
	 * We expect r_get_pg to do the right thing with missing or empty results.
	 */
	retval = r_get_pg(rvalue, function, fcinfo);

	POP_PLERRCONTEXT;
	UNPROTECT(3);

	return retval;
}


/* ----------
 * compile_plr_function
 *
 * Note: it's important for this to fall through quickly if the function
 * has already been compiled.
 * ----------
 */
plr_function *
compile_plr_function(FunctionCallInfo fcinfo)
{
	Oid					funcOid = fcinfo->flinfo->fn_oid;
	HeapTuple			procTup;
	Form_pg_proc		procStruct;
	plr_function	   *function;
	plr_func_hashkey	hashkey;
	bool				hashkey_valid = false;
	ERRORCONTEXTCALLBACK;

	/*
	 * Lookup the pg_proc tuple by Oid; we'll need it in any case
	 */
	procTup = SearchSysCache(PROCOID,
							 ObjectIdGetDatum(funcOid),
							 0, 0, 0);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "plpgsql: cache lookup for proc %u failed", funcOid);
	procStruct = (Form_pg_proc) GETSTRUCT(procTup);

	/* set up error context */
	PUSH_PLERRCONTEXT(plr_error_callback, NameStr(procStruct->proname));

	/*
	 * See if there's already a cache entry for the current FmgrInfo.
	 * If not, try to find one in the hash table.
	 */
	function = (plr_function *) fcinfo->flinfo->fn_extra;

	if (!function)
	{
		/* First time through in this backend?  If so, init hashtable */
		if (!plr_HashTable)
			plr_HashTableInit();

		/* Compute hashkey using function signature and actual arg types */
		compute_function_hashkey(fcinfo->flinfo, procStruct, &hashkey);
		hashkey_valid = true;

		/* And do the lookup */
		function = plr_HashTableLookup(&hashkey);

		/*
		 * first time through for this statement, set
		 * firstpass to TRUE
		 */
		load_r_cmd(PG_STATE_FIRSTPASS);
	}

	if (function)
	{
		/* We have a compiled function, but is it still valid? */
		if (!(function->fn_xmin == HeapTupleHeaderGetXmin(procTup->t_data) &&
			  function->fn_cmin == HeapTupleHeaderGetCmin(procTup->t_data)))
		{
			/*
			 * Nope, drop the hashtable entry.  XXX someday, free all the
			 * subsidiary storage as well.
			 */
			plr_HashTableDelete(function);

			/* free some of the subsidiary storage */
			xpfree(function->proname);
			R_ReleaseObject(function->fun);
			xpfree(function);

			function = NULL;
		}
	}

	/*
	 * If the function wasn't found or was out-of-date, we have to compile it
	 */
	if (!function)
	{
		/*
		 * Calculate hashkey if we didn't already; we'll need it to store
		 * the completed function.
		 */
		if (!hashkey_valid)
			compute_function_hashkey(fcinfo->flinfo, procStruct, &hashkey);

		/*
		 * Do the hard part.
		 */
		function = do_compile(fcinfo, procTup, &hashkey);
	}

	ReleaseSysCache(procTup);

	/*
	 * Save pointer in FmgrInfo to avoid search on subsequent calls
	 */
	fcinfo->flinfo->fn_extra = (void *) function;

	POP_PLERRCONTEXT;

	/*
	 * Finally return the compiled function
	 */
	return function;
}


/*
 * This is the slow part of compile_plr_function().
 */
static plr_function *
do_compile(FunctionCallInfo fcinfo,
		   HeapTuple procTup,
		   plr_func_hashkey *hashkey)
{
	Form_pg_proc		procStruct = (Form_pg_proc) GETSTRUCT(procTup);
	bool				is_trigger = CALLED_AS_TRIGGER(fcinfo) ? true : false;
	plr_function	   *function = NULL;

	Oid						fn_oid = fcinfo->flinfo->fn_oid;
	char					internal_proname[MAX_PRONAME_LEN];
	char				   *proname;
	Oid						result_typid;
	HeapTuple				langTup;
	HeapTuple				typeTup;
	Form_pg_language		langStruct;
	Form_pg_type			typeStruct;
	StringInfo				proc_internal_def = makeStringInfo();
	StringInfo				proc_internal_args = makeStringInfo();
	char				   *proc_source;
	MemoryContext			oldcontext;

	/* grab the function name */
	proname = NameStr(procStruct->proname);

	/* Build our internal proc name from the functions Oid */
	sprintf(internal_proname, "PLR%u", fn_oid);

	/*
	 * analyze the functions arguments and returntype and store
	 * the in-/out-functions in the function block and create
	 * a new hashtable entry for it.
	 *
	 * Then load the procedure into the R interpreter.
	 */

	/* the function structure needs to live until we explicitly delete it */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	/* Allocate a new procedure description block */
	function = (plr_function *) palloc(sizeof(plr_function));
	if (function == NULL)
		elog(ERROR, "plr: out of memory");
	MemSet(function, 0, sizeof(plr_function));

	function->proname = pstrdup(proname);
	function->fn_xmin = HeapTupleHeaderGetXmin(procTup->t_data);
	function->fn_cmin = HeapTupleHeaderGetCmin(procTup->t_data);

	/* Lookup the pg_language tuple by Oid*/
	langTup = SearchSysCache(LANGOID,
							 ObjectIdGetDatum(procStruct->prolang),
							 0, 0, 0);
	if (!HeapTupleIsValid(langTup))
	{
		xpfree(function->proname);
		xpfree(function);
		elog(ERROR, "plr: cache lookup for language %u failed",
			 procStruct->prolang);
	}
	langStruct = (Form_pg_language) GETSTRUCT(langTup);
	function->lanpltrusted = langStruct->lanpltrusted;
	ReleaseSysCache(langTup);

	/* get the functions return type */
	if (procStruct->prorettype == ANYARRAYOID ||
		procStruct->prorettype == ANYELEMENTOID)
	{
		result_typid = get_fn_expr_rettype(fcinfo->flinfo);
		if (result_typid == InvalidOid)
			result_typid = procStruct->prorettype;
	}
	else
		result_typid = procStruct->prorettype;

	/*
	 * Get the required information for input conversion of the
	 * return value.
	 */
	if (!is_trigger)
	{
		function->result_typid = result_typid;
		typeTup = SearchSysCache(TYPEOID,
								 ObjectIdGetDatum(function->result_typid),
								 0, 0, 0);
		if (!HeapTupleIsValid(typeTup))
		{
			xpfree(function->proname);
			xpfree(function);
			elog(ERROR, "plr: cache lookup for return type %u failed",
				 procStruct->prorettype);
		}
		typeStruct = (Form_pg_type) GETSTRUCT(typeTup);

		/* Disallow pseudotype return type except VOID or RECORD */
		/* (note we already replaced ANYARRAY/ANYELEMENT) */
		if (typeStruct->typtype == 'p')
		{
			if (procStruct->prorettype == VOIDOID ||
				procStruct->prorettype == RECORDOID)
				 /* okay */ ;
			else if (procStruct->prorettype == TRIGGEROID)
			{
				xpfree(function->proname);
				xpfree(function);
				elog(ERROR, "plr functions cannot return type %s"
					 "\n\texcept when used as triggers",
					 format_type_be(procStruct->prorettype));
			}
			else
			{
				xpfree(function->proname);
				xpfree(function);
				elog(ERROR, "plr functions cannot return type %s",
					 format_type_be(procStruct->prorettype));
			}
		}

		if (typeStruct->typrelid != InvalidOid ||
			procStruct->prorettype == RECORDOID)
			function->result_istuple = true;

		perm_fmgr_info(typeStruct->typinput, &(function->result_in_func));

		/* is return type an array? */
		if (OidIsValid(get_element_type(function->result_typid)))
			function->result_elem = typeStruct->typelem;
		else	/* not ant array */
			function->result_elem = InvalidOid;

		ReleaseSysCache(typeTup);

		/*
		 * if we have an array type, get the element type's in_func
		 */
		if (function->result_elem != InvalidOid)
		{
			int16		typlen;
			bool		typbyval;
			char		typdelim;
			Oid			typinput,
						typelem;
			FmgrInfo	inputproc;
			char		typalign;

			get_type_io_data(function->result_elem, IOFunc_input,
									 &typlen, &typbyval, &typalign,
									 &typdelim, &typelem, &typinput);

			perm_fmgr_info(typinput, &inputproc);

			function->result_elem_in_func = inputproc;
			function->result_elem_typbyval = typbyval;
			function->result_elem_typlen = typlen;
			function->result_elem_typalign = typalign;
		}
	}

	/*
	 * Get the required information for output conversion
	 * of all procedure arguments
	 */
	if (!is_trigger)
	{
		int		i;

		function->nargs = procStruct->pronargs;
		for (i = 0; i < function->nargs; i++)
		{
			/*
			 * Since we already did the replacement of polymorphic
			 * argument types by actual argument types while computing
			 * the hashkey, we can just use those results.
			 */
			function->arg_typid[i] = hashkey->argtypes[i];

			typeTup = SearchSysCache(TYPEOID,
						ObjectIdGetDatum(function->arg_typid[i]),
									 0, 0, 0);
			if (!HeapTupleIsValid(typeTup))
			{
				xpfree(function->proname);
				xpfree(function);
				elog(ERROR, "plr: cache lookup for argument type %u failed",
					 function->arg_typid[i]);
			}
			typeStruct = (Form_pg_type) GETSTRUCT(typeTup);

			/* Disallow pseudotype argument */
			/* (note we already replaced ANYARRAY/ANYELEMENT) */
			if (typeStruct->typtype == 'p')
			{
				xpfree(function->proname);
				xpfree(function);
				elog(ERROR, "plr functions cannot take type %s",
					 format_type_be(function->arg_typid[i]));
			}

			if (typeStruct->typrelid != InvalidOid)
				function->arg_is_rel[i] = 1;
			else
				function->arg_is_rel[i] = 0;

			perm_fmgr_info(typeStruct->typoutput, &(function->arg_out_func[i]));

			/* is argument an array? */
			if (OidIsValid(get_element_type(function->arg_typid[i])))
				function->arg_elem[i] = typeStruct->typelem;
			else	/* not ant array */
				function->arg_elem[i] = InvalidOid;

			if (i > 0)
				appendStringInfo(proc_internal_args, ",");
			appendStringInfo(proc_internal_args, "arg%d", i + 1);

			ReleaseSysCache(typeTup);

			if (function->arg_elem[i] != InvalidOid)
			{
				int16		typlen;
				bool		typbyval;
				char		typdelim;
				Oid			typoutput,
							typelem;
				FmgrInfo	outputproc;
				char		typalign;

				get_type_io_data(function->arg_elem[i], IOFunc_output,
										 &typlen, &typbyval, &typalign,
										 &typdelim, &typelem, &typoutput);

				perm_fmgr_info(typoutput, &outputproc);

				function->arg_elem_out_func[i] = outputproc;
				function->arg_elem_typbyval[i] = typbyval;
				function->arg_elem_typlen[i] = typlen;
				function->arg_elem_typalign[i] = typalign;
			}
		}
	}
	else
	{
		/* trigger procedure has fixed args */
		appendStringInfo(proc_internal_args,
			"TGname,TGrelid,TGrelatts,TGwhen,TGlevel,TGop,TGnew,TGold,args");
	}

	/*
	 * Create the R command to define the internal
	 * procedure
	 */
	appendStringInfo(proc_internal_def,
					 "%s <- function(%s) {",
					 internal_proname,
					 proc_internal_args->data);

	/* Add user's function definition to proc body */
	proc_source = DatumGetCString(DirectFunctionCall1(textout,
							  PointerGetDatum(&procStruct->prosrc)));

	appendStringInfo(proc_internal_def, "%s}", proc_source);

	/* parse or find the R function */
	if(proc_source && proc_source[0])
		function->fun = plr_parse_func_body(proc_internal_def->data);
	else
		function->fun = Rf_findFun(Rf_install(proname), R_GlobalEnv);

	R_PreserveObject(function->fun);

	pfree(proc_source);
	freeStringInfo(proc_internal_def);

	/* test that this is really a function. */
	if(function->fun == R_NilValue)
	{
		xpfree(function->proname);
		xpfree(function);
		elog(ERROR, "plr: cannot create internal procedure %s",
			 internal_proname);
	}

	/* switch back to the context we were called with */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * add it to the hash table
	 */
	plr_HashTableInsert(function, hashkey);

	return function;
}

static SEXP
plr_parse_func_body(const char *body)
{
	SEXP	rbody;
	SEXP	fun;
	int		status;

	PROTECT(rbody = NEW_CHARACTER(1));
	SET_STRING_ELT(rbody, 0, COPY_TO_USER_STRING(body));

	PROTECT(fun = VECTOR_ELT(R_ParseVector(rbody, -1, &status), 0));
	if (status != PARSE_OK)
	{
	    UNPROTECT(2);
		if (last_R_error_msg)
			elog(ERROR, "%s", last_R_error_msg);
		else
			elog(ERROR, "R parse error in function body");
	}

	UNPROTECT(2);
	return(fun);
}

SEXP
call_r_func(SEXP fun, SEXP rargs)
{
	int		i;
	int		errorOccurred;
	SEXP	obj,
			args,
			call,
			ans;
	long	n = length(rargs);

	if(n > 0)
	{
		PROTECT(obj = args = allocList(n));
		for (i = 0; i < n; i++)
		{
			SETCAR(obj, VECTOR_ELT(rargs, i));
			obj = CDR(obj);
		}
		UNPROTECT(1);
		PROTECT(call = lcons(fun, args));
	}
	else
	{
		PROTECT(call = allocVector(LANGSXP,1));
		SETCAR(call, fun);
	}

	ans = R_tryEval(call, R_GlobalEnv, &errorOccurred);
	UNPROTECT(1);

	if(errorOccurred)
	{
		if (last_R_error_msg)
			elog(ERROR, "%s", last_R_error_msg);
		else
			elog(ERROR, "%s", "caught error calling R function");
	}
	return ans;
}

static SEXP
plr_convertargs(plr_function *function, FunctionCallInfo fcinfo)
{
	int		i;
	SEXP	rargs,
			el;

	/*
	 * Create an array of R objects with the number of elements
	 * equal to the number of arguments.
	 */
	PROTECT(rargs = allocVector(VECSXP, function->nargs));

	/*
	 * iterate over the arguments, convert each of them and put them in
	 * the array.
	 */
	for (i = 0; i < function->nargs; i++)
	{
		if (fcinfo->argnull[i])
		{
			/* fast track for null arguments */
			PROTECT(el = NEW_CHARACTER(1));
			SET_STRING_ELT(el, 0, NA_STRING);
		}
		else if (function->arg_is_rel[i])
		{
			/* for tuple args, convert to a one row data.frame */
			TupleTableSlot *slot = (TupleTableSlot *) fcinfo->arg[i];
			HeapTuple		tuples = slot->val;
			TupleDesc		tupdesc = slot->ttc_tupleDescriptor;

			PROTECT(el = pg_tuple_get_r_frame(1, &tuples, tupdesc));
		}
		else if (function->arg_elem[i] == 0)
		{
			/* for scalar args, convert to a one row vector */
			Datum		dvalue = fcinfo->arg[i];
			Oid			arg_typid = function->arg_typid[i];
			FmgrInfo	arg_out_func = function->arg_out_func[i];

			PROTECT(el = pg_scalar_get_r(dvalue, arg_typid, arg_out_func));
		}
		else
		{
			/* better be a pg array arg, convert to a multi-row vector */
			Datum		dvalue = fcinfo->arg[i];
			FmgrInfo	out_func = function->arg_elem_out_func[i];
			int			typlen = function->arg_elem_typlen[i];
			bool		typbyval = function->arg_elem_typbyval[i];
			char		typalign = function->arg_elem_typalign[i];

			PROTECT(el = pg_array_get_r(dvalue, out_func, typlen, typbyval, typalign));
		}

		SET_VECTOR_ELT(rargs, i, el);
		UNPROTECT(1);
	}

	UNPROTECT(1);

	return(rargs);
}

#ifndef PG_VERSION_73_COMPAT

/*
 * error context callback to let us supply a call-stack traceback
 */
static void
plr_error_callback(void *arg)
{
	if (arg)
		errcontext("In PL/R function %s", (char *) arg);
}

#endif /* not PG_VERSION_73_COMPAT */
