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
 * plr.c - Language handler and support functions
 */
#include "plr.h"
#include "funcapi.h"
#include "miscadmin.h"

/*
 * Global data
 */
static bool plr_firstcall = true;
static bool	plr_interp_started = false;
static FunctionCallInfo plr_current_fcinfo = NULL;
static HTAB *plr_HashTable;

/*
 * defines
 */
#define MAX_PRONAME_LEN		64
#define FUNCS_PER_USER		64

/* hash table support */
#define plr_HashTableLookup(NAME, PRODESC) \
do { \
	plr_HashEnt *hentry; char key[MAX_PRONAME_LEN]; \
	\
	MemSet(key, 0, MAX_PRONAME_LEN); \
	snprintf(key, MAX_PRONAME_LEN - 1, "%s", NAME); \
	hentry = (plr_HashEnt*) hash_search(plr_HashTable, \
										 key, HASH_FIND, NULL); \
	if (hentry) \
		PRODESC = hentry->prodesc; \
	else \
		PRODESC = NULL; \
} while(0)

#define plr_HashTableInsert(PRODESC) \
do { \
	plr_HashEnt *hentry; bool found; char key[MAX_PRONAME_LEN]; \
	\
	MemSet(key, 0, MAX_PRONAME_LEN); \
	snprintf(key, MAX_PRONAME_LEN - 1, "%s", PRODESC->proname); \
	hentry = (plr_HashEnt*) hash_search(plr_HashTable, \
										 key, HASH_ENTER, &found); \
	if (hentry == NULL) \
		elog(ERROR, "out of memory in plr_HashTable"); \
	if (found) \
		elog(WARNING, "trying to insert a function name that exists."); \
	hentry->prodesc = PRODESC; \
} while(0)

#define plr_HashTableDelete(PRODESC) \
do { \
	plr_HashEnt *hentry; char key[MAX_PRONAME_LEN]; \
	\
	MemSet(key, 0, MAX_PRONAME_LEN); \
	snprintf(key, MAX_PRONAME_LEN - 1, "%s", PRODESC->proname); \
	hentry = (plr_HashEnt*) hash_search(plr_HashTable, \
										 key, HASH_REMOVE, NULL); \
	if (hentry == NULL) \
		elog(WARNING, "trying to delete function name that does not exist."); \
} while(0)

#define CurrentTriggerData ((TriggerData *) fcinfo->context)

/* hash table */
typedef struct plr_hashent
{
	char			internal_proname[MAX_PRONAME_LEN];
	plr_proc_desc  *prodesc;
} plr_HashEnt;

/*
 * static declarations
 */
static void perm_fmgr_info(Oid functionId, FmgrInfo *finfo);
static char *get_lib_pathstr(Oid funcid);
static char *get_load_self_ref_cmd(Oid funcid);
static void start_interp(void);
static void plr_init_interp(Oid funcid);
static void plr_init_all(Oid funcid);
static void plr_init_load_modules(void);
static HeapTuple plr_trigger_handler(PG_FUNCTION_ARGS);
static Datum plr_func_handler(PG_FUNCTION_ARGS);
static plr_proc_desc *compile_plr_function(Oid fn_oid, bool is_trigger);
static plr_proc_desc *GetProdescByName(char *name);
static SEXP plr_parseFunctionBody(const char *body);
static SEXP plr_convertargs(plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
static SEXP pg_get_r(plr_proc_desc *prodesc, FunctionCallInfo fcinfo, int idx);
static void pg_get_one_r(char *value, Oid arg_out_fn_oid, SEXP *obj, int elnum);
static void get_r_vector(Oid typtype, SEXP *obj, int numels);
static Datum r_get_pg(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
static Datum get_tuplestore(SEXP rval, plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
static Datum get_scalar_datum(SEXP rval, plr_proc_desc *prodesc);
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
	Datum		retval;
	FunctionCallInfo save_fcinfo;

	/* initialize R if needed */
	if(plr_firstcall)
		plr_init_all(fcinfo->flinfo->fn_oid);

	/* Connect to SPI manager */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "plr: cannot connect to SPI manager");

	/*
	 * Determine if called as function or trigger and
	 * call appropriate subhandler
	 */
	save_fcinfo = plr_current_fcinfo;

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

	plr_current_fcinfo = save_fcinfo;

	return retval;
}

static void
perm_fmgr_info(Oid functionId, FmgrInfo *finfo)
{
	fmgr_info_cxt(functionId, finfo, TopMemoryContext);
	finfo->fn_mcxt = QueryContext;
}


static char *
get_lib_pathstr(Oid funcid)
{
	HeapTuple			procedureTuple;
	Form_pg_proc		procedureStruct;
	Oid					language;
	HeapTuple			languageTuple;
	Form_pg_language	languageStruct;
	Oid					lang_funcid;
	Datum				tmp;
	bool				isnull;
	char			   *raw_path;
	char			   *cooked_path;

	/* get the pg_proc entry */
	procedureTuple = SearchSysCache(PROCOID,
									ObjectIdGetDatum(funcid),
									0, 0, 0);
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "get_lib_pathstr: cache lookup for function %u failed",
			 funcid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	/* now get the pg_language entry */
	language = procedureStruct->prolang;
	ReleaseSysCache(procedureTuple);

	languageTuple = SearchSysCache(LANGOID,
								   ObjectIdGetDatum(language),
								   0, 0, 0);
	if (!HeapTupleIsValid(languageTuple))
		elog(ERROR, "get_lib_pathstr: cache lookup for language %u failed",
			 language);
	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);
	lang_funcid = languageStruct->lanplcallfoid;
	ReleaseSysCache(languageTuple);

	/* finally, get the pg_proc entry for the language handler */
	procedureTuple = SearchSysCache(PROCOID,
									ObjectIdGetDatum(lang_funcid),
									0, 0, 0);
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "get_lib_pathstr: cache lookup for function %u failed",
			 lang_funcid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	tmp = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_probin, &isnull);
	raw_path = DatumGetCString(DirectFunctionCall1(byteaout, tmp));
	cooked_path = expand_dynamic_library_name(raw_path);

	ReleaseSysCache(procedureTuple);

	return cooked_path;
}

static char *
get_load_self_ref_cmd(Oid funcid)
{
	char   *libstr = get_lib_pathstr(funcid);
	char   *buf = (char *) palloc(strlen(libstr) + 12 + 1);

	sprintf(buf, "dyn.load(\"%s\")", libstr);
	return buf;
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
	    error("plr: invalid R call: %s", cmd);
	}
	/* Loop is needed here as EXPSEXP may be of length > 1 */
	for(i = 0; i < length(cmdexpr); i++)
	    ans = eval(VECTOR_ELT(cmdexpr, i), R_GlobalEnv);
	UNPROTECT(2);
}

/*
 * start_interp() - start embedded R
 */
static void
start_interp(void)
{
	int			argc;
	char	   *argv[] = {"PL/R", "--gui=none", "--silent", "--no-save"};

	/* refuse to start more than once */
	if (plr_interp_started == true)
		return;

	argc = sizeof(argv)/sizeof(argv[0]);
	Rf_initEmbeddedR(argc, argv);
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
		"options(error = expression(NULL))",

		/* set up the postgres error handler in R */
		"pg_throw_error <-function(msg) {.C(\"throw_pg_error\", as.character(msg))}",
		"options(error = expression(pg_throw_error(geterrmessage())))",

		/* install the commands for SPI support in the interpreter */
		"pg_quote_literal <-function(sql) {.Call(\"plr_quote_literal\", sql)}",
		"pg_quote_ident <-function(sql) {.Call(\"plr_quote_ident\", sql)}",
		"pg_spi_exec <-function(sql) {.Call(\"plr_SPI_exec\", sql)}",
/*
		"pg_spi_prepare <-function(msg) {.C(\"plr_spi_prepare\", as.character(msg))}",
		"pg_spi_execp <-function(msg) {.C(\"plr_spi_execp\", as.character(msg))}",
		"pg_spi_lastoid <-function(msg) {.C(\"plr_spi_lastoid\", as.character(msg))}",
*/
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
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "plr_init_interp: SPI_connect failed");
	plr_init_load_modules();
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "plr_init_interp: SPI_finish failed");

}

/*
 * plr_init_load_modules() - Load procedures from
 *				  table plr_modules (if it exists)
 */
static void
plr_init_load_modules(void)
{
	int			spi_rc;
	char	   *cmd;
	int			i;
	int			fno;

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
}

static void
plr_init_all(Oid funcid)
{
	HASHCTL		ctl;

	/* set up the functions caching hash table */
	ctl.keysize = MAX_PRONAME_LEN;
	ctl.entrysize = sizeof(plr_HashEnt);

	/*
	 * use FUNCS_PER_USER, defined above as a guess of how
	 * many hash table entries to create, initially
	 */
	plr_HashTable = hash_create("plr hash", FUNCS_PER_USER, &ctl, HASH_ELEM);

	/* now initialize EmbeddedR */
	plr_init_interp(funcid);

	plr_firstcall = false;
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
	plr_proc_desc  *prodesc;
	SEXP			fun;
	SEXP			rargs;
	SEXP			rvalue;
	Datum			retval;
	MemoryContext	oldcontext;

	/* Find or compile the function */
	prodesc = compile_plr_function(fcinfo->flinfo->fn_oid, false);
	fun = prodesc->fun;

	/* Convert all call arguments */
	PROTECT(rargs = plr_convertargs(prodesc, fcinfo));

	/* Call the R function */
	PROTECT(fun);
	PROTECT(rvalue = callRFunction(fun, rargs));

	/* switch out of current memory context into the function's context */
	oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	/* convert the return value from an R object to a Datum */
	if(rvalue != R_NilValue)
		retval = r_get_pg(rvalue, prodesc, fcinfo);
	else
	{
		/* return NULL if R code returned undef */
		retval = (Datum) 0;
		fcinfo->isnull = true;
	}

	/* switch back to old memory context */
	MemoryContextSwitchTo(oldcontext);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "plr: SPI_finish() failed");

	UNPROTECT(3);

	return retval;
}

/*
 * compile_plr_function	- compile (or hopefully just look up) function
 */
static plr_proc_desc *
compile_plr_function(Oid fn_oid, bool is_trigger)
{
	HeapTuple		procTup;
	Form_pg_proc	procStruct;
	char			internal_proname[MAX_PRONAME_LEN];
	plr_proc_desc  *prodesc = NULL;
	char		   *proname;
	int				i;

	/* We'll need the pg_proc tuple in any case... */
	procTup = SearchSysCache(PROCOID,
							 ObjectIdGetDatum(fn_oid),
							 0, 0, 0);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "plr: cache lookup for proc %u failed", fn_oid);
	procStruct = (Form_pg_proc) GETSTRUCT(procTup);

	/* grab the function name */
	proname = pstrdup(NameStr(procStruct->proname));

	/* Build our internal proc name from the functions Oid */
	sprintf(internal_proname, "PLR%u", fn_oid);

	/* Lookup the prodesc in the hashtable based on internal_proname */
	prodesc = GetProdescByName(internal_proname);

	/*
	 * If it's present, must check whether it's still up to date.
	 * This is needed because CREATE OR REPLACE FUNCTION can modify the
	 * function's pg_proc entry without changing its OID.
	 */
	if (prodesc != NULL)
	{
		bool		uptodate;

		uptodate = (prodesc->fn_xmin == HeapTupleHeaderGetXmin(procTup->t_data) &&
			prodesc->fn_cmin == HeapTupleHeaderGetCmin(procTup->t_data));

		if (!uptodate)
		{
			plr_HashTableDelete(prodesc);
			prodesc = NULL;
		}
	}

	/*
	 * If we haven't found it in the hashtable, we analyze
	 * the functions arguments and returntype and store
	 * the in-/out-functions in the prodesc block and create
	 * a new hashtable entry for it.
	 *
	 * Then we load the procedure into the R interpreter.
	 */
	if (prodesc == NULL)
	{
		HeapTuple			langTup;
		HeapTuple			typeTup;
		Form_pg_language	langStruct;
		Form_pg_type		typeStruct;
		StringInfo			proc_internal_def = makeStringInfo();
		StringInfo			proc_internal_args = makeStringInfo();
		char		 	   *proc_source;

		/* Allocate a new procedure description block */
		prodesc = (plr_proc_desc *) malloc(sizeof(plr_proc_desc));
		if (prodesc == NULL)
			elog(ERROR, "plr: out of memory");
		MemSet(prodesc, 0, sizeof(plr_proc_desc));
		prodesc->proname = strdup(internal_proname);
		prodesc->fn_xmin = HeapTupleHeaderGetXmin(procTup->t_data);
		prodesc->fn_cmin = HeapTupleHeaderGetCmin(procTup->t_data);

		/* Lookup the pg_language tuple by Oid*/
		langTup = SearchSysCache(LANGOID,
								 ObjectIdGetDatum(procStruct->prolang),
								 0, 0, 0);
		if (!HeapTupleIsValid(langTup))
		{
			free(prodesc->proname);
			free(prodesc);
			elog(ERROR, "plr: cache lookup for language %u failed",
				 procStruct->prolang);
		}
		langStruct = (Form_pg_language) GETSTRUCT(langTup);
		prodesc->lanpltrusted = langStruct->lanpltrusted;
		ReleaseSysCache(langTup);

		/*
		 * Get the required information for input conversion of the
		 * return value.
		 */
		if (!is_trigger)
		{
			typeTup = SearchSysCache(TYPEOID,
								ObjectIdGetDatum(procStruct->prorettype),
									 0, 0, 0);
			if (!HeapTupleIsValid(typeTup))
			{
				free(prodesc->proname);
				free(prodesc);
				elog(ERROR, "plr: cache lookup for return type %u failed",
					 procStruct->prorettype);
			}
			typeStruct = (Form_pg_type) GETSTRUCT(typeTup);

			/* Disallow pseudotype result, except VOID or RECORD */
			if (typeStruct->typtype == 'p')
			{
				if (procStruct->prorettype == VOIDOID ||
					procStruct->prorettype == RECORDOID)
					 /* okay */ ;
				else if (procStruct->prorettype == TRIGGEROID)
				{
					free(prodesc->proname);
					free(prodesc);
					elog(ERROR, "plr functions cannot return type %s"
						 "\n\texcept when used as triggers",
						 format_type_be(procStruct->prorettype));
				}
				else
				{
					free(prodesc->proname);
					free(prodesc);
					elog(ERROR, "plr functions cannot return type %s",
						 format_type_be(procStruct->prorettype));
				}
			}

			if (typeStruct->typrelid != InvalidOid ||
				procStruct->prorettype == RECORDOID)
				prodesc->result_istuple = true;

			prodesc->result_typid = procStruct->prorettype;
			perm_fmgr_info(typeStruct->typinput, &(prodesc->result_in_func));
			prodesc->result_elem = typeStruct->typelem;

			ReleaseSysCache(typeTup);

			/*
			 * Check to see if we have an array type --
			 * if so get it's in_func
			 */
			if (prodesc->result_elem != 0)
			{
				int			typlen;
				bool		typbyval;
				char		typdelim;
				Oid			typinput,
							typelem;
				FmgrInfo	inputproc;
				char		typalign;

				system_cache_lookup(prodesc->result_elem, true, &typlen, &typbyval,
									&typdelim, &typelem, &typinput, &typalign);
				perm_fmgr_info(typinput, &inputproc);

				prodesc->result_elem_in_func = inputproc;
				prodesc->result_elem_typbyval = typbyval;
				prodesc->result_elem_typlen = typlen;
				prodesc->result_elem_typalign = typalign;
			}
		}

		/*
		 * Get the required information for output conversion
		 * of all procedure arguments
		 */
		if (!is_trigger)
		{
			prodesc->nargs = procStruct->pronargs;
			for (i = 0; i < prodesc->nargs; i++)
			{
				typeTup = SearchSysCache(TYPEOID,
							ObjectIdGetDatum(procStruct->proargtypes[i]),
										 0, 0, 0);
				if (!HeapTupleIsValid(typeTup))
				{
					free(prodesc->proname);
					free(prodesc);
					elog(ERROR, "plr: cache lookup for argument type %u failed",
						 procStruct->proargtypes[i]);
				}
				typeStruct = (Form_pg_type) GETSTRUCT(typeTup);

				/* Disallow pseudotype argument */
				if (typeStruct->typtype == 'p')
				{
					free(prodesc->proname);
					free(prodesc);
					elog(ERROR, "plr functions cannot take type %s",
						 format_type_be(procStruct->proargtypes[i]));
				}

				if (typeStruct->typrelid != InvalidOid)
					prodesc->arg_is_rel[i] = 1;
				else
					prodesc->arg_is_rel[i] = 0;

				prodesc->arg_typid[i] = procStruct->proargtypes[i];
				perm_fmgr_info(typeStruct->typoutput, &(prodesc->arg_out_func[i]));
				prodesc->arg_elem[i] = typeStruct->typelem;

				if (i > 0)
					appendStringInfo(proc_internal_args, ",");
				appendStringInfo(proc_internal_args, "arg%d", i + 1);

				ReleaseSysCache(typeTup);


				if (prodesc->arg_elem[i] != 0)
				{
					int			typlen;
					bool		typbyval;
					char		typdelim;
					Oid			typoutput,
								typelem;
					FmgrInfo	outputproc;
					char		typalign;

					system_cache_lookup(prodesc->arg_elem[i], false, &typlen, &typbyval,
										&typdelim, &typelem, &typoutput, &typalign);
					perm_fmgr_info(typoutput, &outputproc);

					prodesc->arg_elem_out_func[i] = outputproc;
					prodesc->arg_elem_typbyval[i] = typbyval;
					prodesc->arg_elem_typlen[i] = typlen;
					prodesc->arg_elem_typalign[i] = typalign;
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
			prodesc->fun = plr_parseFunctionBody(proc_internal_def->data);
		else
			prodesc->fun = Rf_findFun(Rf_install(proname), R_GlobalEnv);

		pfree(proc_source);
		freeStringInfo(proc_internal_def);

		/* test that this is really a function. */
		if(prodesc->fun == R_NilValue)
		{
			free(prodesc->proname);
			free(prodesc);
			elog(ERROR, "plr: cannot create internal procedure %s",
				 internal_proname);
		}

		/* Add the proc description block to the hashtable */
		plr_HashTableInsert(prodesc);
	}

	ReleaseSysCache(procTup);

	return prodesc;
}

/*
 * GetProdescByName
 *		Returns a prodesc given a proname, or NULL if name not found.
 */
static plr_proc_desc *
GetProdescByName(char *name)
{
	plr_proc_desc *prodesc;

	if (PointerIsValid(name))
		plr_HashTableLookup(name, prodesc);
	else
		prodesc = NULL;

	return prodesc;
}

static SEXP
plr_parseFunctionBody(const char *body)
{
	SEXP	txt;
	SEXP	fun;
	int		status;

	PROTECT(txt = NEW_CHARACTER(1));
	SET_STRING_ELT(txt, 0, COPY_TO_USER_STRING(body));

	PROTECT(fun = VECTOR_ELT(R_ParseVector(txt, -1, &status), 0));
	if (status != PARSE_OK) {
	    UNPROTECT(2);
		elog(ERROR, "plr: R parse error");
	}

	UNPROTECT(2);
	return(fun);
}

SEXP
callRFunction(SEXP fun, SEXP rargs)
{
	int		i;
	int		errorOccurred;
	SEXP	c,
			call,
			ans;
	long	n = Rf_length(rargs);

	if(n > 0)
	{
		PROTECT(c = call = Rf_allocList(n));
		for (i = 0; i < n; i++)
		{
			SETCAR(c, VECTOR_ELT(rargs, i));
			c = CDR(c);
		}

		call = Rf_lcons(fun, call);
		UNPROTECT(1);
	}
	else
	{
		call = Rf_allocVector(LANGSXP,1);
		SETCAR(call, fun);
	}

	PROTECT(call);
	ans = R_tryEval(call, R_GlobalEnv, &errorOccurred);
	UNPROTECT(1);

	if(errorOccurred)
		elog(ERROR, "Caught an error calling R function");

	return ans;
}

static SEXP
plr_convertargs(plr_proc_desc *prodesc, FunctionCallInfo fcinfo)
{
	int		i;
	SEXP	rargs,
			el;

	/*
	 * Create an array of R objects with the number of elements
	 * equal to the number of arguments.
	 */
	PROTECT(rargs = allocVector(VECSXP, prodesc->nargs));

	/*
	 * iterate over the arguments, convert each of them and put them in
	 * the array.
	 */
	for (i = 0; i < prodesc->nargs; i++)
	{
		el = pg_get_r(prodesc, fcinfo, i);
		SET_VECTOR_ELT(rargs, i, el);
	}

	UNPROTECT(1);

	return(rargs);
}

/*
 * given a pg value, convert to its R value representation
 */
static SEXP
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
void
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
void
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
static Datum
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
			return get_scalar_datum(rval, prodesc);
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

static Datum
get_scalar_datum(SEXP rval, plr_proc_desc *prodesc)
{
	Datum		dvalue;
	SEXP		obj;
	char	   *value;
	FmgrInfo	result_in_func = prodesc->result_in_func;
	Oid			result_elem = prodesc->result_elem;

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
		PROTECT(dfcol = CAR(ATTRIB(dfcol)));

	nr = length(dfcol);
	UNPROTECT(2);

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

