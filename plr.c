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
 * plr.c - Language handler and support functions
 */
#include "plr.h"

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
#define OPTIONS_NULL_CMD	"options(error = expression(NULL))"
#define THROWERROR_CMD \
			"pg.throwerror <-function(msg) " \
			"{.C(\"throw_pg_error\", as.character(msg))}"
#define OPTIONS_THROWERROR_CMD \
			"options(error = expression(pg.throwerror(geterrmessage())))"
#define QUOTE_LITERAL_CMD \
			"pg.quoteliteral <-function(sql) " \
			"{.Call(\"plr_quote_literal\", sql)}"
#define QUOTE_IDENT_CMD \
			"pg.quoteident <-function(sql) " \
			"{.Call(\"plr_quote_ident\", sql)}"
#define SPI_EXEC_CMD \
			"pg.spi.exec <-function(sql) {.Call(\"plr_SPI_exec\", sql)}"
#define TYPEOIDS_CMD 	"ABSTIMEOID <- as.integer(702); " \
						"ACLITEMOID <- as.integer(1033); " \
						"ANYARRAYOID <- as.integer(2277); " \
						"ANYOID <- as.integer(2276); " \
						"BITOID <- as.integer(1560); " \
						"BOOLOID <- as.integer(16); " \
						"BOXOID <- as.integer(603); " \
						"BPCHAROID <- as.integer(1042); " \
						"BYTEAOID <- as.integer(17); " \
						"CASHOID <- as.integer(790); " \
						"CHAROID <- as.integer(18); " \
						"CIDOID <- as.integer(29); " \
						"CIDROID <- as.integer(650); " \
						"CIRCLEOID <- as.integer(718); " \
						"CSTRINGOID <- as.integer(2275); " \
						"DATEOID <- as.integer(1082); " \
						"FLOAT4OID <- as.integer(700); " \
						"FLOAT8OID <- as.integer(701); " \
						"INETOID <- as.integer(869); " \
						"INT2OID <- as.integer(21); " \
						"INT2VECTOROID <- as.integer(22); " \
						"INT4OID <- as.integer(23); " \
						"INT8OID <- as.integer(20); " \
						"INTERNALOID <- as.integer(2281); " \
						"INTERVALOID <- as.integer(1186); " \
						"LANGUAGE_HANDLEROID <- as.integer(2280); " \
						"LINEOID <- as.integer(628); " \
						"LSEGOID <- as.integer(601); " \
						"MACADDROID <- as.integer(829); " \
						"NAMEOID <- as.integer(19); " \
						"NUMERICOID <- as.integer(1700); " \
						"OIDOID <- as.integer(26); " \
						"OIDVECTOROID <- as.integer(30); " \
						"OPAQUEOID <- as.integer(2282); " \
						"PATHOID <- as.integer(602); " \
						"POINTOID <- as.integer(600); " \
						"POLYGONOID <- as.integer(604); " \
						"RECORDOID <- as.integer(2249); " \
						"REFCURSOROID <- as.integer(1790); " \
						"REGCLASSOID <- as.integer(2205); " \
						"REGOPERATOROID <- as.integer(2204); " \
						"REGOPEROID <- as.integer(2203); " \
						"REGPROCEDUREOID <- as.integer(2202); " \
						"REGPROCOID <- as.integer(24); " \
						"REGTYPEOID <- as.integer(2206); " \
						"RELTIMEOID <- as.integer(703); " \
						"TEXTOID <- as.integer(25); " \
						"TIDOID <- as.integer(27); " \
						"TIMEOID <- as.integer(1083); " \
						"TIMESTAMPOID <- as.integer(1114); " \
						"TIMESTAMPTZOID <- as.integer(1184); " \
						"TIMETZOID <- as.integer(1266); " \
						"TINTERVALOID <- as.integer(704); " \
						"TRIGGEROID <- as.integer(2279); " \
						"UNKNOWNOID <- as.integer(705); " \
						"VARBITOID <- as.integer(1562); " \
						"VARCHAROID <- as.integer(1043); " \
						"VOIDOID <- as.integer(2278); " \
						"XIDOID <- as.integer(28); "
#define SPI_PREPARE_CMD \
			"pg.spi.prepare <-function(sql, argtypes) " \
			"{.Call(\"plr_SPI_prepare\", sql, argtypes)}"
#define SPI_EXECP_CMD \
			"pg.spi.execp <-function(sql, argvalues) " \
			"{.Call(\"plr_SPI_execp\", sql, argvalues)}"
#define SPI_LASTOID_CMD \
			"pg.spi.lastoid <-function() " \
			"{.Call(\"plr_SPI_lastoid\")}"
#define REVAL \
			"pg.reval <- function(arg1) {eval(parse(text = arg1))}"

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
static void start_interp(void);
static void plr_init_interp(Oid funcid);
static void plr_init_all(Oid funcid);
static void plr_init_load_modules(void);
static HeapTuple plr_trigger_handler(PG_FUNCTION_ARGS);
static Datum plr_func_handler(PG_FUNCTION_ARGS);
static plr_proc_desc *compile_plr_function(Oid fn_oid, bool is_trigger);
static plr_proc_desc *get_prodesc_by_name(char *name);
static SEXP plr_parse_func_body(const char *body);
static SEXP plr_convertargs(plr_proc_desc *prodesc, FunctionCallInfo fcinfo);

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
		OPTIONS_NULL_CMD,

		/* set up the postgres error handler in R */
		THROWERROR_CMD,
		OPTIONS_THROWERROR_CMD,

		/* install the commands for SPI support in the interpreter */
		QUOTE_LITERAL_CMD,
		QUOTE_IDENT_CMD,
		SPI_EXEC_CMD,
		TYPEOIDS_CMD,
		SPI_PREPARE_CMD,
		SPI_EXECP_CMD,
		SPI_LASTOID_CMD,

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
	PROTECT(fun = prodesc->fun);

	/* Convert all call arguments */
	PROTECT(rargs = plr_convertargs(prodesc, fcinfo));

	/* Call the R function */
	PROTECT(rvalue = call_r_func(fun, rargs));

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
	UNPROTECT(3);

	/* switch back to old memory context */
	MemoryContextSwitchTo(oldcontext);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "plr: SPI_finish() failed");

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
	prodesc = get_prodesc_by_name(internal_proname);

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

			/* special case -- NAME looks like an array, but treat as a scalar */
			if (prodesc->result_typid == NAMEOID)
				prodesc->result_elem = 0;
			else
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

				/* special case -- NAME looks like an array, but treat as a scalar */
				if (prodesc->arg_typid[i] == NAMEOID)
					prodesc->arg_elem[i] = 0;
				else
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
			prodesc->fun = plr_parse_func_body(proc_internal_def->data);
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
 * get_prodesc_by_name
 *		Returns a prodesc given a proname, or NULL if name not found.
 */
static plr_proc_desc *
get_prodesc_by_name(char *name)
{
	plr_proc_desc *prodesc;

	if (PointerIsValid(name))
		plr_HashTableLookup(name, prodesc);
	else
		prodesc = NULL;

	return prodesc;
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
		elog(ERROR, "plr: R parse error");
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
		if (fcinfo->argnull[i])
		{
			/* fast track for null arguments */
			PROTECT(el = NEW_CHARACTER(1));
			SET_STRING_ELT(el, 0, NA_STRING);
		}
		else if (prodesc->arg_is_rel[i])
		{
			/* for tuple args, convert to a one row data.frame */
			TupleTableSlot *slot = (TupleTableSlot *) fcinfo->arg[i];
			HeapTuple		tuples = slot->val;
			TupleDesc		tupdesc = slot->ttc_tupleDescriptor;

			PROTECT(el = pg_tuple_get_r_frame(1, &tuples, tupdesc));
		}
		else if (prodesc->arg_elem[i] == 0)
		{
			/* for scalar args, convert to a one row vector */
			Datum		dvalue = fcinfo->arg[i];
			Oid			arg_typid = prodesc->arg_typid[i];
			FmgrInfo	arg_out_func = prodesc->arg_out_func[i];

			PROTECT(el = pg_scalar_get_r(dvalue, arg_typid, arg_out_func));
		}
		else
		{
			/* better be a pg array arg, convert to a multi-row vector */
			Datum		dvalue = fcinfo->arg[i];
			FmgrInfo	out_func = prodesc->arg_elem_out_func[i];
			int			typlen = prodesc->arg_elem_typlen[i];
			bool		typbyval = prodesc->arg_elem_typbyval[i];
			char		typalign = prodesc->arg_elem_typalign[i];

			PROTECT(el = pg_array_get_r(dvalue, out_func, typlen, typbyval, typalign));
		}

		SET_VECTOR_ELT(rargs, i, el);
		UNPROTECT(1);
	}

	UNPROTECT(1);

	return(rargs);
}

