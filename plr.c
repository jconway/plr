/**********************************************************************
 * plr.c		- PostgreSQL support for R as a
 *				  procedural language (PL)
 *
 * by Joe Conway <mail@joeconway.com>
 *
 * Copyright (c) 2002 by PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
 *
 * Heavily based on pltcl by Jan Wieck
 * and
 * on REmbeddedPostgres by
 * Duncan Temple Lang <duncan@research.bell-labs.com>
 * http://www.omegahat.org/RSPostgres/
 *
 * License: GPL version 2 or newer. http://www.gnu.org/copyleft/gpl.html
 **********************************************************************/

#include <unistd.h>
#include <fcntl.h>
#include <setjmp.h>

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

/*
 * Global data
 */
static bool plr_firstcall = true;
static int plr_call_level = 0;
static FunctionCallInfo plr_current_fcinfo = NULL;
static HTAB *plr_HashTable;
static int	plr_restart_in_progress = 0;

/*
 * defines
 */
#define MAX_PRONAME_LEN		64
#define FUNCS_PER_USER		64

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

/* convert C string to text pointer */
#define GET_TEXT(cstrp) \
	DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(cstrp)))
/* convert text pointer to C string */
#define GET_STR(textp) \
	DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

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
	FmgrInfo	result_in_func;
	Oid			result_in_elem;
	int			nargs;
	FmgrInfo	arg_out_func[FUNC_MAX_ARGS];
	Oid			arg_type[FUNC_MAX_ARGS];
	Oid			arg_elem[FUNC_MAX_ARGS];
	int			arg_is_rel[FUNC_MAX_ARGS];
	SEXP		fun;	/* compiled R function */
	SEXP		args;	/* converted args */
}	plr_proc_desc;

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
static void plr_init_interp(void);
static void plr_init_all(void);
static HeapTuple plr_trigger_handler(PG_FUNCTION_ARGS);
static Datum plr_func_handler(PG_FUNCTION_ARGS);
static plr_proc_desc *compile_plr_function(Oid fn_oid, bool is_trigger);
static plr_proc_desc *GetProdescByName(char *name);
static SEXP plr_parseFunctionBody(const char *body);
static SEXP callRFunction(SEXP fun, SEXP rargs);
static SEXP plr_convertargs(plr_proc_desc *prodesc, FunctionCallInfo fcinfo);
static SEXP pg_get_r(Datum dvalue, FmgrInfo arg_out_func, Oid arg_type, Oid arg_elem);
static void pg_get_one_r(char *value, Oid arg_out_fn_oid, SEXP *obj, int elnum);
static void get_r_vector(Oid typtype, SEXP *obj, int numels);
static Datum r_get_pg(SEXP rval, FmgrInfo arg_in_func, Oid arg_in_elem);
static SEXP pg_get_r_tuple(TupleDesc desc, HeapTuple tuple, Relation relation);
static void system_cache_lookup(Oid element_type, bool input, int *typlen,
					bool *typbyval, char *typdelim, Oid *typelem,
					Oid *proc, char *typalign);

/*
 * external declarations
 */
extern int Rf_initEmbeddedR(int argc, char **argv);
extern Datum plr_call_handler(PG_FUNCTION_ARGS);

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
		plr_init_all();

	/* Connect to SPI manager */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "plr: cannot connect to SPI manager");

	/* Keep track about the nesting of R-SPI-R-... calls */
	plr_call_level++;

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

	plr_call_level--;

	return retval;

}

/*
 * This routine is a crock, and so is everyplace that calls it.  The problem
 * is that the cached form of pltcl functions/queries is allocated permanently
 * (mostly via malloc()) and never released until backend exit.  Subsidiary
 * data structures such as fmgr info records therefore must live forever
 * as well.  A better implementation would store all this stuff in a per-
 * function memory context that could be reclaimed at need.  In the meantime,
 * fmgr_info_cxt must be called specifying TopMemoryContext so that whatever
 * it might allocate, and whatever the eventual function might allocate using
 * fn_mcxt, will live forever too.
 */
static void
perm_fmgr_info(Oid functionId, FmgrInfo *finfo)
{
	fmgr_info_cxt(functionId, finfo, TopMemoryContext);
}

/*
 * plr_init_interp() - initialize an R interpreter
 */
static void
plr_init_interp(void)
{
	int			argc;
	char	   *argv[] = {"PL/R", "--gui=none", "--silent", "--no-save"};

	/* start EmbeddedR */
	argc = sizeof(argv)/sizeof(argv[0]);
	Rf_initEmbeddedR(argc, argv);

	/* now install the commands for SPI support */

		/* FIXME - nothing currently */

}

static void
plr_init_all(void)
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
	plr_init_interp();

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
	sigjmp_buf		save_restart;
	MemoryContext	oldcontext;

	/* Find or compile the function */
	prodesc = compile_plr_function(fcinfo->flinfo->fn_oid, false);
	fun = prodesc->fun;

	/*
	 * Catch elog(ERROR) during build of the R command
	 */
	memcpy(&save_restart, &Warn_restart, sizeof(save_restart));
	if (sigsetjmp(Warn_restart, 1) != 0)
	{
		memcpy(&Warn_restart, &save_restart, sizeof(Warn_restart));
		plr_restart_in_progress = 1;
		if (--plr_call_level == 0)
			plr_restart_in_progress = 0;
		siglongjmp(Warn_restart, 1);
	}

	/* Convert all call arguments */
	PROTECT(rargs = plr_convertargs(prodesc, fcinfo));

	/* Call the R function */
	PROTECT(fun);
	PROTECT(rvalue = callRFunction(fun, rargs));

	/* switch out of current memory context into the function's context */
	oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	/* convert the return value from an R object to a Datum */
	if(rvalue != R_NilValue)
		retval = r_get_pg(rvalue, prodesc->result_in_func, prodesc->result_in_elem);
	else
		retval = (Datum) NULL;

	/* switch back to old memory context */
	MemoryContextSwitchTo(oldcontext);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "plr: SPI_finish() failed");

	UNPROTECT(3);

	memcpy(&Warn_restart, &save_restart, sizeof(Warn_restart));

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
	if (!is_trigger)
		sprintf(internal_proname, "PLR.%s.%u", proname, fn_oid);
	else
		sprintf(internal_proname, "PLR.%s.%u_trigger", proname, fn_oid);

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

			/* Disallow pseudotype result, except VOID */
			if (typeStruct->typtype == 'p')
			{
				if (procStruct->prorettype == VOIDOID)
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

			if (typeStruct->typrelid != InvalidOid)
			{
				free(prodesc->proname);
				free(prodesc);
				elog(ERROR, "plr: return types of tuples not supported yet");
			}

			perm_fmgr_info(typeStruct->typinput, &(prodesc->result_in_func));
			prodesc->result_in_elem = typeStruct->typelem;

			ReleaseSysCache(typeTup);
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
				{
					prodesc->arg_is_rel[i] = 1;
					if (i > 0)
						appendStringInfo(proc_internal_args, " ");

					appendStringInfo(proc_internal_args, "__PLR_Tup_%d", i + 1);
					ReleaseSysCache(typeTup);
					continue;
				}
				else
					prodesc->arg_is_rel[i] = 0;

				perm_fmgr_info(typeStruct->typoutput, &(prodesc->arg_out_func[i]));
				prodesc->arg_type[i] = procStruct->proargtypes[i];
				prodesc->arg_elem[i] = typeStruct->typelem;

				if (i > 0)
					appendStringInfo(proc_internal_args, " ");
				appendStringInfo(proc_internal_args, "pg%d", i + 1);

				ReleaseSysCache(typeTup);
			}
		}
		else
		{
			/* trigger procedure has fixed args */
			appendStringInfo(proc_internal_args,
				"TG_name TG_relid TG_relatts TG_when TG_level TG_op __PLR_Tup_NEW __PLR_Tup_OLD args");
		}

		/*
		 * Create the R command to define the internal
		 * procedure
		 */
		appendStringInfo(proc_internal_def,
						 "%s <- function(%s) {",
						 internal_proname,
						 proc_internal_args->data);

		/*
		 * prefix procedure body with
		 * ??? stuff to define SPI functions ???
		 * 
		 */
		appendStringInfo(proc_internal_def, "%s", "");
		if (!is_trigger)
		{
			for (i = 0; i < prodesc->nargs; i++)
			{
				if (!prodesc->arg_is_rel[i])
					continue;

				/* stuff to do in R (if anything ???) if argument is a tuple */
				appendStringInfo(proc_internal_def, "%s", "");
			}
		}
		else
		{
			/* stuff to do in R (if anything ???) for NEW and OLD tuples */
			appendStringInfo(proc_internal_def, "%s", "");
			appendStringInfo(proc_internal_def, "%s", "");

			/* stuff to do in R (if anything ???) for trigger args */
			appendStringInfo(proc_internal_def, "%s", "");
		}

		/* Add user's function definition to proc body */
		proc_source = DatumGetCString(DirectFunctionCall1(textout,
								  PointerGetDatum(&procStruct->prosrc)));

		appendStringInfo(proc_internal_def, "%s\n}", proc_source);

		/* parse or find the R function */
		if(proc_source && proc_source[0])
		{
			prodesc->fun = plr_parseFunctionBody(proc_internal_def->data);
elog(NOTICE, "parse: %s", proc_internal_def->data);
		}
		else
		{
			prodesc->fun = Rf_findFun(Rf_install(proname), R_GlobalEnv);
elog(NOTICE, "found: %s", proc_internal_def->data);
		}

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
	SEXP	args;
	SEXP	txt;
	SEXP	fun;
/*Rf_parse*/
	PROTECT(fun = Rf_findFun(Rf_install("parsePostgresFunction"), R_GlobalEnv));
	PROTECT(txt = NEW_CHARACTER(1));

	SET_STRING_ELT(txt, 0, COPY_TO_USER_STRING(body));
	PROTECT(args = NEW_LIST(1));
	SET_VECTOR_ELT(args, 0, txt);
	fun = callRFunction(fun, args);

	UNPROTECT(2);

	return(fun);
}

static SEXP
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
		el = pg_get_r(fcinfo->arg[i],
					  prodesc->arg_out_func[i],
					  prodesc->arg_type[i],
					  prodesc->arg_elem[i]);
		SET_VECTOR_ELT(rargs, i, el);
	}

	UNPROTECT(1);

	return(rargs);
}

/*
 * given a pg value, convert to its R value representation
 */
static SEXP
pg_get_r(Datum dvalue, FmgrInfo arg_out_func, Oid arg_type, Oid arg_elem)
{
	SEXP	obj = NULL;
	char   *value;

elog(NOTICE, "arg_out_func.fn_oid = %u", arg_out_func.fn_oid);
elog(NOTICE, "arg_type = %u", arg_type);
elog(NOTICE, "arg_elem = %u", arg_elem);

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
			get_r_vector(arg_type, &obj, 1);

			/* add our value to it */
			pg_get_one_r(value, arg_type, &obj, 0);
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
		int			typlen;
		bool		typbyval;
		char		typdelim;
		Oid			typoutput,
					typelem;
		FmgrInfo	outputproc;
		char		typalign;
		int			i,
					nitems,
					ndim,
				   *dim;
		char	   *p;

		/* only support one-dim arrays for the moment */
		ndim = ARR_NDIM(v);
		if (ndim > 1)
			elog(ERROR, "plr: multiple dimension arrays are not yet supported");

		element_type = ARR_ELEMTYPE(v);
		system_cache_lookup(element_type, false, &typlen, &typbyval,
							&typdelim, &typelem, &typoutput, &typalign);
		fmgr_info(typoutput, &outputproc);

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
			value = DatumGetCString(FunctionCall3(&outputproc,
													  itemvalue,
												   ObjectIdGetDatum(typelem),
													  Int32GetDatum(-1)));
			p = att_addlength(p, typlen, PointerGetDatum(p));
			p = (char *) att_align(p, typalign);

			if (value != NULL)
				pg_get_one_r(value, arg_type, &obj, i);
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
r_get_pg(SEXP rval, FmgrInfo arg_in_func, Oid arg_in_elem)
{
	SEXP	obj = NULL;
	Datum	dvalue;
	char   *value;

	PROTECT(obj);

	obj =  AS_CHARACTER(rval);
	value = CHAR(STRING_ELT(obj, 0));

	if (value != NULL)
		dvalue = FunctionCall3(&arg_in_func,
								CStringGetDatum(value),
								ObjectIdGetDatum(arg_in_elem),
								Int32GetDatum(-1));
	else
		dvalue = (Datum) 0;

	UNPROTECT(1);

	return dvalue;
}

static  SEXP
pg_get_r_tuple(TupleDesc desc, HeapTuple tuple, Relation relation)
{
	SEXP		rtup,
				attrNames;

    /* This is the version that returns a reference. */
	PROTECT(rtup = NEW_NUMERIC(3));

	NUMERIC_DATA(rtup)[0] = (double) (long) tuple;
	NUMERIC_DATA(rtup)[1] = (double) (long) desc;
	NUMERIC_DATA(rtup)[2] = (double) (long) relation;  
	PROTECT(attrNames = NEW_CHARACTER(1));
	SET_STRING_ELT(attrNames, 0, COPY_TO_USER_STRING("PostgresTupleRef"));
	SET_CLASS(rtup, attrNames);

	UNPROTECT(2);

	return(rtup);
}    

static void
system_cache_lookup(Oid element_type,
					bool input,
					int *typlen,
					bool *typbyval,
					char *typdelim,
					Oid *typelem,
					Oid *proc,
					char *typalign)
{
	HeapTuple	typeTuple;
	Form_pg_type typeStruct;

	typeTuple = SearchSysCache(TYPEOID,
							   ObjectIdGetDatum(element_type),
							   0, 0, 0);
	if (!HeapTupleIsValid(typeTuple))
		elog(ERROR, "cache lookup failed for type %u", element_type);
	typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);

	*typlen = typeStruct->typlen;
	*typbyval = typeStruct->typbyval;
	*typdelim = typeStruct->typdelim;
	*typelem = typeStruct->typelem;
	*typalign = typeStruct->typalign;
	if (input)
		*proc = typeStruct->typinput;
	else
		*proc = typeStruct->typoutput;
	ReleaseSysCache(typeTuple);
}
