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
 * pg_backend_support.c - Postgres backend support functions
 */
#include "plr.h"

#include "optimizer/clauses.h"

/* Example format: "/usr/local/pgsql/lib" */
#ifndef PKGLIBDIR
#error "PKGLIBDIR needs to be defined to compile this file."
#endif

/* GUC variable */
extern char *Dynamic_library_path;

/* compiled function hash table */
extern HTAB *plr_HashTable;

/*
 * static declarations
 */
static char *get_lib_pathstr(Oid funcid);
static char *expand_dynamic_library_name(const char *name);
static char *substitute_libpath_macro(const char *name);
static char *find_in_dynamic_libpath(const char *basename);
static bool file_exists(const char *name);

#ifdef PG_VERSION_73_COMPAT
/*************************************************************************
 * working with postgres 7.3 compatible sources
 *************************************************************************/
ArrayType *
construct_md_array(Datum *elems,
				   int ndims,
				   int *dims,
				   int *lbs,
				   Oid elmtype, int elmlen, bool elmbyval, char elmalign)
{
	int			ndatabytes;
	int			nbytes;
	ArrayType  *array = NULL;
	ArrayType  *tmparray = NULL;
	int			nelems = ArrayGetNItems(ndims, dims);

	/* build up 1-D array */
	tmparray = construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign);

	if (tmparray != NULL)
	{
		/* convert it to a ndims-array */
		ndatabytes = ARR_SIZE(tmparray) - ARR_OVERHEAD(1);
		nbytes = ndatabytes + ARR_OVERHEAD(ndims);
		array = (ArrayType *) palloc(nbytes);

		array->size = nbytes;
		array->ndim = ndims;
		array->flags = 0;
		array->elemtype = elmtype;
		memcpy(ARR_DIMS(array), dims, ndims * sizeof(int));
		memcpy(ARR_LBOUND(array), lbs, ndims * sizeof(int));
		memcpy(ARR_DATA_PTR(array), ARR_DATA_PTR(tmparray), ndatabytes);

		pfree(tmparray);
	}

	return array;
}

/*
 * get_element_type
 *
 *		Given the type OID, get the typelem (InvalidOid if not an array type).
 *
 * NB: this only considers varlena arrays to be true arrays; InvalidOid is
 * returned if the input is a fixed-length array type.
 */
Oid
get_element_type(Oid typid)
{
	HeapTuple	tp;

	tp = SearchSysCache(TYPEOID,
						ObjectIdGetDatum(typid),
						0, 0, 0);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
		Oid			result;

		if (typtup->typlen == -1)
			result = typtup->typelem;
		else
			result = InvalidOid;
		ReleaseSysCache(tp);
		return result;
	}
	else
		return InvalidOid;
}

/*
 * get_array_type
 *
 *		Given the type OID, get the corresponding array type.
 *		Returns InvalidOid if no array type can be found.
 *
 * NB: this only considers varlena arrays to be true arrays.
 */
Oid
get_array_type(Oid typid)
{
	HeapTuple	tp;

	tp = SearchSysCache(TYPEOID,
						ObjectIdGetDatum(typid),
						0, 0, 0);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
		char	   *array_typename;
		Oid			namespaceId;

		array_typename = makeArrayTypeName(NameStr(typtup->typname));
		namespaceId = typtup->typnamespace;
		ReleaseSysCache(tp);

		tp = SearchSysCache(TYPENAMENSP,
							PointerGetDatum(array_typename),
							ObjectIdGetDatum(namespaceId),
							0, 0);

		pfree(array_typename);

		if (HeapTupleIsValid(tp))
		{
			Oid			result;

			typtup = (Form_pg_type) GETSTRUCT(tp);
			if (typtup->typlen == -1 && typtup->typelem == typid)
				result = HeapTupleGetOid(tp);
			else
				result = InvalidOid;
			ReleaseSysCache(tp);
			return result;
		}
	}
	return InvalidOid;
}

/*
 * get_type_io_data
 *
 *		A six-fer:	given the type OID, return typlen, typbyval, typalign,
 *					typdelim, typelem, and IO function OID. The IO function
 *					returned is controlled by IOFuncSelector
 */
void
get_type_io_data(Oid typid,
				 IOFuncSelector which_func,
				 int16 *typlen,
				 bool *typbyval,
				 char *typalign,
				 char *typdelim,
				 Oid *typelem,
				 Oid *func)
{
	HeapTuple	typeTuple;
	Form_pg_type typeStruct;

	typeTuple = SearchSysCache(TYPEOID,
							   ObjectIdGetDatum(typid),
							   0, 0, 0);
	if (!HeapTupleIsValid(typeTuple))
		/* internal error */
		elog(ERROR, "cache lookup failed for type %u", typid);
	typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);

	*typlen = typeStruct->typlen;
	*typbyval = typeStruct->typbyval;
	*typalign = typeStruct->typalign;
	*typdelim = typeStruct->typdelim;
	*typelem = typeStruct->typelem;
	switch (which_func)
	{
		case IOFunc_input:
			*func = typeStruct->typinput;
			break;
		case IOFunc_output:
			*func = typeStruct->typoutput;
			break;
		default:
			/* should never happen */
			elog(ERROR, "get_type_io_data called with invalid IO function selector");
	}
	ReleaseSysCache(typeTuple);
}


/*-------------------------------------------------------------------------
 *		Support routines for extracting info from fn_expr parse tree
 *
 * These are needed by polymorphic functions, which accept multiple possible
 * input types and need help from the parser to know what they've got.
 *-------------------------------------------------------------------------
 */

/*
 * This just returns InvalidOid because in 7.3.x information is not available
 */
Oid
get_fn_expr_rettype(FmgrInfo *flinfo)
{
	return InvalidOid;
}

/*
 * This just returns InvalidOid because in 7.3.x information is not available
 */
Oid
get_fn_expr_argtype(FmgrInfo *flinfo, int argnum)
{
	return InvalidOid;
}


#else
/*************************************************************************
 * working with postgres 7.4 compatible sources
 *************************************************************************/


#endif /* PG_VERSION_73_COMPAT */

/*
 * Compute the hashkey for a given function invocation
 *
 * The hashkey is returned into the caller-provided storage at *hashkey.
 */
void
compute_function_hashkey(FmgrInfo *flinfo,
						 Form_pg_proc procStruct,
						 plr_func_hashkey *hashkey)
{
	int		i;

	/* Make sure any unused bytes of the struct are zero */
	MemSet(hashkey, 0, sizeof(plr_func_hashkey));

	hashkey->funcOid = flinfo->fn_oid;

	/* get the argument types */
	for (i = 0; i < procStruct->pronargs; i++)
	{
		Oid			argtypeid = procStruct->proargtypes[i];

		/*
		 * Check for polymorphic arguments. If found, use the actual
		 * parameter type from the caller's FuncExpr node, if we
		 * have one.
		 *
		 * We can support arguments of type ANY the same way as normal
		 * polymorphic arguments.
		 */
		if (argtypeid == ANYARRAYOID || argtypeid == ANYELEMENTOID ||
			argtypeid == ANYOID)
		{
			argtypeid = get_fn_expr_argtype(flinfo, i);
			if (!OidIsValid(argtypeid))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("could not determine actual argument "
								"type for polymorphic function \"%s\"",
								NameStr(procStruct->proname))));
		}

		hashkey->argtypes[i] = argtypeid;
	}
}

void
plr_HashTableInit(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(plr_func_hashkey);
	ctl.entrysize = sizeof(plr_HashEnt);
	ctl.hash = tag_hash;
	plr_HashTable = hash_create("PLR function cache",
								FUNCS_PER_USER,
								&ctl,
								HASH_ELEM | HASH_FUNCTION);
}

plr_function *
plr_HashTableLookup(plr_func_hashkey *func_key)
{
	plr_HashEnt	   *hentry;

	hentry = (plr_HashEnt*) hash_search(plr_HashTable,
										(void *) func_key,
										HASH_FIND,
										NULL);
	if (hentry)
		return hentry->function;
	else
		return (plr_function *) NULL;
}

void
plr_HashTableInsert(plr_function *function,
					plr_func_hashkey *func_key)
{
	plr_HashEnt	   *hentry;
	bool			found;

	hentry = (plr_HashEnt*) hash_search(plr_HashTable,
										(void *) func_key,
										HASH_ENTER,
										&found);
	if (hentry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	if (found)
		elog(WARNING, "trying to insert a function that exists");

	hentry->function = function;
	/* prepare back link from function to hashtable key */
	function->fn_hashkey = &hentry->key;
}

void
plr_HashTableDelete(plr_function *function)
{
	plr_HashEnt	   *hentry;

	hentry = (plr_HashEnt*) hash_search(plr_HashTable,
										(void *) function->fn_hashkey,
										HASH_REMOVE,
										NULL);
	if (hentry == NULL)
		elog(WARNING, "trying to delete function that does not exist");
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
		/* internal error */
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	/* now get the pg_language entry */
	language = procedureStruct->prolang;
	ReleaseSysCache(procedureTuple);

	languageTuple = SearchSysCache(LANGOID,
								   ObjectIdGetDatum(language),
								   0, 0, 0);
	if (!HeapTupleIsValid(languageTuple))
		/* internal error */
		elog(ERROR, "cache lookup failed for language %u", language);
	languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);
	lang_funcid = languageStruct->lanplcallfoid;
	ReleaseSysCache(languageTuple);

	/* finally, get the pg_proc entry for the language handler */
	procedureTuple = SearchSysCache(PROCOID,
									ObjectIdGetDatum(lang_funcid),
									0, 0, 0);
	if (!HeapTupleIsValid(procedureTuple))
		/* internal error */
		elog(ERROR, "cache lookup failed for function %u", lang_funcid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	tmp = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_probin, &isnull);
	raw_path = DatumGetCString(DirectFunctionCall1(byteaout, tmp));
	cooked_path = expand_dynamic_library_name(raw_path);

	ReleaseSysCache(procedureTuple);

	return cooked_path;
}

char *
get_load_self_ref_cmd(Oid funcid)
{
	char   *libstr = get_lib_pathstr(funcid);
	char   *buf = (char *) palloc(strlen(libstr) + 12 + 1);

	sprintf(buf, "dyn.load(\"%s\")", libstr);
	return buf;
}

void
perm_fmgr_info(Oid functionId, FmgrInfo *finfo)
{
	fmgr_info_cxt(functionId, finfo, TopMemoryContext);
	INIT_AUX_FMGR_ATTS;
}

static bool
file_exists(const char *name)
{
	struct stat st;

	AssertArg(name != NULL);

	if (stat(name, &st) == 0)
		return S_ISDIR(st.st_mode) ? false : true;
	else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES))
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not access file \"%s\": %m", name)));

	return false;
}

/*
 * If name contains a slash, check if the file exists, if so return
 * the name.  Else (no slash) try to expand using search path (see
 * find_in_dynamic_libpath below); if that works, return the fully
 * expanded file name.	If the previous failed, append DLSUFFIX and
 * try again.  If all fails, return NULL.
 *
 * A non-NULL result will always be freshly palloc'd.
 */
static char *
expand_dynamic_library_name(const char *name)
{
	bool		have_slash;
	char	   *new;
	char	   *full;

	AssertArg(name);

	have_slash = (strchr(name, '/') != NULL);

	if (!have_slash)
	{
		full = find_in_dynamic_libpath(name);
		if (full)
			return full;
	}
	else
	{
		full = substitute_libpath_macro(name);
		if (file_exists(full))
			return full;
		pfree(full);
	}

	new = palloc(strlen(name) + strlen(DLSUFFIX) + 1);
	strcpy(new, name);
	strcat(new, DLSUFFIX);

	if (!have_slash)
	{
		full = find_in_dynamic_libpath(new);
		pfree(new);
		if (full)
			return full;
	}
	else
	{
		full = substitute_libpath_macro(new);
		pfree(new);
		if (file_exists(full))
			return full;
		pfree(full);
	}

	return NULL;
}


/*
 * Substitute for any macros appearing in the given string.
 * Result is always freshly palloc'd.
 */
static char *
substitute_libpath_macro(const char *name)
{
	size_t		macroname_len;
	char	   *replacement = NULL;

	AssertArg(name != NULL);

	if (name[0] != '$')
		return pstrdup(name);

	macroname_len = strcspn(name + 1, "/") + 1;

	if (strncmp(name, "$libdir", macroname_len) == 0)
		replacement = PKGLIBDIR;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("invalid macro name in dynamic library path")));

	if (name[macroname_len] == '\0')
		return pstrdup(replacement);
	else
	{
		char	   *new;

		new = palloc(strlen(replacement) + (strlen(name) - macroname_len) + 1);

		strcpy(new, replacement);
		strcat(new, name + macroname_len);

		return new;
	}
}


/*
 * Search for a file called 'basename' in the colon-separated search
 * path Dynamic_library_path.  If the file is found, the full file name
 * is returned in freshly palloc'd memory.  If the file is not found,
 * return NULL.
 */
static char *
find_in_dynamic_libpath(const char *basename)
{
	const char *p;
	size_t		baselen;

	AssertArg(basename != NULL);
	AssertArg(strchr(basename, '/') == NULL);
	AssertState(Dynamic_library_path != NULL);

	p = Dynamic_library_path;
	if (strlen(p) == 0)
		return NULL;

	baselen = strlen(basename);

	for (;;)
	{
		size_t		len;
		char	   *piece;
		char	   *mangled;
		char	   *full;

		len = strcspn(p, ":");

		if (len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("zero-length component in DYNAMIC_LIBRARY_PATH")));

		piece = palloc(len + 1);
		strncpy(piece, p, len);
		piece[len] = '\0';

		mangled = substitute_libpath_macro(piece);
		pfree(piece);

		/* only absolute paths */
		if (mangled[0] != '/')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("DYNAMIC_LIBRARY_PATH component is not absolute")));

		full = palloc(strlen(mangled) + 1 + baselen + 1);
		sprintf(full, "%s/%s", mangled, basename);
		pfree(mangled);

		elog(DEBUG2, "find_in_dynamic_libpath: trying %s", full);

		if (file_exists(full))
			return full;

		pfree(full);

		if (p[len] == '\0')
			break;
		else
			p += len + 1;
	}

	return NULL;
}
