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

/*
 * static declarations
 */
static char *get_lib_pathstr(Oid funcid);
static char *expand_dynamic_library_name(const char *name);
static char *substitute_libpath_macro(const char *name);
static char *find_in_dynamic_libpath(const char *basename);
static bool file_exists(const char *name);

#ifdef PG_VERSION_73_COMPAT
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

Oid
get_fn_expr_rettype(FunctionCallInfo fcinfo)
{
	/* can't return anything useful if we have no FmgrInfo */
	if (fcinfo->flinfo == NULL)
		return InvalidOid;
	
	return get_func_rettype(fcinfo->flinfo->fn_oid);
}

#endif /* PG_VERSION_73_COMPAT */

/*
 * isArrayTypeName(typeName)
 *	  - given a type name determine if it is an array type name
 * 
 * this must be kept consistent with makeArrayTypeName() above
 */
bool
isArrayTypeName(const char *typeName)
{
	return (typeName[0] == '_');
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
	finfo->fn_mcxt = thisQueryContext;
	finfo->fn_expr = (Node *) NULL;
}

void
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

Oid
get_typelem(Oid element_type)
{
	Oid				typelem;
	HeapTuple		typeTuple;
	Form_pg_type	typeStruct;

	typeTuple = SearchSysCache(TYPEOID,
							   ObjectIdGetDatum(element_type),
							   0, 0, 0);
	if (!HeapTupleIsValid(typeTuple))
		elog(ERROR, "cache lookup failed for type %u", element_type);
	typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);

	typelem = typeStruct->typelem;

	ReleaseSysCache(typeTuple);

	return typelem;
}

static bool
file_exists(const char *name)
{
	struct stat st;

	AssertArg(name != NULL);

	if (stat(name, &st) == 0)
		return S_ISDIR(st.st_mode) ? false : true;
	else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES))
		elog(ERROR, "stat failed on %s: %s", name, strerror(errno));

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
		elog(ERROR, "invalid macro name in dynamic library path");

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
			elog(ERROR, "zero length dynamic_library_path component");

		piece = palloc(len + 1);
		strncpy(piece, p, len);
		piece[len] = '\0';

		mangled = substitute_libpath_macro(piece);
		pfree(piece);

		/* only absolute paths */
		if (mangled[0] != '/')
			elog(ERROR, "dynamic_library_path component is not absolute");

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
