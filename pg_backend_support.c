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
#include "postgres.h"

#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/time.h>
#include <ctype.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif

#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/guc.h"

bool	IsPostmasterEnvironment = false;
int		log_min_messages = NOTICE;
bool	ExitOnAnyError = false;

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


/*-------------------------------------------------------------------------
 *		Support routines for 7.4 style error reporting
 *
 *-------------------------------------------------------------------------
 */

/* Global variables */
ErrorContextCallback *error_context_stack = NULL;

/* GUC parameters */
PGErrorVerbosity Log_error_verbosity = PGERROR_VERBOSE;
bool		Log_timestamp = false;	/* show timestamps in stderr output */
bool		Log_pid = false;		/* show PIDs in stderr output */

#ifdef HAVE_SYSLOG
/*
 * 0 = only stdout/stderr
 * 1 = stdout+stderr and syslog
 * 2 = syslog only
 * ... in theory anyway
 */
int			Use_syslog = 0;
char	   *Syslog_facility;	/* openlog() parameters */
char	   *Syslog_ident;

static void write_syslog(int level, const char *line);

#else

#define Use_syslog 0

#endif /* HAVE_SYSLOG */


/*
 * ErrorData holds the data accumulated during any one ereport() cycle.
 * Any non-NULL pointers must point to palloc'd data in ErrorContext.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */

typedef struct ErrorData
{
	int			elevel;			/* error level */
	bool		output_to_server; /* will report to server log? */
	bool		output_to_client; /* will report to client? */
	bool		show_funcname;	/* true to force funcname inclusion */
	const char *filename;		/* __FILE__ of ereport() call */
	int			lineno;			/* __LINE__ of ereport() call */
	const char *funcname;		/* __func__ of ereport() call */
	int			sqlerrcode;		/* encoded ERRSTATE */
	char	   *message;		/* primary error message */
	char	   *detail;			/* detail error message */
	char	   *hint;			/* hint message */
	char	   *context;		/* context message */
	int			cursorpos;		/* cursor index into query string */
	int			saved_errno;	/* errno at entry */
} ErrorData;

/* We provide a small stack of ErrorData records for re-entrant cases */
#define ERRORDATA_STACK_SIZE  5

static ErrorData errordata[ERRORDATA_STACK_SIZE];

static int	errordata_stack_depth = -1; /* index of topmost active frame */

static int	recursion_depth = 0;		/* to detect actual recursion */


/* Macro for checking errordata_stack_depth is reasonable */
#define CHECK_STACK_DEPTH() \
	do { \
		if (errordata_stack_depth < 0) \
		{ \
			errordata_stack_depth = -1; \
			ereport(ERROR, (errmsg_internal("errstart was not called"))); \
		} \
	} while (0)

static void appendStringInfoString(StringInfo str, const char *s);
static bool appendStringInfoVA(StringInfo str, const char *fmt, va_list args);
static void enlargeStringInfo(StringInfo str, int needed);
static void send_message_to_server_log(ErrorData *edata);
static void send_message_to_frontend(ErrorData *edata);
static char *expand_fmt_string(const char *fmt, ErrorData *edata);
static const char *useful_strerror(int errnum);
static const char *error_severity(int elevel);
static const char *print_timestamp(void);
static const char *print_pid(void);

/*
 * appendStringInfoString
 *
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
static void
appendStringInfoString(StringInfo str, const char *s)
{
	appendBinaryStringInfo(str, s, strlen(s));
}

/*
 * appendStringInfoVA
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return true; if not (because there's not enough space), return false
 * without modifying str.  Typically the caller would enlarge str and retry
 * on false return --- see appendStringInfo for standard usage pattern.
 *
 * XXX This API is ugly, but there seems no alternative given the C spec's
 * restrictions on what can portably be done with va_list arguments: you have
 * to redo va_start before you can rescan the argument list, and we can't do
 * that from here.
 */
static bool
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
{
	int			avail,
				nprinted;

	Assert(str != NULL);

	/*
	 * If there's hardly any space, don't bother trying, just fail to make
	 * the caller enlarge the buffer first.
	 */
	avail = str->maxlen - str->len - 1;
	if (avail < 16)
		return false;

	/*
	 * Assert check here is to catch buggy vsnprintf that overruns
	 * the specified buffer length.  Solaris 7 in 64-bit mode is
	 * an example of a platform with such a bug.
	 */
#ifdef USE_ASSERT_CHECKING
	str->data[str->maxlen - 1] = '\0';
#endif

	nprinted = vsnprintf(str->data + str->len, avail, fmt, args);

	Assert(str->data[str->maxlen - 1] == '\0');

	/*
	 * Note: some versions of vsnprintf return the number of chars
	 * actually stored, but at least one returns -1 on failure. Be
	 * conservative about believing whether the print worked.
	 */
	if (nprinted >= 0 && nprinted < avail - 1)
	{
		/* Success.  Note nprinted does not include trailing null. */
		str->len += nprinted;
		return true;
	}

	/* Restore the trailing null so that str is unmodified. */
	str->data[str->len] = '\0';
	return false;
}

/*
 * enlargeStringInfo
 *
 * Make sure there is enough space for 'needed' more bytes
 * ('needed' does not include the terminating null).
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
static void
enlargeStringInfo(StringInfo str, int needed)
{
	int			newlen;

	needed += str->len + 1;		/* total space required now */
	if (needed <= str->maxlen)
		return;					/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each
	 * append; for efficiency, double the buffer size each time it
	 * overflows. Actually, we might need to more than double it if
	 * 'needed' is big...
	 */
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	str->data = (char *) repalloc(str->data, newlen);

	str->maxlen = newlen;
}

/*
 * errstart --- begin an error-reporting cycle
 *
 * Create a stack entry and store the given parameters in it.  Subsequently,
 * errmsg() and perhaps other routines will be called to further populate
 * the stack entry.  Finally, errfinish() will be called to actually process
 * the error report.
 *
 * Returns TRUE in normal case.  Returns FALSE to short-circuit the error
 * report (if it's a warning or lower and not to be reported anywhere).
 */
bool
errstart(int elevel, const char *filename, int lineno,
		 const char *funcname)
{
	ErrorData  *edata;
	bool		output_to_server = false;
	bool		output_to_client = false;

	/*
	 * First decide whether we need to process this report at all;
	 * if it's warning or less and not enabled for logging, just
	 * return FALSE without starting up any error logging machinery.
	 */

	/*
	 * Convert initialization errors into fatal errors. This is probably
	 * redundant, because Warn_restart_ready won't be set anyway.
	 */
	if (elevel == ERROR && IsInitProcessingMode())
		elevel = FATAL;

	/*
	 * If we are inside a critical section, all errors become PANIC
	 * errors.	See miscadmin.h.
	 */
	if (elevel >= ERROR)
	{
		if (CritSectionCount > 0)
			elevel = PANIC;
	}

	/* Determine whether message is enabled for server log output */
	if (IsPostmasterEnvironment)
	{
		/* Complicated because LOG is sorted out-of-order for this purpose */
		if (elevel == LOG || elevel == COMMERROR)
		{
			if (log_min_messages == LOG)
				output_to_server = true;
			else if (log_min_messages < FATAL)
				output_to_server = true;
		}
		else
		{
			/* elevel != LOG */
			if (log_min_messages == LOG)
			{
				if (elevel >= FATAL)
					output_to_server = true;
			}
			/* Neither is LOG */
			else if (elevel >= log_min_messages)
				output_to_server = true;
		}
	}
	else
	{
		/* In bootstrap/standalone case, do not sort LOG out-of-order */
		output_to_server = (elevel >= log_min_messages);
	}

	/* Determine whether message is enabled for client output */
	if (whereToSendOutput == Remote && elevel != COMMERROR)
	{
		/*
		 * client_min_messages is honored only after we complete the
		 * authentication handshake.  This is required both for security
		 * reasons and because many clients can't handle NOTICE messages
		 * during authentication.
		 */
		if (ClientAuthInProgress)
			output_to_client = (elevel >= ERROR);
		else
			output_to_client = (elevel >= client_min_messages ||
								elevel == INFO);
	}

	/* Skip processing effort if non-error message will not be output */
	if (elevel < ERROR && !output_to_server && !output_to_client)
		return false;

	/*
	 * Okay, crank up a stack entry to store the info in.
	 */

	if (recursion_depth++ > 0)
	{
		/*
		 * Ooops, error during error processing.  Clear ErrorContext and force
		 * level up to ERROR or greater, as discussed at top of file.  Adjust
		 * output decisions too.
		 */
		MemoryContextReset(ErrorContext);
		output_to_server = true;
		if (whereToSendOutput == Remote && elevel != COMMERROR)
			output_to_client = true;
		elevel = Max(elevel, ERROR);
		/*
		 * If we recurse more than once, the problem might be something
		 * broken in a context traceback routine.  Abandon them too.
		 */
		if (recursion_depth > 2)
			error_context_stack = NULL;
	}
	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/* Wups, stack not big enough */
		int		i;

		elevel = Max(elevel, ERROR);
		/*
		 * Don't forget any FATAL/PANIC status on the stack (see comments
		 * in errfinish)
		 */
		for (i = 0; i < errordata_stack_depth; i++)
			elevel = Max(elevel, errordata[i].elevel);
		/* Clear the stack and try again */
		errordata_stack_depth = -1;
		ereport(elevel, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	/* Initialize data for this error frame */
	edata = &errordata[errordata_stack_depth];
	MemSet(edata, 0, sizeof(ErrorData));
	edata->elevel = elevel;
	edata->output_to_server = output_to_server;
	edata->output_to_client = output_to_client;
	edata->filename = filename;
	edata->lineno = lineno;
	edata->funcname = funcname;
	/* Select default errcode based on elevel */
	if (elevel >= ERROR)
		edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
	else if (elevel == WARNING)
		edata->sqlerrcode = ERRCODE_WARNING;
	else
		edata->sqlerrcode = ERRCODE_SUCCESSFUL_COMPLETION;
	/* errno is saved here so that error parameter eval can't change it */
	edata->saved_errno = errno;

	recursion_depth--;
	return true;
}

/*
 * errfinish --- end an error-reporting cycle
 *
 * Produce the appropriate error report(s) and pop the error stack.
 *
 * If elevel is ERROR or worse, control does not return to the caller.
 * See elog.h for the error level definitions.
 */
void
errfinish(int dummy, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	int			elevel = edata->elevel;
	MemoryContext oldcontext;
	ErrorContextCallback *econtext;

	CHECK_STACK_DEPTH();

	/*
	 * Call any context callback functions.  We can treat ereports occuring
	 * in callback functions as re-entrant rather than recursive case, so
	 * don't increment recursion_depth yet.
	 */
	for (econtext = error_context_stack;
		 econtext != NULL;
		 econtext = econtext->previous)
	{
		(*econtext->callback) (econtext->arg);
	}

	/* Now we are ready to process the error. */
	recursion_depth++;

	/*
	 * Do processing in ErrorContext, which we hope has enough reserved space
	 * to report an error.
	 */
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	/* Send to server log, if enabled */
	if (edata->output_to_server)
		send_message_to_server_log(edata);

	/*
	 * Abort any old-style COPY OUT in progress when an error is detected.
	 * This hack is necessary because of poor design of old-style copy
	 * protocol.  Note we must do this even if client is fool enough to
	 * have set client_min_messages above ERROR, so don't look at
	 * output_to_client.
	 */
	if (elevel >= ERROR && whereToSendOutput == Remote)
		pq_endcopyout(true);

	/* Send to client, if enabled */
	if (edata->output_to_client)
		send_message_to_frontend(edata);

	/* Now free up subsidiary data attached to stack entry, and release it */
	if (edata->message)
		pfree(edata->message);
	if (edata->detail)
		pfree(edata->detail);
	if (edata->hint)
		pfree(edata->hint);
	if (edata->context)
		pfree(edata->context);

	MemoryContextSwitchTo(oldcontext);

	errordata_stack_depth--;
	recursion_depth--;

	/*
	 * If the error level is ERROR or more, we are not going to return to
	 * caller; therefore, if there is any stacked error already in progress
	 * it will be lost.  This is more or less okay, except we do not want
	 * to have a FATAL or PANIC error downgraded because the reporting process
	 * was interrupted by a lower-grade error.  So check the stack and make
	 * sure we panic if panic is warranted.
	 */
	if (elevel >= ERROR)
	{
		int		i;

		for (i = 0; i <= errordata_stack_depth; i++)
			elevel = Max(elevel, errordata[i].elevel);

		/*
		 * Also, be sure to reset the stack to empty.  We do not clear
		 * ErrorContext here, though; PostgresMain does that later on.
		 */
		errordata_stack_depth = -1;
		recursion_depth = 0;
		error_context_stack = NULL;
	}

	/*
	 * Perform error recovery action as specified by elevel.
	 */
	if (elevel == ERROR || elevel == FATAL)
	{
		/* Prevent immediate interrupt while entering error recovery */
		ImmediateInterruptOK = false;

		/*
		 * If we just reported a startup failure, the client will
		 * disconnect on receiving it, so don't send any more to the
		 * client.
		 */
		if (!Warn_restart_ready && whereToSendOutput == Remote)
			whereToSendOutput = None;

		/*
		 * For a FATAL error, we let proc_exit clean up and exit.
		 *
		 * There are several other cases in which we treat ERROR as FATAL
		 * and go directly to proc_exit:
		 *
		 * 1. ExitOnAnyError mode switch is set (initdb uses this).
		 * 
		 * 2. we have not yet entered the main backend loop (ie, we are in
		 * the postmaster or in backend startup); we have noplace to recover.
		 *
		 * 3. the error occurred after proc_exit has begun to run.  (It's
		 * proc_exit's responsibility to see that this doesn't turn into
		 * infinite recursion!)
		 *
		 * In the last case, we exit with nonzero exit code to indicate that
		 * something's pretty wrong.  We also want to exit with nonzero exit
		 * code if not running under the postmaster (for example, if we are
		 * being run from the initdb script, we'd better return an error
		 * status).
		 */
		if (elevel == FATAL ||
			ExitOnAnyError ||
			!Warn_restart_ready ||
			proc_exit_inprogress)
		{
			/*
			 * fflush here is just to improve the odds that we get to see
			 * the error message, in case things are so hosed that
			 * proc_exit crashes.  Any other code you might be tempted to
			 * add here should probably be in an on_proc_exit callback
			 * instead.
			 */
			fflush(stdout);
			fflush(stderr);
			proc_exit(proc_exit_inprogress || !IsUnderPostmaster);
		}

		/*
		 * Guard against infinite loop from errors during error recovery.
		 */
		if (InError)
			ereport(PANIC, (errmsg("error during error recovery, giving up")));
		InError = true;

		/*
		 * Otherwise we can return to the main loop in postgres.c.
		 */
		siglongjmp(Warn_restart, 1);
	}

	if (elevel >= PANIC)
	{
		/*
		 * Serious crash time. Postmaster will observe nonzero process
		 * exit status and kill the other backends too.
		 *
		 * XXX: what if we are *in* the postmaster?  abort() won't kill
		 * our children...
		 */
		ImmediateInterruptOK = false;
		fflush(stdout);
		fflush(stderr);
		abort();
	}

	/* We reach here if elevel <= WARNING. OK to return to caller. */
}


/*
 * errcode --- add SQLSTATE error code to the current error
 *
 * The code is expected to be represented as per MAKE_SQLSTATE().
 */
int
errcode(int sqlerrcode)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->sqlerrcode = sqlerrcode;

	return 0;					/* return value does not matter */
}


/*
 * errcode_for_file_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of disk file access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_file_access(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (edata->saved_errno)
	{
		/* Permission-denied failures */
		case EPERM:				/* Not super-user */
		case EACCES:			/* Permission denied */
#ifdef EROFS
		case EROFS:				/* Read only file system */
#endif
			edata->sqlerrcode = ERRCODE_INSUFFICIENT_PRIVILEGE;
			break;

		/* Object not found */
		case ENOENT:			/* No such file or directory */
			edata->sqlerrcode = ERRCODE_UNDEFINED_OBJECT;
			break;

		/* Duplicate object */
		case EEXIST:			/* File exists */
			edata->sqlerrcode = ERRCODE_DUPLICATE_OBJECT;
			break;

		/* Wrong object type or state */
		case ENOTDIR:			/* Not a directory */
		case EISDIR:			/* Is a directory */
		case ENOTEMPTY:			/* Directory not empty */
			edata->sqlerrcode = ERRCODE_WRONG_OBJECT_TYPE;
			break;

		/* Insufficient resources */
		case ENOSPC:			/* No space left on device */
			edata->sqlerrcode = ERRCODE_DISK_FULL;
			break;

		case ENFILE:			/* File table overflow */
		case EMFILE:			/* Too many open files */
			edata->sqlerrcode = ERRCODE_INSUFFICIENT_RESOURCES;
			break;

		/* Hardware failure */
		case EIO:				/* I/O error */
			edata->sqlerrcode = ERRCODE_IO_ERROR;
			break;

		/* All else is classified as internal errors */
		default:
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			break;
	}

	return 0;					/* return value does not matter */
}

/*
 * errcode_for_socket_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of socket access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_socket_access(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (edata->saved_errno)
	{
		/* Loss of connection */
		case EPIPE:
#ifdef ECONNRESET
		case ECONNRESET:
#endif
			edata->sqlerrcode = ERRCODE_CONNECTION_FAILURE;
			break;

		/* All else is classified as internal errors */
		default:
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			break;
	}

	return 0;					/* return value does not matter */
}


/*
 * This macro handles expansion of a format string and associated parameters;
 * it's common code for errmsg(), errdetail(), etc.  Must be called inside
 * a routine that is declared like "const char *fmt, ..." and has an edata
 * pointer set up.  The message is assigned to edata->targetfield, or
 * appended to it if appendval is true.
 *
 * Note: we pstrdup the buffer rather than just transferring its storage
 * to the edata field because the buffer might be considerably larger than
 * really necessary.
 */
#define EVALUATE_MESSAGE(targetfield, appendval)  \
	{ \
		char		   *fmtbuf; \
		StringInfoData	buf; \
		/* Internationalize the error format string */ \
		fmt = gettext(fmt); \
		/* Expand %m in format string */ \
		fmtbuf = expand_fmt_string(fmt, edata); \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) \
			appendStringInfo(&buf, "%s\n", edata->targetfield); \
		/* Generate actual output --- have to use appendStringInfoVA */ \
		for (;;) \
		{ \
			va_list		args; \
			bool		success; \
			va_start(args, fmt); \
			success = appendStringInfoVA(&buf, fmtbuf, args); \
			va_end(args); \
			if (success) \
				break; \
			enlargeStringInfo(&buf, buf.maxlen); \
		} \
		/* Done with expanded fmt */ \
		pfree(fmtbuf); \
		/* Save the completed message into the stack item */ \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}


/*
 * errmsg --- add a primary error message text to the current error
 *
 * In addition to the usual %-escapes recognized by printf, "%m" in
 * fmt is replaced by the error message for the caller's value of errno.
 *
 * Note: no newline is needed at the end of the fmt string, since
 * ereport will provide one for the output methods that need it.
 */
int
errmsg(const char *fmt, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(message, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errmsg_internal --- add a primary error message text to the current error
 *
 * This is exactly like errmsg() except that strings passed to errmsg_internal
 * are customarily left out of the internationalization message dictionary.
 * This should be used for "can't happen" cases that are probably not worth
 * spending translation effort on.
 */
int
errmsg_internal(const char *fmt, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(message, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail --- add a detail error message text to the current error
 */
int
errdetail(const char *fmt, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(detail, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errhint --- add a hint error message text to the current error
 */
int
errhint(const char *fmt, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(hint, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errcontext --- add a context error message text to the current error
 *
 * Unlike other cases, multiple calls are allowed to build up a stack of
 * context information.  We assume earlier calls represent more-closely-nested
 * states.
 */
int
errcontext(const char *fmt, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(context, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errfunction --- add reporting function name to the current error
 *
 * This is used when backwards compatibility demands that the function
 * name appear in messages sent to old-protocol clients.  Note that the
 * passed string is expected to be a non-freeable constant string.
 */
int
errfunction(const char *funcname)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->funcname = funcname;
	edata->show_funcname = true;

	return 0;					/* return value does not matter */
}

/*
 * errposition --- add cursor position to the current error
 */
int
errposition(int cursorpos)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->cursorpos = cursorpos;

	return 0;					/* return value does not matter */
}


/*
 * elog_finish --- finish up for old-style API
 *
 * The elog() macro already called errstart, but with ERROR rather than
 * the true elevel.
 */
void
elog_finish(int elevel, const char *fmt, ...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	CHECK_STACK_DEPTH();

	/*
	 * We need to redo errstart() because the elog macro had to call it
	 * with bogus elevel.
	 */
	errordata_stack_depth--;
	errno = edata->saved_errno;
	if (!errstart(elevel, edata->filename, edata->lineno, edata->funcname))
		return;					/* nothing to do */

	/*
	 * Format error message just like errmsg().
	 */
	recursion_depth++;
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	EVALUATE_MESSAGE(message, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/*
	 * And let errfinish() finish up.
	 */
	errfinish(0);
}

#ifdef HAVE_SYSLOG

#ifndef PG_SYSLOG_LIMIT
#define PG_SYSLOG_LIMIT 128
#endif

/*
 * Write a message line to syslog if the syslog option is set.
 *
 * Our problem here is that many syslog implementations don't handle
 * long messages in an acceptable manner. While this function doesn't
 * help that fact, it does work around by splitting up messages into
 * smaller pieces.
 */
static void
write_syslog(int level, const char *line)
{
	static bool openlog_done = false;
	static unsigned long seq = 0;
	static int	syslog_fac = LOG_LOCAL0;

	int			len = strlen(line);

	if (Use_syslog == 0)
		return;

	if (!openlog_done)
	{
		if (strcasecmp(Syslog_facility, "LOCAL0") == 0)
			syslog_fac = LOG_LOCAL0;
		if (strcasecmp(Syslog_facility, "LOCAL1") == 0)
			syslog_fac = LOG_LOCAL1;
		if (strcasecmp(Syslog_facility, "LOCAL2") == 0)
			syslog_fac = LOG_LOCAL2;
		if (strcasecmp(Syslog_facility, "LOCAL3") == 0)
			syslog_fac = LOG_LOCAL3;
		if (strcasecmp(Syslog_facility, "LOCAL4") == 0)
			syslog_fac = LOG_LOCAL4;
		if (strcasecmp(Syslog_facility, "LOCAL5") == 0)
			syslog_fac = LOG_LOCAL5;
		if (strcasecmp(Syslog_facility, "LOCAL6") == 0)
			syslog_fac = LOG_LOCAL6;
		if (strcasecmp(Syslog_facility, "LOCAL7") == 0)
			syslog_fac = LOG_LOCAL7;
		openlog(Syslog_ident, LOG_PID | LOG_NDELAY, syslog_fac);
		openlog_done = true;
	}

	/*
	 * We add a sequence number to each log message to suppress "same"
	 * messages.
	 */
	seq++;

	/* divide into multiple syslog() calls if message is too long */
	/* or if the message contains embedded NewLine(s) '\n' */
	if (len > PG_SYSLOG_LIMIT || strchr(line, '\n') != NULL)
	{
		int			chunk_nr = 0;

		while (len > 0)
		{
			char		buf[PG_SYSLOG_LIMIT + 1];
			int			buflen;
			int			i;

			/* if we start at a newline, move ahead one char */
			if (line[0] == '\n')
			{
				line++;
				len--;
				continue;
			}

			strncpy(buf, line, PG_SYSLOG_LIMIT);
			buf[PG_SYSLOG_LIMIT] = '\0';
			if (strchr(buf, '\n') != NULL)
				*strchr(buf, '\n') = '\0';

			buflen = strlen(buf);

			/* trim to multibyte letter boundary */
			buflen = pg_mbcliplen(buf, buflen, buflen);
			if (buflen <= 0)
				return;
			buf[buflen] = '\0';

			/* already word boundary? */
			if (!isspace((unsigned char) line[buflen]) &&
				line[buflen] != '\0')
			{
				/* try to divide at word boundary */
				i = buflen - 1;
				while (i > 0 && !isspace((unsigned char) buf[i]))
					i--;

				if (i > 0)		/* else couldn't divide word boundary */
				{
					buflen = i;
					buf[i] = '\0';
				}
			}

			chunk_nr++;

			syslog(level, "[%lu-%d] %s", seq, chunk_nr, buf);
			line += buflen;
			len -= buflen;
		}
	}
	else
	{
		/* message short enough */
		syslog(level, "[%lu] %s", seq, line);
	}
}

#endif   /* HAVE_SYSLOG */


/*
 * Write error report to server's log
 */
static void
send_message_to_server_log(ErrorData *edata)
{
	StringInfoData	buf;

	initStringInfo(&buf);

	appendStringInfo(&buf, "%s:  ", error_severity(edata->elevel));

	if (Log_error_verbosity >= PGERROR_VERBOSE)
	{
		/* unpack MAKE_SQLSTATE code */
		char	tbuf[12];
		int		ssval;
		int		i;

		ssval = edata->sqlerrcode;
		for (i = 0; i < 5; i++)
		{
			tbuf[i] = PGUNSIXBIT(ssval);
			ssval >>= 6;
		}
		tbuf[i] = '\0';
		appendStringInfo(&buf, "%s: ", tbuf);
	}

	if (edata->message)
		appendStringInfoString(&buf, edata->message);
	else
		appendStringInfoString(&buf, gettext("missing error text"));

	if (edata->cursorpos > 0)
		appendStringInfo(&buf, gettext(" at character %d"), edata->cursorpos);

	appendStringInfoChar(&buf, '\n');

	if (Log_error_verbosity >= PGERROR_DEFAULT)
	{
		if (edata->detail)
			appendStringInfo(&buf, gettext("DETAIL:  %s\n"), edata->detail);
		if (edata->hint)
			appendStringInfo(&buf, gettext("HINT:  %s\n"), edata->hint);
		if (edata->context)
			appendStringInfo(&buf, gettext("CONTEXT:  %s\n"), edata->context);
		if (Log_error_verbosity >= PGERROR_VERBOSE)
		{
			if (edata->funcname && edata->filename)
				appendStringInfo(&buf, gettext("LOCATION:  %s, %s:%d\n"),
								 edata->funcname, edata->filename,
								 edata->lineno);
			else if (edata->filename)
				appendStringInfo(&buf, gettext("LOCATION:  %s:%d\n"),
								 edata->filename, edata->lineno);
		}
	}

	/*
	 * If the user wants the query that generated this error logged, do so.
	 * We use debug_query_string to get at the query, which is kinda useless
	 * for queries triggered by extended query protocol; how to improve?
	 */
	if (edata->elevel >= log_min_error_statement && debug_query_string != NULL)
		appendStringInfo(&buf, gettext("STATEMENT:  %s\n"),
						 debug_query_string);


#ifdef HAVE_SYSLOG
	/* Write to syslog, if enabled */
	if (Use_syslog >= 1)
	{
		int			syslog_level;

		switch (edata->elevel)
		{
			case DEBUG5:
			case DEBUG4:
			case DEBUG3:
			case DEBUG2:
			case DEBUG1:
				syslog_level = LOG_DEBUG;
				break;
			case LOG:
			case COMMERROR:
			case INFO:
				syslog_level = LOG_INFO;
				break;
			case NOTICE:
			case WARNING:
				syslog_level = LOG_NOTICE;
				break;
			case ERROR:
				syslog_level = LOG_WARNING;
				break;
			case FATAL:
				syslog_level = LOG_ERR;
				break;
			case PANIC:
			default:
				syslog_level = LOG_CRIT;
				break;
		}

		write_syslog(syslog_level, buf.data);
	}
#endif   /* HAVE_SYSLOG */

	/* Write to stderr, if enabled */
	if (Use_syslog <= 1 || whereToSendOutput == Debug)
	{
		/*
		 * Timestamp and PID are only used for stderr output --- we assume
		 * the syslog daemon will supply them for us in the other case.
		 */
		fprintf(stderr, "%s%s%s",
				Log_timestamp ? print_timestamp() : "",
				Log_pid ? print_pid() : "",
				buf.data);
	}

	pfree(buf.data);
}


/*
 * Write error report to client
 */
static void
send_message_to_frontend(ErrorData *edata)
{
	StringInfoData msgbuf;

	/* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
	pq_beginmessage(&msgbuf);
	pq_sendbyte(&msgbuf, edata->elevel != ERROR ? 'N' : 'E');

	/* Old style --- gin up a backwards-compatible message */
	StringInfoData buf;

	initStringInfo(&buf);

	appendStringInfo(&buf, "%s:  ", error_severity(edata->elevel));

	if (edata->show_funcname && edata->funcname)
		appendStringInfo(&buf, "%s: ", edata->funcname);

	if (edata->message)
		appendStringInfo(&buf, "%s", edata->message);
	else
		appendStringInfoString(&buf, gettext("missing error text"));

	if (edata->cursorpos > 0)
		appendStringInfo(&buf, gettext(" at character %d"),
						 edata->cursorpos);

	appendStringInfoChar(&buf, '\n');

	if (edata->detail)
		appendStringInfo(&buf, "DETAIL:  %s\n", edata->detail);

	if (edata->hint)
		appendStringInfo(&buf, "HINT:  %s\n", edata->hint);

	if (edata->context)
		appendStringInfo(&buf, "CONTEXT:  %s\n", edata->context);

	pq_sendstring(&msgbuf, buf.data);

	pfree(buf.data);

	pq_endmessage(&msgbuf);

	/*
	 * This flush is normally not necessary, since postgres.c will flush
	 * out waiting data when control returns to the main loop. But it
	 * seems best to leave it here, so that the client has some clue what
	 * happened if the backend dies before getting back to the main loop
	 * ... error/notice messages should not be a performance-critical path
	 * anyway, so an extra flush won't hurt much ...
	 */
	pq_flush();
}


/*
 * Support routines for formatting error messages.
 */


/*
 * expand_fmt_string --- process special format codes in a format string
 *
 * We must replace %m with the appropriate strerror string, since vsnprintf
 * won't know what to do with it.
 *
 * The result is a palloc'd string.
 */
static char *
expand_fmt_string(const char *fmt, ErrorData *edata)
{
	StringInfoData	buf;
	const char *cp;

	initStringInfo(&buf);

	for (cp = fmt; *cp; cp++)
	{
		if (cp[0] == '%' && cp[1] != '\0')
		{
			cp++;
			if (*cp == 'm')
			{
				/*
				 * Replace %m by system error string.  If there are any %'s
				 * in the string, we'd better double them so that vsnprintf
				 * won't misinterpret.
				 */
				const char *cp2;

				cp2 = useful_strerror(edata->saved_errno);
				for (; *cp2; cp2++)
				{
					if (*cp2 == '%')
						appendStringInfoCharMacro(&buf, '%');
					appendStringInfoCharMacro(&buf, *cp2);
				}
			}
			else
			{
				/* copy % and next char --- this avoids trouble with %%m */
				appendStringInfoCharMacro(&buf, '%');
				appendStringInfoCharMacro(&buf, *cp);
			}
		}
		else
			appendStringInfoCharMacro(&buf, *cp);
	}

	return buf.data;
}


/*
 * A slightly cleaned-up version of strerror()
 */
static const char *
useful_strerror(int errnum)
{
	/* this buffer is only used if errno has a bogus value */
	static char errorstr_buf[48];
	const char   *str;

	str = strerror(errnum);

	/*
	 * Some strerror()s return an empty string for out-of-range errno.
	 * This is ANSI C spec compliant, but not exactly useful.
	 */
	if (str == NULL || *str == '\0')
	{
		/*
		 * translator: This string will be truncated at 47 characters
		 * expanded.
		 */
		snprintf(errorstr_buf, sizeof(errorstr_buf),
				 gettext("operating system error %d"), errnum);
		str = errorstr_buf;
	}

	return str;
}


/*
 * error_severity --- get localized string representing elevel
 */
static const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = gettext("DEBUG");
			break;
		case LOG:
		case COMMERROR:
			prefix = gettext("LOG");
			break;
		case INFO:
			prefix = gettext("INFO");
			break;
		case NOTICE:
			prefix = gettext("NOTICE");
			break;
		case WARNING:
			prefix = gettext("WARNING");
			break;
		case ERROR:
			prefix = gettext("ERROR");
			break;
		case FATAL:
			prefix = gettext("FATAL");
			break;
		case PANIC:
			prefix = gettext("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}


/*
 * Return a timestamp string like
 *
 *	 "2000-06-04 13:12:03 "
 */
static const char *
print_timestamp(void)
{
	time_t		curtime;
	static char buf[21];		/* format `YYYY-MM-DD HH:MM:SS ' */

	curtime = time(NULL);

	strftime(buf, sizeof(buf),
			 "%Y-%m-%d %H:%M:%S ",
			 localtime(&curtime));

	return buf;
}


/*
 * Return a string like
 *
 *	   "[123456] "
 *
 * with the current pid.
 */
static const char *
print_pid(void)
{
	static char buf[10];		/* allow `[123456] ' */

	snprintf(buf, sizeof(buf), "[%d] ", (int) MyProcPid);
	return buf;
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
compute_function_hashkey(FunctionCallInfo fcinfo,
						 Form_pg_proc procStruct,
						 plr_func_hashkey *hashkey)
{
	int		i;

	/* Make sure any unused bytes of the struct are zero */
	MemSet(hashkey, 0, sizeof(plr_func_hashkey));

	/* get function OID */
	hashkey->funcOid = fcinfo->flinfo->fn_oid;

	/* if trigger, get relation OID */
	if (CALLED_AS_TRIGGER(fcinfo))
	{
		TriggerData *trigdata = (TriggerData *) fcinfo->context;

		hashkey->trigrelOid = RelationGetRelid(trigdata->tg_relation);
	}

	/* get the argument types */
	for (i = 0; i < procStruct->pronargs; i++)
	{
		Oid			argtypeid = procStruct->proargtypes[i];

		/*
		 * Check for polymorphic arguments. If found, use the actual
		 * parameter type from the caller's FuncExpr node, if we have one.
		 *
		 * We can support arguments of type ANY the same way as normal
		 * polymorphic arguments.
		 */
		if (argtypeid == ANYARRAYOID || argtypeid == ANYELEMENTOID ||
			argtypeid == ANYOID)
		{
			argtypeid = get_fn_expr_argtype(fcinfo->flinfo, i);
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
