# location of R library
r_libdir = ${R_HOME}/bin
# location of R includes
r_includespec = ${R_HOME}/include

subdir = src/pl/plr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

# we can only build PL/R if libR is available
# Since there is no official way to determine this,
# we see if there is a file that is named like a shared library.
ifneq (,$(wildcard $(r_libdir)/libR*$(DLSUFFIX)*))
shared_libr = yes
endif

override CPPFLAGS := -I$(srcdir) -I$(r_includespec) $(CPPFLAGS)
override CPPFLAGS += -DPKGLIBDIR=\"$(pkglibdir)\" -DDLSUFFIX=\"$(DLSUFFIX)\"
rpath :=

SGMLDOCS	:= doc/plr.sgml
BUILDDOCS	:= jade -c ${DOCBOOKSTYLE}/catalog -d $(top_builddir)/doc/src/sgml/stylesheet.dsl -i output-html -t sgml
MODULE_big	:= plr
PG_CPPFLAGS	:= -I$(r_includespec)
SRCS		+= plr.c pg_conversion.c pg_backend_support.c pg_userfuncs.c pg_rsupport.c
OBJS		:= $(SRCS:.c=.o)
SHLIB_LINK	:= -L$(r_libdir) -lR

DATA_built	:= plr.sql 
DOCS		:= README.plr
REGRESS		:= plr
EXTRA_CLEAN	:= doc/HTML.index

include $(top_srcdir)/contrib/contrib-global.mk
