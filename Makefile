# location of R library
r_libdir = /usr/local/lib/R/bin/
# location of R includes
r_includespec = /usr/local/lib/R/include

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

MODULE_big = plr
PG_CPPFLAGS = -I$(r_includespec)
OBJS	= plr.o
SHLIB_LINK = -L$(r_libdir) -lR

DATA_built = plr.sql 
DOCS = README.plr
REGRESS = plr

include $(top_srcdir)/contrib/contrib-global.mk
