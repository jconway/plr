# location of R library
r_libdir = ${R_HOME}/bin
# location of R includes
r_includespec = ${R_HOME}/include

subdir = contrib/plr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

# we can only build PL/R if libR is available
# Since there is no official way to determine this,
# we see if there is a file that is named like a shared library.
ifneq (,$(wildcard $(r_libdir)/libR*$(DLSUFFIX)*))
shared_libr = yes
endif

# If we don't have a shared library and the platform doesn't allow it
# to work without, we have to skip it.
ifneq (,$(findstring yes, $(shared_libr)$(allow_nonpic_in_shlib)))

override CPPFLAGS := -I$(srcdir) -I$(r_includespec) $(CPPFLAGS)
override CPPFLAGS += -DPKGLIBDIR=\"$(pkglibdir)\" -DDLSUFFIX=\"$(DLSUFFIX)\"
rpath :=

MODULE_big	:= plr
PG_CPPFLAGS	+= -I$(r_includespec)
SRCS		+= plr.c pg_conversion.c pg_backend_support.c pg_userfuncs.c pg_rsupport.c
OBJS		:= $(SRCS:.c=.o)
SHLIB_LINK	+= -L$(r_libdir) -lR

DATA_built	:= plr.sql 
DOCS		:= README.plr
REGRESS		:= plr
EXTRA_CLEAN	:= doc/HTML.index expected/plr.out

include $(top_srcdir)/contrib/contrib-global.mk

installcheck: submake
ifeq ($(findstring 7.3,$(VERSION)),7.3)
	cp -f $(top_builddir)/$(subdir)/expected/plr.out.7.3 $(top_builddir)/$(subdir)/expected/plr.out
else
	cp -f $(top_builddir)/$(subdir)/expected/plr.out.7.4 $(top_builddir)/$(subdir)/expected/plr.out
endif
	$(top_builddir)/src/test/regress/pg_regress $(REGRESS)

else # can't build

all:
	@echo ""; \
	 echo "*** Cannot build PL/R because libR is not a shared library." ; \
	 echo "*** You might have to rebuild your R installation.  Refer to"; \
	 echo "*** the documentation for details."; \
	 echo ""

endif # can't build
