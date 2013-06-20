AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/client \
       -I${top_srcdir}/src  \
		 -I${top_srcdir}/test

LDADD= \
	  $(top_builddir)/test/lib/libgtest.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a

AM_LDFLAGS=-L${top_builddir}/src/client/.libs  -ltairmcclientapi  -lpthread

sbin_PROGRAMS=tair_mc_client_api_test

tair_mc_client_api_test_SOURCES=tair_mc_client_api_test.cpp  test_api.cpp
