SUBDIRS=interface_test unit_test statistics_test retry_all_test  tair_mc_client_api_test
AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
			 -I$(TBLIB_ROOT)/include/tbnet \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/packets \
			 -I${top_srcdir}/src/storage/fdb \
			 -I${top_srcdir}/src/storage/mdb \
			 -I${top_srcdir}/src/storage/kdb \
			 -I${top_srcdir}/src/storage \
			 -I${top_srcdir}/src/client \
       -I${top_srcdir}/src  \
       -I${top_srcdir}/src/storage/kdb/kyotocabinet \
       -I${top_srcdir}/test

LDADD= \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
	  $(top_builddir)/src/storage/mdb/libmdb.a \
	  $(top_builddir)/src/storage/fdb/libfdb.a \
	  $(top_builddir)/src/storage/kdb/libkdb.a \
    ${top_builddir}/src/storage/kdb/kyotocabinet/libkyotocabinet.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a 

AM_LDFLAGS=-lpthread -lz

#bin_PROGRAMS=get
#get_SOURCES=get.cpp
