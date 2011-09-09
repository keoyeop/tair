SUBDIRS=leveldb

AM_CPPFLAGS=-I${top_builddir}/src/storage/ldb/leveldb/include -I${top_srcdir}/src -I${top_srcdir}/src/common -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I.

LDADD=$(TBLIB_ROOT)/lib/libtbsys.a ${top_builddir}/src/storage/ldb/leveldb/libleveldb.a

AM_LDFLAGS=-lpthread   ${GCOV_LIB}
noinst_LIBRARIES=libldb.a

libldb_a_SOURCES=ldb_manager.cpp ldb_manager.hpp ldb_bucket.hpp ldb_bucket.cpp
