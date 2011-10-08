SUBDIRS=leveldb

AM_CPPFLAGS=-I${top_builddir}/src/storage/ldb/leveldb/include -I${top_builddir}/src/storage/ldb/leveldb/ -I${top_srcdir}/src -I${top_srcdir}/src/storage -I${top_srcdir}/src/common -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I.

LDADD=$(TBLIB_ROOT)/lib/libtbsys.a ${top_builddir}/src/storage/ldb/leveldb/libleveldb.a

AM_LDFLAGS=-lpthread -lrt ${GCOV_LIB}
noinst_LIBRARIES=libldb.a

libldb_a_SOURCES=ldb_manager.cpp ldb_manager.hpp ldb_bucket.hpp ldb_bucket.cpp stat_manager.cpp stat_manager.hpp ldb_define.hpp ldb_define.cpp bg_task.hpp bg_task.cpp ldb_comparator.hpp ldb_comparator.cpp ldb_gc_factory.hpp ldb_gc_factory.cpp


# test_ldb_LDADD=libldb.a $(TBLIB_ROOT)/lib/libtbsys.a ${top_builddir}/src/storage/ldb/leveldb/libleveldb.a -lgtest -lsnappy
# noinst_PROGRAMS=test_ldb
# test_ldb_SOURCES=test_ldb.cpp
