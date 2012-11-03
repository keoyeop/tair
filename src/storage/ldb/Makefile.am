AM_CPPFLAGS=-DWITH_TBUTIL -fPIC -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I${top_builddir}/src/storage/ldb/leveldb/include -I${top_builddir}/src/storage/ldb/leveldb/ -I${top_srcdir}/src -I${top_srcdir}/src/storage -I${top_srcdir}/src/common -I.

if WITH_TCMALLOC
AM_CPPFLAGS += -DWITH_TCMALLOC
endif

AM_LDFLAGS=-lpthread -lrt ${GCOV_LIB}
noinst_LIBRARIES=libldb.a

libldb_a_SOURCES=ldb_manager.cpp ldb_manager.hpp ldb_instance.hpp ldb_instance.cpp stat_manager.cpp stat_manager.hpp ldb_define.hpp ldb_define.cpp bg_task.hpp bg_task.cpp ldb_comparator.hpp ldb_numeric_comparator.cpp ldb_comparator.cpp ldb_gc_factory.hpp ldb_gc_factory.cpp ldb_cache_stat.hpp ldb_cache_stat.cpp ldb_bloom.cpp ldb_remote_sync_logger.hpp ldb_remote_sync_logger.cpp


view_cache_stat_LDADD=libldb.a ${top_builddir}/src/common/libtair_common.a $(TBLIB_ROOT)/lib/libtbsys.a

sbin_PROGRAMS=view_cache_stat ldb_rsync
view_cache_stat_SOURCES=view_cache_stat.cpp

ldb_rsync_SOURCES=ldb_rsync.cpp
ldb_rsync_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
		  -I${top_srcdir}/src/plugin -I${top_srcdir}/src/statserver/include
ldb_rsync_LDADD=libldb.a ./libleveldb.a ./libsnappy.a  ${top_builddir}/src/storage/mdb/.libs/libmdb.a \
		${top_builddir}/src/client/.libs/libtairclientapi.a\
		${top_builddir}/src/common/libtair_common.a $(TBLIB_ROOT)/lib/libtbnet.a $(TBLIB_ROOT)/lib/libtbsys.a\
		${BOOST_LDFLAGS} @BOOST_THREAD_LIB@ -lpthread -lz -lrt

##############################
# leveldb && snappy
##############################
PORTCFLAGS= -fno-builtin-memcmp \
	    -pthread -DOS_LINUX \
	    -DSNAPPY -I$./snappy \
	    ${LEVELDB_PORT_CFLAGS}

noinst_LIBRARIES += libleveldb.a libsnappy.a

leveldb_srcdir=./leveldb
snappy_srcdir=./snappy

libleveldb_a_CPPFLAGS=${AM_CPPFLAGS} ${PORTCFLAGS}
libleveldb_a_SOURCES= \
	${leveldb_srcdir}/db/builder.cc ${leveldb_srcdir}/db/c.cc ${leveldb_srcdir}/db/db_impl.cc \
	${leveldb_srcdir}/db/db_iter.cc \
	${leveldb_srcdir}/db/filename.cc ${leveldb_srcdir}/db/dbformat.cc ${leveldb_srcdir}/db/log_reader.cc \
	${leveldb_srcdir}/db/log_writer.cc ${leveldb_srcdir}/db/memtable.cc ${leveldb_srcdir}/db/repair.cc \
	${leveldb_srcdir}/db/table_cache.cc ${leveldb_srcdir}/db/version_edit.cc ${leveldb_srcdir}/db/version_set.cc \
	${leveldb_srcdir}/db/write_batch.cc ${leveldb_srcdir}/port/port_posix.cc \
	${leveldb_srcdir}/table/block.cc ${leveldb_srcdir}/table/block_builder.cc ${leveldb_srcdir}/table/format.cc \
	${leveldb_srcdir}/table/iterator.cc ${leveldb_srcdir}/table/merger.cc ${leveldb_srcdir}/table/table.cc \
	${leveldb_srcdir}/table/table_builder.cc ${leveldb_srcdir}/table/two_level_iterator.cc \
	${leveldb_srcdir}/table/filter_block.cc ${leveldb_srcdir}/table/filter_block.h \
	${leveldb_srcdir}/util/arena.cc ${leveldb_srcdir}/util/cache.cc ${leveldb_srcdir}/util/coding.cc \
	${leveldb_srcdir}/util/comparator.cc ${leveldb_srcdir}/util/crc32c.cc \
	${leveldb_srcdir}/util/env.cc ${leveldb_srcdir}/util/env_posix.cc ${leveldb_srcdir}/util/hash.cc \
	${leveldb_srcdir}/util/histogram.cc ${leveldb_srcdir}/util/logging.cc \
	${leveldb_srcdir}/util/options.cc ${leveldb_srcdir}/util/status.cc ${leveldb_srcdir}/util/config.h \
	${leveldb_srcdir}/util/config.cc ${leveldb_srcdir}/util/filter_policy.cc ${leveldb_srcdir}/util/bloom.cc \
	${leveldb_srcdir}/db/builder.h ${leveldb_srcdir}/db/db_iter.h ${leveldb_srcdir}/db/filename.h \
	${leveldb_srcdir}/db/log_reader.h ${leveldb_srcdir}/db/memtable.h ${leveldb_srcdir}/db/snapshot.h \
	${leveldb_srcdir}/db/version_edit.h ${leveldb_srcdir}/db/version_set.h \
	${leveldb_srcdir}/db/write_batch_internal.h ${leveldb_srcdir}/db/db_impl.h ${leveldb_srcdir}/db/dbformat.h \
	${leveldb_srcdir}/db/log_format.h ${leveldb_srcdir}/db/log_writer.h ${leveldb_srcdir}/db/skiplist.h \
	${leveldb_srcdir}/db/table_cache.h ${leveldb_srcdir}/port/atomic_pointer.h ${leveldb_srcdir}/port/atomic_count.h \
	${leveldb_srcdir}/port/port.h ${leveldb_srcdir}/port/port_android.h ${leveldb_srcdir}/port/port_example.h \
	${leveldb_srcdir}/port/port_posix.h ${leveldb_srcdir}/port/sha1_portable.h ${leveldb_srcdir}/table/block.h \
	${leveldb_srcdir}/table/block_builder.h ${leveldb_srcdir}/table/format.h ${leveldb_srcdir}/table/iterator_wrapper.h \
	${leveldb_srcdir}/table/merger.h ${leveldb_srcdir}/table/two_level_iterator.h ${leveldb_srcdir}/util/arena.h \
	${leveldb_srcdir}/util/coding.h ${leveldb_srcdir}/util/config.h \
	${leveldb_srcdir}/util/crc32c.h ${leveldb_srcdir}/util/hash.h ${leveldb_srcdir}/util/histogram.h \
	${leveldb_srcdir}/util/logging.h ${leveldb_srcdir}/util/mutexlock.h ${leveldb_srcdir}/util/posix_logger.h \
	${leveldb_srcdir}/util/random.h ${leveldb_srcdir}/include/${leveldb_srcdir}/c.h \
	${leveldb_srcdir}/include/${leveldb_srcdir}/cache.h ${leveldb_srcdir}/include/${leveldb_srcdir}/db.h \
	${leveldb_srcdir}/include/${leveldb_srcdir}/env.h ${leveldb_srcdir}/include/${leveldb_srcdir}/iterator.h \
	${leveldb_srcdir}/include/${leveldb_srcdir}/options.h ${leveldb_srcdir}/include/${leveldb_srcdir}/slice.h \
	${leveldb_srcdir}/include/${leveldb_srcdir}/table.h ${leveldb_srcdir}/include/${leveldb_srcdir}/table_builder.h \
	${leveldb_srcdir}/include/${leveldb_srcdir}/write_batch.h ${leveldb_srcdir}/include/${leveldb_srcdir}/status.h \
	${leveldb_srcdir}/include/${leveldb_srcdir}/comparator.h ${leveldb_srcdir}/include/${leveldb_srcdir}/filter_policy.h

libsnappy_a_SOURCES= \
	${snappy_srcdir}/snappy-internal.h ${snappy_srcdir}/snappy-c.h ${snappy_srcdir}/snappy-c.cc \
	${snappy_srcdir}/snappy-sinksource.cc ${snappy_srcdir}/snappy-sinksource.h \
	${snappy_srcdir}/snappy-stubs-internal.h ${snappy_srcdir}/snappy-stubs-internal.cc\
	${snappy_srcdir}/snappy-stubs-public.h.in ${snappy_srcdir}/snappy.cc ${snappy_srcdir}/snappy.h\
	${snappy_srcdir}/config.h.in

##############################