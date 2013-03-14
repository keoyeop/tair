AM_CPPFLAGS=-DWITH_TBUTIL -fPIC -I$(TBLIB_ROOT)/include/tbsys -I$(TBLIB_ROOT)/include/tbnet -I${top_builddir}/src/storage/ldb/leveldb/include -I${top_builddir}/src/storage/ldb/leveldb/ -I${top_srcdir}/src -I${top_srcdir}/src/storage -I${top_srcdir}/src/storage/ldb -I${top_srcdir}/src/common -I.
AM_LDFLAGS=-lpthread ${GCOV_LIB} 

ldb_path=${top_srcdir}/src/storage/ldb
ldb_libs=${ldb_path}/libldb.a ${ldb_path}/libleveldb.a ${ldb_path}/libsnappy.a \
	${top_builddir}/src/common/libtair_common.a \
	${top_builddir}/src/storage/mdb/.libs/libmdb.a \
	$(TBLIB_ROOT)/lib/libtbsys.a -lrt -lz

util_srcs=ldb_util.cpp ldb_util.hpp

noinst_PROGRAMS=ldb_hash_to_map
ldb_hash_to_map_SOURCES=ldb_hash_to_map.cpp
ldb_hash_to_map_LDADD=${ldb_libs} ${TCMALLOC_LDFLAGS}

sbin_PROGRAMS=view_cache_stat ldb_rsync ldb_sst_picker ldb_manifest_merger ldb_dump

view_cache_stat_SOURCES=view_cache_stat.cpp
view_cache_stat_LDADD=${ldb_path}/libldb.a ${top_builddir}/src/common/libtair_common.a $(TBLIB_ROOT)/lib/libtbsys.a -lrt

ldb_rsync_SOURCES=ldb_rsync.cpp ${util_srcs}
ldb_rsync_CPPFLAGS=${AM_CPPFLAGS} -I${top_srcdir}/src/packets -I${top_srcdir}/src/dataserver \
		  -I${top_srcdir}/src/plugin -I${top_srcdir}/src/statserver/include
ldb_rsync_LDADD=${top_builddir}/src/client/.libs/libtairclientapi.a $(TBLIB_ROOT)/lib/libtbnet.a ${ldb_libs}

ldb_sst_picker_SOURCES=ldb_sst_picker.cpp ${util_srcs}
ldb_sst_picker_LDADD=${ldb_libs}

ldb_manifest_merger_SOURCES=ldb_manifest_merger.cpp ${util_srcs}
ldb_manifest_merger_LDADD=${ldb_libs}

ldb_dump_SOURCES=ldb_dump.cpp ${util_srcs}
ldb_dump_LDADD=${ldb_libs}
