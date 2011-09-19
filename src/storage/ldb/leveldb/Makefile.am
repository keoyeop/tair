# TODO enable config
SNAPPYCFLAGS=-DSNAPPY
SNAPPYLDFLAGS=-lsnappy
GOOGLE_PERFTOOLSLDFLAGS=
PORTCFLAGS= -fno-builtin-memcmp -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX

AM_CPPFLAGS=-I. -I./include ${PORTCFLAGS} ${PLATFORMCFLAGS} ${SNAPPYCFLAGS}
AM_LDFLAGS=-lpthread ${GCOV_LIB} ${SNAPPYLDFLAGS} ${GOOGLE_PERFTOOLSLDFLAGS}

noinst_LIBRARIES=libleveldb.a

libleveldb_a_SOURCES= \
	./db/builder.cc ./db/c.cc ./db/db_impl.cc ./db/db_iter.cc ./db/filename.cc ./db/dbformat.cc \
	./db/log_reader.cc ./db/log_writer.cc ./db/memtable.cc ./db/repair.cc ./db/table_cache.cc \
	./db/version_edit.cc ./db/version_set.cc ./db/write_batch.cc ./port/port_posix.cc \
	./table/block.cc ./table/block_builder.cc ./table/format.cc ./table/iterator.cc \
	./table/merger.cc ./table/table.cc ./table/table_builder.cc ./table/two_level_iterator.cc \
	./util/arena.cc ./util/cache.cc ./util/coding.cc ./util/comparator.cc ./util/crc32c.cc \
	./util/env.cc ./util/env_posix.cc ./util/hash.cc ./util/histogram.cc ./util/logging.cc \
	./util/options.cc ./util/status.cc ./util/config.h ./util/config.cc
