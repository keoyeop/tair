AM_CPPFLAGS= -fPIC -I${top_srcdir}/src \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet \
	    -I${top_srcdir}/src/storage/fdb \
	    -I${top_srcdir}/src/storage/mdb \
            -I${top_srcdir}/src/common \
	    -I${top_srcdir}/src/storage/ \
            -I${top_srcdir}/src/packets \
            -I${top_srcdir}/src/client \
            -I${top_srcdir}/src/dataserver \
	    -I${top_srcdir}/src/plugin

if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

LDADD=$(top_srcdir)/src/client/.libs/libtairclientapi.a \
		$(top_srcdir)/src/common/libtair_common.a \
	  $(TBLIB_ROOT)/lib/libtbnet.a \
	  $(TBLIB_ROOT)/lib/libtbsys.a \
	  $(COMPRESS_LDFLAGS) \
		-lz

AM_LDFLAGS=-lpthread -lz -lrt ${GCOV_LIB}

sbin_PROGRAMS=dump_transfer

dump_transfer_SOURCES=dump_transfer.cpp
dump_transfer_LDADD=${LDADD}
