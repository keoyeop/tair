AM_CPPFLAGS= -I$(TBLIB_ROOT)/include/tbsys \
	     -I$(TBLIB_ROOT)/include/tbnet \
	     -I${top_srcdir}/src \
	     -I${top_srcdir}/src/common \
	     -I${top_srcdir}/src/packets \
	     -I${top_srcdir}/src/plugin \
	     -I${top_srcdir}/src/client

AM_LDFLAGS= -rdynamic  ${GCOV_LIB}

if WITH_COMPRESS
COMPRESS_LDFLAGS= -lsnappy
endif

LDADD=$(top_builddir)/src/packets/libtair_packets.a \
	  $(top_builddir)/src/client/.libs/libtairclientapi.a \
		$(top_builddir)/src/common/libtair_common.a \
		$(TBLIB_ROOT)/lib/libtbnet.a \
		$(TBLIB_ROOT)/lib/libtbsys.a \
		-lz -lrt -lpthread \
		$(COMPRESS_LDFLAGS) \
		-ldl

sbin_PROGRAMS=inval_server

inval_server_SOURCES=inval_server.cpp inval_server.hpp \
					 inval_loader.cpp inval_loader.hpp \
					 inval_retry_thread.cpp inval_retry_thread.hpp \
					 inval_processor.cpp inval_processor.hpp \
					 inval_stat_helper.cpp inval_stat_helper.hpp \
					 inval_request_storage.cpp inval_request_storage.hpp \
					 inval_request_packet_wrapper.hpp \
					 inval_group.hpp inval_group.cpp \
					 inval_heartbeat_thread.cpp inval_heartbeat_thread.hpp \
					 inval_periodic_worker.hpp inval_periodic_worker.cpp
