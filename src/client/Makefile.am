AM_CPPFLAGS= -fPIC -I${top_srcdir}/src \
	    -I$(TBLIB_ROOT)/include/tbsys \
	    -I$(TBLIB_ROOT)/include/tbnet \
	    -I${top_srcdir}/src/storage/fdb \
	    -I${top_srcdir}/src/storage/mdb \
            -I${top_srcdir}/src/common \
	    -I${top_srcdir}/src/storage/ \
            -I${top_srcdir}/src/packets \
            -I${top_srcdir}/src/dataserver \
	    -I${top_srcdir}/src/plugin \
	    ${BOOST_CPPFLAGS}

LDADD=$(TBLIB_ROOT)/lib/libtbnet.a \
      $(TBLIB_ROOT)/lib/libtbsys.a \
      $(top_srcdir)/src/storage/ldb/leveldb/libsnappy.a

api_source_list=tair_client_api.cpp tair_client_api_impl.cpp \
		${top_srcdir}/src/packets/packet_factory.cpp \
		tair_client_capi.hpp tair_client_api.hpp \
	       	tair_client_api_impl.hpp tair_client_capi.hpp \
		tair_client.hpp

AM_LDFLAGS=-lpthread -lz ${BOOST_LDFLAGS} @BOOST_THREAD_LIB@  ${GCOV_LIB}

lib_LTLIBRARIES=libtairclientapi.la libtairclientapi_c.la libtairmulticlusterclientapi.la
sbin_PROGRAMS=tairclient simpletest

tairclient_SOURCES=tair_client.cpp
tairclient_LDADD=libtairclientapi.la ${LDADD}
tairclient_LDFLAGS= -static

libtairclientapi_la_SOURCES=${api_source_list}
libtairclientapi_c_la_SOURCES=tair_client_capi.cpp
libtairclientapi_c_la_LIBADD=libtairclientapi.la

libtairmulticlusterclientapi_la_SOURCES=${api_source_list} \
					tair_multi_cluster_client_api.hpp tair_multi_cluster_client_api.cpp \
					cluster_handler_manager.hpp cluster_handler_manager.cpp \
					cluster_info_updater.hpp cluster_info_updater.cpp

simpletest_SOURCES=simpletest.cpp
simpletest_LDADD= libtairclientapi.la ${LDADD}
simpletest_LDFLAGS= -static


include_HEADERS=tair_client_api.hpp tair_client_capi.hpp tair_multi_cluster_client_api.hpp
