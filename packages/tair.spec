Name: %NAME
Version: %VERSION
Release: 1%{?dist}
Summary: Taobao key/value storage system
Group: Application
URL: http://yum.corp.alimama.com
Packager: taobao<opensource@taobao.com>
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root
BuildRequires: t-csrd-tbnet-devel >= 1.0.4
BuildRequires: google-perftools >= 1.7
BuildRequires: boost-devel >= 1.30.0
BuildRequires: automake >= 1.7.0
BuildRequires: libtool >= 1.5.0
BuildRequires: snappy >= 1.0.1
#Requires: openssl-devel >= 0.9
Requires: t-csrd-tbnet-devel >= 1.0.4
Requires: google-perftools >= 1.7
Requires: boost-devel >= 1.30.0
Requires: automake >= 1.7.0
Requires: libtool >= 1.5.0
Requires: snappy >= 1.0.1

%description
Tair is a high performance, distribution key/value storage system.

%package devel
Summary: tair c++ client library
Group: Development/Libraries

%description devel
The %name-devel package contains  libraries and header
files for developing applications that use the %name package.

%prep
%setup

%build
export TBLIB_ROOT=/opt/csr/common
chmod u+x bootstrap.sh
./bootstrap.sh
./configure --prefix=%{_prefix} --with-release=yes --with-kdb=yes --with-ldb=yes --with-boost=%BOOST_DIR --with-tcmalloc --with-compress=yes
make %{?_smp_mflags}

%install
#rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%post
echo %{_prefix}/lib > /etc/ld.so.conf.d/tair-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tair-%{VERSION}.conf
/sbin/ldconfig

%post devel
echo %{_prefix}/lib > /etc/ld.so.conf.d/tair-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tair-%{VERSION}.conf
/sbin/ldconfig

%postun
rm  -f /etc/ld.so.conf.d/tair-%{VERSION}.conf

%files
%defattr(0755, admin, admin)
%{_prefix}/bin
%{_prefix}/sbin
%{_prefix}/lib
%config(noreplace) %{_prefix}/etc/*
%attr(0755, admin, admin) %{_prefix}/set_shm.sh
%attr(0755, admin, admin) %{_prefix}/tair.sh
%attr(0755, admin, admin) %{_prefix}/stat.sh
%attr(0755, admin, admin) %{_prefix}/check_version.sh

%files devel
%{_prefix}/include
%{_prefix}/lib/libtairclientapi.*
%{_prefix}/lib/libtairclientapi_c.*
%{_prefix}/lib/libmdb.*
%{_prefix}/lib/libmdb_c.*

%changelog

*Thu Nov 28 2011 xinshu<xinshu.wzx@taobao.com>
-add with-tcmalloc
*Thu Mar 2 2011 MaoQi <maoqi@taobao.com>
-add post and config(noreplace)
