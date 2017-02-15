#!/bin/bash

set -e

CURRENT_DIR=`pwd`
OPENSSL_VERSION=1.0.2k
OPENSSL_TAR=openssl-${OPENSSL_VERSION}.tar.gz
OPENSSL_DIR=${CURRENT_DIR}/openssl-${OPENSSL_VERSION}

LIBEVENT_VERSION=2.1.8
LIBEVENT_TAR=libevent-${LIBEVENT_VERSION}-stable.tar.gz
LIBEVENT_DIR=${CURRENT_DIR}/libevent-${LIBEVENT_VERSION}-stable

INSTALL_DIR=${CURRENT_DIR}/third-party-install

OPENSSL_DIR=${CURRENT_DIR}/openssl-${OPENSSL_VERSION}

function install-open-ssl() {
	echo "Openssl version ${OPENSSL_VERSION}, tar file ${OPENSSL_TAR}"
	echo "openssl dir ${OPENSSL_DIR}"

	cd ${OPENSSL_DIR}
	if [ "$(uname)" == "Darwin" ]; then
		echo "Configuration and installation of OpenSSL for libevent not implemented for MAC at present, using the openssl libraries installed on the system"
		return 0
	elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
		echo "Configure openssl ... openssl-${OPENSSL_VERSION}"
		./config --openssldir=${INSTALL_DIR} -fPIC -DOPENSSL_PIC  -Wformat -Werror=format-security -fstack-protector
	fi

	echo "Build and install openssl at ${INSTALL_DIR}"
	make
	make install

	cd ${CURRENT_DIR}
	# delete files that are not needed
	rm -rf ${INSTALL_DIR}/bin ${INSTALL_DIR}/certs ${INSTALL_DIR}/man \
	${INSTALL_DIR}/misc ${INSTALL_DIR}/private ${INSTALL_DIR}/openssl.cnf \
	${INSTALL_DIR}/lib/engines ${INSTALL_DIR}/lib/pkgconfig

	rm -rf ${OPENSSL_DIR}
}


function install-libevent() {
	echo "libevent version ${LIBEVENT_VERSION}, tar file ${LIBEVENT_TAR}"
	echo "openssl dir ${LIBEVENT_DIR}"

	# build libevent binaries with the right configuration
	cd ${LIBEVENT_DIR}

	if [ "$(uname)" == "Darwin" ]; then
		echo "Configure libevent ${LIBEVENT_VERSION} for Mac"
		./configure CPPFLAGS="-I/usr/local/opt/openssl/include" LDFLAGS="-L/usr/local/opt/openssl/lib" --disable-shared --with-pic --prefix=${INSTALL_DIR}
	elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
		echo "Configure libevent ${LIBEVENT_VERSION} for Linux"
		./configure CPPFLAGS="-I${INSTALL_DIR}/include" LDFLAGS="-L${INSTALL_DIR}/lib" LIBS="-ldl" --disable-shared --with-pic --prefix=${INSTALL_DIR}
	fi

	make
	make install
	cd ${CURRENT_DIR}

	# delete files that are not needed
	rm -rf ${INSTALL_DIR}/bin ${INSTALL_DIR}/lib/pkgconfig
	rm -f ${INSTALL_DIR}/include/*.h ${INSTALL_DIR}/lib/*.la ${INSTALL_DIR}/lib/libevent_core.a ${INSTALL_DIR}/lib/libevent_extra.a

	# remove the unpacked tarbal
	rm -rf ${LIBEVENT_DIR}
}

# remove the files and directories from previous build, if any and lay out dir for libs
function cleanup-and-prep() {
	# unpack openssl
	rm -rf ${OPENSSL_DIR} ${LIBEVENT_DIR}
	rm -rf ${INSTALL_DIR}

	# unpack the bundled libevent tarball, overwriting existing
	tar -xzf openssl-${OPENSSL_VERSION}.tar.gz
	tar -xzf libevent-${LIBEVENT_VERSION}-stable.tar.gz

	mkdir ${INSTALL_DIR}
	echo "Install directory for third party includes and libs ${INSTALL_DIR}"
}

cleanup-and-prep
install-open-ssl
install-libevent
