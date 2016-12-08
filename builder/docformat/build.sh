#!/bin/bash
if [ $# -ne 1 ];then
    BUSI="docformat"
else
    BUSI=$1
fi


CURDIR=$(cd $(dirname $0); pwd -P)
cd ${CURDIR}
make -j || exit 1

mkdir -p package

VERSION="0.1"

TARGET_DIR=.pack_tmp/${BUSI}-${VERSION}
rm -rf ${TARGET_DIR}
mkdir -p ${TARGET_DIR}/{bin,etc,data}

# bin
cp ${CURDIR}/bin/docformat ${TARGET_DIR}/bin
cp -r ${CURDIR}/script/common/* ${TARGET_DIR}/bin/
# cp -r ${CURDIR}/script/${BUSI}/* ${TARGET_DIR}/bin/
sed "s/BUSI_DEFAULT/${BUSI}/g" -i ${TARGET_DIR}/bin/install.sh

# etc
# cp -r ${CURDIR}/etc/common/* ${TARGET_DIR}/etc/
\cp -rf ${CURDIR}/etc/${BUSI}/* ${TARGET_DIR}/etc/
(cd ${TARGET_DIR}/etc/ && mv docformat.json ${BUSI}.json)

# make pkg
(cd .pack_tmp && tar zcf ../package/${BUSI}-${VERSION}.tar.gz ${BUSI}-${VERSION} --exclude=.svn --exclude=.git)

rm -rf ${TARGET_DIR}
echo "success ${BUSI}-${VERSION}.tar.gz"
cp package/${BUSI}-${VERSION}.tar.gz ../../dist
