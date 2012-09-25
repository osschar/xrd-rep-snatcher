#!/bin/bash

getent group  xrootd >/dev/null || groupadd -r xrootd
getent passwd xrootd >/dev/null || \
       useradd -r -g xrootd -c "Xrootd runtime user" -s /sbin/nologin -d /etc/xrootd xrootd

mkdir -p /etc/xrootd
mkdir -p            /var/run/xrootd /var/log/xrootd
chown xrootd:xrootd /var/run/xrootd /var/log/xrootd

if [ ! -e /etc/sysconfig/xrootd ]; then
   cat > /etc/sysconfig/xrootd <<FNORD
export XROOTD_USER=xrootd
FNORD
fi


cp xrd-rep-snatcher.pl          /usr/bin
cp init.d/xrd-rep-snatcher      /etc/init.d
cp logrotate.d/xrd-rep-snatcher /etc/logrotate.d


if [ ! -e /etc/xrootd/xrd-rep-snatcher.rc ]; then
  cp xrd-rep-snatcher.rc        /etc/xrootd/
fi

if [ ! -e /etc/xrootd/host_to_site.pddd ]; then
  cp example_host_to_site.pddd  /etc/xrootd/host_to_site.pddd
fi


chkconfig --add xrd-rep-snatcher
