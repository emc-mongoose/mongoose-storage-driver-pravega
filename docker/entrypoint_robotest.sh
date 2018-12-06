#!/bin/sh
umask 0000
robot --outputdir /root/mongoose-storage-driver-pravega/build/robotest --suite ${SUITE} --include ${TEST} /root/mongoose-storage-driver-pravega/src/test/robot
rebot /root/mongoose-storage-driver-pravega/build/robotest/output.xml
