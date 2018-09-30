#!/bin/sh
umask 0000
java -Xms1g -Xmx1g -XX:MaxDirectMemorySize=1g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -jar /opt/mongoose/mongoose.jar --storage-driver-type=pravega "$@"
