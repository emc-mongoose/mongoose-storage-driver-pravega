#!/bin/sh
umask 0000
java -jar /opt/mongoose/mongoose.jar --storage-driver-type=pravega "$@"
