#!/bin/bash

cd "`dirname "$0"`"

myjar=`ls target/groupsync-*-with-dependencies.jar 2>/dev/null`

if [ "$myjar" = "" ]; then
    echo "Couldn't find groupsync jar.  Need to build?"
    exit
fi

if [ "$1" = "" ] || [ "$2" = "" ]; then
    echo "Usage: $0 <config file> <replication set>"
    echo

    exit
fi



java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "$myjar:libs/*" edu.nyu.classes.groupsync.brightspace.OAuth ${1+"$@"}

