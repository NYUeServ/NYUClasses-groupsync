#!/bin/bash

if [ ! "$ENVIRONMENT_CLEAN" ]; then
    exec env -i ENVIRONMENT_CLEAN=1 $0 ${1+"$@"}
fi

export PATH=/home/sakai/jdk-current/bin:$PATH

cd "`dirname "$0"`"

myjar=`ls target/groupsync-*-with-dependencies.jar 2>/dev/null`

if [ "$myjar" = "" ]; then
    echo "Couldn't find groupsync jar.  Need to build?"
    exit
fi
    

mkdir -p logs

# Logging to weekday-based files so that we only keep the last 7 days
# worth of stuff by default.  Saves having to logrotate.
java -Dgroupsync-service=true -Xmx1g -Dlogback.configurationFile=config/logback.xml -cp "libs/*:$myjar" \
     edu.nyu.classes.groupsync.main.Main config/config.properties \
     2>&1 | ./log-rotater.pl logs/console.out.%a
