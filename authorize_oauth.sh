#!/bin/bash

if [ "$1" = "" ] || [ "$2" = "" ]; then
    echo "Usage: $0 <oauth user> <oauth secret> <redirect uri> <credentials store path>"
    echo
    echo "Example:"
    echo
    echo "$0 'LONGSTRING.apps.googleusercontent.com' 'SECRETSTRING123' 'https://example.com/' groupsync.credentials"
    echo

    exit
fi

myjar=`ls target/groupsync-*-with-dependencies.jar 2>/dev/null`

if [ "$myjar" = "" ]; then
    echo "Couldn't find groupsync jar.  Need to build?"
    exit
fi


java -cp "$myjar" edu.nyu.classes.groupsync.oauth.Authorize ${1+"$@"}
