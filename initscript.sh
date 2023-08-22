#!/bin/bash
#
# groupsync      Init script for running the groupsync daemon
#
# chkconfig: 35 60 50
#
### BEGIN INIT INFO
# Provides: groupsync
# Required-Start: $network $syslog
# Required-Stop: $network $syslog
# Default-Start: 35
# Default-Stop: 01246
# Short-Description: Groupsync daemon
# Description: Start and stop the Groupsync daemon
### END INIT INFO
# 
# Variables
GROUPSYNC_USER=sakai
GROUPSYNC_HOME="$(dirname $(readlink -f "$0"))"

# Source function library.
#. /etc/rc.d/init.d/functions

groupsync_pid() {
    echo `ps aux | grep -i groupsync-service | grep -v grep | awk '{ print $2 }'`
}

start() {
    pid=$(groupsync_pid)
    if [ -n "$pid" ]; then
        echo "Groupsync is already running (pid: $pid)"
    else
        echo "Starting Groupsync"

        if [ `whoami` = "$GROUPSYNC_USER" ]; then
            nohup ${GROUPSYNC_HOME}/run.sh >/dev/null 2>/dev/null &
        else
            nohup /bin/su ${GROUPSYNC_USER} -c ${GROUPSYNC_HOME}/run.sh >/dev/null 2>/dev/null &
        fi
    fi

    return 0
}
stop() {
    pid=$(groupsync_pid)
    if [ -n "$pid" ]
    then
        echo "Stopping Groupsync"
        kill "$pid"
    else
        echo "Groupsync is not running"
    fi
    return 0
}
status() {
    pid=$(groupsync_pid)
    if [ -n "$pid" ]
    then
        echo "Groupsync is running with pid: $pid"
    else
        echo "Groupsync is not running"
    fi
    return 0
}
case $1 in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 5
        start
        ;;
    status)
        status
        ;;
    *)
        echo $"Usage: $0 {start|stop|restart|status}"
        exit 1
esac
exit $RETVAL
