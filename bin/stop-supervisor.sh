#!/usr/bin/env bash
cd `dirname $0`

echo "Stopping Supervisor ... "
PIDFILE=supervisor.pid
if [ ! -f $PIDFILE ]
then
        echo "error: count not find file $PIDFILE"
        exit 1
else
        kill $(cat $PIDFILE)
        rm $PIDFILE
        echo STOPPED
fi

