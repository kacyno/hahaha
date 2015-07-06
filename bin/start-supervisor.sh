#!/usr/bin/env bash
cd `dirname $0`

echo "Starting Supervisor ... "
PIDFILE=supervisor.pid
if [ -f $PIDFILE ]
then
    if kill -0 `cat $PIDFILE` > /dev/null 2>&1; then
        echo process running as process `cat $PIDFILE`.  Stop it first.
        exit 1
    fi
fi

JAVA=$JAVA_HOME/bin/java

JAVA_HEAP_MAX="-Xmx1000m -server -XX:+UseConcMarkSweepGC"


CLASSPATH="../conf/supervisor-conf/"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar:conf

for f in ../lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

export CLASSPATH

exec nohup "$JAVA" $JAVA_HEAP_MAX -cp $CLASSPATH data.sync.supervisor.main.Supervisor "$@" >/dev/null 2>&1 &

echo $! >$PIDFILE
echo STARTED
