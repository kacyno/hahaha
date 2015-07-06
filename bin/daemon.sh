#!/usr/bin/env bash


usage="Usage: daemon.sh (start|stop) (Queen|Bee)"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

# get arguments

startStop=$1
shift
command=$1
shift

if [ "$command" != "Queen" ] && [ "$command" != "Bee" ] ; then
    echo $usage
fi
case $startStop in

  (start)

            PIDFILE=$command.pid
            if [ -f $PIDFILE ]
            then
                if kill -0 `cat $PIDFILE` > /dev/null 2>&1; then
                    echo process running as process `cat $PIDFILE`.  Stop it first.
                    exit 1
                fi
            fi

            JAVA=$JAVA_HOME/bin/java
            JAVA_OPTS="-server
                    -Xms4096m -Xmx4096m -XX:NewSize=256m -XX:MaxNewSize=256m
                    -XX:+UseParNewGC -XX:+UseConcMarkSweepGC
                    -XX:+UseCMSInitiatingOccupancyOnly
                    -XX:+CMSConcurrentMTEnabled
                    -XX:+CMSScavengeBeforeRemark
                    -XX:CMSInitiatingOccupancyFraction=30"


            CLASSPATH="../conf/"
            CLASSPATH=${CLASSPATH}:"../web/"
            CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar:conf

            for f in ../lib/*.jar; do
              CLASSPATH=${CLASSPATH}:$f;
            done

            export CLASSPATH

            exec "$JAVA" $JAVA_OPTS -cp $CLASSPATH data.sync.core.$command "$@">/dev/null 2>&1 &

            echo $! >$PIDFILE
            echo STARTED
            ;;

  (stop)



        PIDFILE=$command.pid
        if [ ! -f $PIDFILE ]
        then
                echo "error: count not find file $PIDFILE"
                exit 1
        else
                kill $(cat $PIDFILE)
                rm $PIDFILE
                echo STOPPED
        fi

    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac