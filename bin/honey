#!/usr/bin/env bash
cd `dirname $0`

JAVA=$JAVA_HOME/bin/java

CLASSPATH="../conf/"

CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar:conf

for f in ../lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

export CLASSPATH

exec "$JAVA" -cp $CLASSPATH data.sync.client.HoneyClient "$@"
