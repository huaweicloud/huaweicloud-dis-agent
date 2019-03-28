#!/bin/bash -l

JAVACMD="java"
JAVA_START_HEAP="256m"
JAVA_MAX_HEAP="512m"

JAVA_PATH=`which ${JAVACMD}`
if [ "${JAVA_PATH}" == "" ]; then
    echo "No java found."
    exit 1
fi

pdir=$(cd `dirname $0`;cd ../;pwd)
cd ${pdir}

LIB_DIR="${pdir}/lib"
CONFIG_DIR="${pdir}/conf"

CLASSPATH="$LIB_DIR":$(find "$LIB_DIR" -type f -name \*.jar | paste -s -d:):"$CLASSPATH":"$CONFIG_DIR"

OOME_ARGS="-XX:OnOutOfMemoryError=\"/bin/kill -9 %p\""
JVM_ARGS="-server -Xms${JAVA_START_HEAP} -Xmx${JAVA_MAX_HEAP} $JVM_ARGS"
#JVM_DBG_OPTS="-Xdebug -server -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8000"
MAIN_CLASS="com.huawei.bigdata.dis.agent.Agent"
MAIN_CLASS_ARGS=$@
if [ "${MAIN_CLASS_ARGS}" == "" ]; then
    MAIN_CLASS_ARGS="-c ${pdir}/conf/agent.yml"
fi
exec $JAVACMD $JVM_ARGS $JVM_DBG_OPTS "$OOME_ARGS" \
  -cp "$CLASSPATH" \
  $MAIN_CLASS ${MAIN_CLASS_ARGS}