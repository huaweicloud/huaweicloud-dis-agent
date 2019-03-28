#!/bin/bash -l

pdir=$(cd "`dirname $0`";cd ../;pwd)
cd "${pdir}"

MAIN_CLASS="com.huaweicloud.dis.agent.Agent"

process=`ps -ef | grep "${MAIN_CLASS}" | grep "${pdir}/conf/agent.yml" | grep -v grep`
if [ $? -eq 0 ]; then
    pid=`echo ${process} | awk -F" " '{print $2}'`
    echo "DIS Agent [${pid}] is running, please stop it first."
    exit 1
fi
    
JAVACMD="java"
JAVA_START_HEAP="256m"
JAVA_MAX_HEAP="512m"

if [ -n "$1" ]; then
    JAVA_START_HEAP="$1m"
    shift 1
fi

if [ -n "$1" ]; then
    JAVA_MAX_HEAP="$1m"
    shift 1
fi

JAVA_PATH=`which ${JAVACMD}`
if [ "${JAVA_PATH}" == "" ]; then
    echo "No java found."
    exit 1
fi

LIB_DIR="${pdir}/lib"
CONFIG_DIR="${pdir}/conf"

# clean sqlite so
rm -f "${LIB_DIR}/sqlite*-libsqlitejdbc.so"

CLASSPATH="$LIB_DIR":$(find "$LIB_DIR" -type f -name \*.jar | paste -s -d : -):"$CLASSPATH":"$CONFIG_DIR"

OOME_ARGS="-XX:OnOutOfMemoryError=\"/bin/kill -9 %p\""
JVM_ARGS="-Xms${JAVA_START_HEAP} -Xmx${JAVA_MAX_HEAP} -Djava.io.tmpdir=${CONFIG_DIR} -Dlog4j.configurationFile=conf/log4j2.xml $JVM_ARGS"

MAIN_CLASS_ARGS=$@
if [ "${MAIN_CLASS_ARGS}" == "" ]; then
    MAIN_CLASS_ARGS="-c ${pdir}/conf/agent.yml"
fi
exec $JAVACMD $JVM_ARGS $JVM_DBG_OPTS "$OOME_ARGS" \
  -cp "$CLASSPATH" \
  $MAIN_CLASS ${MAIN_CLASS_ARGS} > /dev/null 2>&1 &

sleep 1
if [ $? -eq 0 ]; then
    process=`ps -ef | grep "${MAIN_CLASS}" | grep "${pdir}/conf/agent.yml" | grep -v grep`
    if [ $? -ne 0 ]; then
        echo -e "Failed to start agent, cloud not find Agent process id, please check logs/dis-agent.log for detail information.\n"
        tail -n +`grep -n "ERROR" "${pdir}/logs/dis-agent.log" | tail -1 | awk -F":" '{print $1}'` "${pdir}/logs/dis-agent.log"
        exit 1
    fi
    pid=`echo ${process} | awk -F" " '{print $2}'`
    echo "${pid}" > "${pdir}/agent.pid"
    echo "Success to start DIS Agent [${pid}]."
else
    echo "Failed to start DIS Agent."
fi
