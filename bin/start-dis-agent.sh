#!/bin/bash -l

is_bash=`ps -p $$ | grep bash | wc -l`
if [ ${is_bash} -eq 0 ]; then
	echo "Please use bash to start Agent, e.g. bash bin/`basename $0`"
	exit 1
fi

while getopts 'c:n:s:x:' opt; do
    case $opt in
        c)
            config_value="$OPTARG";;
        n)
            agent_name="$OPTARG";;
        s)
            mem_min="$OPTARG";;
        x)
            mem_max="$OPTARG";;
        ?)
            echo -e "Usage: `basename $0` -c config_file -n agent_name [-s 256m] [-x 512m], -s means min_memory, -x means max_memory"
            exit
    esac
done

pdir=$(cd "`dirname $0`";cd ../;pwd)

CONFIG_PATH=${pdir}/conf/agent.yml
if [ -n "${config_value}" ]; then
    CONFIG_PATH=`echo ${config_value} |  cut -d" " -f1 | xargs -n1 realpath`
fi

export DIS_AGENT_NAME="dis-agent"
if [ -n "${agent_name}" ]; then
    export DIS_AGENT_NAME=${agent_name}
fi

MAIN_CLASS="com.huaweicloud.dis.agent.Agent"
MAIN_CLASS_ARGS="-c ${CONFIG_PATH} -n ${DIS_AGENT_NAME}"

cd "${pdir}"

process=`ps -ef | grep "${MAIN_CLASS}" | grep "\-c ${CONFIG_PATH} " | grep -v grep`
if [ $? -eq 0 ]; then
    pid=`echo ${process} | awk -F" " '{print $2}'`
    echo "DIS Agent [${pid}] with the same config_file [${CONFIG_PATH}] is running, please stop it first."
    exit 1
fi

process=`ps -ef | grep "${MAIN_CLASS}" | grep "\-n ${DIS_AGENT_NAME}$" | grep -v grep`
if [ $? -eq 0 ]; then
    pid=`echo ${process} | awk -F" " '{print $2}'`
    echo "DIS Agent [${pid}] with the same agent_name [${DIS_AGENT_NAME}] is running, please stop it first or use a unique name"
    exit 1
fi

JAVACMD="java"
JAVA_START_HEAP="256m"
JAVA_MAX_HEAP="512m"

if [ -n "${mem_min}" ]; then
    JAVA_START_HEAP="${mem_min}"
    shift 1
fi

if [ -n "${mem_max}" ]; then
    JAVA_MAX_HEAP="${mem_max}"
    shift 1
fi

JAVA_PATH=`which ${JAVACMD}`
if [ "${JAVA_PATH}" == "" ]; then
    echo "No java found, please install JRE1.8+"
    exit 1
fi

JAVA_VERSION=`java -version 2>&1 |awk 'NR==1{ gsub(/"/,""); print $3 }'`
if [[ "${JAVA_VERSION}" =~ "1.7" || "${JAVA_VERSION}" =~ "1.6" || "${JAVA_VERSION}" =~ "1.5" ]]; then
    echo "Java version ${JAVA_VERSION} is too low, please upgrade to 1.8+"
    exit 1
fi

LIB_DIR="${pdir}/lib"
CONFIG_DIR="${pdir}/conf"

process_count=`ps -ef | grep "${MAIN_CLASS}" | grep -v grep | wc -l`
if [ $process_count -eq 0 ]; then
    # clean sqlite so
    rm -f "${LIB_DIR}/"sqlite*-libsqlitejdbc.so
    true > "${pdir}/agent.pid"
fi

# rm Console logger
sed -i '/<AppenderRef ref=\"Console\"\/>/d' "${pdir}/conf/log4j2.xml"

CLASSPATH="$LIB_DIR":$(find "$LIB_DIR" -type f -name \*.jar | paste -s -d : -):"$CLASSPATH":"$CONFIG_DIR"

#OOME_ARGS="-XX:OnOutOfMemoryError=\"/bin/kill -9 %p\""
#"$OOME_ARGS"
JVM_ARGS="-Xms${JAVA_START_HEAP} -Xmx${JAVA_MAX_HEAP} -Djava.io.tmpdir=${LIB_DIR} -Dlog4j.configurationFile=conf/log4j2.xml $JVM_ARGS"
mkdir -p "${pdir}/logs/"
which /bin/tee > /dev/null 2>&1
if [ $? -eq 0 ]; then
	exec $JAVACMD $JVM_ARGS $JVM_DBG_OPTS -cp "$CLASSPATH" $MAIN_CLASS ${MAIN_CLASS_ARGS} 2>&1 | /bin/tee -a "${pdir}/logs/${DIS_AGENT_NAME}.log" &
else
	exec $JAVACMD $JVM_ARGS $JVM_DBG_OPTS -cp "$CLASSPATH" $MAIN_CLASS ${MAIN_CLASS_ARGS} 2>&1 &
fi

sleep 1
if [ $? -eq 0 ]; then
    process=`ps -ef | grep "${MAIN_CLASS}" | grep "\-c ${CONFIG_PATH}" | grep -v grep`
    if [ $? -ne 0 ]; then
        echo -e "Failed to start agent, cloud not find Agent process id, please check logs/${DIS_AGENT_NAME}.log or error message in console for detail information.\n"
        if [ -e "${pdir}/logs/${DIS_AGENT_NAME}.log" ]; then
            tail -50 "${pdir}/logs/${DIS_AGENT_NAME}.log" > "${pdir}/logs/agent-temp.log"
            error_num=`grep -n "ERROR" "${pdir}/logs/agent-temp.log" | tail -1 | awk -F":" '{print $1}'`
            if [ -n "${error_num}" ]; then
                tail -n +${error_num} "${pdir}/logs/agent-temp.log"
            fi
            rm -f "${pdir}/logs/agent-temp.log"
        fi
        exit 1
    fi
    pid=`echo ${process} | awk -F" " '{print $2}'`
    echo "${pid}" >> "${pdir}/agent.pid"
    echo "Success to start DIS Agent [${pid}]."
else
    echo "Failed to start DIS Agent."
fi
