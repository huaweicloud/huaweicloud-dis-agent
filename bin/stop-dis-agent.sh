#!/bin/bash -l

pdir=$(cd `dirname $0`;cd ../;pwd)
cd "${pdir}"

MAIN_CLASS="com.huaweicloud.dis.agent.Agent"

process=`ps -ef | grep "${MAIN_CLASS}" | grep "${pdir}/conf/agent.yml" | grep -v grep`
if [ $? -ne 0 ]; then
    echo "Cloud not find DIS Agent process."
    exit 1
fi

pid=`echo ${process} | awk -F" " '{print $2}'`
kill ${pid}

/bin/echo -n "Stopping DIS Agent [${pid}]."
calcSeconds=`date +"%s"`
while true;
do
    sleep 0.5
    process=`ps -ef | grep "${MAIN_CLASS}" | grep "${pdir}/conf/agent.yml" | grep -v grep`
    if [ $? -eq 0 ]; then
        elapsed=$(expr `date +"%s"` - ${calcSeconds})
        if [ ${elapsed} -gt 15 ]; then
            /bin/echo -n " force stop"
            kill -9 ${pid}
            # clean sqlite so
            rm -f "${pdir}/lib/sqlite*-libsqlitejdbc.so"
        else
            /bin/echo -n "."
        fi
    else
        rm -f "${pdir}/agent.pid"
        echo " Successfully."
        exit 0
    fi
done