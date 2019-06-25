#!/bin/bash -l

is_bash=`ps -p $$ | grep bash | wc -l`
if [ ${is_bash} -eq 0 ]; then
	echo "Please use bash to stop Agent, e.g. bash bin/`basename $0`"
	exit 1
fi

while getopts 'n:' opt; do
    case $opt in
        n)
            agent_name="$OPTARG";;
        ?)
            echo -e "Usage: `basename $0` -n agent_name"
            exit
    esac
done

pdir=$(cd `dirname $0`;cd ../;pwd)
cd "${pdir}"

MAIN_CLASS="com.huaweicloud.dis.agent.Agent"

stop_pid=""
if [ -z "${agent_name}" ]; then
    agent_name=""
else
    agent_name="\-n ${agent_name}\$"
fi

process_num=`ps -ef | grep "${MAIN_CLASS}" | grep "${agent_name}" | grep -v grep | wc -l`
if [ ${process_num} -eq 0 ]; then
    pid=""
elif [ ${process_num} -gt 1 ]; then
    /bin/echo "Find multi DIS Agent process."
    num=0
    /bin/echo -e "Num\tPID \tName\t\tProcess"
    for i in `ps -ef | grep "${MAIN_CLASS}" | grep "${agent_name}" | grep -v grep | awk '{print $2}'`
    do
        pid_arr[$num]=${i}
        /bin/echo -e "${num}\t${i}\t`ps -eo pid,cmd| grep ${i} | grep -v grep | awk '{print $NF"\t"$(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'`"
        num=`expr ${num} + 1`;
    done

    read -p "Please enter the Num to stop the corresponding process: " CONFIRM_NUM

    if [ -z "${CONFIRM_NUM}" ]; then
        /bin/echo "Exit without selecting Num."
        exit
    fi
    pid=${pid_arr[$CONFIRM_NUM]}
else
    pid=`ps -ef | grep "${MAIN_CLASS}" | grep "${agent_name}" | grep -v grep | awk '{print $2}'`
fi

if [ -z "${pid}" ]; then
    /bin/echo "Failed to stop DIS Agent: cloud not find process."
    exit 1
fi

kill ${pid}

/bin/echo -n "Stopping DIS Agent [${pid}]."
calcSeconds=`date +"%s"`
while true;
do
    sleep 0.5
    process=`ps -eo pid,cmd| grep ${pid} | grep -v grep`
    if [ $? -eq 0 ]; then
        elapsed=$(expr `date +"%s"` - ${calcSeconds})
        if [ ${elapsed} -gt 20 ]; then
            /bin/echo -n " force stop"
            kill -9 ${pid}
        else
            /bin/echo -n "."
        fi
    else
        if [ -e "${pdir}/agent.pid" ]; then
            sed -i "/^${pid}$/d" "${pdir}/agent.pid"
        fi

        if [ `ps -ef | grep "${MAIN_CLASS}" | grep -v grep | wc -l` -eq 0 ]; then
            # clean sqlite so
            rm -f "${pdir}/lib/"sqlite*-libsqlitejdbc.so
            true > "${pdir}/agent.pid"
        fi

        /bin/echo " Successfully."
        exit 0
    fi
done