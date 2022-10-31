#!/bin/bash -l

KEY=$1
PASS=$2


pdir=$(cd `dirname $0`;cd ../;pwd)
cd ${pdir}

echo "Please wait..."

result=`java -Dlog4j.configurationFile=conf/log4j2.xml -cp "lib/*" com.huaweicloud.dis.agent.processing.utils.EncryptTool "${PASS}" ${KEY}`

if [ $? -ne 0 ]; then
    echo "Failed to encrypt."
    exit 1
fi

echo "Encrypt result: ${result}"
