# 1.1.3

- 特性
  * DISStream 新增没有分隔符也可以上传的配置 <br>
    增加`isMissLastRecordDelimiter: true`即可启用
  * 启动脚本修改，支持-c (配置文件)，-n (唯一名称)，-s （最小JVM内存），-x （最大JVM内存）入参
  * 支持同一台服务器上启动多个Agent，每个Agent通过-n来指定唯一名称
  * 不同Agent进程根据唯一名称写不同的日志文件与Checkpoint文件
  * 监控文件删除后清理Checkpoint中对应的记录
 
