# Welcome to DIS Agent
    DIS Agent is a client-side program provided by the DIS to collect text logs and upload them securely to the DIS.

Getting Started
---

### Requirements

To get started using dis agent, you will need those things:

1. JRE 1.7 +

### Install DIS Agent
1. Using Xshell, log in to the Linux server on which the DIS Agent will be installed.
2. Upload the DIS Agent package dis-agent-1.0.0.zip to any directory, and use unzip to decompress

```
unzip dis-agent-1.0.0.zip
cd dis-agent-1.0.0
```

### Configuring DIS Agent
1. Run the following command to open the DIS Agent configuration file agent.yml. Modify
   parameters in the file according to actual needs.
```
vim conf/agent.yml
```
2. Parameters in the agent.yml file


| Name               | Description                              | Default                                  |
| :----------------- | :--------------------------------------- | :--------------------------------------- |
| region             | Region in which the DIS is located. Currently, only the cnnorth-1 region is available for selection. | cn-north-1                               |
| ak                 | User's plaintext AK. The My Credential page provides you the option to download your AK/SK file. | -                                        |
| sk                 | User's plaintext SK. The My Credential page provides you the option to download your AK/SK file. | -                                        |
| projectId          | Project ID specific to your region. The My Credential page displays all ProjectIDs. | -                                        |
| endpoint           | DIS gateway address in the format of https://IPaddress:port number. | https://dis.cn-north-1.myhuaweicloud.com |
|                    |                                          |                                          |
| DISStream          | Name of the DIS stream to which files whose names match filePattern will be uploaded. | -                                        |
| filePattern        | Name of the files to be uploaded. Currently, files to be uploaded are searched only by file name. | -                                        |
| initialPosition    | Monitoring start position. Values:  **`END_OF_FILE`** After the DIS Agent starts, it does not upload the files whose names match filePattern. Instead, file uploading starts from the newly added file or file content;  **`START_OF_FILE`** All the files whose names match filePattern will be uploaded to DIS in ascending order of file modification time. | START_OF_FILE                              |
| maxBufferAgeMillis | The maximum number of milliseconds that must elapse before files can be uploaded to the specified DIS stream. l If the buffer is full with files waiting to be uploaded, files will be immediately uploaded to the specified DIS stream. l If the buffer is not full, files will be uploaded to the specified DIS stream after the specified number of milliseconds elapses | 5000                                     |

Agent configuration Example
```yaml
---
# cloud region id
region: cn-north-1
# you ak (get from 'My Credential')
ak: LPEGNKTEDROMCE22H97Y
# you sk (get from 'My Credential')
sk: qJXUUrj8hK0dsK1HWE4KmcImMirQEvypwd8qhUNd
# you project id (get from 'My Credential')
projectId: 8c5be0a9ef014100880db32c149b610c
# the dis endpoint
endpoint: https://dis.cn-north-1.myhuaweicloud.com
# config each flow to monitor file.
flows:
  ### DIS Stream
  - DISStream: YOU_DIS_STREAM_1
    ## only support specified directory, filename can use * to match some files. eg. * means match all file, test*.log means match test1.log or test-12.log and so on.
    filePattern: /tmp/*.log
    ## from where to start: 'START_OF_FILE' or 'END_OF_FILE'
    initialPosition: START_OF_FILE
    ## upload max interval(ms)
    maxBufferAgeMillis: 5000
```

### Starting DIS Agent

Run the following command to start the DIS Agent:

```
bash bin/start-dis-agent.sh
```
If information similar to the following appears, the DIS Agent has started successfully.
```
Success to start Agent [xxxxx].
```

### Stopping DIS Agent
Run the following command to stop the DIS Agent:

```
bash bin/stop-dis-agent.sh
```
If information similar to the following appears, the DIS Agent is stopping:
```
Stopping Agent [xxxxx].....
```
In the command output, [xxxxx] indicates the process ID.
If information similar to the following appears, the DIS Agent has stopped:
```
Stopping Agent [xxxxx]............. Successfully.
```