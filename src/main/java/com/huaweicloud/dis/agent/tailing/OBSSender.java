package com.huaweicloud.dis.agent.tailing;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.metrics.IMetricsScope;
import com.huaweicloud.dis.agent.metrics.Metrics;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.obs.services.model.ProgressListener;
import com.obs.services.model.ProgressStatus;
import com.obs.services.model.PutObjectRequest;

import lombok.Getter;

public class OBSSender extends AbstractSender<SmallFileRecord>
{
    private static final String SERVICE_ERRORS_METRIC = "ServiceErrors";
    
    private static final String RECORDS_ATTEMPTED_METRIC = "RecordSendAttempts";
    
    private static final String RECORD_ERRORS_METRIC = "RecordSendErrors";
    
    private static final String BYTES_SENT_METRIC = "BytesSent";
    
    private static final String SENDER_NAME = "SmallFileSender";
    
    private final static DecimalFormat DF0 = new DecimalFormat("#,###");
    
    @Getter
    private final AgentContext agentContext;
    
    private final OBSFileFlow flow;
    
    private final AtomicLong totalFilesAttempted = new AtomicLong();
    
    private final AtomicLong totalFilesSent = new AtomicLong();
    
    private final AtomicLong totalFilesFailed = new AtomicLong();
    
    private final AtomicLong totalPutFilesCalls = new AtomicLong();
    
    private final AtomicLong totalPutFilesServiceErrors = new AtomicLong();
    
    private final AtomicLong totalPutFilesOtherErrors = new AtomicLong();
    
    private final AtomicLong totalPutFilesLatency = new AtomicLong();
    
    private final AtomicLong activePutFilesCalls = new AtomicLong();
    
    private final Map<String, AtomicLong> totalErrors = new HashMap<>();
    
    public OBSSender(AgentContext agentContext, OBSFileFlow flow)
    {
        Preconditions.checkNotNull(flow);
        this.agentContext = agentContext;
        this.flow = flow;
    }
    
    @Override
    protected long getMaxSendBatchSizeBytes()
    {
        return OBSConstants.MAX_PUT_RECORDS_SIZE_BYTES;
    }
    
    @Override
    protected int getMaxSendBatchSizeRecords()
    {
        return OBSConstants.MAX_PUT_RECORDS_SIZE_RECORDS;
    }
    
    @Override
    protected BufferSendResult<SmallFileRecord> attemptSend(RecordBuffer<SmallFileRecord> buffer)
    {
        activePutFilesCalls.incrementAndGet();
        IMetricsScope metrics = agentContext.beginScope();
        metrics.addDimension(Metrics.DESTINATION_DIMENSION, "OBSStream:" + getDestination());
        try
        {
            BufferSendResult<SmallFileRecord> sendResult;
            List<Integer> sentRecords = new ArrayList<>(buffer.sizeRecords());
            List<PutRecordsRequest> requestRecords = new ArrayList<>();
            long totalBytesSent = 0;
            for (int i = 0; i < buffer.sizeRecords(); i++)
            {
                SmallFileRecord data = buffer.records.get(i);
                final String filePath = data.file().getPath().toAbsolutePath().toString();
                final String fileName = data.file().getPath().toAbsolutePath().getFileName().toString();
                final String fileSize = DF0.format(data.totalLength / 1024);
                
                String obsFile = fileName;
                if (flow.isReservedSubDirectory())
                {
                    obsFile = filePath.substring(flow.getSourceFile().getDirectory().toString().length() + 1);
                    if (SystemUtils.IS_OS_WINDOWS && obsFile.contains(File.separator))
                    {
                        obsFile = obsFile.replaceAll("\\\\", "/");
                    }
                }
                // obs request
                PutObjectRequest putObjectRequest =
                    new PutObjectRequest(flow.getObsBucket(), flow.getDumpDirectory() + obsFile);
                putObjectRequest.setFile(new File(filePath));
                putObjectRequest.setProgressListener(new ProgressListener()
                {
                    @Override
                    public void progressChanged(ProgressStatus status)
                    {
                        // 获取上传进度百分比
                        logger.info("File [{}] upload progress: size [{}KB], percentage [{}%], average speed [{}KB/s]",
                            filePath,
                            fileSize,
                            status.getTransferPercentage(),
                            DF0.format(status.getAverageSpeed() / 1024));
                    }
                });
                // 每上传N*1024 KB数据反馈上传进度
                putObjectRequest.setProgressInterval(flow.getUploadProgressInterval() * 1024L);
                
                // dis request
                PutRecordsRequest request = new PutRecordsRequest();
                List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
                PutRecordsRequestEntry record = new PutRecordsRequestEntry();
                record.setData(ByteBuffer.wrap((flow.isUploadFullPath() ? filePath : fileName).getBytes()));
                record.setPartitionKey(data.partitionKey());
                putRecordsRequestEntryList.add(record);
                request.setStreamName(getDestination());
                request.setRecords(putRecordsRequestEntryList);
                
                requestRecords.add(request);
                Stopwatch timer = Stopwatch.createStarted();
                totalPutFilesCalls.incrementAndGet();
                long elapsed;
                String errorMsg = null;
                
                PutRecordsResult putRecordsResult;
                try
                {
                    logger.info("Start to put file [{} ({})] to bucket [{}] and dis [{}]",
                        filePath,
                        data.file().getId().toString(),
                        flow.getObsBucket(),
                        getDestination());
                    metrics.addCount(RECORDS_ATTEMPTED_METRIC, 1);
                    
                    // upload to obs
                    flow.getOBSClient().putObject(putObjectRequest);
                    logger.debug("put file [{} ({})] to bucket [{}] is ok.",
                        filePath,
                        data.file().getId().toString(),
                        flow.getObsBucket());
                    // upload to dis
                    if (StringUtils.isNotBlank(getDestination()))
                    {
                        putRecordsResult = agentContext.getDISClient().putRecords(request);
                        if (putRecordsResult.getFailedRecordCount().get() == 0)
                        {
                            logger.debug("put file [{} ({})] to dis [{}] is ok.",
                                filePath,
                                data.file().getId().toString(),
                                getDestination());
                            sentRecords.add(i);
                            totalBytesSent += data.file().getSize();
                        }
                    }
                    else
                    {
                        sentRecords.add(i);
                        totalBytesSent += data.file().getSize();
                    }
                }
                catch (Exception e)
                {
                    metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                    totalPutFilesOtherErrors.incrementAndGet();
                    errorMsg = e.getMessage();
                    throw e;
                }
                finally
                {
                    elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
                    totalPutFilesLatency.addAndGet(elapsed);
                    // 记录日志
                    FileFlow.RESULT_LOG_LEVEL logLevel =
                        errorMsg == null ? flow.getResultLogLevel() : FileFlow.RESULT_LOG_LEVEL.ERROR;
                    if (logLevel != FileFlow.RESULT_LOG_LEVEL.OFF)
                    {
                        String sb = new StringBuilder().append(errorMsg == null ? "Success" : "Failed")
                            .append(" to put file [")
                            .append(filePath)
                            .append(" (")
                            .append(data.file().getId().toString())
                            .append(")")
                            .append("] to obs[")
                            .append(flow.getObsBucket())
                            .append("] and dis[")
                            .append(getDestination())
                            .append("] spend ")
                            .append(elapsed)
                            .append("ms")
                            .append(errorMsg == null ? "." : ", errorMsg [" + errorMsg + "].")
                            .toString();
                        
                        switch (logLevel)
                        {
                            case DEBUG:
                                logger.debug(sb);
                                break;
                            case INFO:
                                logger.info(sb);
                                break;
                            case WARN:
                                logger.warn(sb);
                                break;
                            case ERROR:
                                logger.error(sb);
                                break;
                        }
                    }
                }
            }
            
            if (sentRecords.size() == requestRecords.size())
            {
                sendResult = BufferSendResult.succeeded(buffer);
            }
            else
            {
                buffer = buffer.remove(sentRecords);
                sendResult = BufferSendResult.succeeded_partially(buffer, requestRecords.size());
            }
            
            metrics.addData(BYTES_SENT_METRIC, totalBytesSent, StandardUnit.Bytes);
            int failedRecordCount = requestRecords.size() - sentRecords.size();
            metrics.addCount(RECORD_ERRORS_METRIC, failedRecordCount);
            logger.debug("{}:{} Records sent to dis stream {}: {}. Failed records: {}",
                flow.getId(),
                buffer,
                getDestination(),
                sentRecords.size(),
                failedRecordCount);
            totalFilesAttempted.addAndGet(requestRecords.size());
            totalFilesSent.addAndGet(sentRecords.size());
            totalFilesFailed.addAndGet(failedRecordCount);
            
            return sendResult;
        }
        finally
        {
            metrics.commit();
            activePutFilesCalls.decrementAndGet();
        }
    }
    
    @Override
    public String getDestination()
    {
        return flow.getDestination();
    }
    
    @SuppressWarnings("serial")
    @Override
    public Map<String, Object> getMetrics()
    {
        return new HashMap<String, Object>()
        {
            {
                put(SENDER_NAME + ".TotalFilesAttempted", totalFilesAttempted);
                put(SENDER_NAME + ".TotalFilesSent", totalFilesSent);
                put(SENDER_NAME + ".TotalFilesFailed", totalFilesFailed);
                put(SENDER_NAME + ".TotalPutFilesCalls", totalPutFilesCalls);
                put(SENDER_NAME + ".TotalPutFilesServiceErrors", totalPutFilesServiceErrors);
                put(SENDER_NAME + ".TotalPutFilesOtherErrors", totalPutFilesOtherErrors);
                put(SENDER_NAME + ".TotalPutFilesLatency", totalPutFilesLatency);
                put(SENDER_NAME + ".ActivePutFilesCalls", activePutFilesCalls);
                for (Entry<String, AtomicLong> err : totalErrors.entrySet())
                {
                    put(SENDER_NAME + ".Error(" + err.getKey() + ")", err.getValue());
                }
            }
        };
    }
}
