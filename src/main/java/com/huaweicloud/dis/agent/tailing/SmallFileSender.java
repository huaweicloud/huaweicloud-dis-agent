package com.huaweicloud.dis.agent.tailing;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.SystemUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.metrics.IMetricsScope;
import com.huaweicloud.dis.agent.metrics.Metrics;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.iface.data.request.PutFilesRequest;
import com.huaweicloud.dis.iface.data.response.PutFilesResult;

import lombok.Getter;

public class SmallFileSender extends AbstractSender<SmallFileRecord>
{
    private static final String SERVICE_ERRORS_METRIC = "ServiceErrors";
    
    private static final String RECORDS_ATTEMPTED_METRIC = "RecordSendAttempts";
    
    private static final String RECORD_ERRORS_METRIC = "RecordSendErrors";
    
    private static final String BYTES_SENT_METRIC = "BytesSent";
    
    private static final String SENDER_NAME = "SmallFileSender";
    
    @Getter
    private final AgentContext agentContext;
    
    private final SmallFileFlow flow;
    
    private final AtomicLong totalFilesAttempted = new AtomicLong();
    
    private final AtomicLong totalFilesSent = new AtomicLong();
    
    private final AtomicLong totalFilesFailed = new AtomicLong();
    
    private final AtomicLong totalPutFilesCalls = new AtomicLong();
    
    private final AtomicLong totalPutFilesServiceErrors = new AtomicLong();
    
    private final AtomicLong totalPutFilesOtherErrors = new AtomicLong();
    
    private final AtomicLong totalPutFilesLatency = new AtomicLong();
    
    private final AtomicLong activePutFilesCalls = new AtomicLong();
    
    private final Map<String, AtomicLong> totalErrors = new HashMap<>();
    
    private final Map<String, Long> shardIdSequenceNumberMap = new ConcurrentHashMap<>();
    
    public SmallFileSender(AgentContext agentContext, SmallFileFlow flow)
    {
        Preconditions.checkNotNull(flow);
        this.agentContext = agentContext;
        this.flow = flow;
    }
    
    @Override
    protected long getMaxSendBatchSizeBytes()
    {
        return SmallFileConstants.MAX_PUT_RECORDS_SIZE_BYTES;
    }
    
    @Override
    protected int getMaxSendBatchSizeRecords()
    {
        return SmallFileConstants.MAX_PUT_RECORDS_SIZE_RECORDS;
    }
    
    @Override
    protected BufferSendResult<SmallFileRecord> attemptSend(RecordBuffer<SmallFileRecord> buffer)
    {
        activePutFilesCalls.incrementAndGet();
        IMetricsScope metrics = agentContext.beginScope();
        metrics.addDimension(Metrics.DESTINATION_DIMENSION, "SmallFileStream:" + getDestination());
        try
        {
            BufferSendResult<SmallFileRecord> sendResult;
            List<PutFilesRequest> requestRecords = new ArrayList<>();
            List<Integer> sentRecords = new ArrayList<>(buffer.sizeRecords());
            long totalBytesSent = 0;
            for (int i = 0; i < buffer.sizeRecords(); i++)
            {
                SmallFileRecord data = buffer.records.get(i);
                String filePath = data.file().getPath().toAbsolutePath().toString();
                String fileName = data.file().getPath().toAbsolutePath().getFileName().toString();
                
                String obsFile = fileName;
                if (flow.isReservedSubDirectory())
                {
                    obsFile = filePath.substring(flow.getSourceFile().getDirectory().toString().length() + 1);
                    if (SystemUtils.IS_OS_WINDOWS && obsFile.contains(File.separator))
                    {
                        obsFile = obsFile.replaceAll("\\\\", "/");
                    }
                }
                
                PutFilesRequest putFilesRequest = new PutFilesRequest();
                putFilesRequest.setStreamName(flow.getDestination());
                putFilesRequest.setPartitionKey(data.partitionKey());
                putFilesRequest.setFileName(flow.getDumpDirectory() + obsFile);
                putFilesRequest.setFilePath(filePath);
                requestRecords.add(putFilesRequest);
                
                Stopwatch timer = Stopwatch.createStarted();
                totalPutFilesCalls.incrementAndGet();
                long elapsed;
                String errorMsg = null;
                try
                {
                    logger.info("Start to put file [{} ({})] to [{}]",
                        data.file().getPath().toAbsolutePath().toString(),
                        data.file().getId().toString(),
                        getDestination());
                    metrics.addCount(RECORDS_ATTEMPTED_METRIC, 1);
                    // remove small file upload at 07/2019
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
                            .append(data.file().getPath().toAbsolutePath().toString())
                            .append(" (")
                            .append(data.file().getId().toString())
                            .append(")")
                            .append("] to [")
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
