package com.huaweicloud.dis.agent.tailing;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.metrics.IMetricsScope;
import com.huaweicloud.dis.agent.metrics.Metrics;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;

import lombok.Getter;

public class DISSender extends AbstractSender<DISRecord>
{
    private static final String SERVICE_ERRORS_METRIC = "ServiceErrors";
    
    private static final String RECORDS_ATTEMPTED_METRIC = "RecordSendAttempts";
    
    private static final String RECORD_ERRORS_METRIC = "RecordSendErrors";
    
    private static final String BYTES_SENT_METRIC = "BytesSent";
    
    private static final String SENDER_NAME = "DISSender";
    
    @Getter
    private final AgentContext agentContext;
    
    private final DISFileFlow flow;
    
    private final AtomicLong totalRecordsAttempted = new AtomicLong();
    
    private final AtomicLong totalRecordsSent = new AtomicLong();
    
    private final AtomicLong totalRecordsFailed = new AtomicLong();
    
    private final AtomicLong totalPutRecordsCalls = new AtomicLong();
    
    private final AtomicLong totalPutRecordsServiceErrors = new AtomicLong();
    
    private final AtomicLong totalPutRecordsOtherErrors = new AtomicLong();
    
    private final AtomicLong totalPutRecordsLatency = new AtomicLong();
    
    private final AtomicLong activePutRecordsCalls = new AtomicLong();
    
    private final Map<String, AtomicLong> totalErrors = new HashMap<>();
    
    private final Map<String, Long> shardIdSequenceNumberMap = new ConcurrentHashMap<>();
    
    public DISSender(AgentContext agentContext, DISFileFlow flow)
    {
        Preconditions.checkNotNull(flow);
        this.agentContext = agentContext;
        this.flow = flow;
    }
    
    @Override
    protected long getMaxSendBatchSizeBytes()
    {
        return DISConstants.MAX_PUT_RECORDS_SIZE_BYTES;
    }
    
    @Override
    protected int getMaxSendBatchSizeRecords()
    {
        return DISConstants.MAX_PUT_RECORDS_SIZE_RECORDS;
    }
    
    @Override
    protected BufferSendResult<DISRecord> attemptSend(RecordBuffer<DISRecord> buffer)
    {
        activePutRecordsCalls.incrementAndGet();
        IMetricsScope metrics = agentContext.beginScope();
        metrics.addDimension(Metrics.DESTINATION_DIMENSION, "DISStream:" + getDestination());
        try
        {
            BufferSendResult<DISRecord> sendResult;
            List<PutRecordsRequestEntry> requestRecords = new ArrayList<>();
            for (DISRecord data : buffer)
            {
                PutRecordsRequestEntry record = new PutRecordsRequestEntry();
                record.setData(data.data());
                record.setPartitionKey(data.partitionKey());
                requestRecords.add(record);
            }
            PutRecordsRequest request = new PutRecordsRequest();
            request.setStreamName(getDestination());
            request.setRecords(requestRecords);
            PutRecordsResult result;
            Stopwatch timer = Stopwatch.createStarted();
            totalPutRecordsCalls.incrementAndGet();
            long elapsed;
            try
            {
                logger.trace("{}: Sending buffer {} to dis stream {}...", flow.getId(), buffer, getDestination());
                metrics.addCount(RECORDS_ATTEMPTED_METRIC, requestRecords.size());
                result = agentContext.getDISClient().putRecords(request);
                metrics.addCount(SERVICE_ERRORS_METRIC, 0);
            }
            catch (DISClientException e)
            {
                metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                totalPutRecordsServiceErrors.incrementAndGet();
                throw e;
            }
            catch (Exception e)
            {
                metrics.addCount(SERVICE_ERRORS_METRIC, 1);
                totalPutRecordsOtherErrors.incrementAndGet();
                throw e;
            }
            finally
            {
                elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
                totalPutRecordsLatency.addAndGet(elapsed);
            }
            
            List<Integer> sentRecords = new ArrayList<>(requestRecords.size());
            Multiset<String> errors = HashMultiset.create();
            int index = 0;
            long totalBytesSent = 0;
            Map<String, Long> lastSequenceNumberMap = new HashMap<>();
            String errorMsg = null;
            for (final PutRecordsResultEntry responseEntry : result.getRecords())
            {
                final PutRecordsRequestEntry record = requestRecords.get(index);
                if (StringUtils.isNullOrEmpty(responseEntry.getErrorCode()))
                {
                    sentRecords.add(index);
                    totalBytesSent += record.getData().limit();
                    // 统计结果中shardID与最新的sequenceNumber
                    if (flow.getResultLogLevel() != FileFlow.RESULT_LOG_LEVEL.OFF)
                    {
                        Long sequenceNumber = Long.valueOf(responseEntry.getSequenceNumber());
                        String shardId = responseEntry.getPartitionId();
                        if (lastSequenceNumberMap.get(shardId) == null
                            || lastSequenceNumberMap.get(shardId).compareTo(sequenceNumber) < 0)
                        {
                            lastSequenceNumberMap.put(shardId, sequenceNumber);
                        }
                    }
                }
                else
                {
                    logger.trace("{}:{} Record {} returned error code {}: {}",
                        flow.getId(),
                        buffer,
                        index,
                        responseEntry.getErrorCode(),
                        responseEntry.getErrorMessage());
                    String errorInfo = responseEntry.getErrorCode() + " " + responseEntry.getErrorMessage();
                    errors.add(errorInfo);
                    errorMsg = errorInfo;
                }
                ++index;
            }
            shardIdSequenceNumberMap.putAll(lastSequenceNumberMap);
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
            totalRecordsAttempted.addAndGet(requestRecords.size());
            totalRecordsSent.addAndGet(sentRecords.size());
            totalRecordsFailed.addAndGet(failedRecordCount);
            
            if (logger.isDebugEnabled() && !errors.isEmpty())
            {
                synchronized (totalErrors)
                {
                    StringBuilder strErrors = new StringBuilder();
                    for (Multiset.Entry<String> err : errors.entrySet())
                    {
                        AtomicLong counter = totalErrors.computeIfAbsent(err.getElement(), k -> new AtomicLong());
                        counter.addAndGet(err.getCount());
                        if (strErrors.length() > 0)
                            strErrors.append(", ");
                        strErrors.append(err.getElement()).append(": ").append(err.getCount());
                    }
                    logger.debug("{}:{} Errors from dis stream {}: {}",
                        flow.getId(),
                        buffer,
                        flow.getDestination(),
                        strErrors.toString());
                }
            }
            
            // 记录日志
            if (flow.getResultLogLevel() != FileFlow.RESULT_LOG_LEVEL.OFF)
            {
                logShardIdSequenceNumberRecord(lastSequenceNumberMap,
                    requestRecords.size(),
                    sentRecords.size(),
                    elapsed,
                    errorMsg);
            }
            
            return sendResult;
        }
        finally
        {
            metrics.commit();
            activePutRecordsCalls.decrementAndGet();
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
                put(SENDER_NAME + ".TotalRecordsAttempted", totalRecordsAttempted);
                put(SENDER_NAME + ".TotalRecordsSent", totalRecordsSent);
                put(SENDER_NAME + ".TotalRecordsFailed", totalRecordsFailed);
                put(SENDER_NAME + ".TotalPutRecordsCalls", totalPutRecordsCalls);
                put(SENDER_NAME + ".TotalPutRecordsServiceErrors", totalPutRecordsServiceErrors);
                put(SENDER_NAME + ".TotalPutRecordsOtherErrors", totalPutRecordsOtherErrors);
                put(SENDER_NAME + ".TotalPutRecordsLatency", totalPutRecordsLatency);
                put(SENDER_NAME + ".ActivePutRecordsCalls", activePutRecordsCalls);
                for (Entry<String, AtomicLong> err : totalErrors.entrySet())
                {
                    put(SENDER_NAME + ".Error(" + err.getKey() + ")", err.getValue());
                }
                
                if (flow.getResultLogLevel() != FileFlow.RESULT_LOG_LEVEL.OFF)
                {
                    List<String> shardIDs = new ArrayList<>();
                    shardIDs.addAll(shardIdSequenceNumberMap.keySet());
                    Collections.sort(shardIDs);
                    for (String shardID : shardIDs)
                    {
                        put(SENDER_NAME + ".LatestSequenceNumber(ShardId=" + shardID + ")",
                            shardIdSequenceNumberMap.get(shardID));
                    }
                }
            }
        };
    }
    
    private void logShardIdSequenceNumberRecord(Map<String, Long> lastSequenceNumberMap, int size, int success,
        long elapsed, String errorMsg)
    {
        String sequenceNumberInfos = "[]";
        if (lastSequenceNumberMap.size() > 0)
        {
            StringBuilder sb = new StringBuilder("[");
            List<String> strings = new ArrayList<>(lastSequenceNumberMap.keySet());
            Collections.sort(strings);
            for (String key : strings)
            {
                sb.append("ShardId: ")
                    .append(key)
                    .append(", SequenceNumber: ")
                    .append(lastSequenceNumberMap.get(key))
                    .append("|");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
            sequenceNumberInfos = sb.toString();
        }
        
        String log = "Put " + size + " records to [" + getDestination() + "] spend " + elapsed
            + "ms, latestSequenceNumber " + sequenceNumberInfos;
        if (size > success)
        {
            log += ", currentFailedCount " + (size - success) + ", failure info [" + errorMsg + "]";
        }
        switch (flow.getResultLogLevel())
        {
            case DEBUG:
                logger.debug(log);
                break;
            case INFO:
                logger.info(log);
                break;
            case WARN:
                logger.warn(log);
                break;
            case ERROR:
                logger.error(log);
                break;
        }
    }
}
