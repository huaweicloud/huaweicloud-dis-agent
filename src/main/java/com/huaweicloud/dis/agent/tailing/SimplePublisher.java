package com.huaweicloud.dis.agent.tailing;

import com.google.common.annotations.VisibleForTesting;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.Constants;
import com.huaweicloud.dis.agent.IHeartbeatProvider;
import com.huaweicloud.dis.agent.tailing.checkpoints.Checkpointer;
import com.huaweicloud.dis.agent.tailing.checkpoints.FileCheckpointStore;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.exception.DISStreamNotExistsException;
import com.huaweicloud.dis.http.exception.HttpClientErrorException;
import com.huaweicloud.dis.http.exception.RestClientResponseException;
import com.huaweicloud.dis.http.exception.UnknownHttpStatusCodeException;
import com.obs.services.exception.ObsException;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core functionality of a publisher that buffers records into an {@link PublishingQueue} and sends them via an
 * {@link ISender} instance synchronously.
 *
 * @param <R> The record type.
 */
class SimplePublisher<R extends IRecord> implements IHeartbeatProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimplePublisher.class);
    
    protected final Logger logger;
    
    protected final String name;
    
    @VisibleForTesting
    final Checkpointer<R> checkpointer;
    
    protected final ISender<R> sender;
    
    protected volatile boolean isOpen = true;
    
    @Getter
    final AgentContext agentContext;
    
    @Getter
    final FileFlow<R> flow;
    
    @Getter
    final PublishingQueue<R> queue;
    
    private final AtomicLong sendSuccess = new AtomicLong();
    
    private final AtomicLong sendPartialSuccess = new AtomicLong();
    
    private final AtomicLong sendError = new AtomicLong();
    
    private final AtomicLong buffersDropped = new AtomicLong();
    
    private final AtomicLong totalSentBuffers = new AtomicLong();
    
    /**
     * @param agentContext
     * @param flow
     * @param checkpoints
     * @param sender
     */
    public SimplePublisher(AgentContext agentContext, FileFlow<R> flow, FileCheckpointStore checkpoints,
        ISender<R> sender)
    {
        this.logger = LoggerFactory.getLogger(getClass());
        this.agentContext = agentContext;
        this.flow = flow;
        this.queue = new PublishingQueue<>(flow, flow.getPublishQueueCapacity());
        this.sender = sender;
        this.checkpointer = new Checkpointer<>(this.flow, checkpoints);
        this.name = getClass().getSimpleName() + "[" + flow.getId() + "]";
    }
    
    public String name()
    {
        return name;
    }
    
    public void close()
    {
        isOpen = false;
        queue.close();
        queue.discardAllRecords();
    }
    
    /**
     * Returns immediately if the record could not be published because the queue is full (or if publisher is shutting
     * down).
     *
     * @param record
     * @return {@code true} if the record was successfully added to the current buffer, and {@code false} otherwise.
     */
    public boolean publishRecord(R record)
    {
        if (isOpen && queue.offerRecord(record, false))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    /**
     * Flushes any buffered records and makes them available for publishing.
     */
    public void flush()
    {
        queue.flushPendingRecords();
    }
    
    public RecordBuffer<R> pollNextBuffer(boolean block)
    {
        return queue.take(block);
    }
    
    public boolean sendNextBufferSync(boolean block)
    {
        final RecordBuffer<R> buffer = pollNextBuffer(block);
        if (buffer != null)
        {
            sendBufferSync(buffer);
            return true;
        }
        else
            return false;
    }
    
    public void sendBufferSync(RecordBuffer<R> buffer)
    {
        BufferSendResult<R> result = null;
        try
        {
            result = sender.sendBuffer(buffer);
        }
        catch (Throwable t)
        {
            onSendError(buffer, t);
            return;
        }
        LOGGER.debug("Send finish: file {}, offset {}", buffer.checkpointFile().toString(), buffer.checkpointOffset());
        totalSentBuffers.incrementAndGet();
        switch (result.getStatus())
        {
            case SUCCESS:
                onSendSuccess(buffer);
                break;
            case PARTIAL_SUCCESS:
                onSendPartialSuccess(buffer, result);
                break;
        }
    }
    
    protected boolean queueBufferForRetry(RecordBuffer<R> buffer)
    {
        if (isOpen)
        {
            if (queue.queueBufferForRetry(buffer))
            {
                logger.trace("{}:{} Buffer Queued for Retry", name(), buffer);
                return true;
            }
            else
            {
                onBufferDropped(buffer, "retry rejected by queue");
                return false;
            }
        }
        else
        {
            onBufferDropped(buffer, "retry rejected: publisher is closed");
            return false;
        }
    }
    
    protected void onBufferDropped(RecordBuffer<R> buffer, String reason)
    {
        buffersDropped.incrementAndGet();
        logger.trace("{}:{} Buffer Dropped: {}", name(), reason, buffer);
    }
    
    /**
     * This method should not raise any exceptions.
     *
     * @param buffer
     */
    protected void onSendSuccess(RecordBuffer<R> buffer)
    {
        sendSuccess.incrementAndGet();
        logger.trace("{}:{} Send SUCCESS", name(), buffer);
        try
        {
            // 成功结果批量入库
            Map<String, R> lastRecordMap = new LinkedHashMap<>();
            Iterator<R> iterator = buffer.iterator();
            R lastRecord = iterator.next();
            String lastId = lastRecord.file().getId().getId();
            while (iterator.hasNext())
            {
                R thisRecord = iterator.next();
                String thisId = thisRecord.file().getId().getId();
                if (!thisId.equals(lastId) && (lastRecordMap.get(lastId) == null
                    || lastRecordMap.get(lastId).endOffset() < lastRecord.endOffset()))
                {
                    lastRecordMap.put(lastId, lastRecord);
                }
                lastRecord = thisRecord;
                lastId = lastRecord.file().getId().getId();
            }
            if (lastRecordMap.get(lastId) == null || lastRecordMap.get(lastId).endOffset() < lastRecord.endOffset())
            {
                lastRecordMap.put(lastId, lastRecord);
            }
            
            List<IRecord> iRecords = new ArrayList<>(lastRecordMap.values());
            fileCleanPolicy(iRecords);
            checkpointer.saveCheckpoint(iRecords);
        }
        catch (Exception e)
        {
            logger.error("{}:{} Error in onSendSuccess", name(), buffer, e);
        }
    }
    
    /**
     * This method should not raise any exceptions.
     *
     * @param buffer
     * @param result
     * @return {@code true} if buffer was requed for retrying, {@code false} if not for any reason (e.g. queue is
     *         closed).
     */
    protected boolean onSendPartialSuccess(RecordBuffer<R> buffer, BufferSendResult<R> result)
    {
        sendPartialSuccess.incrementAndGet();
        logger.debug("{}:{} Send PARTIAL_SUCCESS: Sent: {}, Failed: {}",
            name(),
            buffer,
            result.sentRecordCount(),
            result.remainingRecordCount());
        return queueBufferForRetry(buffer);
    }
    
    protected boolean onSendPartialSuccessAndRetry(RecordBuffer<R> buffer, BufferSendResult<R> result)
    {
        sendPartialSuccess.incrementAndGet();
        logger.debug("{}:{} Send PARTIAL_SUCCESS: Sent: {}, Failed: {}",
            name(),
            buffer,
            result.sentRecordCount(),
            result.remainingRecordCount());
        sendBufferSync(buffer);
        return true;
    }
    
    /**
     * This method should not raise any exceptions.
     *
     * @param buffer
     * @param t
     * @return {@code true} if buffer was requed for retrying, {@code false} if not for any reason (e.g. queue is
     *         closed, error is non-retriable).
     */
    protected boolean onSendError(RecordBuffer<R> buffer, Throwable t)
    {
        sendError.incrementAndGet();
        // Retry the buffer if it's a runtime exception
        if (isRetriableSendException(t))
        {
            logger.error("{}:{} Retriable send error ({}: {}). Will retry.",
                name(),
                buffer,
                t.getClass().getName(),
                t.getMessage());

            if (t instanceof DISStreamNotExistsException)
            {
                if (!StringUtils.isNullOrEmpty(flow.getStreamId()))
                {
                    logger.error("Stream not found, please check streamId {}", flow.getStreamId());
                }
                else
                {
                    logger.error("Stream not found, please check streamName {}", flow.getDestination());
                }
                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e)
                {
                    logger.error(e.getMessage(), e);
                }
            }

            return queueBufferForRetry(buffer);
        }
        
        if (isDISAuthenticationError(t))
        {
            Throwable cause = t;
            while (cause != null && !(cause instanceof RestClientResponseException))
            {
                cause = cause.getCause();
            }
            
            logger.error("{}:{} Non-retriable send error, responseCode {}, responseMessage {}. Will NOT retry.",
                name(),
                buffer,
                cause == null ? "" : ((RestClientResponseException)cause).getRawStatusCode(),
                cause == null ? "" : ((RestClientResponseException)cause).getResponseBodyAsString(),
                t);
        }
        else
        {
            logger.error("{}:{} Non-retriable send error. Will NOT retry.", name(), buffer, t);
        }
        
        onBufferDropped(buffer, "non-retriable exception (" + t.getClass().getName() + ")");
        return false;
    }
    
    /**
     * 判断异常发生时，是否可以重传数据 (如果有DIS认证异常，或java.lang异常等，一般重传也会失败，故此时不用重传)
     *
     * @param t
     * @return
     */
    protected boolean isRetriableSendException(Throwable t)
    {
        return !isDISAuthenticationError(t) && !t.getClass().getPackage().getName().startsWith("java.lang")
            && !(t instanceof ClosedByInterruptException) && !(t instanceof UnknownHostException)
            && !(t instanceof ObsException) && (t.getCause() == null || isRetriableSendException(t.getCause()));
    }
    
    /**
     * 判断是否为DIS认证失败异常
     *
     * @param t
     * @return
     */
    protected boolean isDISAuthenticationError(Throwable t)
    {
        if (t instanceof HttpClientErrorException)
        {
            return ((HttpClientErrorException)t).getStatusCode() == HttpStatus.SC_BAD_REQUEST
                || ((HttpClientErrorException)t).getStatusCode() == HttpStatus.SC_FORBIDDEN
                || ((HttpClientErrorException)t).getStatusCode() == HttpStatus.SC_UNAUTHORIZED;
        }
        else if (t instanceof UnknownHttpStatusCodeException)
        {
            return ((UnknownHttpStatusCodeException)t).getRawStatusCode() == Constants.AUTHENTICATION_ERROR_HTTP_CODE;
        }
        return (t.getCause() != null && isDISAuthenticationError(t.getCause()));
    }
    
    @Override
    public Object heartbeat(AgentContext agent)
    {
        return queue.heartbeat(agent);
    }
    
    // Use for debugging only please.
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(").append("queue=").append(queue).append(")");
        return sb.toString();
    }
    
    public Map<String, Object> getMetrics()
    {
        Map<String, Object> metrics = queue.getMetrics();
        metrics.putAll(sender.getMetrics());
        metrics.put("SimplePublisher.TotalSentBuffers", totalSentBuffers);
        metrics.put("SimplePublisher.SendSuccess", sendSuccess);
        metrics.put("SimplePublisher.SendPartialSuccess", sendPartialSuccess);
        metrics.put("SimplePublisher.SendError", sendError);
        metrics.put("SimplePublisher.BuffersDropped", buffersDropped);
        return metrics;
    }
    
    public boolean queueCurrentBuffer(R record, TrackedFile currentFile)
    {
        return queue.queueCurrentBuffer(record, currentFile);
    }
    
    public void fileCleanPolicy(List<IRecord> records)
    {
        for (IRecord record : records)
        {
            FileFlow<?> flow = record.file().flow;
            if (flow instanceof SmallFileFlow || flow instanceof OBSFileFlow || flow instanceof DISFileFlow
                && !flow.isFileAppendable() && record.file().getSize() == record.endOffset())
            {
                try
                {
                    Path path = record.file().getPath();
                    FileId fileId = FileId.get(path);
                    if (!record.file().getId().equals(fileId))
                    {
                        logger.error("Failed to clean file {}, it's id changes from {} to {}.",
                            record.file(),
                            record.file().getId(),
                            fileId);
                        return;
                    }
                    if (Constants.DELETE_POLICY_IMMEDIATE.equals(flow.getDeletePolicy()))
                    {
                        Files.delete(path);
                        logger.info("Success to clean file [{}]", record.file());
                    }
                    else if (!StringUtils.isNullOrEmpty(flow.getFileMoveToDir()))
                    {
                        String filePath = path.toString();
                        FileUtils.moveFileToDirectory(new File(filePath), new File(flow.getFileMoveToDir()), true);
                        logger.info("Success to move file {} to {}", filePath, flow.getFileMoveToDir());
                    }
                    else if (!StringUtils.isNullOrEmpty(flow.getFileSuffix()))
                    {
                        String filePath = path.toString();
                        boolean rename = new File(filePath).renameTo(new File(filePath + flow.getFileSuffix()));
                        if (!rename)
                        {
                            logger.error("Failed to rename file {} to {}", filePath, filePath + flow.getFileSuffix());
                        }
                        else
                        {
                            logger.info("Success to rename file {} to {}", filePath, filePath + flow.getFileSuffix());
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.error("Failed to handle file " + record.file(), e);
                }
            }
        }
    }
}
