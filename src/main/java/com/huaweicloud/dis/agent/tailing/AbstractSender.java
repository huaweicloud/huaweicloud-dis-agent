package com.huaweicloud.dis.agent.tailing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation for an {@link ISender}.
 *
 * @param <R> The record type.
 */
public abstract class AbstractSender<R extends IRecord> implements ISender<R>
{
    protected final Logger logger;
    
    public AbstractSender()
    {
        this.logger = LoggerFactory.getLogger(getClass());
    }
    
    @Override
    public BufferSendResult<R> sendBuffer(RecordBuffer<R> buffer)
    {
        if (getMaxSendBatchSizeRecords() > 0 && buffer.sizeRecords() > getMaxSendBatchSizeRecords())
        {
            throw new IllegalArgumentException("Buffer is too large for service call: " + buffer.sizeRecords()
                + " records vs. allowed maximum of " + getMaxSendBatchSizeRecords());
        }
        if (getMaxSendBatchSizeBytes() > 0 && buffer.sizeBytesWithOverhead() > getMaxSendBatchSizeBytes())
        {
            throw new IllegalArgumentException("Buffer is too large for service call: " + buffer.sizeBytesWithOverhead()
                + " bytes vs. allowed maximum of " + getMaxSendBatchSizeBytes());
        }
        return attemptSend(buffer);
    }
    
    protected abstract long getMaxSendBatchSizeBytes();
    
    protected abstract int getMaxSendBatchSizeRecords();
    
    protected abstract BufferSendResult<R> attemptSend(RecordBuffer<R> buffer);
}
