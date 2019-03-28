package com.huaweicloud.dis.agent.tailing;

import lombok.Getter;
import lombok.ToString;

/**
 * The result of a {@link ISender#sendBuffer(RecordBuffer)} call.
 *
 * @param <R> The record type.
 */
@ToString
public class BufferSendResult<R extends IRecord>
{
    public static <R extends IRecord> BufferSendResult<R> succeeded(RecordBuffer<R> buffer)
    {
        return new BufferSendResult<R>(Status.SUCCESS, buffer, buffer.sizeRecords());
    }
    
    public static <R extends IRecord> BufferSendResult<R> succeeded_partially(RecordBuffer<R> retryBuffer,
        int originalRecordCount)
    {
        return new BufferSendResult<R>(Status.PARTIAL_SUCCESS, retryBuffer, originalRecordCount);
    }
    
    @Getter
    private final RecordBuffer<R> buffer;
    
    @Getter
    private final int originalRecordCount;
    
    @Getter
    private final Status status;
    
    private BufferSendResult(Status status, RecordBuffer<R> buffer, int originalRecordCount)
    {
        this.buffer = buffer;
        this.originalRecordCount = originalRecordCount;
        this.status = status;
    }
    
    public int sentRecordCount()
    {
        return status == Status.SUCCESS ? originalRecordCount : (originalRecordCount - buffer.sizeRecords());
    }
    
    public int remainingRecordCount()
    {
        return status == Status.SUCCESS ? 0 : buffer.sizeRecords();
    }
    
    public static enum Status
    {
        SUCCESS, PARTIAL_SUCCESS,
    }
}
