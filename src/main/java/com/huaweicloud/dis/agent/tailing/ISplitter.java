package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;

/**
 * Encapsulates the logic to split a buffer into records. Implementation will define what a record is and how it's
 * delimited.
 */
public interface ISplitter
{
    /**
     * Advances the buffer to the beginning of the next record, or to the end of the buffer if a new record was not
     * found.
     *
     * @param buffer
     * @return The position of the next record in the buffer, or {@code -1} if the beginning of the record was not found
     *         before the end of the buffer.
     */
    public int locateNextRecord(ByteBuffer buffer);
}
