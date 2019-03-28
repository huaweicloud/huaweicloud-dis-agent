package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;

import com.huaweicloud.dis.agent.ByteBuffers;

/**
 * Returns one record per line. TODO: Limitation of this splitter is that it requires a newline at the end of the file,
 * otherwise it will miss the last record. We should be able to handle this at the level of the {@link IParser}
 * implementation. TODO: Should we parametrize the line delimiter?
 */
public class SingleLineSplitter implements ISplitter
{
    public final char delimiter;
    
    public SingleLineSplitter(char delimiter)
    {
        this.delimiter = delimiter;
    }
    
    @Override
    public int locateNextRecord(ByteBuffer buffer)
    {
        // TODO: Skip empty records, commented records, header lines, etc...
        // (based on FileFlow configuration?).
        return ByteBuffers.advanceBufferToNextLine(buffer, delimiter);
    }
}
