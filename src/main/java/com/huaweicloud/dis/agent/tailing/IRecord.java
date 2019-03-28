package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;

public interface IRecord
{
    public ByteBuffer data();
    
    public long dataLength();
    
    public long lengthWithOverhead();
    
    public long length();
    
    public TrackedFile file();
    
    public long endOffset();
    
    public long startOffset();
    
    public boolean shouldSkip();
    
    /**
     * This method should make sure the truncated record has the appropriate terminator (e.g. newline).
     */
    public void truncate();
    
    /**
     * @return A string representation of the data in this record, encoded with UTF-8; use for debugging only please.
     */
    @Override
    public String toString();
}
