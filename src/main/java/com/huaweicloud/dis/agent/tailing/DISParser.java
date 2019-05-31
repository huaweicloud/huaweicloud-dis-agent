package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;

public class DISParser extends AbstractParser<DISRecord>
{
    
    public DISParser(FileFlow<DISRecord> flow)
    {
        super(flow);
    }
    
    public DISParser(FileFlow<DISRecord> flow, int bufferSize)
    {
        super(flow, bufferSize);
    }
    
    @Override
    protected synchronized DISRecord buildRecord(TrackedFile recordFile, ByteBuffer data, long offset, int length)
    {
        return new DISRecord(recordFile, offset, length, data);
    }
    
    @Override
    protected int getMaxRecordSize()
    {
        return flow.getMaxRecordSizeBytes();
    }
}
