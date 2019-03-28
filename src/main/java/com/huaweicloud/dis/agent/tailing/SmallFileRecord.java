package com.huaweicloud.dis.agent.tailing;

import com.google.common.base.Preconditions;

public class SmallFileRecord extends AbstractRecord
{
    protected final String partitionKey;
    
    public SmallFileRecord(TrackedFile file)
    {
        super(file, 0, file.size);
        Preconditions.checkNotNull(file);
        partitionKey = generatePartitionKey(file.getFlow().getPartitionKeyOption());
    }
    
    public String partitionKey()
    {
        return partitionKey;
    }
    
    @Override
    public long lengthWithOverhead()
    {
        return length() + SmallFileConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    public long length()
    {
        return file.size;
    }
    
    @Override
    protected int getMaxDataSize()
    {
        return file.getFlow().getMaxRecordSizeBytes();
    }
}
