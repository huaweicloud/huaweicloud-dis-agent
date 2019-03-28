package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class DISRecord extends AbstractRecord
{
    protected final String partitionKey;
    
    public DISRecord(TrackedFile file, long offset, ByteBuffer data)
    {
        super(file, offset, data);
        Preconditions.checkNotNull(file);
        partitionKey = generatePartitionKey(((DISFileFlow)file.getFlow()).getPartitionKeyOptionList());
    }
    
    public DISRecord(TrackedFile file, long offset, byte[] data)
    {
        super(file, offset, data);
        Preconditions.checkNotNull(file);
        partitionKey = generatePartitionKey(((DISFileFlow)file.getFlow()).getPartitionKeyOptionList());
    }
    
    public String partitionKey()
    {
        return partitionKey;
    }
    
    @Override
    public long lengthWithOverhead()
    {
        return length() + DISConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    protected int getMaxDataSize()
    {
        return file.getFlow().getMaxRecordSizeBytes();
    }
    
    @VisibleForTesting
    String generatePartitionKey(List<String> options)
    {
        String[] keys = new String[options.size()];
        for (int i = 0; i < options.size(); i++)
        {
            keys[i] = generatePartitionKey(options.get(i));
        }
        return StringUtils.join(keys, DISConstants.PARTITION_KEY_SPLIT);
    }
}
