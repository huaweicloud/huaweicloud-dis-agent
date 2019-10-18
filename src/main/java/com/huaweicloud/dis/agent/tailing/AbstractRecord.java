package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.huaweicloud.dis.agent.ByteBuffers;
import org.apache.commons.lang3.SystemUtils;

/**
 * Base implementation of an {@link IRecord} interface.
 */
public abstract class AbstractRecord implements IRecord
{
    protected boolean shouldSkip = false;
    
    protected final ByteBuffer data;
    
    protected final TrackedFile file;
    
    protected final long startOffset;
    
    protected final long totalLength;
    
    protected boolean isEndWithRecordDelimiter = false;
    
    public AbstractRecord(TrackedFile file, long offset, long totalLength, ByteBuffer data)
    {
        Preconditions.checkArgument(offset >= 0,
            "The offset of a record (%s) must be a non-negative integer (File: %s)",
            offset,
            file);
        this.file = file;
        this.startOffset = offset;
        if (data == null)
        {
            skip();
            this.data = null;
            this.totalLength = 0;
            return;
        }
        // 不使用data.remaining()，因为可能由于编码问题导致位移计算不准确(如GBK转为UTF8)
        // this.totalLength = data.remaining();
        this.totalLength = totalLength;
        // 如果结尾是分隔符 且 上传不需要保留分隔符，则去掉最后的字符
        if (totalLength > 0 && file.flow.getRecordDelimiter() == data.get(data.limit() - 1))
        {
            isEndWithRecordDelimiter = true;
            if (!file.flow.isRemainRecordDelimiter())
            {
                // Windows下需要去除\r\n，Linux下只需去除\n
                if (SystemUtils.IS_OS_WINDOWS) {
                    byte[] tmpArray = new byte[data.array().length - 2];
                    data.get(tmpArray, 0, tmpArray.length);
                    this.data = ByteBuffer.wrap(tmpArray);
                } else {
                    byte[] tmpArray = new byte[data.array().length - 1];
                    data.get(tmpArray, 0, tmpArray.length);
                    this.data = ByteBuffer.wrap(tmpArray);
                }
                return ;
            }
        }
        this.data = data;
    }
    
    public AbstractRecord(TrackedFile file, long offset, long totalLength)
    {
        Preconditions.checkArgument(offset >= 0,
            "The offset of a record (%s) must be a non-negative integer (File: %s)",
            offset,
            file);
        this.file = file;
        this.startOffset = offset;
        this.totalLength = totalLength;
        this.data = ByteBuffer.allocate(0);
    }
    
    public AbstractRecord(TrackedFile file, long offset, long totalLength, byte[] data)
    {
        this(file, offset, totalLength, ByteBuffer.wrap(data));
    }
    
    @Override
    public long dataLength()
    {
        return data == null ? 0 : data.remaining();
    }
    
    @Override
    public long length()
    {
        return dataLength();
    }
    
    @Override
    public long endOffset()
    {
        return startOffset + totalLength;
    }
    
    @Override
    public long startOffset()
    {
        return startOffset;
    }
    
    @Override
    public ByteBuffer data()
    {
        return data;
    }
    
    @Override
    public TrackedFile file()
    {
        return file;
    }
    
    @Override
    public boolean shouldSkip()
    {
        return this.shouldSkip;
    }
    
    public void skip()
    {
        this.shouldSkip = true;
    }
    
    @Override
    public void truncate()
    {
        if (length() > file.getFlow().getMaxRecordSizeBytes())
        {
            byte[] terminatorBytes = file.getFlow().getRecordTerminatorBytes();
            int originalPosition = data.position();
            data.limit(originalPosition + getMaxDataSize());
            if (file.flow.isRemainRecordDelimiter() && isEndWithRecordDelimiter)
            {
                // go to the position where we want to put the terminator
                data.position(originalPosition + getMaxDataSize() - terminatorBytes.length);
                // put the terminator
                // TODO:
                // We might have to handle the case where the last character of the truncated record contains
                // multiple bytes. In this case, the terminator itself might not be decoded as intended.
                data.put(terminatorBytes);
                data.position(originalPosition);
            }
        }
    }
    
    /**
     * NOTE: Use for debugging only please.
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        String strData =
            ByteBuffers.toString(data, file.getFlow().getFileEncoding()).replace("\n", "\\n").replace(")", "\\)");
        if (strData.length() > 50)
            strData = strData.substring(0, 47) + "...";
        sb.append(getClass().getSimpleName())
            .append("(file=")
            .append(file)
            .append(",startOffset=")
            .append(startOffset)
            .append(",endOffset=")
            .append(endOffset())
            .append(",length=")
            .append(length())
            .append(",lengthIncludingOverhead=")
            .append(lengthWithOverhead())
            .append(",data=(")
            .append(strData)
            .append(")")
            .append(")");
        return sb.toString();
    }
    
    /**
     * This size limit is used by {@link AbstractRecord#truncate()} to truncate the data of the record to the limit
     *
     * @return max size of the data blob
     */
    protected abstract int getMaxDataSize();
    
    @VisibleForTesting
    String generatePartitionKey(String partitionKeyOption)
    {
        if (FileFlow.PartitionKeyOption.contains(partitionKeyOption))
        {
            FileFlow.PartitionKeyOption option = FileFlow.PartitionKeyOption.valueOf(partitionKeyOption);
            if (option == FileFlow.PartitionKeyOption.RANDOM_INT)
            {
                return String.valueOf(ThreadLocalRandom.current().nextInt(1000000));
            }
            
            if (option == FileFlow.PartitionKeyOption.FILE_NAME)
            {
                return file.getPath().getFileName().toString();
            }
            
            if (option == FileFlow.PartitionKeyOption.DETERMINISTIC)
            {
                Hasher hasher = Hashing.md5().newHasher();
                hasher.putBytes(data.array());
                return hasher.hash().toString();
            }
            
            if (option == FileFlow.PartitionKeyOption.RANDOM_DOUBLE)
            {
                return String.valueOf(ThreadLocalRandom.current().nextDouble(1000000));
            }
            
            return null;
        }
        else
        {
            return partitionKeyOption;
        }
    }
}
