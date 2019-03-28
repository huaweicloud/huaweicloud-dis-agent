package com.huaweicloud.dis.agent.tailing;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SmallFileParser extends AbstractParser<SmallFileRecord>
{
    private final AtomicLong totalFilesParsed = new AtomicLong();
    
    private final AtomicLong totalFilesBytesConsumed = new AtomicLong();
    
    private final AtomicLong totalFilesProcessed = new AtomicLong();
    
    private final AtomicLong totalFilesSkipped = new AtomicLong();
    
    public SmallFileParser(FileFlow<SmallFileRecord> flow)
    {
        super(flow);
    }
    
    public SmallFileParser(FileFlow<SmallFileRecord> flow, int bufferSize)
    {
        super(flow, bufferSize);
    }
    
    @Override
    protected synchronized SmallFileRecord buildRecord(TrackedFile recordFile, ByteBuffer data, long offset)
    {
        return new SmallFileRecord(recordFile);
    }
    
    @Override
    protected int getMaxRecordSize()
    {
        return flow.getMaxRecordSizeBytes();
    }
    
    @Override
    public synchronized SmallFileRecord readRecord()
    {
        if (currentFile != null)
        {
            totalFilesParsed.incrementAndGet();
            totalFilesBytesConsumed.addAndGet(currentFile.getSize());
            totalFilesProcessed.incrementAndGet();
            return new SmallFileRecord(currentFile);
        }
        return null;
    }
    
    @Override
    public synchronized boolean startParsingFile(TrackedFile file)
    {
        currentFile = file;
        return true;
    }
    
    @Override
    public synchronized boolean continueParsingWithFile(TrackedFile file)
    {
        if (file != null && file.isOpen())
        {
            file.close();
        }
        return true;
    }
    
    @Override
    public synchronized boolean switchParsingToFile(TrackedFile file)
    {
        return true;
    }
    
    @Override
    public synchronized boolean stopParsing(String reason)
    {
        
        currentFile = null;
        return true;
    }
    
    @SuppressWarnings("serial")
    @Override
    public Map<String, Object> getMetrics()
    {
        super.getMetrics();
        final String className = getClass().getSimpleName();
        return new HashMap<String, Object>()
        {
            {
                put(className + ".TotalFilesParsed", totalFilesParsed);
                put(className + ".TotalFilesBytesConsumed", totalFilesBytesConsumed);
                put(className + ".TotalFilesProcessed", totalFilesProcessed);
                put(className + ".TotalFilesSkipped", totalFilesSkipped);
            }
        };
    }
}
