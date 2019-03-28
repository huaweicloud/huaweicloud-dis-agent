package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.agent.ByteBuffers;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;

/**
 * Replace the newline with a whitespace Remove leading and trailing spaces for each line
 * <p>
 * Configuration looks like:
 * <p>
 * { "optionName": "SINGLELINE" }
 */
public class SingleLineDataConverter extends BaseDataConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleLineDataConverter.class);
    
    public SingleLineDataConverter(Configuration config)
    {
        super(config);
    }
    
    @Override
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException
    {
        String dataStr = ByteBuffers.toString(data, charset);
        
        return ByteBuffer.wrap(dataStr.getBytes(charset));
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
