package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;

import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;

public class BracketsDataConverter extends BaseDataConverter
{
    public BracketsDataConverter(Configuration config)
    {
        super(config);
    }
    
    @Override
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException
    {
        final byte[] dataBin = new byte[data.remaining()];
        data.get(dataBin);
        
        StringBuilder sb = new StringBuilder(2 + dataBin.length);
        sb.append('{').append(new String(dataBin, charset)).append('}');
        
        return ByteBuffer.wrap(sb.toString().getBytes());
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
