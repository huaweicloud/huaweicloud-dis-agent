package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;

public abstract class BaseDataConverter implements IDataConverter
{
    protected Configuration config;
    
    protected Charset charset;
    
    public BaseDataConverter(Configuration config)
    {
        this.config = config;
        if (config.getConfigMap().get(ProcessingUtilsFactory.FILE_ENCODING_KEY) != null)
        {
            this.charset = (Charset)config.getConfigMap().get(ProcessingUtilsFactory.FILE_ENCODING_KEY);
        }
        else
        {
            this.charset = StandardCharsets.UTF_8;
        }
    }
    
    @Override
    public abstract ByteBuffer convert(ByteBuffer data)
        throws DataConversionException;
}
