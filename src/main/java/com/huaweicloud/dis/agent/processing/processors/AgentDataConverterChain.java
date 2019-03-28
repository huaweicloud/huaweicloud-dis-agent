package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;

/**
 * Class that applies data conversion through each converter in the list in the order that's configured in config file
 */
public class AgentDataConverterChain implements IDataConverter
{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AgentDataConverterChain.class);
    
    private List<IDataConverter> dataConverters = new LinkedList<IDataConverter>();
    
    public AgentDataConverterChain(IDataConverter... dataConverters)
    {
        if (dataConverters == null || dataConverters.length == 0)
            throw new IllegalArgumentException("No data converter specified");
        
        for (IDataConverter converter : dataConverters)
        {
            this.dataConverters.add(converter);
        }
        
        LOGGER.debug("Using data converter chain: " + this.toString());
    }
    
    public AgentDataConverterChain(List<IDataConverter> dataConverters)
    {
        if (dataConverters == null || dataConverters.isEmpty())
            throw new IllegalArgumentException("No data converter specified");
        
        this.dataConverters = dataConverters;
        
        LOGGER.debug("Using data converter chain: " + this.toString());
    }
    
    public List<IDataConverter> getDataConverters()
    {
        return dataConverters;
    }
    
    @Override
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException
    {
        ByteBuffer result = data;
        
        for (IDataConverter converter : dataConverters)
        {
            if (converter != null)
            {
                try
                {
                    if (result == null)
                        return null;
                    result = converter.convert(result);
                }
                catch (Exception e)
                {
                    LOGGER.debug("Unable to convert data by " + converter.toString() + " due to " + e.getMessage());
                    throw new DataConversionException("Unable to convert data by " + converter.toString(), e);
                }
            }
        }
        
        return result;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("AgentDataConverterChain [");
        for (IDataConverter converter : dataConverters)
        {
            sb.append(converter.toString() + " ");
        }
        sb.append("]");
        
        return sb.toString();
    }
    
}
