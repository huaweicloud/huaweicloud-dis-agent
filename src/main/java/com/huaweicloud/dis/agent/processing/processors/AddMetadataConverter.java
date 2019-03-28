package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import com.huaweicloud.dis.agent.ByteBuffers;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import com.huaweicloud.dis.agent.processing.interfaces.IJSONPrinter;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;

/**
 * Build record as JSON object with a "metadata" key for arbitrary KV pairs and "message" key with the raw data
 * <p>
 * Remove leading and trailing spaces for each line
 * <p>
 * Configuration looks like:
 * <p>
 * { "optionName": "ADDMETADATA", "metadata": { "key": "value", "foo": { "bar": "baz" } } }
 */
public class AddMetadataConverter extends BaseDataConverter
{
    
    private Object metadata;
    
    private final IJSONPrinter jsonProducer;
    
    public AddMetadataConverter(Configuration config)
    {
        super(config);
        metadata = config.getConfigMap().get("metadata");
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);
    }
    
    @Override
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException
    {
        
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        String dataStr = ByteBuffers.toString(data, charset);
        boolean hasLineBreak = false;
        if (dataStr.endsWith(IDataConverter.NEW_LINE))
        {
            hasLineBreak = true;
            dataStr = dataStr.substring(0, (dataStr.length() - IDataConverter.NEW_LINE.length()));
        }
        
        recordMap.put("metadata", metadata);
        recordMap.put("data", dataStr);
        
        String dataJson = jsonProducer.writeAsString(recordMap);
        if (hasLineBreak)
        {
            dataJson += IDataConverter.NEW_LINE;
        }
        return ByteBuffer.wrap(dataJson.getBytes(charset));
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
