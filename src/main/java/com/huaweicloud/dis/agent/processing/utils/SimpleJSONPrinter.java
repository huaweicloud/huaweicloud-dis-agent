package com.huaweicloud.dis.agent.processing.utils;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IJSONPrinter;

/**
 * Simple implementation to write a record map into a single-line JSON string
 */
public class SimpleJSONPrinter implements IJSONPrinter
{
    
    @Override
    public String writeAsString(Map<String, Object> recordMap)
        throws DataConversionException
    {
        try
        {
            return new ObjectMapper().writeValueAsString(recordMap);
        }
        catch (JsonProcessingException e)
        {
            throw new DataConversionException("Unable to convert record map to JSON string", e);
        }
    }
    
}
