package com.huaweicloud.dis.agent.processing.utils;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.interfaces.IJSONPrinter;

/**
 * Write the given map in pretty printed JSON string
 */
public class PrettyJSONPrinter implements IJSONPrinter
{
    
    @Override
    public String writeAsString(Map<String, Object> recordMap)
        throws DataConversionException
    {
        try
        {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(recordMap);
        }
        catch (JsonProcessingException e)
        {
            throw new DataConversionException("Unable to convert record map to JSON string", e);
        }
    }
    
}
