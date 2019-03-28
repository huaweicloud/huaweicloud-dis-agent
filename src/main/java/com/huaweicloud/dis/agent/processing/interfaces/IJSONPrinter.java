package com.huaweicloud.dis.agent.processing.interfaces;

import java.util.Map;

import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;

public interface IJSONPrinter
{
    
    /**
     * Write a map into a readable string in JSON format
     *
     * @param recordMap
     * @return
     * @throws DataConversionException
     */
    public String writeAsString(Map<String, Object> recordMap)
        throws DataConversionException;
}
