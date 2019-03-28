package com.huaweicloud.dis.agent.processing.interfaces;

import java.util.List;
import java.util.Map;

import com.huaweicloud.dis.agent.processing.exceptions.LogParsingException;

public interface ILogParser
{
    
    public void setPattern(String pattern);
    
    public void setFields(List<String> fields);
    
    /**
     * Parse the record by pairing the values in the record with the given fields
     *
     * @param record
     * @param fields
     * @return
     * @throws LogParsingException
     */
    public Map<String, Object> parseLogRecord(String record, List<String> fields)
        throws LogParsingException;
}
