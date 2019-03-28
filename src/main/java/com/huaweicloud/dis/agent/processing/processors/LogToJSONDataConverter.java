package com.huaweicloud.dis.agent.processing.processors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.huaweicloud.dis.agent.ByteBuffers;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.processing.exceptions.DataConversionException;
import com.huaweicloud.dis.agent.processing.exceptions.LogParsingException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import com.huaweicloud.dis.agent.processing.interfaces.IJSONPrinter;
import com.huaweicloud.dis.agent.processing.interfaces.ILogParser;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;

/**
 * Parse the log entries from log file, and convert the log entries into JSON.
 * <p>
 * Configuration of this converter looks like: { "optionName": "LOGTOJSON", "logFormat": "COMMONAPACHELOG",
 * "matchPattern": "OPTIONAL_REGEX", "customFieldNames": [ "column1", "column2", ... ] }
 */
public class LogToJSONDataConverter extends BaseDataConverter
{
    
    private List<String> fields;
    
    private ILogParser logParser;
    
    private IJSONPrinter jsonProducer;
    
    public LogToJSONDataConverter(Configuration config)
    {
        super(config);
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);
        logParser = ProcessingUtilsFactory.getLogParser(config);
        if (config.containsKey(ProcessingUtilsFactory.CUSTOM_FIELDS_KEY))
        {
            fields = config.readList(ProcessingUtilsFactory.CUSTOM_FIELDS_KEY, String.class);
        }
    }
    
    @Override
    public ByteBuffer convert(ByteBuffer data)
        throws DataConversionException
    {
        String dataStr = ByteBuffers.toString(data, charset);
        boolean hasLineBreak = false;
        // Preserve the NEW_LINE at the end of the JSON record
        if (dataStr.endsWith(IDataConverter.NEW_LINE))
        {
            hasLineBreak = true;
            dataStr = dataStr.substring(0, (dataStr.length() - IDataConverter.NEW_LINE.length()));
        }
        
        Map<String, Object> recordMap;
        
        try
        {
            recordMap = logParser.parseLogRecord(dataStr, fields);
        }
        catch (LogParsingException e)
        {
            // ignore the record if a LogParsingException is thrown
            // the record is filtered out in this case
            return null;
        }
        
        String dataJson = jsonProducer.writeAsString(recordMap);
        if (hasLineBreak)
        {
            dataJson += IDataConverter.NEW_LINE;
        }
        return ByteBuffer.wrap(dataJson.getBytes(charset));
    }
}
