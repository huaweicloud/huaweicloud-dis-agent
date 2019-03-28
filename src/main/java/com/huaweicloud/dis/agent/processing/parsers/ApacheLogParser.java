package com.huaweicloud.dis.agent.processing.parsers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.huaweicloud.dis.agent.config.ConfigurationException;
import com.huaweicloud.dis.agent.processing.exceptions.LogParsingException;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;

/**
 * Class for parsing and transforming records of apache log files
 * <p>
 * Currently supported format (with typical configuration) Apache common log Apache combined log Apache error log
 */
public class ApacheLogParser extends BaseLogParser
{
    
    /**
     * See https://httpd.apache.org/docs/1.3/logs.html#common for reference of common log format and combined log format
     */
    public static final List<String> COMMON_LOG_FIELDS =
        ImmutableList.of("host", "ident", "authuser", "datetime", "request", "response", "bytes");
    
    public static final List<String> COMBINED_LOG_FIELDS =
        new ImmutableList.Builder<String>().addAll(COMMON_LOG_FIELDS).add("referer").add("agent").build();
    
    public static final List<String> ERROR_LOG_FIELDS =
        ImmutableList.of("timestamp", "module", "severity", "processid", "threadid", "client", "message");
    
    public static final Pattern COMMON_APACHE_LOG_ENTRY_PATTERN =
        Pattern.compile(PatternConstants.COMMON_APACHE_LOG + ".*");
    
    public static final Pattern COMBINED_APACHE_LOG_ENTRY_PATTERN =
        Pattern.compile(PatternConstants.COMBINED_APACHE_LOG + ".*");
    
    public static final Pattern APACHE_ERROR_LOG_ENTRY_PATTERN = Pattern.compile(PatternConstants.APACHE_ERROR_LOG);
    
    public ApacheLogParser(ProcessingUtilsFactory.LogFormat format, String matchPattern, List<String> customFields)
    {
        super(format, matchPattern, customFields);
    }
    
    @Override
    public Map<String, Object> parseLogRecord(String record, List<String> fields)
        throws LogParsingException
    {
        if (fields == null)
        {
            fields = getFields();
        }
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        Matcher matcher = logEntryPattern.matcher(record);
        
        if (!matcher.matches())
        {
            throw new LogParsingException("Invalid log entry given the entry pattern");
        }
        
        if (matcher.groupCount() != fields.size())
        {
            throw new LogParsingException("The parsed fields don't match the given fields");
        }
        
        for (int i = 0; i < fields.size(); i++)
        {
            String value = matcher.group(i + 1);
            
            // the field is not found if it shows as "-"
            if (value != null && value.equals("-"))
            {
                value = null;
            }
            recordMap.put(fields.get(i), value);
        }
        
        return recordMap;
    }
    
    protected void initializeByDefaultFormat(ProcessingUtilsFactory.LogFormat format)
    {
        switch (format)
        {
            case COMMONAPACHELOG:
                this.logEntryPattern = COMMON_APACHE_LOG_ENTRY_PATTERN;
                this.fields = COMMON_LOG_FIELDS;
                return;
            case COMBINEDAPACHELOG:
                this.logEntryPattern = COMBINED_APACHE_LOG_ENTRY_PATTERN;
                this.fields = COMBINED_LOG_FIELDS;
                return;
            case APACHEERRORLOG:
                this.logEntryPattern = APACHE_ERROR_LOG_ENTRY_PATTERN;
                this.fields = ERROR_LOG_FIELDS;
                return;
            default:
                throw new ConfigurationException("Log format is not accepted");
        }
    }
}
