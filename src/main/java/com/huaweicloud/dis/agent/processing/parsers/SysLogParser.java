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
 * Class for parsing and transforming records of sys log files
 * <p>
 * Syslog format can vary a lot across platforms depending on the configurations. We are using the most typical form
 * which is composed of:
 * <p>
 * timestamp, hostname, program, processid, message
 */
public class SysLogParser extends BaseLogParser
{
    
    /**
     * The fields below are present in most syslogs
     * <p>
     * TODO: facility is currently omitted because it varies across platforms
     */
    public static final List<String> SYSLOG_FIELDS =
        ImmutableList.of("timestamp", "hostname", "program", "processid", "message");
    
    public static final Pattern BASE_SYSLOG_PATTERN = Pattern.compile(PatternConstants.SYSLOG_BASE);
    
    public static final Pattern RFC3339_SYSLOG_PATTERN = Pattern.compile(PatternConstants.RFC3339_SYSLOG_BASE);
    
    public SysLogParser(ProcessingUtilsFactory.LogFormat format, String matchPattern, List<String> customFields)
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
            // FIXME: what do we deal with the field that's missing?
            // shall we pass in as null or don't even pass in the result?
            recordMap.put(fields.get(i), matcher.group(i + 1));
        }
        
        return recordMap;
    }
    
    @Override
    protected void initializeByDefaultFormat(ProcessingUtilsFactory.LogFormat format)
    {
        switch (format)
        {
            case SYSLOG:
                this.logEntryPattern = BASE_SYSLOG_PATTERN;
                this.fields = SYSLOG_FIELDS;
                return;
            case RFC3339SYSLOG:
                this.logEntryPattern = RFC3339_SYSLOG_PATTERN;
                this.fields = SYSLOG_FIELDS;
                return;
            default:
                throw new ConfigurationException("Log format is not accepted");
        }
    }
    
}
