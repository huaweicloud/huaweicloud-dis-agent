package com.huaweicloud.dis.agent.processing.parsers;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.huaweicloud.dis.agent.config.ConfigurationException;
import com.huaweicloud.dis.agent.processing.exceptions.LogParsingException;
import com.huaweicloud.dis.agent.processing.interfaces.ILogParser;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;

/**
 * Base class for parsing log entries given fields.
 * <p>
 * TODO: support functions like filtering/adding/removing fields.
 */
public abstract class BaseLogParser implements ILogParser
{
    
    /**
     * The pattern used to filter the log entries
     */
    protected Pattern logEntryPattern;
    
    /**
     * The field names contained by each log entry
     * <p>
     * customField must be specified in the configuration if customer choose to use their own pattern to filter logs
     * <p>
     * customField can also be used to override the default field names in the pre-defined schemas (e.g.
     * COMMONAPACHELOG, COMBINEDAPACHELOG)
     */
    protected List<String> fields;
    
    public BaseLogParser()
    {
    }
    
    public BaseLogParser(String pattern, List<String> fields)
    {
        setPattern(pattern);
        setFields(fields);
    }
    
    public BaseLogParser(ProcessingUtilsFactory.LogFormat format, String matchPattern, List<String> customFields)
    {
        // if matchPattern is specified, customFieldNames must be specified as well
        if (matchPattern != null && customFields == null)
        {
            throw new ConfigurationException("matchPattern must be specified when customFieldNames is specified");
        }
        if (matchPattern != null)
        {
            setPattern(matchPattern);
            setFields(customFields);
            return;
        }
        // if matchPattern is not specified, use default format
        initializeByDefaultFormat(format);
        // if customFieldNames is specified to override the default schema, we do a sanity check
        if (customFields != null)
        {
            if (customFields.size() != getFields().size())
            {
                throw new ConfigurationException("Invalid custom field names for the selected log format. "
                    + "Please modify the field names or specify the matchPattern");
            }
            setFields(customFields);
        }
    }
    
    public List<String> getFields()
    {
        return fields;
    }
    
    public void setFields(List<String> fields)
    {
        this.fields = fields;
    }
    
    public String getPattern()
    {
        return logEntryPattern.pattern();
    }
    
    @Override
    public void setPattern(String pattern)
    {
        if (pattern == null)
        {
            throw new ConfigurationException("logPattern cannot be null");
        }
        this.logEntryPattern = Pattern.compile(pattern);
    }
    
    @Override
    public abstract Map<String, Object> parseLogRecord(String record, List<String> fields)
        throws LogParsingException;
    
    protected abstract void initializeByDefaultFormat(ProcessingUtilsFactory.LogFormat format);
}
