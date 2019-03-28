package com.huaweicloud.dis.agent.processing.utils;

import java.util.List;

import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.config.ConfigurationException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import com.huaweicloud.dis.agent.processing.interfaces.IJSONPrinter;
import com.huaweicloud.dis.agent.processing.interfaces.ILogParser;
import com.huaweicloud.dis.agent.processing.parsers.ApacheLogParser;
import com.huaweicloud.dis.agent.processing.parsers.SysLogParser;
import com.huaweicloud.dis.agent.processing.processors.*;

/**
 * The factory to create:
 * <p>
 * IDataConverter ILogParser IJSONPrinter
 */
public class ProcessingUtilsFactory
{
    
    public static enum DataConversionOption
    {
        ADDMETADATA, SINGLELINE, CSVTOJSON, LOGTOJSON, ADDBRACKETS
    }
    
    public static enum LogFormat
    {
        COMMONAPACHELOG, COMBINEDAPACHELOG, APACHEERRORLOG, SYSLOG, RFC3339SYSLOG
    }
    
    public static enum JSONFormat
    {
        COMPACT, PRETTYPRINT
    }
    
    public static final String CONVERSION_OPTION_NAME_KEY = "optionName";
    
    public static final String LOGFORMAT_KEY = "logFormat";
    
    public static final String MATCH_PATTERN_KEY = "matchPattern";
    
    public static final String CUSTOM_FIELDS_KEY = "customFieldNames";
    
    public static final String JSONFORMAT_KEY = "jsonFormat";

    public static final String FILE_ENCODING_KEY = "fileEncoding";
    
    public static IDataConverter getDataConverter(Configuration config)
        throws ConfigurationException
    {
        if (config == null)
        {
            return null;
        }
        
        DataConversionOption option = config.readEnum(DataConversionOption.class, CONVERSION_OPTION_NAME_KEY);
        return buildConverter(option, config);
    }
    
    public static ILogParser getLogParser(Configuration config)
        throws ConfigurationException
    {
        // format must be specified
        LogFormat format = config.readEnum(LogFormat.class, LOGFORMAT_KEY);
        List<String> customFields = null;
        if (config.containsKey(CUSTOM_FIELDS_KEY))
            customFields = config.readList(CUSTOM_FIELDS_KEY, String.class);
        String matchPattern = config.readString(MATCH_PATTERN_KEY, null);
        
        return buildLogParser(format, matchPattern, customFields);
    }
    
    public static IJSONPrinter getPrinter(Configuration config)
        throws ConfigurationException
    {
        JSONFormat format = config.readEnum(JSONFormat.class, JSONFORMAT_KEY, JSONFormat.COMPACT);
        switch (format)
        {
            case COMPACT:
                return new SimpleJSONPrinter();
            case PRETTYPRINT:
                return new PrettyJSONPrinter();
            default:
                throw new ConfigurationException("JSON format " + format.name() + " is not accepted");
        }
    }
    
    private static ILogParser buildLogParser(LogFormat format, String matchPattern, List<String> customFields)
    {
        switch (format)
        {
            case COMMONAPACHELOG:
            case COMBINEDAPACHELOG:
            case APACHEERRORLOG:
                return new ApacheLogParser(format, matchPattern, customFields);
            case SYSLOG:
            case RFC3339SYSLOG:
                return new SysLogParser(format, matchPattern, customFields);
            default:
                throw new ConfigurationException("Log format " + format.name() + " is not accepted");
        }
    }
    
    private static IDataConverter buildConverter(DataConversionOption option, Configuration config)
        throws ConfigurationException
    {
        switch (option)
        {
            case ADDMETADATA:
                return new AddMetadataConverter(config);
            case SINGLELINE:
                return new SingleLineDataConverter(config);
            case CSVTOJSON:
                return new CSVToJSONDataConverter(config);
            case LOGTOJSON:
                return new LogToJSONDataConverter(config);
            case ADDBRACKETS:
                return new BracketsDataConverter(config);
            default:
                throw new ConfigurationException("Specified option is not implemented yet: " + option);
        }
    }
}
