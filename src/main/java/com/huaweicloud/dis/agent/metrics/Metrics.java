package com.huaweicloud.dis.agent.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.tailing.DISConstants;

public class Metrics implements IMetricsContext
{
    
    public static final int BYTES_BEHIND_INFO_LEVEL = 5 * DISConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES;
    
    public static final int BYTES_BEHIND_WARN_LEVEL = 10 * DISConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES;
    
    public static final Pattern SENDER_TOTAL_RECORDS_SENT_METRIC = Pattern.compile("^.*Sender.TotalRecordsSent$");
    
    public static final Pattern PARSER_TOTAL_BYTES_CONSUMED_METRIC = Pattern.compile("^.*Parser.TotalBytesConsumed$");
    
    public static final Pattern FILE_TAILER_FILES_BEHIND_METRIC = Pattern.compile("^.*FileTailer.FilesBehind$");
    
    public static final Pattern FILE_TAILER_BYTES_BEHIND_METRIC = Pattern.compile("^.*FileTailer.BytesBehind$");
    
    public static final Pattern PARSER_TOTAL_RECORDS_PARSED_METRIC = Pattern.compile("^.*Parser.TotalRecordsParsed$");
    
    public static final Pattern PARSER_TOTAL_RECORDS_PROCESSED_METRIC =
        Pattern.compile("^.*Parser.TotalRecordsProcessed$");
    
    public static final Pattern PARSER_TOTAL_RECORDS_SKIPPED_METRIC = Pattern.compile("^.*Parser.TotalRecordsSkipped$");
    
    public static final String DESTINATION_DIMENSION = "Destination";
    
    // 小文件传输指标(已解析的文件数量)
    public static final Pattern PARSER_TOTAL_FILES_PARSED_METRIC = Pattern.compile("^.*Parser.TotalFilesParsed$");
    
    public static final Pattern PARSER_TOTAL_FILES_BYTES_CONSUMED_METRIC =
        Pattern.compile("^.*Parser.TotalFilesBytesConsumed$");
    
    public static final Pattern PARSER_TOTAL_FILES_PROCESSED_METRIC = Pattern.compile("^.*Parser.TotalFilesProcessed$");
    
    public static final Pattern PARSER_TOTAL_FILES_SKIPPED_METRIC = Pattern.compile("^.*Parser.TotalFilesSkipped$");
    
    public static final Pattern SENDER_TOTAL_FILES_SENT_METRIC = Pattern.compile("^.*Sender.TotalFilesSent$");
    
    private IMetricsFactory factory;
    
    public Metrics(AgentContext context)
    {
        List<IMetricsFactory> factories = new ArrayList<>();
        if (context.logEmitMetrics() && LogMetricsScope.LOGGER.isDebugEnabled())
        {
            factories.add(new LogMetricsFactory());
        }
        
        if (factories.size() == 0)
        {
            factory = new NullMetricsFactory();
        }
        else if (factories.size() == 1)
        {
            factory = factories.get(0);
        }
        else
        {
            factory = new CompositeMetricsFactory(factories);
        }
    }
    
    @Override
    public IMetricsScope beginScope()
    {
        IMetricsScope scope = factory.createScope();
        return scope;
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T getMetric(Map<String, Object> metrics, Pattern key, T fallback)
    {
        T val = null;
        for (String metricKey : metrics.keySet())
        {
            if (key.matcher(metricKey).matches())
            {
                val = (T)metrics.get(metricKey);
                break;
            }
        }
        if (val != null)
            return val;
        else
            return fallback;
    }
    
    @SuppressWarnings({"unchecked"})
    public static <T> T getMetric(Map<String, Object> metrics, String key, T fallback)
    {
        T val = (T)metrics.get(key);
        if (val != null)
            return val;
        else
            return fallback;
    }
}