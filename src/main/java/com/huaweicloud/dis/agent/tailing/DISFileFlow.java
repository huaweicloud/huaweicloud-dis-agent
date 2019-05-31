package com.huaweicloud.dis.agent.tailing;

import com.google.common.collect.Range;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.Constants;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.config.ConfigurationException;
import com.huaweicloud.dis.agent.metrics.Metrics;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;
import com.huaweicloud.dis.agent.tailing.checkpoints.FileCheckpointStore;
import com.huaweicloud.dis.iface.data.request.StreamType;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ToString(callSuper = true)
public class DISFileFlow extends FileFlow<DISRecord>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DISFileFlow.class);
    
    public static final Range<Long> VALID_MAX_BUFFER_AGE_RANGE_MILLIS =
        Range.closed(TimeUnit.MILLISECONDS.toMillis(500), TimeUnit.MINUTES.toMillis(15));
    
    public static final Range<Integer> VALID_MAX_BUFFER_SIZE_RECORDS_RANGE =
        Range.closed(1, DISConstants.MAX_BUFFER_SIZE_RECORDS);
    
    public static final Range<Integer> VALID_MAX_BUFFER_SIZE_BYTES_RANGE =
        Range.closed(1, DISConstants.MAX_BUFFER_SIZE_BYTES);
    
    public static final Range<Long> VALID_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_RANGE =
        Range.closed(TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));
    
    public static final Range<Long> VALID_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_RANGE =
        Range.closed(TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));
    
    @Getter
    protected final String id;

    @Getter
    protected final String destination;
    
    @Getter
    protected final List<String> partitionKeyOptionList;
    
    @Getter
    protected final String streamType;
    
    @Getter
    protected final int maxRecordSizeBytes;
    
    public DISFileFlow(AgentContext context, Configuration config)
    {
        super(context, config);
        destination = readString(DISConstants.DESTINATION_KEY);
        id = "dis:" + destination + ":" + sourceFile.toString();
        partitionKeyOptionList = new ArrayList<>();
        if (partitionKeyOption != null)
        {
            for (String option : this.partitionKeyOption.split(DISConstants.PARTITION_KEY_SPLIT, -1))
            {
                partitionKeyOptionList.add(option);
            }
        }

        if (StringUtils.isEmpty(getStreamId()))
        {
            streamType = describeStream(destination).getStreamType();
        }
        else
        {
            // 如果配置streamId，则不需describeStream，从配置项读取
            streamType = config.readString(STREAM_TYPE, "COMMON");
            try
            {
                StreamType.valueOf(streamType);
            }
            catch (Exception e)
            {
                throw new IllegalArgumentException("streamType should be " + Arrays.toString(StreamType.values()));
            }
        }

        // 高级通道单条记录限制5MB;其他限制1MB
        if (StreamType.ADVANCED.getType().equals(streamType))
        {
            maxRecordSizeBytes = 5 * Constants.ONE_MB;
        }
        else
        {
            maxRecordSizeBytes = Constants.ONE_MB;
        }
        LOGGER.info("DISStream {} type is {}", destination, streamType);
    }
    
    @Override
    public int getPerRecordOverheadBytes()
    {
        return DISConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    public int getMaxRecordSizeBytes()
    {
        return maxRecordSizeBytes;
    }
    
    @Override
    public int getPerBufferOverheadBytes()
    {
        return DISConstants.PER_BUFFER_OVERHEAD_BYTES;
    }
    
    @Override
    public FileTailer<DISRecord> createNewTailer(FileCheckpointStore checkpoints, ExecutorService sendingExecutor)
        throws IOException
    {
        AsyncPublisherService<DISRecord> publisher = getPublisher(checkpoints, sendingExecutor);
        return new FileTailer<DISRecord>(agentContext, this, publisher, buildParser(), checkpoints);
    }
    
    @Override
    protected SourceFileTracker buildSourceFileTracker()
        throws IOException
    {
        return new SourceFileTracker(agentContext, this);
    }
    
    @Override
    protected AsyncPublisherService<DISRecord> getPublisher(FileCheckpointStore checkpoints,
        ExecutorService sendingExecutor)
    {
        return new AsyncPublisherService<>(agentContext, this, checkpoints, buildSender(), sendingExecutor);
    }
    
    @Override
    protected IDataConverter buildConverterChain(List<Configuration> conversionOptions)
        throws ConfigurationException
    {
        if (conversionOptions == null || conversionOptions.size() == 0)
        {
            // add default SINGLELINE Converter
            Map<String, Object> signleLineMap = new HashMap<>();
            signleLineMap.put(ProcessingUtilsFactory.CONVERSION_OPTION_NAME_KEY,
                ProcessingUtilsFactory.DataConversionOption.SINGLELINE);
            signleLineMap.put(ProcessingUtilsFactory.FILE_ENCODING_KEY, this.fileEncoding);
            conversionOptions = Collections.singletonList(new Configuration(signleLineMap));
        }
        else
        {
            for (int i = 0; i < conversionOptions.size(); i++)
            {
                Map<String, Object> configMap = new HashMap<>(conversionOptions.get(i).getConfigMap());
                configMap.put(ProcessingUtilsFactory.FILE_ENCODING_KEY, this.fileEncoding);
                conversionOptions.set(i, new Configuration(configMap));
            }
        }
        return super.buildConverterChain(conversionOptions);
    }
    
    @Override
    protected IParser<DISRecord> buildParser()
    {
        return new DISParser(this, getParserBufferSize());
    }
    
    @Override
    protected ISender<DISRecord> buildSender()
    {
        return new DISSender(agentContext, this);
    }
    
    @Override
    public int getParserBufferSize()
    {
        return DISConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES;
    }
    
    @Override
    protected Range<Long> getWaitOnEmptyPublishQueueMillisValidRange()
    {
        return VALID_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_RANGE;
    }
    
    @Override
    protected long getDefaultWaitOnEmptyPublishQueueMillis()
    {
        return DISConstants.DEFAULT_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS;
    }
    
    @Override
    protected Range<Long> getWaitOnPublishQueueMillisValidRange()
    {
        return VALID_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_RANGE;
    }
    
    @Override
    protected long getDefaultWaitOnPublishQueueMillis()
    {
        return DISConstants.DEFAULT_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS;
    }
    
    @Override
    protected Range<Integer> getMaxBufferSizeBytesValidRange()
    {
        return VALID_MAX_BUFFER_SIZE_BYTES_RANGE;
    }
    
    @Override
    protected int getDefaultMaxBufferSizeBytes()
    {
        return DISConstants.MAX_BUFFER_SIZE_BYTES;
    }
    
    @Override
    protected Range<Integer> getBufferSizeRecordsValidRange()
    {
        return VALID_MAX_BUFFER_SIZE_RECORDS_RANGE;
    }
    
    @Override
    protected int getDefaultBufferSizeRecords()
    {
        return DISConstants.DEFAULT_PUT_RECORDS_SIZE_RECORDS;
    }
    
    @Override
    protected Range<Long> getMaxBufferAgeMillisValidRange()
    {
        return VALID_MAX_BUFFER_AGE_RANGE_MILLIS;
    }
    
    @Override
    protected long getDefaultMaxBufferAgeMillis()
    {
        return DISConstants.DEFAULT_MAX_BUFFER_AGE_MILLIS;
    }
    
    @Override
    public long getDefaultRetryInitialBackoffMillis()
    {
        return DISConstants.DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS;
    }
    
    @Override
    public long getDefaultRetryMaxBackoffMillis()
    {
        return DISConstants.DEFAULT_RETRY_MAX_BACKOFF_MILLIS;
    }
    
    @Override
    public int getDefaultPublishQueueCapacity()
    {
        return DISConstants.DEFAULT_PUBLISH_QUEUE_CAPACITY;
    }
    
    @Override
    protected int getDefaultSendingThreadSize()
    {
        return DISConstants.DEFAULT_SENDING_THREAD_SIZE;
    }
    
    @Override
    protected long getDefaultMaxFileCheckingMillis()
    {
        if (isFileAppendable())
        {
            // 如果文件可以追加，此时表示如果记录不是分隔符结尾，则等待此时间之后认为记录已写完，如果Flow的missLastRecordDelimiter=true，则会上传此记录
            return DISConstants.DEFAULT_MISS_LAST_RECORD_DELIMITER_CHECKING_MILLIS;
        }
        else
        {
            // 如果文件不可以追加，则等待文件全部写完之后，再上传
            return DISConstants.DEFAULT_MAX_FILE_CHECKING_MILLIS;
        }
    }
    
    public String getMetricLog(Map<String, Object> metrics)
    {
        AtomicLong zero = new AtomicLong(0);
        long bytesBehind = Metrics.getMetric(metrics, Metrics.FILE_TAILER_BYTES_BEHIND_METRIC, 0L);
        int filesBehind = Metrics.getMetric(metrics, Metrics.FILE_TAILER_FILES_BEHIND_METRIC, 0);
        long bytesConsumed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_BYTES_CONSUMED_METRIC, zero).get();
        long recordsParsed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_RECORDS_PARSED_METRIC, zero).get();
        long recordsProcessed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_RECORDS_PROCESSED_METRIC, zero).get();
        long recordsSkipped = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_RECORDS_SKIPPED_METRIC, zero).get();
        long recordsSent = Metrics.getMetric(metrics, Metrics.SENDER_TOTAL_RECORDS_SENT_METRIC, zero).get();
        
        String info = "Tailer Progress: Tailer has parsed %d records (%d bytes), " + "transformed %d records, "
            + "skipped %d records, " + "and has successfully sent %d records to destination.";
        return String.format(info, recordsParsed, bytesConsumed, recordsProcessed, recordsSkipped, recordsSent);
    }
}
