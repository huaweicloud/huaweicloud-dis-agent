package com.huaweicloud.dis.agent.tailing;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Range;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.Constants;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.metrics.Metrics;
import com.huaweicloud.dis.agent.tailing.checkpoints.FileCheckpointStore;
import com.huaweicloud.dis.core.util.StringUtils;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.internal.utils.AccessLoggerUtils;

import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
public class OBSFileFlow extends FileFlow<SmallFileRecord>
{
    /**
     * 文件检测最少等待时间
     */
    protected static final long DEFAULT_MIN_TIME_BETWEEN_FILE_POLLS_MILLIS = 1_000L;
    
    public static final Range<Long> VALID_MAX_BUFFER_AGE_RANGE_MILLIS =
        Range.closed(TimeUnit.MILLISECONDS.toMillis(500), TimeUnit.MINUTES.toMillis(15));
    
    public static final Range<Integer> VALID_MAX_BUFFER_SIZE_RECORDS_RANGE =
        Range.closed(1, OBSConstants.MAX_BUFFER_SIZE_RECORDS);
    
    public static final Range<Integer> VALID_MAX_BUFFER_SIZE_BYTES_RANGE =
        Range.closed(1, OBSConstants.MAX_BUFFER_SIZE_BYTES);
    
    public static final Range<Long> VALID_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_RANGE =
        Range.closed(TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));
    
    public static final Range<Long> VALID_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_RANGE =
        Range.closed(TimeUnit.SECONDS.toMillis(1), TimeUnit.MINUTES.toMillis(15));
    
    /**
     * 文件转储的目录名，多级目录使用/隔开
     */
    protected static final String DUMP_DIRECTORY = "dumpDirectory";

    protected static final String RESERVED_SUB_DIRECTORY = "reservedSubDirectory";
    
    protected static final String OBS_BUCKET = "OBSBucket";
    
    protected static final String OBS_ENDPOINT = "OBSEndpoint";
    
    protected static final String DISABLE_DNS_BUCKET = "disableDnsBucket";
    
    protected static final String UPLOAD_PROGRESS_INTERVAL = "uploadProgressInterval";
    
    protected static final String UPLOAD_FULL_PATH = "uploadFullPath";
    
    @Getter
    protected final String id;
    
    @Getter
    protected final String destination;
    
    @Getter
    protected final String dumpDirectory;
    
    @Getter
    protected final boolean reservedSubDirectory;
    
    @Getter
    protected final String obsBucket;
    
    @Getter
    protected final String obsEndpoint;
    
    @Getter
    protected final boolean disableDnsBucket;
    
    @Getter
    protected final int uploadProgressInterval;
    
    @Getter
    protected final boolean uploadFullPath;
    
    protected ObsClient obsClient;
    
    public OBSFileFlow(AgentContext context, Configuration config)
    {
        super(context, config);
        destination = readString(OBSConstants.DESTINATION_KEY);
        id = "obs-file:" + destination + ":" + sourceFile.toString();
        
        String dir = readString(DUMP_DIRECTORY, "");
        if (StringUtils.isNullOrEmpty(dir))
        {
            dumpDirectory = "";
        }
        else
        {
            // 去掉开始的/，并添加结尾的/
            dir = dir.startsWith("/") ? dir.substring(1, dir.length()) : dir;
            dir = dir.endsWith("/") ? dir : dir + "/";
            dumpDirectory = dir;
        }

        reservedSubDirectory = readBoolean(RESERVED_SUB_DIRECTORY,false);
        
        obsBucket = readString(OBS_BUCKET);
        
        obsEndpoint = readString(OBS_ENDPOINT);
        
        disableDnsBucket = readBoolean(DISABLE_DNS_BUCKET, false);
        
        // default 10MB
        uploadProgressInterval = readInteger(UPLOAD_PROGRESS_INTERVAL, 10 * 1024);
        
        uploadFullPath = readBoolean(UPLOAD_FULL_PATH, false);
        
        getOBSClient();
        if (!obsClient.headBucket(obsBucket))
        {
            throw new IllegalArgumentException("bucket [" + obsBucket + "] is not exist.");
        }
        
        try
        {
            describeStream(destination);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("describe stream [" + destination + "] error: " + e.getMessage());
        }
    }
    
    @Override
    public int getPerRecordOverheadBytes()
    {
        return OBSConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    public int getMaxRecordSizeBytes()
    {
        return Constants.ONE_MB;
    }
    
    @Override
    public int getPerBufferOverheadBytes()
    {
        return OBSConstants.PER_BUFFER_OVERHEAD_BYTES;
    }
    
    @Override
    public FileTailer<SmallFileRecord> createNewTailer(FileCheckpointStore checkpoints, ExecutorService sendingExecutor)
        throws IOException
    {
        AsyncPublisherService<SmallFileRecord> publisher = getPublisher(checkpoints, sendingExecutor);
        return new FileTailer<SmallFileRecord>(agentContext, this, publisher, buildParser(), checkpoints);
    }
    
    @Override
    protected SourceFileTracker buildSourceFileTracker()
        throws IOException
    {
        return new SourceFileTracker(agentContext, this);
    }
    
    @Override
    protected AsyncPublisherService<SmallFileRecord> getPublisher(FileCheckpointStore checkpoints,
        ExecutorService sendingExecutor)
    {
        return new AsyncPublisherService<SmallFileRecord>(agentContext, this, checkpoints, buildSender(),
            sendingExecutor);
    }
    
    @Override
    protected IParser<SmallFileRecord> buildParser()
    {
        return new SmallFileParser(this, getParserBufferSize());
    }
    
    @Override
    protected ISender<SmallFileRecord> buildSender()
    {
        return new OBSSender(agentContext, this);
    }
    
    @Override
    public int getParserBufferSize()
    {
        return OBSConstants.DEFAULT_PARSER_BUFFER_SIZE_BYTES;
    }
    
    @Override
    protected Range<Long> getWaitOnEmptyPublishQueueMillisValidRange()
    {
        return VALID_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_RANGE;
    }
    
    @Override
    protected long getDefaultWaitOnEmptyPublishQueueMillis()
    {
        return OBSConstants.DEFAULT_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS;
    }
    
    @Override
    protected Range<Long> getWaitOnPublishQueueMillisValidRange()
    {
        return VALID_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_RANGE;
    }
    
    @Override
    protected long getDefaultWaitOnPublishQueueMillis()
    {
        return OBSConstants.DEFAULT_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS;
    }
    
    @Override
    protected Range<Integer> getMaxBufferSizeBytesValidRange()
    {
        return VALID_MAX_BUFFER_SIZE_BYTES_RANGE;
    }
    
    @Override
    protected int getDefaultMaxBufferSizeBytes()
    {
        return OBSConstants.MAX_BUFFER_SIZE_BYTES;
    }
    
    @Override
    protected Range<Integer> getBufferSizeRecordsValidRange()
    {
        return VALID_MAX_BUFFER_SIZE_RECORDS_RANGE;
    }
    
    @Override
    protected int getDefaultBufferSizeRecords()
    {
        return OBSConstants.DEFAULT_PUT_RECORDS_SIZE_RECORDS;
    }
    
    @Override
    protected Range<Long> getMaxBufferAgeMillisValidRange()
    {
        return VALID_MAX_BUFFER_AGE_RANGE_MILLIS;
    }
    
    @Override
    protected long getDefaultMaxBufferAgeMillis()
    {
        return OBSConstants.DEFAULT_MAX_BUFFER_AGE_MILLIS;
    }
    
    @Override
    public long getDefaultRetryInitialBackoffMillis()
    {
        return OBSConstants.DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS;
    }
    
    @Override
    public long getDefaultRetryMaxBackoffMillis()
    {
        return OBSConstants.DEFAULT_RETRY_MAX_BACKOFF_MILLIS;
    }
    
    @Override
    public int getDefaultPublishQueueCapacity()
    {
        return OBSConstants.DEFAULT_PUBLISH_QUEUE_CAPACITY;
    }
    
    public long minTimeBetweenFilePollsMillis()
    {
        return DEFAULT_MIN_TIME_BETWEEN_FILE_POLLS_MILLIS;
    }
    
    @Override
    protected int getDefaultSendingThreadSize()
    {
        return OBSConstants.DEFAULT_SENDING_THREAD_SIZE;
    }
    
    @Override
    protected long getDefaultMaxFileCheckingMillis()
    {
        return OBSConstants.DEFAULT_MAX_FILE_CHECKING_MILLIS;
    }
    
    public String getMetricLog(Map<String, Object> metrics)
    {
        AtomicLong zero = new AtomicLong(0);
        long filesParsed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_FILES_PARSED_METRIC, zero).get();
        long filesBytesConsumed =
            Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_FILES_BYTES_CONSUMED_METRIC, zero).get();
        long filesProcessed = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_FILES_PROCESSED_METRIC, zero).get();
        long filesSkipped = Metrics.getMetric(metrics, Metrics.PARSER_TOTAL_FILES_SKIPPED_METRIC, zero).get();
        long filesSent = Metrics.getMetric(metrics, Metrics.SENDER_TOTAL_FILES_SENT_METRIC, zero).get();
        
        String info = "Tailer Progress: Tailer has parsed %d files (%d bytes), " + "transformed %d files, "
            + "skipped %d files, " + "and has successfully sent %d files to destination.";
        return String.format(info, filesParsed, filesBytesConsumed, filesProcessed, filesSkipped, filesSent);
    }
    
    public ObsClient getOBSClient()
    {
        if (obsClient == null)
        {
            synchronized (OBSFileFlow.class)
            {
                if (obsClient == null)
                {
                    AccessLoggerUtils.ACCESSLOG_ENABLED = false;
                    ObsConfiguration obsConfiguration = new ObsConfiguration();
                    obsConfiguration.setEndPoint(obsEndpoint);
                    obsConfiguration.setDisableDnsBucket(disableDnsBucket);
                    obsClient = new ObsClient(agentContext.getCredentials().getAccessKeyId(),
                        agentContext.getCredentials().getSecretKey(), agentContext.getCredentials().getSecurityToken(),
                        obsConfiguration);
                }
            }
        }
        return obsClient;
    }
}
