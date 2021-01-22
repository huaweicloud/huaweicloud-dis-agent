package com.huaweicloud.dis.agent.tailing;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.huaweicloud.dis.agent.AgentContext;
import com.huaweicloud.dis.agent.Constants;
import com.huaweicloud.dis.agent.config.Configuration;
import com.huaweicloud.dis.agent.config.ConfigurationException;
import com.huaweicloud.dis.agent.processing.interfaces.IDataConverter;
import com.huaweicloud.dis.agent.processing.processors.AgentDataConverterChain;
import com.huaweicloud.dis.agent.processing.utils.ProcessingUtilsFactory;
import com.huaweicloud.dis.agent.tailing.checkpoints.FileCheckpointStore;
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest;
import com.huaweicloud.dis.iface.stream.response.DescribeStreamResult;

import lombok.Getter;
import lombok.ToString;

/**
 * A class that represent a data flow consisting of a source and destination. The source is a colletion of files to be
 * tailed (represented by a {@link SourceFile} instance) and the destination is implementation-dependent.
 *
 * @param <R> The record type of this flow.
 */
@ToString
public abstract class FileFlow<R extends IRecord> extends Configuration
{
    private static final long DEFAULT_MIN_TIME_BETWEEN_FILE_POLLS_MILLIS = 100L;
    
    private static final String MIN_TIME_BETWEEN_FILE_POLLS_MILLIS_KEY = "minTimeBetweenFilePollsMillis";
    
    private static final int DEFAULT_HEADER_BYTES_LENGTH = 512;
    
    public static final String FILE_PATTERN_KEY = "filePattern";
    
    public static final String MAX_BUFFER_SIZE_BYTES_KEY = "maxBufferSizeBytes";
    
    public static final String MAX_BUFFER_SIZE_RECORDS_KEY = "maxBufferSizeRecords";
    
    public static final String MAX_BUFFER_AGE_MILLIS_KEY = "maxBufferAgeMillis";
    
    public static final String WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_KEY = "waitOnFullPublishQueueMillis";
    
    public static final String WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_KEY = "waitOnEmptyPublishQueueMillis";
    
    public static final String WAIT_ON_FULL_RETRY_QUEUE_MILLIS_KEY = "waitOnFullRetryQueueMillis";
    
    public static final String INITIAL_POSITION_KEY = "initialPosition";
    
    public static final String DEFAULT_TRUNCATED_RECORD_TERMINATOR = String.valueOf(Constants.NEW_LINE);
    
    public static final String CONVERSION_OPTION_KEY = "dataProcessingOptions";
    
    /**
     * 是否保留换行符\n (true: 上传时保留\n; false: 上传时去掉\n)
     */
    public static final String IS_REMAIN_RECORD_DELIMITER = "isRemainRecordDelimiter";
    
    /**
     * 是否忽略空值 (true: 忽略空值; false: 上传空值)
     */
    public static final String IS_IGNORE_EMPTY_DATA = "isIgnoreEmptyData";
    
    /**
     * 文件header字节大小
     */
    public static final String HEADER_BYTES_LENGTH = "headerBytesLength";
    
    /**
     * 发送线程池大小
     */
    public static final String SENDING_THREAD_SIZE = "sendingThreadSize";
    
    /**
     * DIS上传结果回显
     */
    public static final String CONFIG_RESULT_LOG_LEVEL_KEY = "resultLogLevel";
    
    /**
     * 文件上传完成之后增加的后缀名称(只适用于小文件上传以及固定内容文件上传)
     */
    public static final String FILE_SUFFIX = "fileSuffix";
    
    /**
     * 文件上传完成之后移动到的目录(只适用于小文件上传以及固定内容文件上传)
     */
    public static final String FILE_MOVE_TO_DIR = "fileMoveToDir";
    
    /**
     * 文件上传完成之后的清理策略(never: 不清理; immediate: 立刻清理)
     */
    public static final String DELETE_POLICY = "deletePolicy";
    
    /**
     * 记录分隔符
     */
    public static final String RECORD_DELIMITER = "recordDelimiter";
    
    /**
     * 文件是否会追加内容
     */
    public static final String IS_FILE_APPENDABLE = "isFileAppendable";

    /**
     * 数据结尾处是否缺失分隔符，如数据最后没有分隔符但是又需要上传，请设置此值为true，此时如果没有分隔符，则等待{@link #MAX_FILE_CHECKING_MILLIS}之后上传
     * 默认值false：有分隔符才做为一条记录上传，如没有则等待写入再上传。
     */
    public static final String IS_MISS_LAST_RECORD_DELIMITER = "isMissLastRecordDelimiter";

    /**
     * 文件检测最长等待时间
     */
    private static final String MAX_FILE_CHECKING_MILLIS = "maxFileCheckingMillis";

    /**
     * 是否忽略长时间没有更新的文件，需要配合参数 {@link #IGNORE_NOT_UPDATED_FILE_SECONDS} 使用
     */
    private static final String IGNORE_NOT_UPDATED_FILE_ENABLED = "ignoreNotUpdatedFileEnabled";

    /**
     * 文件多长时间没有更新时，忽略该文件，当参数 {@link #IGNORE_NOT_UPDATED_FILE_ENABLED} 为 true 时生效
     */
    private static final String IGNORE_NOT_UPDATED_FILE_SECONDS = "ignoreNotUpdatedFileSeconds";
    
    /**
     * 递归目录选项开关
     */
    public static final String DIRECTORY_RECURSION_ENABLED = "directoryRecursionEnabled";
    
    /**
     * 文件排序类型
     */
    public static final String FILE_COMPARATOR = "fileComparator";
    
    /**
     * 文件编码
     */
    public static final String FILE_ENCODING = "fileEncoding";
    
    /**
     * 启用
     */
    public static final String ENABLE = "enable";

    /**
     * 通道ID
     */
    public static final String STREAM_ID = "streamId";

    /**
     * 通道类型(ADVANCED/COMMON)
     */
    public static final String STREAM_TYPE = "streamType";

    @Getter
    protected final AgentContext agentContext;
    
    @Getter
    protected final SourceFile sourceFile;
    
    @Getter
    protected final int maxBufferSizeRecords;
    
    @Getter
    protected final int maxBufferSizeBytes;
    
    @Getter
    protected final long maxBufferAgeMillis;
    
    @Getter
    protected final long waitOnFullPublishQueueMillis;
    
    @Getter
    protected final long waitOnEmptyPublishQueueMillis;
    
    @Getter
    protected final InitialPosition initialPosition;
    
    @Getter
    protected final int skipHeaderLines;
    
    @Getter
    protected FileTailer<R> tailer;
    
    @Getter
    protected final byte[] recordTerminatorBytes;
    
    @Getter
    protected ISplitter recordSplitter;
    
    @Getter
    protected final long retryInitialBackoffMillis;
    
    @Getter
    protected final long retryMaxBackoffMillis;
    
    @Getter
    protected final int publishQueueCapacity;
    
    @Getter
    protected final IDataConverter dataConverter;
    
    @Getter
    protected final boolean remainRecordDelimiter;
    
    @Getter
    protected final boolean ignoreEmptyData;
    
    @Getter
    protected final int headerBytesLength;
    
    @Getter
    protected final int sendingThreadSize;
    
    @Getter
    protected final String fileSuffix;
    
    @Getter
    protected final String fileMoveToDir;
    
    @Getter
    protected final String deletePolicy;
    
    @Getter
    protected final char recordDelimiter;
    
    @Getter
    protected final boolean fileAppendable;

    @Getter
    protected final boolean missLastRecordDelimiter;
    
    @Getter
    protected final long maxFileCheckingMillis;

    @Getter
    protected final boolean ignoreNotUpdatedFileEnabled;

    @Getter
    protected final long ignoreNotUpdatedFileSeconds;
    
    @Getter
    protected final boolean directoryRecursionEnabled;
    
    @Getter
    protected final FileComparatorEnum fileComparator;
    
    @Getter
    protected final Charset fileEncoding;
    
    @Getter
    protected final String partitionKeyOption;
    
    @Getter
    protected final boolean enable;

    /**
     * 在日志输出DIS响应体结果开关
     */
    @Getter
    protected RESULT_LOG_LEVEL resultLogLevel;

    @Getter
    protected final String streamId;

    protected FileFlow(AgentContext context, Configuration config)
    {
        super(config);
        this.agentContext = context;
        enable = readBoolean(ENABLE, true);
        maxBufferAgeMillis = readLong(MAX_BUFFER_AGE_MILLIS_KEY, getDefaultMaxBufferAgeMillis());
        Configuration.validateRange(maxBufferAgeMillis, getMaxBufferAgeMillisValidRange(), MAX_BUFFER_AGE_MILLIS_KEY);
        maxBufferSizeRecords = readInteger(MAX_BUFFER_SIZE_RECORDS_KEY, getDefaultBufferSizeRecords());
        Configuration
            .validateRange(maxBufferSizeRecords, getBufferSizeRecordsValidRange(), MAX_BUFFER_SIZE_RECORDS_KEY);
        maxBufferSizeBytes = readInteger(MAX_BUFFER_SIZE_BYTES_KEY, getDefaultMaxBufferSizeBytes());
        Configuration.validateRange(maxBufferSizeBytes, getMaxBufferSizeBytesValidRange(), MAX_BUFFER_SIZE_BYTES_KEY);
        
        waitOnFullPublishQueueMillis =
            readLong(WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_KEY, getDefaultWaitOnPublishQueueMillis());
        Configuration.validateRange(waitOnFullPublishQueueMillis,
            getWaitOnPublishQueueMillisValidRange(),
            WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS_KEY);
        
        waitOnEmptyPublishQueueMillis =
            readLong(WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_KEY, getDefaultWaitOnEmptyPublishQueueMillis());
        Configuration.validateRange(waitOnEmptyPublishQueueMillis,
            getWaitOnEmptyPublishQueueMillisValidRange(),
            WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS_KEY);
        
        initialPosition = readEnum(InitialPosition.class, INITIAL_POSITION_KEY, InitialPosition.START_OF_FILE);
        
        // TODO: Add validation interval to following values
        retryInitialBackoffMillis = readLong("retryInitialBackoffMillis", getDefaultRetryInitialBackoffMillis());
        retryMaxBackoffMillis = readLong("retryMaxBackoffMillis", getDefaultRetryMaxBackoffMillis());
        publishQueueCapacity = readInteger("publishQueueCapacity", getDefaultPublishQueueCapacity());
        
        skipHeaderLines = readInteger("skipHeaderLines", 0);
        
        // 记录分隔符
        recordDelimiter = readString(RECORD_DELIMITER, String.valueOf(Constants.NEW_LINE)).charAt(0);
        
        // 文件编码
        fileEncoding = Charset.forName(readString(FILE_ENCODING, StandardCharsets.UTF_8.name()));
        
        String pattern = readString("multiLineStartPattern", null);
        recordSplitter = Strings.isNullOrEmpty(pattern) ? new SingleLineSplitter(this.recordDelimiter)
            : new RegexSplitter(pattern, fileEncoding);
        
        String terminatorConfig = readString("truncatedRecordTerminator", String.valueOf(recordDelimiter));
        if (terminatorConfig == null || terminatorConfig.getBytes(StandardCharsets.UTF_8).length >= Constants.ONE_MB)
        {
            throw new ConfigurationException("Record terminator not specified or exceeds the maximum record size");
        }
        recordTerminatorBytes = terminatorConfig.getBytes(StandardCharsets.UTF_8);
        
        remainRecordDelimiter = readBoolean(IS_REMAIN_RECORD_DELIMITER, false);
        ignoreEmptyData = readBoolean(IS_IGNORE_EMPTY_DATA, true);
        
        headerBytesLength = readInteger(HEADER_BYTES_LENGTH, DEFAULT_HEADER_BYTES_LENGTH);
        sendingThreadSize = readInteger(SENDING_THREAD_SIZE, getDefaultSendingThreadSize());
        
        // 获取结果日志开关
        String logLevel = readString(CONFIG_RESULT_LOG_LEVEL_KEY, RESULT_LOG_LEVEL.INFO.name());
        
        if ("false".equalsIgnoreCase(logLevel.trim()) || "off".equalsIgnoreCase(logLevel.trim()))
        {
            this.resultLogLevel = RESULT_LOG_LEVEL.OFF;
        }
        else
        {
            try
            {
                this.resultLogLevel = RESULT_LOG_LEVEL.valueOf(logLevel.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                this.resultLogLevel = RESULT_LOG_LEVEL.INFO;
            }
        }
        
        fileSuffix = readString(FILE_SUFFIX, Constants.DEFAULT_FILE_SUFFIX);
        
        fileMoveToDir = readString(FILE_MOVE_TO_DIR, "");
        
        deletePolicy = readString(DELETE_POLICY, Constants.DELETE_POLICY_NEVER);
        
        fileAppendable = readBoolean(IS_FILE_APPENDABLE, true);

        missLastRecordDelimiter = readBoolean(IS_MISS_LAST_RECORD_DELIMITER, false);

        long maxFileCheckingMillisTemp = readLong(MAX_FILE_CHECKING_MILLIS, getDefaultMaxFileCheckingMillis());
        maxFileCheckingMillis =
            maxFileCheckingMillisTemp < minTimeBetweenFilePollsMillis() ? minTimeBetweenFilePollsMillis()
                : maxFileCheckingMillisTemp;

        ignoreNotUpdatedFileEnabled = readBoolean(IGNORE_NOT_UPDATED_FILE_ENABLED, false);

        ignoreNotUpdatedFileSeconds = readLong(IGNORE_NOT_UPDATED_FILE_SECONDS, -1L);
        
        directoryRecursionEnabled = readBoolean(DIRECTORY_RECURSION_ENABLED, false);
        
        fileComparator = FileComparatorEnum
            .valueOf(readString(FILE_COMPARATOR, FileComparatorEnum.NEWEST_FIRST.name()).toUpperCase());
        
        List<Configuration> dataProcessingOptions =
            readList(CONVERSION_OPTION_KEY, Configuration.class, Collections.EMPTY_LIST);
        dataConverter = buildConverterChain(dataProcessingOptions);
        
        sourceFile = buildSourceFile();
        
        partitionKeyOption = readString(Constants.PARTITION_KEY, PartitionKeyOption.RANDOM_INT.name());

        streamId = readString(STREAM_ID, "");
    }
    
    public synchronized FileTailer<R> createTailer(FileCheckpointStore checkpoints)
        throws IOException
    {
        Preconditions.checkState(tailer == null, "Tailer for this flow is already initialized.");
        
        return tailer = createNewTailer(checkpoints, agentContext.createFlowSendingExecutor(this));
    }
    
    public synchronized FileTailer<R> createTailer(FileCheckpointStore checkpoints, ExecutorService sendingExecutor)
        throws IOException
    {
        Preconditions.checkState(tailer == null, "Tailer for this flow is already initialized.");
        return tailer = createNewTailer(checkpoints, sendingExecutor);
    }
    
    public boolean logEmitInternalMetrics()
    {
        return this.readBoolean("log.emitInternalMetrics", false);
    }
    
    public long minTimeBetweenFilePollsMillis()
    {
        return readLong(MIN_TIME_BETWEEN_FILE_POLLS_MILLIS_KEY, DEFAULT_MIN_TIME_BETWEEN_FILE_POLLS_MILLIS);
    }
    
    public long maxTimeBetweenFileTrackerRefreshMillis()
    {
        return maxFileCheckingMillis;
    }
    
    public abstract String getId();
    
    public abstract String getDestination();
    
    public abstract int getMaxRecordSizeBytes();
    
    public abstract int getPerRecordOverheadBytes();
    
    public abstract int getPerBufferOverheadBytes();
    
    protected abstract FileTailer<R> createNewTailer(FileCheckpointStore checkpoints, ExecutorService sendingExecutor)
        throws IOException;
    
    protected abstract AsyncPublisherService<R> getPublisher(FileCheckpointStore checkpoints,
        ExecutorService sendingExecutor);
    
    protected SourceFile buildSourceFile()
    {
        return new SourceFile(this, readString(FILE_PATTERN_KEY));
    }
    
    protected IDataConverter buildConverterChain(List<Configuration> conversionOptions)
        throws ConfigurationException
    {
        if (conversionOptions == null || conversionOptions.isEmpty())
            return null;
        
        List<IDataConverter> converters = new LinkedList<IDataConverter>();
        
        for (Configuration conversionOption : conversionOptions)
        {
            converters.add(ProcessingUtilsFactory.getDataConverter(conversionOption));
        }
        
        try
        {
            return new AgentDataConverterChain(converters);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Not able to create converter chain. ", e);
        }
    }
    
    protected abstract SourceFileTracker buildSourceFileTracker()
        throws IOException;
    
    protected abstract IParser<R> buildParser();
    
    protected abstract ISender<R> buildSender();
    
    public abstract int getParserBufferSize();
    
    // TODO: Instead of this plethora of abstract getters, consider using
    // tables for defaults and validation ranges.
    protected abstract Range<Long> getWaitOnEmptyPublishQueueMillisValidRange();
    
    protected abstract long getDefaultWaitOnEmptyPublishQueueMillis();
    
    protected abstract Range<Long> getWaitOnPublishQueueMillisValidRange();
    
    protected abstract long getDefaultWaitOnPublishQueueMillis();
    
    protected abstract Range<Integer> getMaxBufferSizeBytesValidRange();
    
    protected abstract int getDefaultMaxBufferSizeBytes();
    
    protected abstract Range<Integer> getBufferSizeRecordsValidRange();
    
    protected abstract int getDefaultBufferSizeRecords();
    
    protected abstract Range<Long> getMaxBufferAgeMillisValidRange();
    
    protected abstract long getDefaultMaxBufferAgeMillis();
    
    protected abstract long getDefaultRetryInitialBackoffMillis();
    
    protected abstract long getDefaultRetryMaxBackoffMillis();
    
    protected abstract int getDefaultPublishQueueCapacity();
    
    protected abstract int getDefaultSendingThreadSize();
    
    protected abstract long getDefaultMaxFileCheckingMillis();
    
    protected abstract String getMetricLog(Map<String, Object> metrics);
    
    public static enum InitialPosition
    {
        START_OF_FILE, END_OF_FILE
    }
    
    /**
     * 接口响应信息回显日志级别
     */
    public static enum RESULT_LOG_LEVEL
    {
        OFF, DEBUG, INFO, WARN, ERROR
    }
    
    public static enum PartitionKeyOption
    {
        RANDOM_DOUBLE, DETERMINISTIC, RANDOM_INT, FILE_NAME;
        
        public static boolean contains(String name)
        {
            if (name == null)
            {
                return false;
            }
            PartitionKeyOption[] season = values();
            for (PartitionKeyOption s : season)
            {
                if (s.name().equals(name))
                {
                    return true;
                }
            }
            
            return false;
        }
    }
    
    public static enum FileComparatorEnum
    {
        NEWEST_FIRST, OLDEST_FIRST
    }
    
    protected DescribeStreamResult describeStream(String streamName)
    {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        return agentContext.getDISClient().describeStream(describeStreamRequest);
    }
}
