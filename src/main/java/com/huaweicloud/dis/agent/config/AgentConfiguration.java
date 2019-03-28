package com.huaweicloud.dis.agent.config;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.huaweicloud.dis.DISConfig;

public class AgentConfiguration extends Configuration
{
    static final String[] VALID_LOG_LEVELS = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR"};
    
    static final long DEFAULT_SENDING_THREADS_KEEPALIVE_MILLIS = 60_000L;
    
    static final int DEFAULT_SENDING_THREADS_MAX_QUEUE_SIZE = 100;
    
    static final String DEFAULT_CHECKPOINTS_FILE = "conf" + File.separator + "since.db";
    
    static final int DEFAULT_MAX_SENDING_THREADS_PER_CORE = 12;
    
    static final int DEFAULT_CW_QUEUE_SIZE = 10_000;
    
    static final boolean DEFAULT_CW_EMIT_METRICS = true;
    
    static final long DEFAULT_CW_BUFFER_TIME_MILLIS = 30_000L;
    
    static final boolean DEFAULT_LOG_EMIT_METRICS = false;
    
    static final boolean DEFAULT_LOG_EMIT_INTERNAL_METRICS = false;
    
    static final int DEFAULT_LOG_STATUS_REPORTING_PERIOD_SECONDS = 30;
    
    static final int DEFAULT_CHECKPOINT_TTL_DAYS = 7;
    
    protected static final int DEFAULT_ASSUME_ROLE_DURATION_SECONDS = (int)TimeUnit.HOURS.toSeconds(1);
    
    static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 10_000L;
    
    public static final String CONFIG_ACCESS_KEY = "ak";
    
    public static final String CONFIG_SECRET_KEY = "sk";
    
    public static final String SHUTDOWN_TIMEOUT_MILLIS_KEY = "shutdownTimeoutMillis";
    
    public static final String LOG_FILE_KEY = "log.file";
    
    public static final String LOG_LEVEL_KEY = "log.level";
    
    public static final String LOG_MAX_BACKUP_INDEX_KEY = "log.maxBackupIndex";
    
    public static final String LOG_MAX_FILE_SIZE_KEY = "log.maxFileSize";
    
    public static final String CONFIG_REGION_KEY = "region";
    
    public static final String CONFIG_DATA_ENCRYPT_ENABLED_KEY = "dataEncryptEnabled";
    
    public static final String CONFIG_DATA_PASSWORD_KEY = "dataPassword";
    
    public static final String CONFIG_ENDPOINT_KEY = "endpoint";
    
    public static final String CONFIG_PROJECTID_KEY = "projectId";
    
    public static final String CONFIG_DEFAULT_MAX_PER_ROUTE_KEY = "httpClientDefaultMaxPerRoute";
    
    public static final String CONFIG_DEFAULT_MAX_TOTAL_KEY = "httpClientDefaultMaxTotal";
    
    public static final String CONFIG_CONNECTION_TIMEOUT_KEY = "connectionTimeOutSeconds";
    
    public static final String CONFIG_SOCKET_TIMEOUT_KEY = "socketTimeOutSeconds";
    
    public static final String CONFIG_BODY_SERIALIZE_TYPE_KEY = "bodySerializeType";
    
    public static final String CONFIG_PROVIDER_CLASS_KEY = "configProviderClass";
    
    public AgentConfiguration(Map<String, Object> config)
    {
        super(config);
    }
    
    public long shutdownTimeoutMillis()
    {
        return readLong(SHUTDOWN_TIMEOUT_MILLIS_KEY, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }
    
    public String accessKeyId()
    {
        return this.readString(CONFIG_ACCESS_KEY, null);
    }
    
    public String secretKey()
    {
        return this.readString(CONFIG_SECRET_KEY, null);
    }
    
    public AgentConfiguration(Configuration config)
    {
        this(config.getConfigMap());
    }
    
    public String logLevel()
    {
        return readString(LOG_LEVEL_KEY, null);
    }
    
    public Path logFile()
    {
        return readPath(LOG_FILE_KEY, null);
    }
    
    public int logMaxBackupIndex()
    {
        return readInteger(LOG_MAX_BACKUP_INDEX_KEY, -1);
    }
    
    public long logMaxFileSize()
    {
        return readLong(LOG_MAX_FILE_SIZE_KEY, -1L);
    }
    
    public boolean logEmitMetrics()
    {
        return this.readBoolean("log.emitMetrics", DEFAULT_LOG_EMIT_METRICS);
    }
    
    public boolean logEmitInternalMetrics()
    {
        return this.readBoolean("log.emitInternalMetrics", DEFAULT_LOG_EMIT_INTERNAL_METRICS);
    }
    
    public int logStatusReportingPeriodSeconds()
    {
        return this.readInteger("log.statusReportingPeriodSeconds", DEFAULT_LOG_STATUS_REPORTING_PERIOD_SECONDS);
    }
    
    public int maxSendingThreads()
    {
        return this.readInteger("maxSendingThreads", defaultMaxSendingThreads());
    }
    
    public long sendingThreadsKeepAliveMillis()
    {
        return this.readLong("sendingThreadsKeepAliveMillis", DEFAULT_SENDING_THREADS_KEEPALIVE_MILLIS);
    }
    
    public int sendingThreadsMaxQueueSize()
    {
        return this.readInteger("sendingThreadsMaxQueueSize", DEFAULT_SENDING_THREADS_MAX_QUEUE_SIZE);
    }
    
    public int maxSendingThreadsPerCore()
    {
        return readInteger("maxSendingThreadsPerCore", DEFAULT_MAX_SENDING_THREADS_PER_CORE);
    }
    
    public int defaultMaxSendingThreads()
    {
        return Math.max(2, maxSendingThreadsPerCore() * Runtime.getRuntime().availableProcessors());
    }
    
    public int maxConnections()
    {
        return readInteger("maxConnections", defaultMaxConnections());
    }
    
    public int defaultMaxConnections()
    {
        return Math.max(50, maxSendingThreads());
    }
    
    public boolean useTcpKeepAlive()
    {
        return readBoolean("useTcpKeepAlive", false);
    }
    
    public boolean useHttpGzip()
    {
        return readBoolean("useHttpGzip", false);
    }
    
    public Path checkpointFile()
    {
        return this.readPath("checkpointFile", Paths.get(DEFAULT_CHECKPOINTS_FILE));
    }
    
    public int checkpointTimeToLiveDays()
    {
        return this.readInteger("checkpointTimeToLiveDays", DEFAULT_CHECKPOINT_TTL_DAYS);
    }
    
    public String disEndpoint()
    {
        return this.readString(CONFIG_ENDPOINT_KEY, null);
    }
}
