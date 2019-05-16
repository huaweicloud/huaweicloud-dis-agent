package com.huaweicloud.dis.agent;

import java.util.concurrent.TimeUnit;

public class Constants
{
    public static final char NEW_LINE = '\n';
    
    public static final long DEFAULT_RETRY_INITIAL_BACKOFF_MILLIS = 100;
    
    public static final long DEFAULT_RETRY_MAX_BACKOFF_MILLIS = 10_000;
    
    public static final int DEFAULT_PUBLISH_QUEUE_CAPACITY = 100;
    
    public static final long DEFAULT_MAX_BUFFER_AGE_MILLIS = TimeUnit.SECONDS.toMillis(5);
    
    public static final long DEFAULT_WAIT_ON_FULL_PUBLISH_QUEUE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    
    public static final long DEFAULT_WAIT_ON_EMPTY_PUBLISH_QUEUE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    
    public static final String YAML_CONFIG = ".yml";
    
    public static final String JSON_CONFIG = ".json";
    
    public static final String HTTP_PREFIX = "http://";
    
    public static final String HTTPS_PREFIX = "https://";
    
    public static final String PARTITION_KEY_SPLIT = ",";

    public static final int ONE_MB = 1024 * 1024;

    /**
     * 用于加密(如sk/data.password)的key值
     */
    public static final String CONFIG_ENCRYPT_KEY = "encrypt.key";
    /**
     * AK/SK认证失败返回码
     */
    public static final int AUTHENTICATION_ERROR_HTTP_CODE = 441;
    
    /**
     * 文件上传完成之后增加的后缀名称默认值
     */
    public static final String DEFAULT_FILE_SUFFIX = ".COMPLETED";
    
    /**
     * 文件上传完成之后的清理策略_立刻删除
     */
    public static final String DELETE_POLICY_IMMEDIATE = "immediate";
    
    /**
     * 文件上传完成之后的清理策略_立刻删除
     */
    public static final String DELETE_POLICY_NEVER = "never";
    
    /**
     * 文件检测最长等待时间
     */
    public static final long DEFAULT_MAX_FILE_CHECKING_MILLIS = 5_000L;

    public static final String PARTITION_KEY = "partitionKeyOption";

}
