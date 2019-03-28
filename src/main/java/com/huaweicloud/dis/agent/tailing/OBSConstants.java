package com.huaweicloud.dis.agent.tailing;

import com.huaweicloud.dis.agent.Constants;

public class OBSConstants extends Constants
{
    public static final String DESTINATION_KEY = "OBSStream";
    
    public static final String PARTITION_KEY = "partitionKeyOption";
    
    public static final int PER_RECORD_OVERHEAD_BYTES = 0;
    
    public static final int PER_BUFFER_OVERHEAD_BYTES = 0;
    
    public static final int MAX_PUT_RECORDS_SIZE_RECORDS = 10;
    
    public static final int DEFAULT_PUT_RECORDS_SIZE_RECORDS = 1;
    
    public static final long MAX_PUT_RECORDS_SIZE_BYTES = 2 * 1024 * 1024 * 1024L;
    
    public static final int MAX_BUFFER_SIZE_RECORDS = MAX_PUT_RECORDS_SIZE_RECORDS;
    
    public static final int MAX_BUFFER_SIZE_BYTES = 128 * 1024 * 1024;
    
    public static final int DEFAULT_PARSER_BUFFER_SIZE_BYTES = MAX_BUFFER_SIZE_BYTES;
    
    public static final int DEFAULT_SENDING_THREAD_SIZE = 10;
}
