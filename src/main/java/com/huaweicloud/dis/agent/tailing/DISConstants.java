package com.huaweicloud.dis.agent.tailing;

import com.huaweicloud.dis.agent.Constants;

public class DISConstants extends Constants
{
    public static final String DESTINATION_KEY = "DISStream";
    
    public static final int PER_RECORD_OVERHEAD_BYTES = 0;
    
    public static final int PER_BUFFER_OVERHEAD_BYTES = 0;
    
    public static final int MAX_PUT_RECORDS_SIZE_RECORDS = 10000;
    
    public static final int DEFAULT_PUT_RECORDS_SIZE_RECORDS = 500;
    
    public static final int MAX_PUT_RECORDS_SIZE_BYTES = 5 * Constants.ONE_MB;
    
    public static final int MAX_BUFFER_SIZE_RECORDS = MAX_PUT_RECORDS_SIZE_RECORDS;
    
    public static final int MAX_BUFFER_SIZE_BYTES = 4 * Constants.ONE_MB;
    
    public static final int DEFAULT_PARSER_BUFFER_SIZE_BYTES = 6 * Constants.ONE_MB;
    
    public static final int DEFAULT_SENDING_THREAD_SIZE = 1;
}
