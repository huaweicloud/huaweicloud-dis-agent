package com.huaweicloud.dis.agent.processing.exceptions;

/**
 * Exception thrown by ILogParser
 */
@SuppressWarnings("serial")
public class LogParsingException extends RuntimeException
{
    
    public LogParsingException(String message)
    {
        super(message);
    }
    
    public LogParsingException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
