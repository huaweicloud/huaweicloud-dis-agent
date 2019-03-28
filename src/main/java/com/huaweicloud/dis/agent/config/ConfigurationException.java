package com.huaweicloud.dis.agent.config;

/**
 * Raised if an error occurred during configuration.
 */
@SuppressWarnings("serial")
public class ConfigurationException extends RuntimeException
{
    public ConfigurationException(String message)
    {
        super(message);
    }
    
    public ConfigurationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}