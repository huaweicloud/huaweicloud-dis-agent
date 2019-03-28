package com.huaweicloud.dis.agent.metrics;

/**
 * An {@link IMetricsFactory} that creates {@link IMetricsScope} that output to the logger.
 */
public class LogMetricsFactory implements IMetricsFactory
{
    
    @Override
    public LogMetricsScope createScope()
    {
        return new LogMetricsScope();
    }
    
}
