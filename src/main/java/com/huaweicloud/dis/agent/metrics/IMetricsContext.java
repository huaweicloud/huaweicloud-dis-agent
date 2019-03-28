package com.huaweicloud.dis.agent.metrics;

public interface IMetricsContext
{
    /**
     * @return An instance of {@link IMetricsScope}.
     */
    public abstract IMetricsScope beginScope();
}
