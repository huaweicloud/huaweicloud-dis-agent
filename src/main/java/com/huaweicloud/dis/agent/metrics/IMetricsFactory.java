package com.huaweicloud.dis.agent.metrics;

/**
 * Factory for {@link IMetricsScope} objects.
 */
public interface IMetricsFactory
{
    /**
     * @return a new {@link IMetricsScope} object of the type constructed by this factory.
     */
    public IMetricsScope createScope();
}
