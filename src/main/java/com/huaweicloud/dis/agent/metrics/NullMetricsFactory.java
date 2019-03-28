package com.huaweicloud.dis.agent.metrics;

public class NullMetricsFactory implements IMetricsFactory
{
    @Override
    public IMetricsScope createScope()
    {
        return new NullMetricsScope();
    }
}
