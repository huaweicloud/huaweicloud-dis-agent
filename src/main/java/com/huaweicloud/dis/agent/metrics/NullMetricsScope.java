package com.huaweicloud.dis.agent.metrics;

import java.util.Collections;
import java.util.Set;

import com.huaweicloud.dis.agent.watch.model.Dimension;

public class NullMetricsScope extends AbstractMetricsScope
{
    @Override
    protected Set<Dimension> realGetDimensions()
    {
        return Collections.emptySet();
    }
}
