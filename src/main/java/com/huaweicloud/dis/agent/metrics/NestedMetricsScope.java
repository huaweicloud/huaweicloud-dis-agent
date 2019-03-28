package com.huaweicloud.dis.agent.metrics;

import java.util.Set;

import com.huaweicloud.dis.agent.watch.model.Dimension;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;

public class NestedMetricsScope implements IMetricsScope
{
    private final IMetricsScope delegate;
    
    public NestedMetricsScope(IMetricsScope delegate)
    {
        this.delegate = delegate;
    }
    
    @Override
    public void addData(String name, double value, StandardUnit unit)
    {
        delegate.addData(name, value, unit);
    }
    
    @Override
    public void addCount(String name, long amount)
    {
        delegate.addCount(name, amount);
    }
    
    @Override
    public void addTimeMillis(String name, long duration)
    {
        delegate.addTimeMillis(name, duration);
    }
    
    @Override
    public void addDimension(String name, String value)
    {
        throw new UnsupportedOperationException("Cannot add dimensions for nested metrics.");
    }
    
    @Override
    public void commit()
    {
        // TODO: Implement reference counting
    }
    
    @Override
    public void cancel()
    {
        throw new UnsupportedOperationException("Cannot cancel nested metrics.");
    }
    
    @Override
    public boolean closed()
    {
        return delegate.closed();
    }
    
    @Override
    public Set<Dimension> getDimensions()
    {
        return delegate.getDimensions();
    }
}
