package com.huaweicloud.dis.agent.metrics;

import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.huaweicloud.dis.agent.watch.model.Dimension;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;

/**
 * An base implementation of an {@link IMetricsScope} that accumulates data from multiple calls to addData with the same
 * name parameter. It tracks min, max, sample count, and sum for each named metric.
 */
@NotThreadSafe
public abstract class AbstractMetricsScope implements IMetricsScope
{
    private boolean closed = false;
    
    /**
     * Adds data points to the scope {@link IMetricsScope}. Multiple calls to this method with the same name will have
     * their data accumulated.
     *
     * @see IMetricsScope#addData(String, double, StandardUnit)
     */
    @Override
    public final void addData(String name, double value, StandardUnit unit)
    {
        Preconditions.checkState(!closed, "Scope is already closed.");
        realAddData(name, value, unit);
    }
    
    protected void realAddData(String name, double value, StandardUnit unit)
    {
    }
    
    @Override
    public void addCount(String name, long amount)
    {
        addData(name, amount, StandardUnit.Count);
    }
    
    @Override
    public void addTimeMillis(String name, long duration)
    {
        addData(name, duration, StandardUnit.Milliseconds);
    }
    
    @Override
    public final void addDimension(String name, String value)
    {
        Preconditions.checkState(!closed, "Scope is already closed.");
        realAddDimension(name, value);
    }
    
    protected void realAddDimension(String name, String value)
    {
    }
    
    @Override
    public final void commit()
    {
        Preconditions.checkState(!closed, "Scope is already closed.");
        try
        {
            realCommit();
        }
        finally
        {
            closed = true;
        }
    }
    
    @Override
    public final void cancel()
    {
        Preconditions.checkState(!closed, "Scope is already closed.");
        try
        {
            realCancel();
        }
        finally
        {
            closed = true;
        }
    }
    
    protected void realCancel()
    {
    }
    
    protected void realCommit()
    {
    }
    
    @Override
    public boolean closed()
    {
        return closed;
    }
    
    /**
     * @return a set of dimensions for an IMetricsScope
     */
    @Override
    public Set<Dimension> getDimensions()
    {
        Preconditions.checkState(!closed, "Scope is already closed.");
        return realGetDimensions();
    }
    
    protected abstract Set<Dimension> realGetDimensions();
}
