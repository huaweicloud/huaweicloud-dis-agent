package com.huaweicloud.dis.agent.metrics;

import java.util.*;

import com.huaweicloud.dis.agent.watch.model.Dimension;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;

/**
 * A composite {@link IMetricsScope} that delegates all method calls to a list of underlying instances.
 */
public class CompositeMetricsScope extends AbstractMetricsScope
{
    private final List<IMetricsScope> scopes;
    
    /**
     * @param scopes
     */
    public CompositeMetricsScope(IMetricsScope... scopes)
    {
        this(Arrays.asList(scopes));
    }
    
    /**
     * @param scopes
     */
    public CompositeMetricsScope(Collection<IMetricsScope> scopes)
    {
        this.scopes = new ArrayList<>(scopes);
    }
    
    @Override
    protected void realAddData(String name, double value, StandardUnit unit)
    {
        for (IMetricsScope scope : this.scopes)
            scope.addData(name, value, unit);
    }
    
    @Override
    protected void realAddDimension(String name, String value)
    {
        for (IMetricsScope scope : this.scopes)
            scope.addDimension(name, value);
    }
    
    @Override
    protected void realCommit()
    {
        for (IMetricsScope scope : this.scopes)
            scope.commit();
    }
    
    @Override
    protected void realCancel()
    {
        for (IMetricsScope scope : this.scopes)
            scope.cancel();
    }
    
    @Override
    protected Set<Dimension> realGetDimensions()
    {
        return scopes.isEmpty() ? Collections.<Dimension> emptySet() : scopes.get(0).getDimensions();
    }
}
