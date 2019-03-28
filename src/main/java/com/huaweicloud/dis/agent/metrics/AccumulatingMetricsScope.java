package com.huaweicloud.dis.agent.metrics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.huaweicloud.dis.agent.watch.model.Dimension;
import com.huaweicloud.dis.agent.watch.model.MetricDatum;
import com.huaweicloud.dis.agent.watch.model.StandardUnit;
import com.huaweicloud.dis.agent.watch.model.StatisticSet;

/**
 * An base implementation of an {@link IMetricsScope} that accumulates data from multiple calls to addData with the same
 * name parameter. It tracks min, max, sample count, and sum for each named metric.
 */
@NotThreadSafe
public abstract class AccumulatingMetricsScope extends AbstractMetricsScope
{
    
    protected final Set<Dimension> dimensions = new HashSet<Dimension>();
    
    protected final Map<String, MetricDatum> data = new HashMap<String, MetricDatum>();
    
    protected final long startTime = System.currentTimeMillis();
    
    @Override
    protected void realAddData(String name, double value, StandardUnit unit)
    {
        MetricDatum datum = data.get(name);
        if (datum == null)
        {
            data.put(name,
                new MetricDatum().withMetricName(name).withUnit(unit).withStatisticValues(
                    new StatisticSet().withMaximum(value).withMinimum(value).withSampleCount(1.0).withSum(value)));
        }
        else
        {
            if (!datum.getUnit().equals(unit.name()))
            {
                throw new IllegalArgumentException("Cannot add to existing metric with different unit");
            }
            StatisticSet statistics = datum.getStatisticValues();
            statistics.setMaximum(Math.max(value, statistics.getMaximum()));
            statistics.setMinimum(Math.min(value, statistics.getMinimum()));
            statistics.setSampleCount(statistics.getSampleCount() + 1);
            statistics.setSum(statistics.getSum() + value);
        }
    }
    
    @Override
    protected void realAddDimension(String name, String value)
    {
        dimensions.add(new Dimension().withName(name).withValue(value));
    }
    
    @Override
    protected Set<Dimension> realGetDimensions()
    {
        return dimensions;
    }
}
