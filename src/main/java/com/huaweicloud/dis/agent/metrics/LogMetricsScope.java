package com.huaweicloud.dis.agent.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huaweicloud.dis.agent.watch.model.Dimension;
import com.huaweicloud.dis.agent.watch.model.MetricDatum;
import com.huaweicloud.dis.agent.watch.model.StatisticSet;

/**
 * An AccumulatingMetricsScope that outputs via log4j.
 */
public class LogMetricsScope extends AccumulatingMetricsScope
{
    public static final Logger LOGGER = LoggerFactory.getLogger(LogMetricsScope.class);
    
    public LogMetricsScope()
    {
        super();
    }
    
    @Override
    protected void realCommit()
    {
        if (!data.values().isEmpty())
        {
            StringBuilder output = new StringBuilder();
            output.append("Metrics:\n");
            
            output.append("Dimensions: ");
            boolean needsComma = false;
            for (Dimension dimension : getDimensions())
            {
                output.append(
                    String.format("%s[%s: %s]", needsComma ? ", " : "", dimension.getName(), dimension.getValue()));
                needsComma = true;
            }
            output.append("\n");
            
            for (MetricDatum datum : data.values())
            {
                StatisticSet statistics = datum.getStatisticValues();
                output.append(String.format("Name=%50s\tMin=%.2f\tMax=%.2f\tCount=%.2f\tSum=%.2f\tAvg=%.2f\tUnit=%s\n",
                    datum.getMetricName(),
                    statistics.getMinimum(),
                    statistics.getMaximum(),
                    statistics.getSampleCount(),
                    statistics.getSum(),
                    statistics.getSum() / statistics.getSampleCount(),
                    datum.getUnit()));
            }
            LOGGER.debug(output.toString());
        }
    }
}
