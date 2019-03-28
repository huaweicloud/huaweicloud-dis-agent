package com.huaweicloud.dis.agent.metrics;

import java.util.List;

/**
 * An ICWMetricsPublisher is a publisher that contains the logic to publish metrics.
 *
 * @param <KeyType> is a class that stores information about a MetricDatum. This is useful when wanting to compare
 *            MetricDatums or aggregate similar MetricDatums.
 */

public interface ICWMetricsPublisher<KeyType>
{
    
    /**
     * Given a list of MetricDatumWithKey, this method extracts the MetricDatum from each MetricDatumWithKey and
     * publishes those datums.
     *
     * @param dataToPublish a list containing all the MetricDatums to publish
     */
    
    public void publishMetrics(List<MetricDatumWithKey<KeyType>> dataToPublish);
}
