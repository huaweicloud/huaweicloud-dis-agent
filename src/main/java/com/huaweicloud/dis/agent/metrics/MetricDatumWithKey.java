package com.huaweicloud.dis.agent.metrics;

import java.util.Objects;

import com.huaweicloud.dis.agent.watch.model.MetricDatum;

/**
 * This class is used to store a MetricDatum as well as KeyType which stores specific information about that particular
 * MetricDatum.
 *
 * @param <KeyType> is a class that stores information about a MetricDatum. This is useful to compare MetricDatums,
 *            aggregate similar MetricDatums or store information about a datum that may be relevant to the user (i.e.
 *            MetricName, CustomerId, TimeStamp, etc).
 *            <p>
 *            Example:
 *            <p>
 *            Let SampleMetricKey be a KeyType that takes in the time in which the datum was created.
 *            <p>
 *            MetricDatumWithKey<SampleMetricKey> sampleDatumWithKey = new MetricDatumWithKey<SampleMetricKey>(new
 *            SampleMetricKey(System.currentTimeMillis()), datum)
 */
public class MetricDatumWithKey<KeyType>
{
    public KeyType key;
    
    public MetricDatum datum;
    
    /**
     * @param key an object that stores relevant information about a MetricDatum (e.g. MetricName, accountId, TimeStamp)
     * @param datum data point
     */
    
    public MetricDatumWithKey(KeyType key, MetricDatum datum)
    {
        this.key = key;
        this.datum = datum;
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hash(key, datum);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricDatumWithKey<?> other = (MetricDatumWithKey<?>)obj;
        return Objects.equals(other.key, key) && Objects.equals(other.datum, datum);
    }
    
}
