package com.plumanalytics.codetest;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import com.plumanalytics.codetest.TestMetricPublisher.CountInstance;

/**
 * Publish Metrics
 */
public interface MetricPublisher {

  public MetricMessage createMessage(String line) throws ParseException;
  public void publishMetric(MetricMessage metricMessage);
  public default Map<Date, Map<String, CountInstance>> getMetricMapByDate(){ throw new RuntimeException("Not supported");}
  public default Map<String, CountInstance> getAggregateCountMapById(){ throw new RuntimeException("Not supported");};

}
