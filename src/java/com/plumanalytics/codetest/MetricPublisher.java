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
  public Map<Date, Map<String, CountInstance>> getMetricMapByDate();
  public Map<String, CountInstance> getAggregateCountMapById();

}
