package com.plumanalytics.codetest;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import com.plumanalytics.codetest.TestMetricPublisher.CountInstance;

public class MericMessageProducer implements MetricPublisher{

	@Override
	public MetricMessage createMessage(String line) throws ParseException {
		MetricMessage message = new TestMetricMessage();
		message.init(line);
		return message;
	}

	@Override
	public void publishMetric(MetricMessage metricMessage) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<Date, Map<String, CountInstance>> getMetricMapByDate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, CountInstance> getAggregateCountMapById() {
		// TODO Auto-generated method stub
		return null;
	}

}
