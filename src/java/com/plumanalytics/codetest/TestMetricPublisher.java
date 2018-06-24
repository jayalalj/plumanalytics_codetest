package com.plumanalytics.codetest;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.plumanalytics.codetest.TestMetricPublisher.CountInstance;

/**
 * Used to publish metrics from test files
 */
public class TestMetricPublisher implements MetricPublisher {

	Map<Date, Map<String, CountInstance>> metricMapByDate = new HashMap<Date, Map<String, CountInstance>>();
	Map<String, CountInstance> aggregateCountMapById = new HashMap<String, CountInstance>();

	public Map<Date, Map<String, CountInstance>> getMetricMapByDate() {
		return metricMapByDate;
	}

	public Map<String, CountInstance> getAggregateCountMapById() {
		return aggregateCountMapById;
	}

	@Override
	public void publishMetric(MetricMessage message) {
		// message is per thread, per line
		TestMetricMessage metricMessage = (TestMetricMessage) message;
		// multiple message can have same date, risk of replacing, this is a
		// critical section
		Map<String, CountInstance> oneMetric;
		synchronized (metricMapByDate) {
			oneMetric = metricMapByDate.get(metricMessage.getMetricDate());
			if (oneMetric == null) {
				oneMetric = new HashMap<String, CountInstance>();
				metricMapByDate.put(metricMessage.getMetricDate(), oneMetric);
			}
		}

		//Message id for given date is unique, no overstepping here
		CountInstance countInstance = oneMetric.get(metricMessage.getId());
		if (countInstance == null) {
			countInstance = new CountInstance();
			oneMetric.put(metricMessage.getId(), countInstance);
		}
        
		countInstance.addCounts(metricMessage.getCount1(), metricMessage.getCount2(), metricMessage.getCount3());

		//Id is unique only within date, no dt groping, again critical section
		CountInstance aggrCountInstance ;
		synchronized (aggregateCountMapById) {
			 aggrCountInstance = aggregateCountMapById.get(metricMessage.getId());

			if (aggrCountInstance == null) {
				aggrCountInstance = new CountInstance();
				aggregateCountMapById.put(metricMessage.getId(), aggrCountInstance);

			}
		}

		aggrCountInstance.addCounts(metricMessage.getCount1(), metricMessage.getCount2(),	metricMessage.getCount3());
	}

	public MetricMessage createMessage(String line) throws ParseException {
		MetricMessage message = new TestMetricMessage();
		message.init(line);
		return message;
	}

	public String toString() {
		StringBuilder asString = new StringBuilder();
		asString.append(metricMapByDate.toString());
		asString.append("\n");
		asString.append(aggregateCountMapById.toString());
		return asString.toString();
	}

	class CountInstance {
		int count1;
		int count2;
		int count3;

		synchronized void addCounts(int count1, int count2, int count3) {
			this.count1 += count1;
			this.count2 += count2;
			this.count3 += count3;
		}

		public String toString() {
			return count1 + "\t" + count2 + "\t" + count3;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + count1;
			result = prime * result + count2;
			result = prime * result + count3;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			CountInstance other = (CountInstance) obj;
			if (count1 != other.count1)
				return false;
			if (count2 != other.count2)
				return false;
			if (count3 != other.count3)
				return false;
			return true;
		}

		private TestMetricPublisher getOuterType() {
			return TestMetricPublisher.this;
		}
		
		
		
	}
	
}
