package com.plumanalytics.codetest;

import java.io.File;
import java.net.URL;
import java.util.Date;
import java.util.Map;

import com.plumanalytics.codetest.TestMetricPublisher.CountInstance;

public class Application {

	public static void main(String args[]) {
		Map<Date, Map<String, CountInstance>> dateMapCurr = null, dateMapPrev = null;
		Map<String, CountInstance> countMapByIdCurr = null, countMapByIdPrv = null;
		try {
			//loop over given time and execute metric message processing.
			//in each iteration compare processed values stored in MetricProcessor class maps with previous ran values.
			for (int i = 0; i < 2000 ; i++) {
				URL url = MetricProcessor.class.getResource("/test-data");
				File testDataDir = new File(url.toURI());
				MetricProcessor processor = new MetricProcessor(testDataDir);
				processor.run();
				
				//testing for consistency
				dateMapPrev = dateMapCurr;
				dateMapCurr = processor.getPublisher().getMetricMapByDate();
				countMapByIdPrv = countMapByIdCurr;
				countMapByIdCurr = processor.getPublisher().getAggregateCountMapById();
				if (i > 0) {
					System.out.println("--validating validateConsistancyDateMap -- ");
					validateConsistancyDateMap(dateMapCurr, dateMapPrev);
					System.out.println("--validating validateCountInstMap -- ");
					validateCountInstMap(countMapByIdCurr, countMapByIdPrv);
				}
			}

		} catch (Throwable e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		System.out.println("!!! Go Eagles !!!");
	}

	private static void validateConsistancyDateMap(Map<Date, Map<String, CountInstance>> dateMapCurr,
			Map<Date, Map<String, CountInstance>> dateMapPrev) {
		for (Date dt : dateMapCurr.keySet()) {
			if (!dateMapPrev.containsKey(dt)) {
				System.out.println("broken, date map values not match-----" + dt);
				throw new RuntimeException("broken, date map values not match-----" + dt);
			}

			// validate inner maps
			Map<String, CountInstance> curInstMap = dateMapCurr.get(dt);
			Map<String, CountInstance> prvInstMap = dateMapPrev.get(dt);
			validateCountInstMap(curInstMap, prvInstMap);

		}
	}

	private static void validateCountInstMap(Map<String, CountInstance> curInstMap,
			Map<String, CountInstance> prvInstMap) {
		for (String key : curInstMap.keySet()) {
			if (!prvInstMap.containsKey(key)) {
				System.out.println("broken, Instance map keys not mached");
				throw new RuntimeException("broken, Instance map keys not mached");
			}
			if (!prvInstMap.get(key).equals(curInstMap.get(key))) {
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%    broken, Instance map values not match-----" + key);
				System.out.println(prvInstMap.get(key).toString());
				System.out.println(curInstMap.get(key).toString());
				throw new RuntimeException("broken, Instance map values not match-----" + key);
			}
		}
	}

}
