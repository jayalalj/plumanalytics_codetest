package com.plumanalytics.codetest;

import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.plumanalytics.codetest.TestMetricPublisher.CountInstance;

/**
 * Process metric files using a thread pool
 */
public class MetricProcessor {

	File sourceDir;
	MetricPublisher publisher = new TestMetricPublisher();

	public static void main(String args[]) {
		Map<Date, Map<String, CountInstance>> dateMapCurr = null, dateMapPrev = null;
		Map<String, CountInstance> countMapByIdCurr = null, countMapByIdPrv = null;
		try {
			for (int i = 0; i < 2000 ; i++) {
				URL url = MetricProcessor.class.getResource("/test-data");
				File testDataDir = new File(url.toURI());
				MetricProcessor processor = new MetricProcessor(testDataDir);
				processor.run();
				dateMapPrev = dateMapCurr;
				dateMapCurr = processor.publisher.getMetricMapByDate();
				countMapByIdPrv = countMapByIdCurr;
				countMapByIdCurr = processor.publisher.getAggregateCountMapById();
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
				System.out.println("broken, Incetance map keys not mached");
				throw new RuntimeException("broken, Incetance map keys not mached");
			}
			if (!prvInstMap.get(key).equals(curInstMap.get(key))) {
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%    broken, Incetance map values not match-----" + key);
				System.out.println(prvInstMap.get(key).toString());
				System.out.println(curInstMap.get(key).toString());
				throw new RuntimeException("broken, Incetance map values not match-----" + key);
			}
		}
	}

	private MetricProcessor(File sourceDir) {
		this.sourceDir = sourceDir;

	}

	protected void processFilesMultiThreaded(List<File> fileList) throws InterruptedException {
		LinkedBlockingQueue<Runnable> workQueueu = new LinkedBlockingQueue<Runnable>(5);
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(10, 10, 5000L, TimeUnit.MILLISECONDS, workQueueu, new ThreadPoolExecutor.CallerRunsPolicy());
		for (File oneFile : fileList) {
			threadPool.execute(new ProcessFileThread(oneFile));
		}
		threadPool.shutdown();
		while (!threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
			System.out.println("Waiting for queue to complete. QueueSize=" + workQueueu.size() + " ActiveThreads="+ threadPool.getActiveCount());
		}
		// System.out.println("File list processing complete.");
	}

	protected List<File> listFiles() {
		return Arrays.asList(this.sourceDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().endsWith(".txt");
			}
		}));
	}

	public void run() throws InterruptedException {
		processFilesMultiThreaded(listFiles());
		System.out.println(this.publisher);
	}

	private class ProcessFileThread implements Runnable {
		File sourceFile;

		ProcessFileThread(File sourceFile) {
			this.sourceFile = sourceFile;
		}

		@Override
		public void run() {
			// System.out.println(Thread.currentThread().getId() + " -
			// Processing file: " + sourceFile.getName());
			try {
				BufferedReader reader = new BufferedReader(new FileReader(sourceFile));
				String line = null;
				while ((line = reader.readLine()) != null) {
					try {
						MetricMessage message = publisher.createMessage(line);
						publisher.publishMetric(message);
					} catch (Throwable e) {
						throw new RuntimeException(
								"Unable to parse date from row in file: " + sourceFile.getAbsolutePath() + " - " + line,e);
					}
				}
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException("Failed to process file: " + sourceFile.getAbsolutePath(), e);
			}
		}
	}

}
