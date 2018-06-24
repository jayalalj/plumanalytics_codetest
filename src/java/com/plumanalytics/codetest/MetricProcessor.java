package com.plumanalytics.codetest;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Process metric files using a thread pool
 */
public class MetricProcessor {

	private File sourceDir;
	MetricPublisher publisher = new TestMetricPublisher();
	BlockingQueue<MetricMessage> blockingQueue =  new LinkedBlockingQueue<>();
	
	protected MetricProcessor(File sourceDir) {
		this.sourceDir = sourceDir;

	}

	protected void processFilesMultiThreaded(List<File> fileList) throws InterruptedException {
		LinkedBlockingQueue<Runnable> producerWorkQueueu = new LinkedBlockingQueue<Runnable>(5);
		ThreadPoolExecutor producerThreadPool = new ThreadPoolExecutor(10, 10, 5000L, TimeUnit.MILLISECONDS, producerWorkQueueu, new ThreadPoolExecutor.CallerRunsPolicy());
		for (File oneFile : fileList) {
			producerThreadPool.execute(new MetricProcessFileThread(oneFile, publisher, blockingQueue));
		}

		producerThreadPool.shutdown();
		while (!producerThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
			System.out.println("Waiting for queue to complete. QueueSize=" + producerWorkQueueu.size() + " ActiveThreads=" + producerThreadPool.getActiveCount());
		}
		System.out.println("File processing complete "+blockingQueue.size());
		
		for (MetricMessage msg : blockingQueue) {
			MetricMessagePublisherThread pubThread = new MetricMessagePublisherThread(publisher, msg);
			Thread pt = new Thread(pubThread);
			pt.start();
			pt.join();
		}

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
		// comment map printing, with sysouts cannot loop enough to test.
		// System.out.println(this.publisher);
	}


}
