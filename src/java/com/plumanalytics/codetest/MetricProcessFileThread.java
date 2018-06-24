package com.plumanalytics.codetest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class MetricProcessFileThread implements Runnable {
	File sourceFile;
	MetricPublisher publisher;
	BlockingQueue<MetricMessage> blockingQueue;

	MetricProcessFileThread(File sourceFile, MetricPublisher publisher, BlockingQueue<MetricMessage> blockingQueue) {
		this.sourceFile = sourceFile;
		this.publisher = publisher;
		this.blockingQueue = blockingQueue;
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
					blockingQueue.put(message);
					//publisher.publishMetric(message);
				} catch (Throwable e) {
					throw new RuntimeException(	"Unable to parse date from row in file: " + sourceFile.getAbsolutePath() + " - " + line,e);
				}
			}
			reader.close();
		} catch (IOException e) {
			throw new RuntimeException("Failed to process file: " + sourceFile.getAbsolutePath(), e);
		}
	}
}