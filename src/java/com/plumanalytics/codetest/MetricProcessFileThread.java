package com.plumanalytics.codetest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MetricProcessFileThread implements Runnable {
	private File sourceFile;
	private MetricPublisher publisher;

	MetricProcessFileThread(File sourceFile, MetricPublisher publisher) {
		this.sourceFile = sourceFile;
		this.publisher = publisher;
	}

	@Override
	public void run() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(sourceFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				try {
					MetricMessage message = publisher.createMessage(line);
					publisher.publishMetric(message);
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