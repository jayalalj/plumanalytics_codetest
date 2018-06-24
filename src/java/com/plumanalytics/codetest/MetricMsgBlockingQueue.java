package com.plumanalytics.codetest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MetricMsgBlockingQueue {
	
	private static BlockingQueue<MetricMessage> BLOCKING_QUEUE = new LinkedBlockingQueue<MetricMessage>();

	private MetricMsgBlockingQueue() {
	}

	public static void addMessage(MetricMessage message) throws InterruptedException {
		BLOCKING_QUEUE.put(message);
	}

}
