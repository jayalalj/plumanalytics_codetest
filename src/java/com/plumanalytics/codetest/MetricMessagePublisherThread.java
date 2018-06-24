package com.plumanalytics.codetest;

public class MetricMessagePublisherThread implements Runnable {
	MetricPublisher publisher;
	MetricMessage msg;

	public MetricMessagePublisherThread(MetricPublisher publisher, MetricMessage msg) {
		this.publisher = publisher;
		this.msg = msg;
	}

	@Override
	public void run() {
		try {
				TestMetricMessage tmm = (TestMetricMessage) this.msg;
				System.out.println("consuming message   : " + tmm.getMetricDate() + " - " + tmm.getId());
				publisher.publishMetric(msg);
			
			
//			messagesToProcess.stream().forEach(msg -> publisher.publishMetric(msg));;
//			System.out.println("consuming thread   :" + blockingQueue.size());
//			int count = Constants.MESSAGES_FOR_PUBLISHER_THREAD;
//			
//				MetricMessage msg = blockingQueue.take();
//				TestMetricMessage tmm = (TestMetricMessage) msg;
//				System.out.println("consuming message   : " + tmm.getMetricDate() + " - " + tmm.getId());
//				publisher.publishMetric(msg);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("InterruptedException", e);
		}
	}

}
