package org.elasticsearch.river.kafka;

import org.elasticsearch.client.Client;

public class KafkaRiverWorkerData {
	public long offset;
	public MessageHandler msgHandler;
	public KafkaClient kafka;
	public Client client;
	public KafkaRiverConfig riverConfig;
	public StatsReporter statsd;
	public long statsLastPrintTime;
	public Stats stats;

	public KafkaRiverWorkerData(Stats stats) {
		this.stats = stats;
	}
}