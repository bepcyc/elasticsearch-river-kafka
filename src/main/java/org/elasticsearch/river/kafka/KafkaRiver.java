/* Copyright 2013 Endgame, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.common.InvalidMessageSizeException;
import kafka.common.OffsetOutOfRangeException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

/**
 * KafkaRiver
 * 
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

	private final Client client;
	private final KafkaRiverConfig riverConfig;

	private volatile boolean closed = false;
	private volatile Thread thread;

	@Inject
	public KafkaRiver(RiverName riverName, RiverSettings settings, Client client) {
		super(riverName, settings);
		this.client = client;

		try {
			logger.info("KafkaRiver created: name={}, type={}", riverName.getName(), riverName.getType());
			this.riverConfig = new KafkaRiverConfig(riverName.getName(), settings);
		} catch (Exception e) {
			logger.error("Unexpected Error occurred", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void start() {
		try {
			logger.info("creating kafka river: zookeeper = {}, name = {}, message_handler_factory_class = {}", riverConfig.zookeeper,
					riverConfig.riverName, riverConfig.factoryClass);
			logger.info("partition = {}, topic = {},offset = {} ", riverConfig.partition, riverConfig.topic, riverConfig.offset);
			logger.info("bulkSize = {}, bulkTimeout = {}", riverConfig.bulkSize, riverConfig.bulkTimeout);

			KafkaRiverWorker worker = new KafkaRiverWorker(this.createMessageHandler(client, riverConfig), riverConfig, client);

			thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka_river").newThread(worker);
			thread.start();
		} catch (Exception e) {
			logger.error("Unexpected Error occurred", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			if (closed) {
				return;
			}
			logger.info("closing kafka river");
			closed = true;
			if (thread != null) {
				thread.interrupt();
			}
		} catch (Exception e) {
			logger.error("Unexpected Error occurred", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * createMessageHandler
	 * 
	 * 
	 * @param client
	 * @param config
	 * @return
	 * @throws Exception
	 */
	private MessageHandler createMessageHandler(Client client, KafkaRiverConfig config) throws Exception {
		MessageHandlerFactory handlerfactory = null;
		try {
			handlerfactory = (MessageHandlerFactory) Class.forName(config.factoryClass).newInstance();
		} catch (Exception e) {
			logger.error("Unexpected Error occurred", e);
			throw new RuntimeException(e);
		}

		return handlerfactory.createMessageHandler(client);
	}

	public static void main(String[] args) throws IOException {
		Map<String, Object> kafkaMap = new HashMap<String, Object>();
		String host = "10.120.104.124";
		kafkaMap.put("zookeeper", host + ":2181");
		String topic = "testomad";
		kafkaMap.put("topic", topic);
		kafkaMap.put("partition", "-1");
		// kafka.put("message_handler_factory_class", "my.factory.class.MyFactory");

		Map<String, Object> index = new HashMap<String, Object>();
		index.put("bulk_size_bytes", "1717171");
		index.put("bulk_timeout", "111ms");

		Map<String, Object> statsd = new HashMap<String, Object>();
		statsd.put("host", host);
		statsd.put("port", "8125");
		statsd.put("prefix", "boo.yeah");

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("kafka", kafkaMap);
		map.put("index", index);
		map.put("statsd", statsd);
		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(host, 9300));
		/*
		 * Properties props = new Properties(); props.put("zookeeper.connect", "omad1.server.163.org:2181"); props.put("group.id", "test");
		 * props.put("zookeeper.session.timeout.ms", "10000"); props.put("zookeeper.sync.time.ms", "200"); props.put("auto.commit.interval.ms",
		 * "1000"); props.put("auto.offset.reset", "smallest"); ConsumerConfig config = new ConsumerConfig(props); ConsumerConnector consumer =
		 * kafka.consumer.Consumer.createJavaConsumerConnector(config);
		 * 
		 * Map<String, Integer> topicCountMap = new HashMap<String, Integer>(); topicCountMap.put(topic, new Integer(1)); Map<String,
		 * List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap); // KafkaStream<byte[], byte[]> stream =
		 * consumerMap.get(topic); List<KafkaStream<byte[], byte[]>> partitions = consumerMap.get(topic); ExecutorService threadPool =
		 * Executors.newFixedThreadPool(1 * 2); // start
		 * 
		 * for (KafkaStream<byte[], byte[]> partition : partitions) { ConsumerIterator<byte[], byte[]> it = partition.iterator(); while (it.hasNext())
		 * { // connector.commitOffsets();手动提交offset,当autocommit.enable=false时使用 MessageAndMetadata<byte[], byte[]> item = it.next(); byte[] playload
		 * = item.message(); System.out.println(new String(playload)); } }
		 */

		RiverSettings settings = new RiverSettings(ImmutableSettings.settingsBuilder().build(), map);
		KafkaRiverConfig c = new KafkaRiverConfig("testRiver", settings);
		KafkaRiver r = new KafkaRiver(new RiverName("kafka", "kafka"), settings, client);
		r.start();
		System.in.read();
	}

	/**
	 * KafkaRiverWorker
	 * 
	 * 
	 */
	public class KafkaRiverWorker implements Runnable {

		KafkaRiverWorkerData river = new KafkaRiverWorkerData(new Stats());

		public KafkaRiverWorker(MessageHandler msgHandler, KafkaRiverConfig riverConfig, Client client) throws Exception {
			this.river.msgHandler = msgHandler;
			this.river.client = client;
			this.river.riverConfig = riverConfig;
			reconnectToKafka();
			resetStats();
			initStatsd(riverConfig);
		}

		void initStatsd(KafkaRiverConfig riverConfig) {
			river.statsd = new StatsReporter(riverConfig);
			if (river.statsd.isEnabled()) {
				logger.info("Created statsd client for prefix={}, host={}, port={}", riverConfig.statsdPrefix, riverConfig.statsdHost,
						riverConfig.statsdPort);
			} else {
				logger.info("Note: statsd is not configured, only console metrics will be provided");
			}
		}

		void resetStats() {
			river.statsLastPrintTime = System.currentTimeMillis();
			river.stats.reset();
		}

		ConsumerConnector consumer = null;

		private void createConsumerConfig() {
			Properties props = new Properties();
			props.put("zookeeper.connect", river.riverConfig.zookeeper);
			props.put("group.id", river.riverConfig.riverName);
			props.put("zookeeper.session.timeout.ms", "10000");
			props.put("zookeeper.sync.time.ms", "200");
			props.put("auto.commit.interval.ms", "1000");
			if (river.riverConfig.offset <= -1) {
				props.put("auto.offset.reset", "largest");
			} else {
				// ZkUtils.maybeDeletePath(props.getProperty("zookeeper.connect"), "/consumers/" + props.getProperty("group.id"));
				props.put("auto.offset.reset", "smallest");
			}
			props.put("auto.offset.reset", "largest");
			logger.info("auto.offset.reset {}", props.getProperty("auto.offset.reset"));
			ConsumerConfig config = new ConsumerConfig(props);
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
		}

		public void run() {
			if (river.riverConfig.partition < 0) {
				runAll();
			} else {
				runSingle();
			}
		}

		public void runAll() {
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(river.riverConfig.topic, new Integer(1));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			// KafkaStream<byte[], byte[]> stream = consumerMap.get(topic);
			List<KafkaStream<byte[], byte[]>> partitions = consumerMap.get(river.riverConfig.topic);
			// start
			final MessageExecutor executor = new MessageExecutor() {

				@Override
				public void execute(byte[] playload) {
					BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
					try {
						logger.info(new String(playload));
						river.msgHandler.handle(bulkRequestBuilder, playload);
					} catch (Exception e) {
						e.printStackTrace();
					}
					executeBuilder(bulkRequestBuilder);
				}

			};

			for (KafkaStream<byte[], byte[]> partition : partitions) {
				try {
					ConsumerIterator<byte[], byte[]> it = partition.iterator();
					while (it.hasNext()) {
						// connector.commitOffsets();手动提交offset,当autocommit.enable=false时使用
						MessageAndMetadata<byte[], byte[]> item = it.next();
						try {
							long numMsg = 0;
							++numMsg;
							++river.stats.numMessages;
							try {
								executor.execute(item.message());
								logger.info("handleMessages processed {} messages stats {} ", numMsg, river.stats.numMessages);
							} catch (Exception e) {
								logger.warn("Failed handling message", e);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		void initKakfa() {
			if (river.riverConfig.partition < 0) {
				createConsumerConfig();
			} else {
				// from single partition
				this.river.kafka = new KafkaClient(river.riverConfig.zookeeper, river.riverConfig.topic, river.riverConfig.partition);
				this.river.offset = river.kafka.getOffset(river.riverConfig.riverName, river.riverConfig.topic, river.riverConfig.partition,
						river.riverConfig.offset);
				// this.river.offset = 0;
			}
		}

		void handleMessages(BulkRequestBuilder bulkRequestBuilder, ByteBufferMessageSet msgs) {
			long numMsg = 0;
			for (MessageAndOffset mo : msgs) {
				++numMsg;
				++river.stats.numMessages;
				try {
					ByteBuffer buf = mo.message().payload();
					byte[] playload = new byte[buf.remaining()];
					buf.get(playload);
					river.msgHandler.handle(bulkRequestBuilder, playload);
					river.offset = mo.nextOffset();
				} catch (Exception e) {
					logger.warn("Failed handling message", e);
				}
			}
			logger.info("handleMessages processed {} messages", numMsg);
		}

		void executeBuilder(BulkRequestBuilder bulkRequestBuilder) {
			if (bulkRequestBuilder == null || bulkRequestBuilder.numberOfActions() == 0)
				return;
			logger.info("BulkRequestBuilder processed {} messages", bulkRequestBuilder.numberOfActions());
			++river.stats.flushes;
			BulkResponse response = bulkRequestBuilder.execute().actionGet();
			if (response.hasFailures()) {
				logger.warn("failed to execute " + response.buildFailureMessage());
			}
			for (BulkItemResponse resp : response) {
				if (resp.isFailed()) {
					river.stats.failed++;
				} else {
					river.stats.succeeded++;
				}
			}
		}

		void processNonEmptyMessageSet(ByteBufferMessageSet msgs) {
			logger.debug("Processing {} bytes of messages ...", msgs.validBytes());
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			handleMessages(bulkRequestBuilder, msgs);
			executeBuilder(bulkRequestBuilder);
		}

		void reconnectToKafka() throws InterruptedException {
			while (true) {
				if (closed)
					break;

				try {
					try {
						if (river.kafka != null) {
							river.kafka.close();
						}
					} catch (Exception e) {
					}

					initKakfa();
					break;
				} catch (Exception e2) {
					logger.error("Error re-connecting to Kafka({}/{}), retrying in 5 sec", e2, river.riverConfig.topic, river.riverConfig.partition);
					Thread.sleep(5000);
				}
			}
		}

		long getBacklogSize() {
			return river.kafka.getNewestOffset(river.riverConfig.topic, river.riverConfig.partition) - river.offset;
		}

		void dumpStats() {
			long elapsed = System.currentTimeMillis() - river.statsLastPrintTime;
			if (elapsed >= 10000) {
				river.stats.backlog = getBacklogSize();
				river.stats.rate = (double) river.stats.numMessages / ((double) elapsed / 1000.0);
				logger.info("{}:{}/{}:{} {} msg ({} msg/s), flushed {} ({} err, {} succ) [msg backlog {}]", river.kafka.brokerHost,
						river.kafka.brokerPort, river.riverConfig.topic, river.riverConfig.partition, river.stats.numMessages,
						String.format("%.2f", river.stats.rate), river.stats.flushes, river.stats.failed, river.stats.succeeded,
						getBytesString(river.stats.backlog));
				river.statsd.reoportStats(river.stats);
				resetStats();
			}
		}

		public void runSingle() {

			try {
				logger.info("KafkaRiverWorker is running...");

				while (true) {
					if (closed)
						break;

					try {

						dumpStats();

						ByteBufferMessageSet msgs = river.kafka.fetch(river.riverConfig.topic, river.riverConfig.partition, river.offset,
								river.riverConfig.bulkSize);
						if (msgs.validBytes() > 0) {
							processNonEmptyMessageSet(msgs);
							river.kafka.saveOffset(river.riverConfig.riverName, river.riverConfig.topic, river.riverConfig.partition, river.offset);
						} else {
							logger.debug("No messages received from Kafka for topic={}, partition={}, offset={}, bulkSize={}",
									river.riverConfig.topic, river.riverConfig.partition, river.offset, river.riverConfig.bulkSize);
							Thread.sleep(1000);
						}
					} catch (InterruptedException e2) {
						break;
					} catch (OffsetOutOfRangeException e) {
						// Assumption: EITHER
						//
						// 1) This River is starting for the first time and Kafka has already aged some data out (so the lowest offset is not 0)
						// OR
						// 2) This river has gotten far enough behind that Kafka has aged off enough data that the offset is no longer valid.
						// If this is the case, this will likely happen everytime Kafka ages off old data unless the data flow decreases in volume.
						resetOffset();
					} catch (InvalidMessageSizeException e) {
						resetOffset();
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e2) {
							break;
						}
					} catch (Exception e) {
						logger.error("Error fetching from Kafka({}:{}/{}:{}), retrying in 5 sec", e, river.kafka.brokerHost, river.kafka.brokerPort,
								river.riverConfig.topic, river.riverConfig.partition);
						try {
							Thread.sleep(5000);
							reconnectToKafka();
						} catch (InterruptedException e2) {
							break;
						}
					}
				} // end while
				river.kafka.close();
				logger.info("KafkaRiverWorker is stopping...");
			} catch (Exception e) {
				logger.error("Unexpected Error Occurred", e);

				// Don't normally like to rethrow exceptions like this, but ES silently ignores them in Plugins
				throw new RuntimeException(e);
			}
		} // end run

		private void resetOffset() {
			if (river.riverConfig.offset <= -1) {
				logger.warn("Encountered OffsetOutOfRangeException, querying Kafka for newest Offset and reseting local offset");
				river.offset = river.kafka.getNewestOffset(river.riverConfig.topic, river.riverConfig.partition);
				logger.warn("Setting offset to oldest offset = {}", river.offset);
			} else if (river.riverConfig.offset == 0) {
				logger.warn("Encountered OffsetOutOfRangeException, querying Kafka for oldest Offset and reseting local offset");
				river.offset = river.kafka.getOldestOffset(river.riverConfig.topic, river.riverConfig.partition);
				logger.warn("Setting offset to oldest offset = {}", river.offset);
			} else {
				logger.warn("Encountered OffsetOutOfRangeException, set Offset and reseting local offset");
				river.offset = river.riverConfig.offset;
				logger.warn("Setting offset to oldest offset = {}", river.offset);
			}
		}
	}

	/**
	 * @param bytes
	 * @return
	 */
	static String getBytesString(long bytes) {
		String size;
		if (Math.floor(bytes / (1024 * 1024 * 1024)) > 0) {
			size = String.format("%.2f GB", (double) bytes / (1024.0 * 1024.0 * 1024.0));
		} else if (Math.floor(bytes / (1024 * 1024)) > 0) {
			size = String.format("%.2f MB", (double) bytes / (1024.0 * 1024.0));
		} else if (Math.floor(bytes / (1024)) > 0) {
			size = String.format("%.2f KB", (double) bytes / (1024.0));
		} else {
			size = bytes + " B";
		}
		return size;
	}
}
