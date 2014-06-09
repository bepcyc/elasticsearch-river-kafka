package org.elasticsearch.river.kafka;

public interface MessageExecutor {

	public void execute(byte[] playload);

}
