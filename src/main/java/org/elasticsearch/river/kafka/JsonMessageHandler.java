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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

/**
 * JsonMessageHandler
 * 
 * Handle a simple json message Uses BulkRequestBuilder to send messages in bulk
 * 
 * example format { "index" : "example_index", "type" : "example_type", "id" : "asdkljflkasjdfasdfasdf", "source" :
 * {"source_data1":"values of source_data1", "source_data2" : 99999 } }
 * 
 * index, type, and source are required id is optional
 * 
 */
public class JsonMessageHandler extends MessageHandler {

	static final ObjectMapper objectMapper = new ObjectMapper();

	final ObjectReader reader = objectMapper.reader(new TypeReference<Map<String, Object>>() {
	});

	static {
		objectMapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
		objectMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
	}

	private Client client;
	private Map<String, Object> messageMap;

	public JsonMessageHandler(Client client) {
		this.client = client;
	}

	protected void readMessage(byte[] playload) throws Exception {
		messageMap = reader.readValue(playload);
	}

	protected String getIndex() {
		Object tags = messageMap.get("tags");
		if (tags != null) {
			return ((ArrayList<String>) tags).get(0);
		} else {
			return getType();
		}
	}

	protected String getType() {
		return (String) messageMap.get("type");
	}

	protected String getId() {
		return (String) messageMap.get("id");
	}

	protected Map<String, Object> getSource() {
		Object msg = messageMap.get("message");
		if (msg instanceof String) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("@timestamp", messageMap.get("@timestamp"));
			addExtra(map);
			map.put("msg", msg);
			return map;
		} else {
			Map<String, Object> map = (Map<String, Object>) msg;
			addExtra(map);
			return map;
		}
	}

	private void addExtra(Map<String, Object> map) {
		if (messageMap.get("host") != null) {
			map.put("@host", messageMap.get("host"));
		}
		if (messageMap.get("path") != null) {
			map.put("@path", messageMap.get("path"));
		}
	}

	protected IndexRequestBuilder createIndexRequestBuilder() {
		// Note: prepareIndex() will automatically create the index if it
		// doesn't exist
		return client.prepareIndex(getIndex(), getType(), getId()).setSource(getSource());
	}

	@Override
	public void handle(BulkRequestBuilder bulkRequestBuilder, byte[] playload) throws Exception {
		this.readMessage(playload);
		if (bulkRequestBuilder != null) {
			bulkRequestBuilder.add(this.createIndexRequestBuilder());
		}
	}

}
