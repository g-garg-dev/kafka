package com.gaurav.kafka.streams.partitioner;

import org.apache.kafka.streams.processor.StreamPartitioner;

public class CustomPartitioner implements StreamPartitioner<String, String> {

	@Override
	public Integer partition(String key, String value, int numPartitions) {
		Integer k = Integer.parseInt(key);
		return k % numPartitions;
	}

}
