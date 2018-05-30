package com.gaurav.kafka.streams.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class CustomPartitioner implements Partitioner{

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer keyInt=Integer.parseInt(key.toString());
        return keyInt%50;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
    
}