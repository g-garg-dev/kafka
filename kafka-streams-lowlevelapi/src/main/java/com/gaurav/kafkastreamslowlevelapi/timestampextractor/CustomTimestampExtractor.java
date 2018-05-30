package com.gaurav.kafkastreamslowlevelapi.timestampextractor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomTimestampExtractor implements TimestampExtractor{

    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        System.out.println("extractor called");
        return 1;
    }

}
