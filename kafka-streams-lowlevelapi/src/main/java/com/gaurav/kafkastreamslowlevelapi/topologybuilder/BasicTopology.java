package com.gaurav.kafkastreamslowlevelapi.topologybuilder;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import com.gaurav.kafkastreamslowlevelapi.constants.IKafkaConstants;
import com.gaurav.kafkastreamslowlevelapi.processor.SourceProcessor;
import com.gaurav.kafkastreamslowlevelapi.timestampextractor.CustomTimestampExtractor;


public class BasicTopology {
    public static void build() {
        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG_CONSUMER); // must be unique within the cluster
                                                                                                                                                                                // instance running of same
                                                                                                                                                                                // consumer group for same
                                                                                                                                                                                // topic

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // serializer
                                                                                                                                                                                                        // deserializer
                                                                                                                                                                                                        // for key
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // serializer
                                                                                                                                                                                                                // deserializer
                                                                                                                                                                                                                // for value

        StreamsConfig config = new StreamsConfig(props);
        Topology builder=new Topology();
        builder.addSource(new CustomTimestampExtractor(),"Source", IKafkaConstants.TOPIC_NAME_STRING);
        builder.addProcessor("Parent", ()->new SourceProcessor(), "Source");
        KafkaStreams stream=new KafkaStreams(builder, config);
        stream.start();
    }
}
