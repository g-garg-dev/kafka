package com.gaurav.kafka.streams.streambuilder;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import com.gaurav.kafka.streams.constants.IKafkaConstants;

public class WordCountConsumerBuilder {
	public static void startWordCountCOnsumerStream() {
		Properties props = new Properties();

		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG_CONSUMER); // must be unique within the cluster
																							// instance running of same
																							// consumer group for same
																							// topic

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // serializer
																										// deserializer
																										// for key
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName()); // serializer
																											// deserializer
																											// for value

		StreamsConfig config = new StreamsConfig(props);
		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> source = builder.stream(IKafkaConstants.WORD_COUNT_TOPIC);
		source.print("word count");
		Topology topology=builder.build();
		KafkaStreams stream=new KafkaStreams(topology,config);
		stream.start();
	}
}
