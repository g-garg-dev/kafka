package com.gaurav.kafka.streams.streambuilder;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

import com.gaurav.kafka.streams.constants.IKafkaConstants;

public class WordCountBuilder {
	public static void startStream() {

		Properties props = new Properties();

		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG); // must be unique to multiple
																							// instance running of same
																							// consumer group for same
																							// topic

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // serializer deserializer for key
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // serializer deserializer for value

		StreamsConfig config = new StreamsConfig(props);
		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> source = builder.stream(IKafkaConstants.TOPIC_NAME_STRING);
		
		
		KStream<String, String> wordsStream = source.flatMapValues((key,value) -> {
			System.out.println("Record received with key "+key +"value "+value);
			return Arrays.asList(value.split("\\W+"));
			
		});
		
		KGroupedStream<String, String> wordsGroupedStream =wordsStream.groupBy(new KeyValueMapper<String, String, String>() {

			@Override
			public String apply(String key, String value) {
				return value;
			}
		});
		
		KTable<String, Long> table=wordsGroupedStream.count();
		
		table.toStream().to(IKafkaConstants.WORD_COUNT_TOPIC,Produced.with(Serdes.String(), Serdes.Long()));

		Topology topology = builder.build();
		KafkaStreams stream = new KafkaStreams(topology, config);
		stream.start();
	}
}
