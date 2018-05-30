package com.gaurav.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.gaurav.kafka.streams.constants.IKafkaConstants;
import com.gaurav.kafka.streams.consumer.ConsumerCreator;
import com.gaurav.kafka.streams.recordcreator.StringRecordCreator;
import com.gaurav.kafka.streams.streambuilder.WordCountBuilder;
import com.gaurav.kafka.streams.streambuilder.WordCountConsumerBuilder;

public class StreamsApp {
	public static void main(String[] args) {
	    StreamsApp app = new StreamsApp();

		app.start();
		
	}
	
	private void startWordCountCOnsumer() {
		int noMessageToFetch=0;
		
		Consumer<String, Long> consumer=ConsumerCreator.createConsumer();
		while(true) {
			final ConsumerRecords<String, Long> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
	}
	
	

	private void start() {
		Thread recordCreate = new Thread(() -> {
			StringRecordCreator.createRecord();
		});

		recordCreate.setName("Producer");
		recordCreate.start();

		Thread streamThread = new Thread(() -> {

			WordCountBuilder.startStream();
		});

		streamThread.setName("Stream Thread");
//		streamThread.start();
//		WordCountConsumerBuilder.startWordCountCOnsumerStream();
	}

}