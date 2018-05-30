package com.gaurav.kafka.streams.recordcreator;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.gaurav.kafka.streams.producer.ProducerCreator;

public class StringRecordCreator {
	public static void createRecord() {
		Producer<String, String> producer = ProducerCreator.createProducerString();
		for (int i = 0; i < 1000000; i++) {
			final ProducerRecord<String, String> record = new ProducerRecord<String, String>(
					com.gaurav.kafka.streams.constants.IKafkaConstants.TOPIC_NAME_STRING,  Integer.toString(i),
					"Hello this is gaurav");

			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + i + " to partition "
						+ metadata.partition() + " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}

//			if(i%100==0) {
//				try {
//					Thread.sleep(3000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
		}
	}
}
