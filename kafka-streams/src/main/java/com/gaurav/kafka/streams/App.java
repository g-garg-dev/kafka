package com.gaurav.kafka.streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import com.gaurav.kafka.streams.constants.IKafkaConstants;
import com.gaurav.kafka.streams.pojo.Transaction;
import com.gaurav.kafka.streams.producer.ProducerCreator;
import com.gaurav.kafka.streams.transactiontype.TransactionType;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		App app = new App();

		app.start();
	}

	private void start() {
		Thread recordCreate = new Thread(() -> {
			 createRecord();
		});

		recordCreate.setName("Producer");
		recordCreate.start();

		Thread streamThread = new Thread(() -> {

			startStream();
		});

		streamThread.setName("Stream Thread");
		streamThread.start();

		Serdes.Long();
	}

	private void startStream() {

		 Properties props=new Properties();
		 
		 props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		 props.put(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG); // must be unique to multiple instance running of same consumer group for same topic
		 
		 props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String());      // serializer deserializer for key
		 props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());		// serializer deserializer for value
		 
		 StreamsConfig config=new StreamsConfig(props);
		 StreamsBuilder builder=new StreamsBuilder();
		 
		 KStream<String, String> source = builder.stream(IKafkaConstants.TOPIC_NAME_NEW);
		 
		 KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));

		 
		 Topology topology=builder.build();
		 KafkaStreams stream=new KafkaStreams(topology, config);
		 stream.start();
	}

	private void createRecord() {
		String transactionIdPrefix = "Transaction:";
		Producer<String, Transaction> producer = ProducerCreator.createProducer();
		for (int i = 0; i < 5000; i++) {
			Transaction transaction = new Transaction();
			transaction.setAccountNo("987123456");
			transaction.setCardNo("1234-5678-9123-2313");
			transaction.setAmount(new Random().nextDouble() * 1000);
			transaction.setName("Gaurav Garg");
			if (i % 3 == 0) {
				transaction.setTransactionType(TransactionType.DEBIT.getValue());
			} else {
				transaction.setTransactionType(TransactionType.CREDIT.getValue());
			}
			final ProducerRecord<String, Transaction> record = new ProducerRecord<String, Transaction>(
					com.gaurav.kafka.streams.constants.IKafkaConstants.TOPIC_NAME, transactionIdPrefix + i,
					transaction);

			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + transactionIdPrefix + i + " to partition "
						+ metadata.partition() + " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}

		}
	}

}