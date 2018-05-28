package com.gaurav.kafka.streams.recordcreator;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.gaurav.kafka.streams.pojo.Transaction;
import com.gaurav.kafka.streams.producer.ProducerCreator;
import com.gaurav.kafka.streams.transactiontype.TransactionType;

public class TransactionRecordCreator {
	public static void createRecord() {
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
