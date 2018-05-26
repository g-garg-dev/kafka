package com.gaurav.kafka.streams.deserializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gaurav.kafka.streams.pojo.Transaction;

public class TransactionDeserializer implements Deserializer<Transaction> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public Transaction deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		Transaction object = null;
		try {
			object = mapper.readValue(data, Transaction.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	@Override
	public void close() {

	}

}
