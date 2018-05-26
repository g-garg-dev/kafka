package com.gaurav.kafka.streams.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gaurav.kafka.streams.pojo.Transaction;

public class TransactionSerializer implements Serializer<Transaction>{

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public byte[] serialize(String topic, Transaction data) {
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retVal = objectMapper.writeValueAsString(data).getBytes();
		} catch (Exception exception) {
			System.out.println("Error in serializing object" + data);
		}
		return retVal;
	}

	@Override
	public void close() {
		
	}

}
