package com.gaurav.kafka.streams.transactiontype;

public enum TransactionType {
	CREDIT("cerdit"),DEBIT("debit");
	
	private String value;
	
	private TransactionType(String value) {
		this.value=value;
	}

	public String getValue() {
		return value;
	}

}
