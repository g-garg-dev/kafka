package com.gaurav.kafkastreamslowlevelapi.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;

public class SourceProcessor implements Processor<String, String> {
    private ProcessorContext context;

    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {

            public void punctuate(long timestamp) {
                System.out.println("punctuation lag gya. Timestamp " + timestamp);

            }
        });
    }

    public void process(String key, String value) {
        System.out.println("Record received key " + key + " value " + value);
    }

    public void punctuate(long timestamp) {
        System.out.println("why this was called");

    }

    public void close() {

    }

}
