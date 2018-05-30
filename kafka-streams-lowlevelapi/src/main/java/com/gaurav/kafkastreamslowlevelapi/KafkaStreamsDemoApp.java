package com.gaurav.kafkastreamslowlevelapi;

import com.gaurav.kafkastreamslowlevelapi.topologybuilder.BasicTopology;

public class KafkaStreamsDemoApp {
    public static void main(String[] args) {
        KafkaStreamsDemoApp app = new KafkaStreamsDemoApp();
        app.start();
    }

    public void start() {
        BasicTopology.build();
    }
}
