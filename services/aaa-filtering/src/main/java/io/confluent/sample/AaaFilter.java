package io.confluent.sample;

import io.confluent.sample.streams.SerdeGenerator;
import io.confluent.sample.streams.StreamApp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;


public class AaaFilter extends StreamApp {
    static Logger logger = LoggerFactory.getLogger(AaaFilter.class.getName());

    public static void main(String[] args) throws Exception {
        AaaFilter streamApp = new AaaFilter();
        Properties extraProperties = new Properties();
        streamApp.run(args, extraProperties);
    }

    @Override
    protected void buildTopology(StreamsBuilder builder) {
        // Building topology
        builder.stream(Constant.TOPIC_IN, Consumed.with(Serdes.String(), SerdeGenerator.<Username>getSerde()))
                .filter((k, v) -> v.getName().toString().startsWith("aaa"))
                .to(Constant.TOPIC_OUT, Produced.with(Serdes.String(), SerdeGenerator.<Username>getSerde()));
    }
}
