package com.spothi.provi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;


@Component
@Slf4j
public class Processor {

    @Autowired
    void streamProcessor(StreamsBuilder builder) {
//        final Properties streamsConfiguration = new Properties();
//        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
//        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
//        streamsConfiguration.put("schema.registry.url", "http://broker:8081");

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://broker:8081");
        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        final Serde<String> STRING_SERDE = Serdes.String();
        keyGenericAvroSerde.configure(serdeConfig, true);
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig,false);
        KStream<String, GenericRecord> textLines =
                builder.stream("test", Consumed.with(STRING_SERDE, valueGenericAvroSerde));

        List<Provider> providers = new ArrayList<>();
        Timer timer = new Timer();
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                if(providers.size() > 0) {
                    log.info(String.format("providers added in the last 10 seconds --- %s", providers.size()));
                    providers.clear();
                }
            }
        };
        textLines.peek((key, value) -> log.info("------" + value.toString() + "------" ))
                .foreach((key, value) -> {
                    ObjectMapper objectMapper = new ObjectMapper();
                    Provider provider = null;
                    try {
                        provider = objectMapper.readValue(value.toString(), Provider.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    providers.add(provider);
                });
        timer.scheduleAtFixedRate(timerTask, 10000,10000);

    }
}
