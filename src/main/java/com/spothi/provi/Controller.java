package com.spothi.provi;

import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@RestController
@Slf4j
public class Controller {
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @GetMapping("/send-message")
    public void sendMessage() {
        String userSchema = "";
        Schema.Parser parser = new Schema.Parser();
        try {
            InputStream inputStream = new ClassPathResource("provider.avsc").getInputStream();
            userSchema = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        Faker faker = new Faker();
        String key = faker.idNumber().valid();
        avroRecord.put("ID", faker.number().digit());
        avroRecord.put("NAME", faker.name().fullName());
        ProducerRecord<Object, Object> record = new ProducerRecord<>("test", key, avroRecord);
        kafkaTemplate.send(record);
    }
}
