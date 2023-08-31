package ru.demo.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import ru.example.demokafka.consumer.DemoConsumer;
import ru.example.demokafka.producer.DemoProducer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://${spring.kafka.bootstrap-servers}", "port=29092"})
class EmbeddedKafkaIntegrationTest {
    @Autowired
    private DemoConsumer consumer;

    @Autowired
    private DemoProducer producer;

    @Value(value = "${application.kafka.topic}")
    private String topic;

    @BeforeEach
    void setup() {
        consumer.resetLatch();
    }

    @Test
    void givenEmbeddedKafkaBroker_whenSendingWithSimpleProducer_thenMessageReceived() throws Exception {
        String data = "тестовое сообщение";
        producer.sendMessage(topic, data);
        boolean messageConsumed = consumer.getLatch().await(10, TimeUnit.SECONDS);
        assertTrue(messageConsumed);
        assertThat(consumer.getData(), containsString(data));
    }
}
