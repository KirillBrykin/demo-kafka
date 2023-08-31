package ru.example.demokafka.consumer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
public class DemoConsumer {

    @Getter
    @Setter
    private String data;

    @Getter
    private CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = "${application.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String msg) {
        log.info("Получено сообщение: {}", msg);
        this.setData(msg);
        latch.countDown();
    }

    public void resetLatch() {
        latch = new CountDownLatch(1);
    }
}