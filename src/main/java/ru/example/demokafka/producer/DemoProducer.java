package ru.example.demokafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoProducer {
    private final KafkaTemplate<String, String> kafkaTemp;

    public void sendMessage(String topic, String msg) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemp.send(topic, msg);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Отправлено в топик: {}, offset: {}, partition: {}, сообщение: {}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().offset(),
                        result.getRecordMetadata().partition(),
                        msg
                );
            } else {
                log.error("Не удалось отправить в топик: {}, по причине: {}, сообщение: {}",
                        result.getRecordMetadata().topic(),
                        ex.getMessage(),
                        msg
                );
            }
        });
    }
}