package ru.example.demokafka.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.example.demokafka.producer.DemoProducer;

@RestController
@RequiredArgsConstructor
public class DemoController {
    private final DemoProducer demoProducer;

    @Value(value = "${application.kafka.topic}")
    private String topic;

    @GetMapping("send")
    public void send(@RequestParam String msg) {
        demoProducer.sendMessage(topic, msg);
    }
}
