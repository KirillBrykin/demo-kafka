package ru.example.demokafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация потребителя
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // ссылка на кафку
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        // группа топиков
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // кол-во значений которое мы прочитаем при обращении к кафке
        // (важно для производительности если значений очень много)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);

        // задаем максимальный интервал между двумя операциями POLL от consumer
        // (если прошло более 5 секунд и нет второго обращения то
        // брокер понимает что потребитель умер и начнет перебалансировку и поиск кому отдать сообщения)
        // Если на стороне потребителя обработка превышает это время и потребитель живой
        // то это повлияет на корректную работу с очередью и на производительность
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000);

        //Три возможных значения:
        // - latest: (default) что означает, что потребители будут читать сообщения из хвоста раздела
        // - earliest: что означает чтение с самого старого смещения в разделе
        // - none: выкинет исключение для потребителя, если для группы потребителей не найдено предыдущее смещение
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Время ожидания перед повторной попыткой неудачного запроса к заданному разделу темы.
        // Это позволяет избежать многократной отправки запросов в замкнутом цикле в некоторых сценариях сбоя.
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 7000);

        // Как будем десериализуем сообщение
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
