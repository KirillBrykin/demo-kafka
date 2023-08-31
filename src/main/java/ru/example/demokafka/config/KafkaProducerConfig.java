package ru.example.demokafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация отправителя
 */
@Configuration
public class KafkaProducerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.client-id}")
    private String clientId;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // ссылка на кафку
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        // задает кол-во реплик топика для того что бы считать что запись сохранена (влияет на скорость работы)
        // best practices: кол-во реплик - 1 = n реплик для настройки
        configProps.put(ProducerConfig.ACKS_CONFIG, "1");

        // Строка идентификатора для передачи на сервер при отправке запросов. Цель этого состоит в том, чтобы иметь возможность отслеживать источник
        // запросов, помимо IP/порта, позволяя включить логическое имя приложения в server- логирование побочного запроса
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // Время ожидания перед повторной попыткой неудачного запроса к заданному разделу темы.
        // Это позволяет избежать многократной отправки запросов в замкнутом цикле в некоторых сценариях сбоя.
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        // Базовое время ожидания перед попыткой повторного подключения к данному хосту.
        // Это позволяет избежать повторного подключения к хосту в тесном цикле.
        // Это отсрочка применяется ко всем попыткам подключения клиента к брокеру.
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);

        // Максимальное время ожидания в миллисекундах при повторном подключении к брокеру, которому неоднократно не удавалось подключиться.
        // Если указано, отсрочка на хост будет экспоненциально увеличиваться для каждого последовательного сбоя подключения, вплоть до этого максимума.
        // После при расчете увеличения отсрочки добавляется 20% случайного джиттера, чтобы избежать шторма соединений.
        configProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 30000);

        // размер сообщения для отправки его n-количеством за раз
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        // Время через которое нужно отправить сообщения в кафку.
        // Если BATCH_SIZE_CONFIG накопился раньше LINGER_MS_CONFIG, то мы отправляем не дожидаясь.
        // Если не накопилось, то по истечению времени LINGER_MS_CONFIG
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 3000);

        // Как будем сериализовывать сообщение
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemp() {
        return new KafkaTemplate<>(producerFactory());
    }
}
