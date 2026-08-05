package com.sanjay.ftgo.orderhistory.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void listenerContainerFactoryHasObservationEnabled() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "test-group");
        ConsumerFactory<Object, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        KafkaConsumerConfig config = new KafkaConsumerConfig();
        ConcurrentKafkaListenerContainerFactory<?, ?> factory =
                (ConcurrentKafkaListenerContainerFactory<?, ?>) config.kafkaListenerContainerFactory(consumerFactory);

        assertThat(factory.getContainerProperties().isObservationEnabled()).isTrue();
    }
}
