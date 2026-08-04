package com.sanjay.ftgo.common.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerConfigTest {

    @Test
    void eventKafkaTemplateHasObservationEnabled() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);

        KafkaProducerConfig config = new KafkaProducerConfig();
        KafkaTemplate<String, String> template = config.eventKafkaTemplate(producerFactory);

        // KafkaTemplate exposes setObservationEnabled() but no public getter, so assert on the
        // backing field directly to verify the config's explicit opt-in actually took effect.
        assertThat((boolean) ReflectionTestUtils.getField(template, "observationEnabled")).isTrue();
    }
}
