package com.sanjay.ftgo.common.contracttest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

// Shared fixture imported by each service's messaging contract test (order-service,
// order-history-service, kitchen-service - see later tasks). Centralizing the embedded broker,
// KafkaTemplate, and MessageVerifierSender/Receiver beans here means the three topics only need
// listing once instead of duplicated across each service's own @EmbeddedKafka annotation.
@TestConfiguration
@EmbeddedKafka(partitions = 1, topics = {"order.events", "kitchen.commands", "saga.replies"})
public class KafkaContractTestSupport {

    @Bean
    public KafkaTemplate<String, String> contractTestKafkaTemplate(EmbeddedKafkaBroker broker) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Bean
    public Consumer<String, String> contractTestKafkaConsumer(EmbeddedKafkaBroker broker) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("contract-test-group", "true", broker);
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        broker.consumeFromAllEmbeddedTopics(consumer);
        return consumer;
    }

    @Bean
    public KafkaMessageVerifierSender kafkaMessageVerifierSender(KafkaTemplate<String, String> contractTestKafkaTemplate) {
        return new KafkaMessageVerifierSender(contractTestKafkaTemplate);
    }

    @Bean
    public KafkaMessageVerifierReceiver kafkaMessageVerifierReceiver(Consumer<String, String> contractTestKafkaConsumer) {
        return new KafkaMessageVerifierReceiver(contractTestKafkaConsumer);
    }
}
