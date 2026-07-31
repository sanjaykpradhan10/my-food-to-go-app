package com.sanjay.ftgo.common.contracttest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

// Shared fixture imported by each service's messaging contract test (order-service,
// kitchen-service). Centralizes the embedded broker, KafkaTemplate, and
// MessageVerifierSender/Receiver beans.
//
// NOTE: @EmbeddedKafka's topic list below does NOT propagate to classes that @Import this
// config - Spring's @Import doesn't carry an imported class's own meta-annotations to the
// importer. Each service's MessagingBase must declare its own @EmbeddedKafka with a topic list
// kept in sync with the one below (see order-service's and kitchen-service's MessagingBase.java).
// This class's own @EmbeddedKafka exists only so this file (and its own
// KafkaMessageVerifierRoundTripTest) is self-contained, not to save the importers any typing.
@TestConfiguration
@EmbeddedKafka(partitions = 1, topics = {
        KafkaContractTestSupport.TOPIC_ORDER_EVENTS,
        KafkaContractTestSupport.TOPIC_KITCHEN_COMMANDS,
        KafkaContractTestSupport.TOPIC_SAGA_REPLIES})
public class KafkaContractTestSupport {

    // Each service's own @EmbeddedKafka topic list (see the class comment above for why it
    // can't just be inherited) should reference these constants rather than re-typing the
    // literal, so a typo or drift fails to compile instead of failing at test-run time.
    public static final String TOPIC_ORDER_EVENTS = "order.events";
    public static final String TOPIC_KITCHEN_COMMANDS = "kitchen.commands";
    public static final String TOPIC_SAGA_REPLIES = "saga.replies";

    // @Primary: services whose @EnableAutoConfiguration also pulls in their own production
    // KafkaTemplate bean (e.g. ftgo-common's OutboxAutoConfiguration -> KafkaProducerConfig's
    // eventKafkaTemplate) would otherwise leave two same-typed candidates for constructor
    // autowiring; contract tests must always resolve to the embedded broker's template.
    @Primary
    @Bean
    public KafkaTemplate<String, String> contractTestKafkaTemplate(EmbeddedKafkaBroker broker) {
        // KafkaTestUtils.producerProps() defaults key.serializer to IntegerSerializer (verified
        // via javap against spring-kafka-test 3.3.16 - undocumented in the Javadoc), which breaks
        // every String-keyed producer in this codebase (e.g. OutboxPublisher keys by aggregate id
        // as a String). Override it to match the String keys this bridge actually sends.
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Bean
    public Consumer<String, String> contractTestKafkaConsumer(EmbeddedKafkaBroker broker) {
        // KafkaTestUtils.consumerProps() defaults key.deserializer to IntegerDeserializer,
        // symmetric with producerProps()'s IntegerSerializer default (see contractTestKafkaTemplate) -
        // override to match the String keys actually on the topic.
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("contract-test-group", "true", broker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
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

    // Spring Cloud Contract's @AutoConfigureMessageVerifier imports several candidate
    // ContractVerifierMessaging wiring configs (Camel/JMS/Stream/Integration/NoOp), each
    // @ConditionalOnMissingBean(ContractVerifierMessaging.class) - a raw-type check. The
    // Integration one is @ConditionalOnClass(Message.class) - always true, since spring-kafka
    // pulls in spring-messaging transitively - and needs a MessageVerifierSender<Message<?>>
    // bean, which this project doesn't provide. Wiring ContractVerifierMessaging ourselves,
    // directly from our sender/receiver, makes every alternative's
    // ConditionalOnMissingBean(ContractVerifierMessaging.class) skip - so the Integration
    // config's incompatible sender requirement is never evaluated. Must be the
    // KafkaContractVerifierMessaging subclass, not a raw ContractVerifierMessaging - see its
    // Javadoc for why.
    @Bean
    public KafkaContractVerifierMessaging contractVerifierMessaging(
            KafkaMessageVerifierSender sender, KafkaMessageVerifierReceiver receiver) {
        return new KafkaContractVerifierMessaging(sender, receiver);
    }
}
