package com.sanjay.ftgo.common.contracttest;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Proves the bridge itself works (send via KafkaTemplate, receive via
// KafkaMessageVerifierReceiver, round-tripping through the embedded broker), independent of any
// actual Spring Cloud Contract-generated contract test that later tasks add on top of it.
//
// spring-kafka's EmbeddedKafkaContextCustomizerFactory looks up @EmbeddedKafka via
// TestContextAnnotationUtils.findMergedAnnotation(testClass, ...), which walks the test class's
// own hierarchy/enclosing classes only -- it does NOT see annotations on classes reached solely
// via @Import (like KafkaContractTestSupport). So @EmbeddedKafka must be repeated here (and by
// every later service test that imports KafkaContractTestSupport) even though
// KafkaContractTestSupport already carries it for its own documentation/self-containment.
@EmbeddedKafka(partitions = 1, topics = {"order.events", "kitchen.commands", "saga.replies"})
@SpringBootTest(classes = KafkaMessageVerifierRoundTripTest.TestConfig.class)
class KafkaMessageVerifierRoundTripTest {

    @Configuration
    @Import(KafkaContractTestSupport.class)
    static class TestConfig {
    }

    @Autowired
    private KafkaTemplate<String, String> contractTestKafkaTemplate;

    @Autowired
    private KafkaMessageVerifierReceiver kafkaMessageVerifierReceiver;

    @Test
    void sentMessageIsReceivedOnTheSameTopic() {
        contractTestKafkaTemplate.send("order.events", "{\"eventType\":\"OrderCreated\"}");

        var received = kafkaMessageVerifierReceiver.receive("order.events", null);

        assertEquals("{\"eventType\":\"OrderCreated\"}", received.getPayload());
    }
}
