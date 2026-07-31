package com.sanjay.ftgo.common.contracttest;

import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;

// Bridges Spring Cloud Contract's generic messaging-verification model onto this project's
// plain spring-kafka setup. Spring Cloud Contract 4.x dropped its own stubbed-Kafka support in
// favor of "bring your own broker" (Testcontainers or, as here, an embedded broker) plus a
// hand-written MessageVerifierSender/Receiver pair -- there is no off-the-shelf Kafka
// integration for a project (like this one) that isn't on Spring Cloud Stream.
//
// Only the two abstract methods are overridden; MessageVerifierSender's other two overloads
// (send(M, String) and send(T, Map, String)) are default methods on the interface that delegate
// to these, per javap against spring-cloud-contract-verifier 4.3.4.
//
// M is byte[], matching KafkaMessageVerifierReceiver - see its Javadoc for why (avoids
// ContractVerifierMessaging.convert() double-wrapping an already-built ContractVerifierMessage).
public class KafkaMessageVerifierSender implements MessageVerifierSender<byte[]> {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageVerifierSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(byte[] message, String destination, YamlContract contract) {
        kafkaTemplate.send(destination, message == null ? null : new String(message, StandardCharsets.UTF_8));
    }

    @Override
    public <T> void send(T payload, Map<String, Object> headers, String destination, YamlContract contract) {
        kafkaTemplate.send(destination, payload == null ? null : payload.toString());
    }
}
