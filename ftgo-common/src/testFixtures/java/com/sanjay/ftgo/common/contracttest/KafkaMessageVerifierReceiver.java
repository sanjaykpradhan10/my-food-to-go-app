package com.sanjay.ftgo.common.contracttest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierReceiver;
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessage;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// See KafkaMessageVerifierSender for the rationale behind this hand-written bridge. Only the
// two abstract methods are overridden; the two single-arg/no-timeout overloads on
// MessageVerifierReceiver are default methods that delegate to these, per javap against
// spring-cloud-contract-verifier 4.3.4.
public class KafkaMessageVerifierReceiver implements MessageVerifierReceiver<ContractVerifierMessage> {

    private final Consumer<String, String> consumer;

    public KafkaMessageVerifierReceiver(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public ContractVerifierMessage receive(String destination, long timeout, TimeUnit timeUnit, YamlContract contract) {
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, destination, Duration.ofMillis(timeUnit.toMillis(timeout)));
        return new ContractVerifierMessage(record.value(), Map.of());
    }

    @Override
    public ContractVerifierMessage receive(String destination, YamlContract contract) {
        return receive(destination, 5, TimeUnit.SECONDS, contract);
    }
}
