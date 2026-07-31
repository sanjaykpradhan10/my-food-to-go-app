package com.sanjay.ftgo.common.contracttest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierReceiver;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

// See KafkaMessageVerifierSender for the rationale behind this hand-written bridge. Only the
// two abstract methods are overridden; the two single-arg/no-timeout overloads on
// MessageVerifierReceiver are default methods that delegate to these, per javap against
// spring-cloud-contract-verifier 4.3.4.
//
// M is the raw message payload (byte[]), not ContractVerifierMessage: ContractVerifierMessaging
// .receive() always wraps whatever this returns in a fresh ContractVerifierMessage via its
// convert(M) hook (verified via javap - convert() does `new ContractVerifierMessage(payload, null)`
// unconditionally). Returning an already-built ContractVerifierMessage here would get double-wrapped,
// so KafkaContractTestSupport's ContractVerifierMessaging bean must be the KafkaContractVerifierMessaging
// subclass, whose convert() override unwraps this byte[] correctly.
public class KafkaMessageVerifierReceiver implements MessageVerifierReceiver<byte[]> {

    private final Consumer<String, String> consumer;

    public KafkaMessageVerifierReceiver(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public byte[] receive(String destination, long timeout, TimeUnit timeUnit, YamlContract contract) {
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, destination, Duration.ofMillis(timeUnit.toMillis(timeout)));
        return record.value().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] receive(String destination, YamlContract contract) {
        return receive(destination, 5, TimeUnit.SECONDS, contract);
    }
}
