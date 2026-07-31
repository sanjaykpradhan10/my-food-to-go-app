package com.sanjay.ftgo.common.contracttest;

import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessage;
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessaging;

import java.util.Map;

// ContractVerifierMessaging<M>'s protected convert(M) hook (verified via javap against
// spring-cloud-contract-verifier 4.3.4) unconditionally does
// `new ContractVerifierMessage(payload, null)` - it has no special case for M already being a
// ContractVerifierMessage. Overriding it here is what makes M = byte[] (see
// KafkaMessageVerifierReceiver) turn into a correctly-payloaded ContractVerifierMessage instead
// of a message whose payload is another message.
public class KafkaContractVerifierMessaging extends ContractVerifierMessaging<byte[]> {

    public KafkaContractVerifierMessaging(KafkaMessageVerifierSender sender, KafkaMessageVerifierReceiver receiver) {
        super(sender, receiver);
    }

    @Override
    protected ContractVerifierMessage convert(byte[] payload) {
        return new ContractVerifierMessage(payload, Map.of());
    }
}
