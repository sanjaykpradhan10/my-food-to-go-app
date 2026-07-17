package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final SagaJoinService sagaJoinService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(SagaJoinService sagaJoinService, ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "accounting-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        sagaJoinService.handleConsumerEvent(event.eventId(), event.orderId(), event.eventType());
    }
}
