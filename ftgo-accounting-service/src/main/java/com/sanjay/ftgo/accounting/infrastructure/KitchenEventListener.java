package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.KitchenEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);
    private static final Set<String> RELEVANT_EVENT_TYPES = Set.of("TicketCreated", "TicketCreationFailed");

    private final SagaJoinService sagaJoinService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(SagaJoinService sagaJoinService, ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "accounting-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        if (!RELEVANT_EVENT_TYPES.contains(event.eventType())) {
            return;
        }
        sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
    }
}
