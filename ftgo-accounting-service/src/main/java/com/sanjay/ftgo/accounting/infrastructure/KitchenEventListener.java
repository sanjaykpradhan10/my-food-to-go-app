package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.KitchenEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final SagaJoinService sagaJoinService;
    private final AuthorizationCancelService authorizationCancelService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(SagaJoinService sagaJoinService,
                                 AuthorizationCancelService authorizationCancelService,
                                 ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationCancelService = authorizationCancelService;
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
        switch (event.eventType()) {
            case "TicketCreated", "TicketCreationFailed" ->
                    sagaJoinService.handleKitchenEvent(event.eventId(), event.orderId(), event.eventType(), event.totalQuantity());
            case "TicketCancelled" -> authorizationCancelService.reverse(event.eventId(), event.orderId(), "CancelOrder");
            default -> { }
        }
    }
}
