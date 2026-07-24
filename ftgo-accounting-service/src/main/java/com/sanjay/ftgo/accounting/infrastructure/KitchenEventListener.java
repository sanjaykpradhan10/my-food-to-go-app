package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationReviseService;
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
    private final AuthorizationReviseService authorizationReviseService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(SagaJoinService sagaJoinService,
                                 AuthorizationReviseService authorizationReviseService,
                                 ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationReviseService = authorizationReviseService;
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
            // TicketCancelled no longer triggers a reversal here: Cancel Order's saga
            // sequence now runs kitchen -> delivery-release -> accounting, so
            // DeliveryEventListener reacts to DeliveryCancelled instead.
            case "TicketQuantityRevised" ->
                    authorizationReviseService.reviseForChoreography(event.eventId(), event.orderId(), event.totalQuantity());
            default -> { }
        }
    }
}
