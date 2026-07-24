package com.sanjay.ftgo.accounting.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.accounting.domain.AuthorizationCancelService;
import com.sanjay.ftgo.accounting.domain.DeliveryEvent;
import com.sanjay.ftgo.accounting.domain.SagaJoinService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final SagaJoinService sagaJoinService;
    private final AuthorizationCancelService authorizationCancelService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(SagaJoinService sagaJoinService,
                                  AuthorizationCancelService authorizationCancelService,
                                  ObjectMapper objectMapper) {
        this.sagaJoinService = sagaJoinService;
        this.authorizationCancelService = authorizationCancelService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "accounting-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "DeliveryScheduled", "DeliverySchedulingFailed" ->
                    sagaJoinService.handleDeliveryEvent(event.eventId(), event.orderId(), event.eventType());
            // Cancel Order's reversal trigger moved here from KitchenEventListener's
            // "TicketCancelled" case: the saga sequence is now kitchen -> delivery-release
            // -> accounting, so accounting must wait for delivery-service's DeliveryCancelled,
            // not react directly to the kitchen event.
            case "DeliveryCancelled" -> authorizationCancelService.reverseForChoreography(event.eventId(), event.orderId());
            default -> { }
        }
    }
}
