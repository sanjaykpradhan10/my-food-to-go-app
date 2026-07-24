package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import com.sanjay.ftgo.delivery.domain.KitchenEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Both cases funnel into the same DeliveryService.release: TicketCreationFailed is Create Order
// compensation (delivery may or may not have scheduled yet), TicketCancelled is the real Cancel
// Order trigger (delivery is always scheduled by then, since Cancel is only reachable from an
// already-APPROVED order). release() handles both uniformly - see DeliveryService's own comment.
@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "delivery-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "TicketCreationFailed", "TicketCancelled" -> deliveryService.release(event.eventId(), event.orderId());
            default -> { }
        }
    }
}
