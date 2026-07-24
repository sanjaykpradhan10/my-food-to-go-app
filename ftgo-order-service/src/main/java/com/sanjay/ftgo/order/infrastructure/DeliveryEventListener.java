package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.DeliveryEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final OrderSagaService orderSagaService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(OrderSagaService orderSagaService, ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "order-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        // DeliveryScheduled doesn't move Order (mirrors KitchenEventListener only reacting to
        // TicketCreationFailed, not TicketCreated) — only the failure case triggers a saga transition here.
        if ("DeliverySchedulingFailed".equals(event.eventType())) {
            orderSagaService.reject(event.orderId(), event.eventId());
        }
    }
}
