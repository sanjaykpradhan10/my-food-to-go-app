package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.OrderCreatedEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed order event: {}", payload, e);
            return;
        }
        // order-service's order.events topic carries every Order lifecycle event; this
        // listener only reacts to the two that concern the kitchen ticket lifecycle.
        // OrderCreatedEvent's fields are reused for OrderCancelled too — we only read
        // eventId/orderId from it in that case, which deserialize fine regardless.
        switch (event.eventType()) {
            case "OrderCreated" -> ticketService.handleOrderCreated(event);
            case "OrderCancelled" -> ticketService.handleOrderCancelled(event.eventId(), event.orderId());
            case "OrderRevisionProposed" -> ticketService.handleOrderRevisionProposed(event);
            case "OrderRevisionCompensationRequested" -> ticketService.handleOrderRevisionRejected(event);
            default -> { }
        }
    }
}
