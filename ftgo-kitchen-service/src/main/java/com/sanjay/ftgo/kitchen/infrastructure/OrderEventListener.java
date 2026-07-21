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
        // order-service's order.events topic now carries every Order lifecycle event
        // (OrderApproved, OrderCancelled, etc.), not just OrderCreated — without this
        // check, deserializing e.g. an OrderApproved payload into OrderCreatedEvent
        // would succeed with null consumerId/restaurantId/lineItems and create a bogus Ticket.
        if (!"OrderCreated".equals(event.eventType())) {
            return;
        }
        ticketService.handleOrderCreated(event);
    }
}
