package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        if ("ConsumerVerificationFailed".equals(event.eventType())) {
            ticketService.handleConsumerVerificationFailed(event.eventId(), event.orderId());
        }
    }
}
