package com.sanjay.ftgo.kitchen.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.kitchen.domain.AccountingEvent;
import com.sanjay.ftgo.kitchen.domain.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final TicketService ticketService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(TicketService ticketService, ObjectMapper objectMapper) {
        this.ticketService = ticketService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "kitchen-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        ticketService.handleAccountingEvent(event.eventId(), event.orderId(), event.eventType());
    }
}
