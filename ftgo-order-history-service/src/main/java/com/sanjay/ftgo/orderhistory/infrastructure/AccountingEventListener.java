package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.AccountingEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        orderViewService.handleAccountingEvent(event.eventId(), event.eventType(), event.orderId());
    }
}
