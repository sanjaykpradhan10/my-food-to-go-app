package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.KitchenEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        orderViewService.handleKitchenEvent(event.eventId(), event.eventType(), event.orderId());
    }
}
