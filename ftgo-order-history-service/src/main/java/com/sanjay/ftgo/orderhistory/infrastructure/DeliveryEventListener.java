package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.DeliveryEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeliveryEventListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public DeliveryEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        DeliveryEvent event;
        try {
            event = objectMapper.readValue(payload, DeliveryEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery event: {}", payload, e);
            return;
        }
        orderViewService.handleDeliveryEvent(event.eventId(), event.eventType(), event.orderId(), event.courierId());
    }
}
