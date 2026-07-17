package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final OrderSagaService orderSagaService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(OrderSagaService orderSagaService, ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "order-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        if ("ConsumerVerificationFailed".equals(event.eventType())) {
            orderSagaService.reject(event.orderId(), event.eventId());
        }
    }
}
