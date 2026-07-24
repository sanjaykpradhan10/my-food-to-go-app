package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.ConsumerVerificationEvent;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ConsumerEventListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public ConsumerEventListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.events", groupId = "delivery-service")
    public void onMessage(String payload) {
        ConsumerVerificationEvent event;
        try {
            event = objectMapper.readValue(payload, ConsumerVerificationEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed consumer event: {}", payload, e);
            return;
        }
        if ("ConsumerVerificationFailed".equals(event.eventType())) {
            deliveryService.release(event.eventId(), event.orderId());
        }
    }
}
