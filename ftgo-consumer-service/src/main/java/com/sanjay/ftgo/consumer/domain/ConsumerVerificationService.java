package com.sanjay.ftgo.consumer.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ConsumerVerificationService {

    private final ConsumerRepository consumerRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public ConsumerVerificationService(ConsumerRepository consumerRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        ObjectMapper objectMapper) {
        this.consumerRepository = consumerRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        Consumer consumer = consumerRepository.findById(event.consumerId()).orElse(null);
        if (consumer == null) {
            publishEvent("ConsumerVerificationFailed", event.orderId(), event.consumerId(), "consumer not found");
        } else if (!consumer.isActive()) {
            publishEvent("ConsumerVerificationFailed", event.orderId(), event.consumerId(), "consumer is not active");
        } else {
            publishEvent("ConsumerVerified", event.orderId(), event.consumerId(), null);
        }
    }

    private void publishEvent(String eventType, Long orderId, Long consumerId, String reason) {
        String eventId = UUID.randomUUID().toString();
        ConsumerVerificationEvent event = new ConsumerVerificationEvent(eventId, eventType, orderId, consumerId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "consumer.events", toJson(event)));
    }

    private String toJson(ConsumerVerificationEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + event.eventType() + " for order " + event.orderId(), e);
        }
    }
}
