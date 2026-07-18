package com.sanjay.ftgo.consumer.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
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

        VerificationResult result = verify(event.consumerId());
        String eventType = result.verified() ? "ConsumerVerified" : "ConsumerVerificationFailed";
        publishEvent(eventType, event.orderId(), event.consumerId(), result.reason());
    }

    @Transactional
    public void handleVerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        VerificationResult result = verify(consumerId);
        String eventType = result.verified() ? "ConsumerVerified" : "ConsumerVerificationFailed";
        publishReply(eventType, orderId, result.reason());
    }

    private VerificationResult verify(Long consumerId) {
        Consumer consumer = consumerRepository.findById(consumerId).orElse(null);
        if (consumer == null) {
            return new VerificationResult(false, "consumer not found");
        }
        if (!consumer.isActive()) {
            return new VerificationResult(false, "consumer is not active");
        }
        return new VerificationResult(true, null);
    }

    private record VerificationResult(boolean verified, String reason) {
    }

    private void publishEvent(String eventType, Long orderId, Long consumerId, String reason) {
        String eventId = UUID.randomUUID().toString();
        ConsumerVerificationEvent event = new ConsumerVerificationEvent(eventId, eventType, orderId, consumerId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "consumer.events", toJson(event)));
    }

    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "consumer", eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga event", e);
        }
    }
}
