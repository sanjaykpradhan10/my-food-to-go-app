package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class AuthorizationCancelService {

    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public AuthorizationCancelService(AuthorizationRepository authorizationRepository,
                                       ProcessedEventRepository processedEventRepository,
                                       AuthorizationDomainEventPublisher domainEventPublisher,
                                       OutboxEventRepository outboxEventRepository,
                                       ObjectMapper objectMapper) {
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    // Choreography: kitchen's TicketCancelled domain event triggers this directly. There is
    // no saga.replies channel in play here, so the reversal is broadcast as a domain event
    // on accounting.events, same as every other choreography-mode transition in this service.
    @Transactional
    public void reverseForChoreography(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null) {
            return;
        }
        List<AuthorizationDomainEvent> events = authorization.reverse();
        authorizationRepository.save(authorization);
        domainEventPublisher.publish(events);
    }

    // Orchestration: CancelOrderSagaOrchestrator sent a ReverseAuthorization command and is
    // waiting on saga.replies to learn the outcome before it can move the order to CANCELLED.
    // Publishing to accounting.events here (as the choreography path does) would leave the
    // orchestrator waiting forever, since nothing in orchestration mode consumes that topic.
    @Transactional
    public void reverseForCommand(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null) {
            return;
        }
        authorization.reverse();
        authorizationRepository.save(authorization);
        publishReply("AuthorizationReversed", orderId, null, sagaType);
    }

    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "accounting", eventType, orderId, reason, sagaType);
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
