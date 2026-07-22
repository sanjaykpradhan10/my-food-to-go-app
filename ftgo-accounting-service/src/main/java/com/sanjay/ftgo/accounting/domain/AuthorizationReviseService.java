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
public class AuthorizationReviseService {

    private static final int AUTHORIZATION_QUANTITY_LIMIT = 10;

    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public AuthorizationReviseService(AuthorizationRepository authorizationRepository,
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

    // Choreography: kitchen's TicketQuantityRevised domain event triggers this directly, same
    // broadcast-on-accounting.events shape as every other choreography-mode transition here.
    @Transactional
    public void reviseForChoreography(String eventId, Long orderId, Integer newTotalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null || newTotalQuantity == null) {
            return;
        }

        if (!isAuthorized(newTotalQuantity)) {
            domainEventPublisher.publish(List.of(
                    new AuthorizationRevisionRejectedEvent(orderId, "order quantity exceeds authorization limit")));
            return;
        }

        List<AuthorizationDomainEvent> events = authorization.reviseAuthorization(newTotalQuantity);
        authorizationRepository.save(authorization);
        domainEventPublisher.publish(events);
    }

    // Orchestration: ReviseOrderSagaOrchestrator sent a ReviseAuthorization command and is
    // waiting on saga.replies, same split as AuthorizationCancelService's choreography/command
    // methods (see docs/session-2026-07-21b.md for why this split matters).
    @Transactional
    public void reviseForCommand(String eventId, Long orderId, Integer newTotalQuantity, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Authorization authorization = authorizationRepository.findByOrderId(orderId).orElse(null);
        if (authorization == null) {
            return;
        }

        if (newTotalQuantity == null || !isAuthorized(newTotalQuantity)) {
            publishReply("AuthorizationRevisionRejected", orderId, "order quantity exceeds authorization limit", sagaType);
            return;
        }

        authorization.reviseAuthorization(newTotalQuantity);
        authorizationRepository.save(authorization);
        publishReply("AuthorizationRevised", orderId, null, sagaType);
    }

    private boolean isAuthorized(int totalQuantity) {
        return totalQuantity <= AUTHORIZATION_QUANTITY_LIMIT;
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
