package com.sanjay.ftgo.accounting.domain;

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
public class SagaJoinService {

    private static final int AUTHORIZATION_QUANTITY_LIMIT = 10;

    private final SagaJoinStateRepository sagaJoinStateRepository;
    private final AuthorizationRepository authorizationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AuthorizationDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public SagaJoinService(SagaJoinStateRepository sagaJoinStateRepository,
                            AuthorizationRepository authorizationRepository,
                            ProcessedEventRepository processedEventRepository,
                            OutboxEventRepository outboxEventRepository,
                            AuthorizationDomainEventPublisher domainEventPublisher,
                            ObjectMapper objectMapper) {
        this.sagaJoinStateRepository = sagaJoinStateRepository;
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleConsumerEvent(String eventId, Long orderId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        SagaJoinState state = sagaJoinStateRepository.findById(orderId).orElseGet(() -> new SagaJoinState(orderId));
        if (state.isResolved() || state.isFailed()) {
            return;
        }

        if ("ConsumerVerificationFailed".equals(eventType)) {
            state.markFailed();
            sagaJoinStateRepository.save(state);
            return;
        }

        state.markConsumerVerified();
        sagaJoinStateRepository.save(state);
        tryResolve(state);
    }

    @Transactional
    public void handleKitchenEvent(String eventId, Long orderId, String eventType, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        SagaJoinState state = sagaJoinStateRepository.findById(orderId).orElseGet(() -> new SagaJoinState(orderId));
        if (state.isResolved() || state.isFailed()) {
            return;
        }

        if ("TicketCreationFailed".equals(eventType)) {
            state.markFailed();
            sagaJoinStateRepository.save(state);
            return;
        }

        state.markTicketCreated(totalQuantity);
        sagaJoinStateRepository.save(state);
        tryResolve(state);
    }

    @Transactional
    public void handleAuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (totalQuantity == null) {
            publishReply("CardAuthorizationFailed", orderId, "order quantity is missing", "CreateOrder");
            return;
        }

        boolean authorized = isAuthorized(totalQuantity);
        AuthorizationResult result = authorized
                ? Authorization.authorize(orderId, totalQuantity)
                : Authorization.decline(orderId, "order quantity exceeds authorization limit", totalQuantity);
        authorizationRepository.save(result.authorization());

        if (authorized) {
            publishReply("CardAuthorized", orderId, null, "CreateOrder");
        } else {
            publishReply("CardAuthorizationFailed", orderId, "order quantity exceeds authorization limit", "CreateOrder");
        }
    }

    private void tryResolve(SagaJoinState state) {
        if (!state.isConsumerVerified() || !state.isTicketCreated()) {
            return;
        }
        state.markResolved();
        sagaJoinStateRepository.save(state);

        boolean authorized = isAuthorized(state.getTotalQuantity());
        AuthorizationResult result = authorized
                ? Authorization.authorize(state.getOrderId(), state.getTotalQuantity())
                : Authorization.decline(state.getOrderId(), "order quantity exceeds authorization limit", state.getTotalQuantity());
        authorizationRepository.save(result.authorization());
        domainEventPublisher.publish(result.events());
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
