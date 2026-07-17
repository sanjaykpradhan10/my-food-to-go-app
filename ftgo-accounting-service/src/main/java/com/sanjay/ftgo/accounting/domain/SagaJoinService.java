package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper objectMapper;

    public SagaJoinService(SagaJoinStateRepository sagaJoinStateRepository,
                            AuthorizationRepository authorizationRepository,
                            ProcessedEventRepository processedEventRepository,
                            OutboxEventRepository outboxEventRepository,
                            ObjectMapper objectMapper) {
        this.sagaJoinStateRepository = sagaJoinStateRepository;
        this.authorizationRepository = authorizationRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
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
            publishReply("CardAuthorizationFailed", orderId, "order quantity is missing");
            return;
        }

        boolean authorized = isAuthorized(totalQuantity);
        authorizationRepository.save(new Authorization(orderId, authorized ? "AUTHORIZED" : "DECLINED"));

        if (authorized) {
            publishReply("CardAuthorized", orderId, null);
        } else {
            publishReply("CardAuthorizationFailed", orderId, "order quantity exceeds authorization limit");
        }
    }

    private void tryResolve(SagaJoinState state) {
        if (!state.isConsumerVerified() || !state.isTicketCreated()) {
            return;
        }
        state.markResolved();
        sagaJoinStateRepository.save(state);

        boolean authorized = isAuthorized(state.getTotalQuantity());
        authorizationRepository.save(new Authorization(state.getOrderId(), authorized ? "AUTHORIZED" : "DECLINED"));

        if (authorized) {
            publishEvent("CardAuthorized", state.getOrderId(), null);
        } else {
            publishEvent("CardAuthorizationFailed", state.getOrderId(), "order quantity exceeds authorization limit");
        }
    }

    private boolean isAuthorized(int totalQuantity) {
        return totalQuantity <= AUTHORIZATION_QUANTITY_LIMIT;
    }

    private void publishEvent(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        AccountingEvent event = new AccountingEvent(eventId, eventType, orderId, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "accounting.events", toJson(event)));
    }

    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "accounting", eventType, orderId, reason);
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
