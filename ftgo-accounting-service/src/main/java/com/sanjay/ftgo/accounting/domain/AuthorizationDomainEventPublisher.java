package com.sanjay.ftgo.accounting.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class AuthorizationDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public AuthorizationDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publish(List<AuthorizationDomainEvent> events) {
        events.forEach(this::publishEvent);
    }

    private void publishEvent(AuthorizationDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        AccountingEvent wireEvent = toWireEvent(eventId, event);
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "accounting.events", toJson(wireEvent)));
    }

    private AccountingEvent toWireEvent(String eventId, AuthorizationDomainEvent event) {
        return switch (event) {
            case CardAuthorizedEvent e -> new AccountingEvent(eventId, "CardAuthorized", e.orderId(), null);
            case CardAuthorizationDeclinedEvent e ->
                    new AccountingEvent(eventId, "CardAuthorizationFailed", e.orderId(), e.reason());
            case AuthorizationReversedEvent e -> new AccountingEvent(eventId, "AuthorizationReversed", e.orderId(), null);
        };
    }

    private String toJson(AccountingEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize accounting event " + event.eventType(), e);
        }
    }
}
