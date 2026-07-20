package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class TicketDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public TicketDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publish(Ticket ticket, List<TicketDomainEvent> events) {
        events.forEach(event -> publishEvent(ticket.getId(), event));
    }

    public void publishCreationFailed(TicketCreationFailedEvent event) {
        publishEvent(null, event);
    }

    private void publishEvent(Long ticketId, TicketDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        KitchenEvent wireEvent = toWireEvent(eventId, ticketId, event);
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "kitchen.events", toJson(wireEvent)));
    }

    private KitchenEvent toWireEvent(String eventId, Long ticketId, TicketDomainEvent event) {
        return switch (event) {
            case TicketCreatedEvent e ->
                    new KitchenEvent(eventId, "TicketCreated", e.orderId(), ticketId, e.totalQuantity(), null);
            case TicketCreationFailedEvent e ->
                    new KitchenEvent(eventId, "TicketCreationFailed", e.orderId(), ticketId, null, e.reason());
            case TicketConfirmedEvent e ->
                    new KitchenEvent(eventId, "TicketConfirmed", e.orderId(), ticketId, null, null);
            case TicketCancelledEvent e ->
                    new KitchenEvent(eventId, "TicketCancelled", e.orderId(), ticketId, null, null);
            case TicketAcceptedEvent e ->
                    new KitchenEvent(eventId, "TicketAccepted", e.orderId(), ticketId, null, null);
            case TicketPreparingStartedEvent e ->
                    new KitchenEvent(eventId, "TicketPreparingStarted", e.orderId(), ticketId, null, null);
            case TicketReadyForPickupEvent e ->
                    new KitchenEvent(eventId, "TicketReadyForPickup", e.orderId(), ticketId, null, null);
            case TicketPickedUpEvent e ->
                    new KitchenEvent(eventId, "TicketPickedUp", e.orderId(), ticketId, null, null);
        };
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize kitchen event", e);
        }
    }
}
