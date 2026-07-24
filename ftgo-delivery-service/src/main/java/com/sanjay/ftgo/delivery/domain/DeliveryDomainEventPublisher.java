package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class DeliveryDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public DeliveryDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publish(Delivery delivery, List<DeliveryDomainEvent> events) {
        events.forEach(event -> publishEvent(delivery.getId(), event));
    }

    public void publishSchedulingFailed(DeliverySchedulingFailedEvent event) {
        publishEvent(null, event);
    }

    private void publishEvent(Long deliveryId, DeliveryDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        DeliveryEvent wireEvent = toWireEvent(eventId, deliveryId, event);
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "delivery.events", toJson(wireEvent)));
    }

    private DeliveryEvent toWireEvent(String eventId, Long deliveryId, DeliveryDomainEvent event) {
        return switch (event) {
            case DeliveryScheduledEvent e ->
                    new DeliveryEvent(eventId, "DeliveryScheduled", e.orderId(), deliveryId, e.courierId(), null);
            case DeliverySchedulingFailedEvent e ->
                    new DeliveryEvent(eventId, "DeliverySchedulingFailed", e.orderId(), deliveryId, null, e.reason());
            case DeliveryPickedUpEvent e ->
                    new DeliveryEvent(eventId, "DeliveryPickedUp", e.orderId(), deliveryId, null, null);
            case DeliveryDeliveredEvent e ->
                    new DeliveryEvent(eventId, "DeliveryDelivered", e.orderId(), deliveryId, null, null);
            case DeliveryCancelledEvent e ->
                    new DeliveryEvent(eventId, "DeliveryCancelled", e.orderId(), deliveryId, null, null);
        };
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize delivery event", e);
        }
    }
}
