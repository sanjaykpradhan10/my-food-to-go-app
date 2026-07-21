package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class OrderDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderDomainEventPublisher(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void publishOrderCreated(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderCreated", order.getId(),
                order.getConsumerId(), order.getRestaurantId(), toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }

    public void publish(List<OrderDomainEvent> events) {
        events.forEach(this::publishEvent);
    }

    private void publishEvent(OrderDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        save(eventId, toWireEvent(eventId, event));
    }

    private OrderEvent toWireEvent(String eventId, OrderDomainEvent event) {
        return switch (event) {
            case OrderApprovedEvent e -> new OrderEvent(eventId, "OrderApproved", e.orderId(), null, null, null);
            case OrderRejectedEvent e -> new OrderEvent(eventId, "OrderRejected", e.orderId(), null, null, null);
            case OrderCancelledEvent e -> new OrderEvent(eventId, "OrderCancelled", e.orderId(), null, null, null);
            case OrderCancelConfirmedEvent e ->
                    new OrderEvent(eventId, "OrderCancelConfirmed", e.orderId(), null, null, null);
            case OrderCancelRejectedEvent e ->
                    new OrderEvent(eventId, "OrderCancelRejected", e.orderId(), null, null, null);
            case OrderRevisionProposedEvent e -> new OrderEvent(eventId, "OrderRevisionProposed", e.orderId(),
                    null, null, toWireLineItems(e.revisedLineItems()));
            case OrderRevisedEvent e -> new OrderEvent(eventId, "OrderRevised", e.orderId(),
                    null, null, toWireLineItems(e.revisedLineItems()));
            case OrderRevisionRejectedEvent e ->
                    new OrderEvent(eventId, "OrderRevisionRejected", e.orderId(), null, null, null);
        };
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private void save(String eventId, OrderEvent wireEvent) {
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "order.events", toJson(wireEvent)));
    }

    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order event " + event.eventType(), e);
        }
    }
}
