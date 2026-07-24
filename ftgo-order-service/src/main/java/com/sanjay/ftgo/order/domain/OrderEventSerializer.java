package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderEventSerializer {

    private final ObjectMapper objectMapper;

    public OrderEventSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public OrderEvent toWireEvent(String eventId, OrderDomainEvent event) {
        return switch (event) {
            case OrderCreatedEvent e -> new OrderEvent(eventId, "OrderCreated", e.orderId(),
                    e.consumerId(), e.restaurantId(), toWireLineItems(e.lineItems()));
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

    public OrderDomainEvent fromWireEvent(OrderEvent wireEvent) {
        return switch (wireEvent.eventType()) {
            case "OrderCreated" -> new OrderCreatedEvent(wireEvent.orderId(), wireEvent.consumerId(),
                    wireEvent.restaurantId(), toDomainLineItems(wireEvent.lineItems()));
            case "OrderApproved" -> new OrderApprovedEvent(wireEvent.orderId());
            case "OrderRejected" -> new OrderRejectedEvent(wireEvent.orderId());
            case "OrderCancelled" -> new OrderCancelledEvent(wireEvent.orderId());
            case "OrderCancelConfirmed" -> new OrderCancelConfirmedEvent(wireEvent.orderId());
            case "OrderCancelRejected" -> new OrderCancelRejectedEvent(wireEvent.orderId());
            case "OrderRevisionProposed" ->
                    new OrderRevisionProposedEvent(wireEvent.orderId(), toDomainLineItems(wireEvent.lineItems()));
            case "OrderRevised" -> new OrderRevisedEvent(wireEvent.orderId(), toDomainLineItems(wireEvent.lineItems()));
            case "OrderRevisionRejected" -> new OrderRevisionRejectedEvent(wireEvent.orderId());
            default -> throw new IllegalArgumentException("Unknown order event type: " + wireEvent.eventType());
        };
    }

    public String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order event " + event.eventType(), e);
        }
    }

    public OrderEvent fromJson(String json) {
        try {
            return objectMapper.readValue(json, OrderEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize order event payload", e);
        }
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private List<OrderLineItem> toDomainLineItems(List<OrderEvent.LineItem> lineItems) {
        if (lineItems == null) {
            return null;
        }
        return lineItems.stream()
                .map(lineItem -> new OrderLineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    public String writeSnapshotData(com.sanjay.ftgo.order.eventsourcing.OrderSnapshotData data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order snapshot", e);
        }
    }

    public com.sanjay.ftgo.order.eventsourcing.OrderSnapshotData readSnapshotData(String json) {
        try {
            return objectMapper.readValue(json, com.sanjay.ftgo.order.eventsourcing.OrderSnapshotData.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize order snapshot", e);
        }
    }
}
