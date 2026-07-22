package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class OrderDomainEventPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventSerializer serializer;

    public OrderDomainEventPublisher(OutboxEventRepository outboxEventRepository, OrderEventSerializer serializer) {
        this.outboxEventRepository = outboxEventRepository;
        this.serializer = serializer;
    }

    public void publishOrderCreated(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderCreated", order.getId(),
                order.getConsumerId(), order.getRestaurantId(), toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }

    // Order itself doesn't transition here (it stays REVISION_PENDING until kitchen's undo is
    // confirmed), so this bypasses the OrderDomainEvent sealed interface entirely, same as
    // publishOrderCreated does for its own one-off case.
    public void publishRevisionCompensationRequested(Order order, String eventId) {
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderRevisionCompensationRequested", order.getId(),
                null, null, toWireLineItems(order.getLineItems()));
        save(eventId, wireEvent);
    }

    public void publish(List<OrderDomainEvent> events) {
        events.forEach(this::publishEvent);
    }

    private void publishEvent(OrderDomainEvent event) {
        String eventId = UUID.randomUUID().toString();
        save(eventId, serializer.toWireEvent(eventId, event));
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private void save(String eventId, OrderEvent wireEvent) {
        outboxEventRepository.save(new OutboxEvent(
                eventId, wireEvent.eventType(), wireEvent.orderId(), "order.events", serializer.toJson(wireEvent)));
    }
}
