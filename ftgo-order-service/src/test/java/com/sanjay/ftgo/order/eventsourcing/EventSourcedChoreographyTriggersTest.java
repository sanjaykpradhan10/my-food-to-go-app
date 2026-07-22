package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCancelledEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;

import java.util.List;

class EventSourcedChoreographyTriggersTest {

    private final Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

    @Test
    void creationTriggerDoesNothing() {
        new EventSourcedChoreographyOrderCreationSagaTrigger().onOrderCreated(order, "evt-1");
        // No collaborator to verify — the point of this test is that construction and the call
        // succeed without throwing, proving there's no hidden publish side effect.
    }

    @Test
    void cancellationTriggerDoesNothing() {
        List<OrderDomainEvent> events = List.of(new OrderCancelledEvent(1L));
        new EventSourcedChoreographyOrderCancellationSagaTrigger().onOrderCancelled(order, events);
    }

    @Test
    void revisionTriggerDoesNothing() {
        List<OrderDomainEvent> events = List.of();
        new EventSourcedChoreographyOrderRevisionSagaTrigger().onOrderRevised(order, events);
    }
}
