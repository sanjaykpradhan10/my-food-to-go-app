package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ChoreographyOrderCancellationSagaTriggerTest {

    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);

    private final ChoreographyOrderCancellationSagaTrigger trigger =
            new ChoreographyOrderCancellationSagaTrigger(domainEventPublisher);

    @Test
    void publishesTheEventsDirectly() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.CANCEL_PENDING);
        List<OrderDomainEvent> events = List.of(new OrderCancelledEvent(1L));

        trigger.onOrderCancelled(order, events);

        verify(domainEventPublisher).publish(events);
    }
}
