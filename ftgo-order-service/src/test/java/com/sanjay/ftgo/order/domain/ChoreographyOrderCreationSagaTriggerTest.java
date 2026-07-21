package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ChoreographyOrderCreationSagaTriggerTest {

    private final OrderDomainEventPublisher domainEventPublisher = mock(OrderDomainEventPublisher.class);

    private final ChoreographyOrderCreationSagaTrigger trigger =
            new ChoreographyOrderCreationSagaTrigger(domainEventPublisher);

    @Test
    void delegatesToOrderDomainEventPublisher() {
        Order order = new Order(1L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);

        trigger.onOrderCreated(order, "event-1");

        verify(domainEventPublisher).publishOrderCreated(order, "event-1");
    }
}
