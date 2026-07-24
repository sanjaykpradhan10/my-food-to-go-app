package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "jpa", matchIfMissing = true)
public class ChoreographyOrderCancellationSagaTrigger implements OrderCancellationSagaTrigger {

    private final OrderDomainEventPublisher domainEventPublisher;

    public ChoreographyOrderCancellationSagaTrigger(OrderDomainEventPublisher domainEventPublisher) {
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    public void onOrderCancelled(Order order, List<OrderDomainEvent> events) {
        domainEventPublisher.publish(events);
    }
}
