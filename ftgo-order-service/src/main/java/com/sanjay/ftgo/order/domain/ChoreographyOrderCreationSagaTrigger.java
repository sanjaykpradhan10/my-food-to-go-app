package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ChoreographyOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    private final OrderDomainEventPublisher domainEventPublisher;

    public ChoreographyOrderCreationSagaTrigger(OrderDomainEventPublisher domainEventPublisher) {
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    public void onOrderCreated(Order order, String eventId) {
        domainEventPublisher.publishOrderCreated(order, eventId);
    }
}
