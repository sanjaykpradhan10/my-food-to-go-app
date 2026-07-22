package com.sanjay.ftgo.order.domain;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ChoreographyOrderRevisionSagaTrigger implements OrderRevisionSagaTrigger {

    private final OrderDomainEventPublisher domainEventPublisher;

    public ChoreographyOrderRevisionSagaTrigger(OrderDomainEventPublisher domainEventPublisher) {
        this.domainEventPublisher = domainEventPublisher;
    }

    @Override
    public void onOrderRevised(Order order, List<OrderDomainEvent> events) {
        domainEventPublisher.publish(events);
    }
}
