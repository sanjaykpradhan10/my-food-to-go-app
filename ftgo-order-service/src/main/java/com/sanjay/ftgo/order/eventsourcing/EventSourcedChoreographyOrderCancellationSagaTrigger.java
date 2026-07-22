package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

// See EventSourcedChoreographyOrderCreationSagaTrigger for why this is a no-op.
@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedChoreographyOrderCancellationSagaTrigger implements OrderCancellationSagaTrigger {

    @Override
    public void onOrderCancelled(Order order, List<OrderDomainEvent> events) {
    }
}
