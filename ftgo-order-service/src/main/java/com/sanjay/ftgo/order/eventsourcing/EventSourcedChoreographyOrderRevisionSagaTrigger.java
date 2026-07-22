package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

// See EventSourcedChoreographyOrderCreationSagaTrigger for why this is a no-op.
@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedChoreographyOrderRevisionSagaTrigger implements OrderRevisionSagaTrigger {

    @Override
    public void onOrderRevised(Order order, List<OrderDomainEvent> events) {
    }
}
