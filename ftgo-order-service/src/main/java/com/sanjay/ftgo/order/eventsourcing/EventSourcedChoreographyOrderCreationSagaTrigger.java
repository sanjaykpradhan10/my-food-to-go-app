package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCreationSagaTrigger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

// Publishing already happened as a side effect of OrderTransitions.create() writing to
// order_events, tailed by the CDC pipeline (see EventSourcedOrderTransitions, Task 8, and the
// Debezium connector config extended in Task 18) — this class exists purely to prevent
// ChoreographyOrderCreationSagaTrigger's JPA-only outbox publish from running a second time.
@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedChoreographyOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    @Override
    public void onOrderCreated(Order order, String eventId) {
    }
}
